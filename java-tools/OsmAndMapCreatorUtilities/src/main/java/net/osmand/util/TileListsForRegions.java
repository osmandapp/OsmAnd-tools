package net.osmand.util;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferShort;
import java.io.ByteArrayInputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.imageio.ImageIO;
import javax.lang.model.util.ElementScanner6;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.data.LatLon;
import net.osmand.data.Multipolygon;
import net.osmand.data.MultipolygonBuilder;
import net.osmand.data.QuadRect;
import net.osmand.data.Ring;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;

public class TileListsForRegions {
	private static final String LOCK_EXTENSION = ".proc";
	private static int ZOOM = 9;
	private static boolean SKIP_EXISTING = true;
	private static String EXTENSION = ".tif";

	public static void main(String[] args) throws IOException {
//		args = new String[] {
//				"--zoom=9",
//				"--filter=italy_lombardia"
//		};
		File directoryWithTargetFiles = new File(args[0]);
		String filter = null; // mauritius
		String prefix = "Hillshade_";
		String regionAttribute = "region_hillshade";
		
		for (int i = 1; i < args.length; i++) {
			if (args[i].startsWith("--prefix=")) {
				prefix = args[i].substring("--prefix=".length());
			} else if (args[i].startsWith("--skip-existing=")) {
				SKIP_EXISTING = Boolean.parseBoolean(args[i].substring("--skip-existing=".length()));
			} else if (args[i].startsWith("--extension=")) {
				EXTENSION = args[i].substring("--extension=".length());
			} else if (args[i].startsWith("--zoom=")) {
				ZOOM = Integer.parseInt(args[i].substring("--zoom=".length()));
			} else if (args[i].startsWith("--region-ocbf-attr=")) {
				// "region_hillshade"
				regionAttribute = args[i].substring("--region-ocbf-attr=".length());
				
			} else if (args[i].startsWith("--filter=")) {
				filter = args[i].substring("--filter=".length());
				if (filter.length() == 0) {
					filter = null;
				}
			}
		}
		OsmandRegions or = new OsmandRegions();
		BinaryMapIndexReader fl = or.prepareFile();
		Map<String, LinkedList<BinaryMapDataObject>> allCountries = or.cacheAllCountries();
		MapIndex mapIndex = fl.getMapIndexes().get(0);
		int regAttr = Algorithms.isEmpty(regionAttribute) ? -1 : mapIndex.getRule(regionAttribute, "yes");
		int downloadName = mapIndex.getRule("download_name", null);
		int boundary = mapIndex.getRule("osmand_region", "boundary");
		int cnt = 1;
		Set<String> failedCountries = new HashSet<String>();
		Set<String> filters = new HashSet<String>();
		if (filter != null) {
			String[] split = filter.split(",");
			for (String s : split) {
				String trimmed = s.trim();
				if (!trimmed.isEmpty()) {
					filters.add(trimmed);
				}
			}
		}
		for (String fullName : allCountries.keySet()) {
			if (fullName == null) {
				continue;
			}
			boolean matches = filters.isEmpty();
			if (!matches) {
				for (String f : filters) {
					if (fullName.contains(f)) {
						matches = true;
						break;
					}
				}
			}
			if (!matches) {
				continue;
			}
			LinkedList<BinaryMapDataObject> lst = allCountries.get(fullName);
			BinaryMapDataObject rc = null;
			for (BinaryMapDataObject r : lst) {
				if (!r.containsType(boundary)) {
					rc = r;
					break;
				}
			}
			if (rc != null && (regAttr == -1 || rc.containsAdditionalType(regAttr))) {
				String dw = rc.getNameByType(downloadName);
				System.out.println("Region " + fullName + " " + cnt++ + " out of " + allCountries.size());
				try {
					process(rc, lst, dw, directoryWithTargetFiles, prefix);
				} catch (Exception e) {
					failedCountries.add(fullName);
					e.printStackTrace();
				}
			} else {
				System.out.println(String.format("Region %s skipped as it doesn't attribute %s = yes. ", fullName, regionAttribute));
			}
		}
		if (!failedCountries.isEmpty()) {
			throw new IllegalStateException("Failed countries " + failedCountries);
		}
	}

	private static void process(BinaryMapDataObject country, List<BinaryMapDataObject> boundaries,
			String downloadName, File directoryWithTargetFiles, String prefix) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		String name = country.getName();
		String dwName = prefix + Algorithms.capitalizeFirstLetterAndLowercase(downloadName) + ".txt";
		final File targetFile = new File(directoryWithTargetFiles, dwName);
		if (!SKIP_EXISTING) {
			File lockFile = new File(directoryWithTargetFiles, dwName + LOCK_EXTENSION);
			lockFile.delete();
			if (!lockFile.exists()) {
				targetFile.delete();
			}
		}
		if (targetFile.exists()) {
			System.out.println("Already processed "+ name);
			return;
		}
		FileOutputStream fos = new FileOutputStream(targetFile);
		BufferedWriter wb = new BufferedWriter(new OutputStreamWriter(fos));
		
		QuadRect qr = new QuadRect(180, -90, -180, 90);
		MultipolygonBuilder bld = new MultipolygonBuilder();
		if(boundaries != null) {
			for (BinaryMapDataObject o : boundaries) {
				bld.addOuterWay(convertToWay(o));
				updateBbox(o, qr);
			}
		} else {
			bld.addOuterWay(convertToWay(country));
			updateBbox(country, qr);			
		}
		Multipolygon polygon  = bld.build();
		int rightLon = (int) Math.floor(MapUtils.getTileNumberX(ZOOM, qr.right));
		int leftLon = (int) Math.floor(MapUtils.getTileNumberX(ZOOM, qr.left));
		int bottomLat = (int) Math.floor(MapUtils.getTileNumberY(ZOOM, qr.bottom));
		int topLat = (int) Math.floor(MapUtils.getTileNumberY(ZOOM, qr.top));
		boolean onetile = leftLon == rightLon && bottomLat == topLat;
		for (int tileX = leftLon; tileX <= rightLon; tileX++) {
			for (int tileY = topLat; tileY <= bottomLat; tileY++) {
				double llon = MapUtils.getLongitudeFromTile(ZOOM, tileX);
				double rlon = MapUtils.getLongitudeFromTile(ZOOM, tileX + 1);
				double tlat = MapUtils.getLatitudeFromTile(ZOOM, tileY);
				double blat = MapUtils.getLatitudeFromTile(ZOOM, tileY + 1);
				boolean isOut = !polygon.containsPoint(tlat / 2 + blat / 2, llon / 2 + rlon / 2) && !onetile;
				if (isOut) {
					LatLon bl = new LatLon(blat, llon);
					LatLon br = new LatLon(blat, rlon);
					LatLon tr = new LatLon(tlat, rlon);
					LatLon tl = new LatLon(tlat, llon);
					for (Ring r : polygon.getOuterRings()) {
						List<Node> border = r.getBorder();
						Node prev = border.get(border.size() - 1);
						for (int i = 0; i < border.size() && isOut; i++) {
							Node n = border.get(i);
							if (MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), tr, tl)) {
								isOut = false;
							} else if (MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), tr, br)) {
								isOut = false;
							} else if (MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), bl, tl)) {
								isOut = false;
							} else if (MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), br, bl)) {
								isOut = false;
							}
							prev = n;
						}
						if (!isOut) {
							break;
						}
					}
				}
				if (!isOut) {
					int x = tileX;
					int y = tileY;
					wb.write("./" + ZOOM + "/" + tileX + "/" + tileY + EXTENSION);
					wb.newLine();
				}
			}
		}
		wb.close();
	}

	private static Way convertToWay(BinaryMapDataObject o) {
		Way w = new Way(-1);
		for(int i = 0; i < o.getPointsLength(); i++) {
			double lat = MapUtils.get31LatitudeY(o.getPoint31YTile(i));
			double lon = MapUtils.get31LongitudeX(o.getPoint31XTile(i));
			w.addNode(new Node(lat, lon, -1));
		}
		return w;
	}

	private static void updateBbox(BinaryMapDataObject country, QuadRect qr) {
		for(int i = 0; i < country.getPointsLength(); i++) {
			double lat = MapUtils.get31LatitudeY(country.getPoint31YTile(i));
			double lon = MapUtils.get31LongitudeX(country.getPoint31XTile(i));
			qr.left = Math.min(lon, qr.left);
			qr.right = Math.max(lon, qr.right);
			qr.top = Math.max(lat, qr.top);
			qr.bottom = Math.min(lat, qr.bottom);
		}
	}

}
