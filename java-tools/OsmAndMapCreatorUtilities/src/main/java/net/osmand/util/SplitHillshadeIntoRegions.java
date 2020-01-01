package net.osmand.util;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.IndexConstants;
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
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;

public class SplitHillshadeIntoRegions {
	private static final Log LOG = PlatformUtil.getLog(SplitHillshadeIntoRegions.class);
	private static final int MIN_ZOOM = 5;
	private static final int MAX_ZOOM = 13;

	public static void main(String[] args) throws IOException {
		File sqliteFile = new File(args[0]);
		File directoryWithTargetFiles = new File(args[1]);
		boolean dryRun = false;
		String filter = null; // mauritius
		for(int i = 2; i < args.length; i++ ){
			if("--dry-run".equals(args[i])) {
				dryRun = true;
			} else if(args[i].startsWith("--filter=")) {
				filter = args[i].substring("--filter=".length());
				if(filter.length() == 0) {
					filter = null;
				}
			}
		}
		OsmandRegions or = new OsmandRegions();
		BinaryMapIndexReader fl = or.prepareFile();
		Map<String, LinkedList<BinaryMapDataObject>> allCountries = or.cacheAllCountries();
		MapIndex mapIndex = fl.getMapIndexes().get(0);
		int hillshade = mapIndex.getRule("region_hillshade", "yes");
		int downloadName = mapIndex.getRule("download_name", null);
		int boundary = mapIndex.getRule("osmand_region", "boundary");
		int cnt = 1;
		Set<String> failedCountries = new HashSet<String>();
		for(String fullName : allCountries.keySet()) {
			LinkedList<BinaryMapDataObject> lst = allCountries.get(fullName);
			if (fullName == null || (filter != null && !fullName.contains(filter))) {
				continue;
			}
			BinaryMapDataObject rc = null;
			for(BinaryMapDataObject r : lst) {
				if(!r.containsType(boundary)) {
					rc = r;
					break;
				}
			}
			System.out.println(fullName );
			if(rc != null && rc.containsAdditionalType(hillshade)) {
				String dw = rc.getNameByType(downloadName);
				System.out.println("Region " + fullName + " " + cnt++ + " out of " + allCountries.size());
				try {
					process(rc, lst, dw, sqliteFile, directoryWithTargetFiles, dryRun);
				} catch(Exception e) {
					failedCountries.add(fullName);
					e.printStackTrace();
				}
			}
		}
		if(!failedCountries.isEmpty()) {
			throw new IllegalStateException("Failed countries " + failedCountries);
		}
	}

	private static void process(BinaryMapDataObject country, List<BinaryMapDataObject> boundaries,
			String downloadName, File sqliteFile, File directoryWithTargetFiles, boolean dryRun) throws IOException, SQLException, InterruptedException, IllegalArgumentException, XmlPullParserException {
		final String suffix = "_" + IndexConstants.BINARY_MAP_VERSION + IndexConstants.BINARY_SRTM_MAP_INDEX_EXT;
		String name = country.getName();
		String dwName = Algorithms.capitalizeFirstLetterAndLowercase(downloadName + suffix);
		Set<String> tileNames = new TreeSet<String>();
		final File targetFile = new File(directoryWithTargetFiles, dwName);
		if(targetFile.exists()) {
			System.out.println("Already processed "+ name);
			return;
		}

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
		int rightLon = (int) Math.floor(MapUtils.getTileNumberX(MAX_ZOOM, qr.right));
		int leftLon = (int) Math.floor(MapUtils.getTileNumberX(MAX_ZOOM, qr.left));
		int bottomLat = (int) Math.floor(MapUtils.getTileNumberY(MAX_ZOOM, qr.bottom));
		int topLat = (int) Math.floor(MapUtils.getTileNumberY(MAX_ZOOM, qr.top));
		boolean onetile = leftLon == rightLon && bottomLat == topLat;
		for(int ilon = leftLon; ilon <= rightLon; ilon++) {
			for(int ilat = bottomLat; ilat <= topLat; ilat++) {
				double llon = MapUtils.getLongitudeFromTile(MAX_ZOOM, ilon);
				double rlon = MapUtils.getLongitudeFromTile(MAX_ZOOM, ilon + 1);
				double tlat = MapUtils.getLatitudeFromTile(MAX_ZOOM, ilat);
				double blat = MapUtils.getLatitudeFromTile(MAX_ZOOM, ilat + 1);
				boolean isOut = !polygon.containsPoint((tlat + blat) * 0.5 , (llon + rlon) * 0.5) && !onetile;
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
							if(MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), tr, tl)) {
								isOut = false;
							} else if(MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), tr, br)) {
								isOut = false;
							} else if(MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), bl, tl)) {
								isOut = false;
							} else if(MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), br, bl)) {
								isOut = false;
							}
							prev = n;
						}
						if(!isOut) {
							break;
						}
					}
				}
				if(!isOut) {
					tileNames.add(ilon + "_" + ilat + "_" + MAX_ZOOM);
				}
			}
		}
		System.out.println();
		System.out.println("-----------------------------");
		System.out.println("PROCESSING "+name + " lon [" + leftLon + " - " + rightLon + "] lat [" + bottomLat + " - " + topLat
				+ "] TOTAL " + tileNames.size() + " files " + tileNames);
		if(dryRun) {
			return;
		}
		File procFile = new File(directoryWithTargetFiles, dwName + ".proc");
		boolean locked = !procFile.createNewFile();
		if (locked) {
			System.out.println("\n\n!!!!!!!! WARNING FILE IS BEING PROCESSED !!!!!!!!!\n\n");
			return;
		}
		procFile.delete();
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
