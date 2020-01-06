package net.osmand.util;

import java.io.File;
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
import java.util.Set;
import java.util.TreeSet;

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

public class SplitHillshadeIntoRegions {
	private static final Log LOG = PlatformUtil.getLog(SplitHillshadeIntoRegions.class);
	private static int MIN_ZOOM = 1;
	private static int MAX_ZOOM = 11;
	private static final int BATCH_SIZE = 100;

	public static void main(String[] args) throws IOException {
		File sqliteFile = new File(args[0]);
		File directoryWithTargetFiles = new File(args[1]);
		boolean dryRun = false;
		String filter = null; // mauritius
		String prefix = "Hillshade_";
		for(int i = 2; i < args.length; i++ ){
			if("--dry-run".equals(args[i])) {
				dryRun = true;
			} else if(args[i].startsWith("--prefix=")) {
				prefix = args[i].substring("--prefix=".length());
			} else if(args[i].startsWith("--maxzoom=")) {
				MAX_ZOOM = Integer.parseInt(args[i].substring("--maxzoom=".length()));
			} else if(args[i].startsWith("--minzoom=")) {
				MIN_ZOOM = Integer.parseInt(args[i].substring("--minzoom=".length()));
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
					process(rc, lst, dw, sqliteFile, directoryWithTargetFiles, prefix, dryRun);
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
			String downloadName, File sqliteFile, File directoryWithTargetFiles, String prefix, boolean dryRun) throws IOException, SQLException, InterruptedException, IllegalArgumentException, XmlPullParserException {
		String name = country.getName();
		String dwName = prefix + Algorithms.capitalizeFirstLetterAndLowercase(downloadName) + ".sqlitedb";
		Set<Long> tileNames = new TreeSet<>();
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
		for(int tileX = leftLon; tileX <= rightLon; tileX++) {
			for(int tileY = topLat; tileY <= bottomLat; tileY++) {
				double llon = MapUtils.getLongitudeFromTile(MAX_ZOOM, tileX);
				double rlon = MapUtils.getLongitudeFromTile(MAX_ZOOM, tileX + 1);
				double tlat = MapUtils.getLatitudeFromTile(MAX_ZOOM, tileY);
				double blat = MapUtils.getLatitudeFromTile(MAX_ZOOM, tileY + 1);
				boolean isOut = !polygon.containsPoint(tlat / 2 + blat / 2 , llon / 2 + rlon / 2) && !onetile;
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
					int x = tileX;
					int y = tileY;
					for (int z = MAX_ZOOM; z >= MIN_ZOOM; z--) {
						tileNames.add(pack(x, y, z));
						x = x >> 1;
						y = y >> 1;
					}
				}
			}
		}
		System.out.println();
		System.out.println("-----------------------------");
		System.out.println("PROCESSING "+name + " lon [" + leftLon + " - " + rightLon + "] lat [" + bottomLat + " - " + topLat
				+ "] TOTAL " + tileNames.size() + " files ");
		if(dryRun) {
			return;
		}
		procFile(sqliteFile, targetFile, tileNames);
	}

	private static void procFile(File sqliteFile, final File targetFile, Set<Long> tileNames) throws IOException, SQLException {
		File procFile = new File(targetFile.getParentFile(), targetFile.getName() + ".proc");
		boolean locked = !procFile.createNewFile();
		if (locked) {
			System.out.println("\n\n!!!!!!!! WARNING FILE IS BEING PROCESSED !!!!!!!!!\n\n");
			return;
		}
		int batch = 0;
		try(Connection sqliteConn = DBDialect.SQLITE.getDatabaseConnection(sqliteFile.getAbsolutePath(), LOG); 
			Connection newFile = DBDialect.SQLITE.getDatabaseConnection(targetFile.getAbsolutePath(), LOG)) {
			prepareNewHillshadeFile(newFile, false, MIN_ZOOM, MAX_ZOOM);
			PreparedStatement ps = sqliteConn.prepareStatement("SELECT image FROM tiles WHERE x = ? AND y = ? AND z = ? AND s = 0");
			PreparedStatement is = newFile.prepareStatement("INSERT INTO tiles(x, y, z, s, image) VALUES(?, ?, ?, 0, ?)");
			for(long s : tileNames) {
				int x = unpack1(s);
				int y = unpack2(s);
				int z = unpack3(s);
				ps.setInt(1, x);
				int yt = (1 << z) - y - 1;
				ps.setInt(2, yt);
				ps.setInt(3, z);
				ResultSet rs = ps.executeQuery();
				if(rs.next()) {
					byte[] image = rs.getBytes(1);
					is.setInt(1, x);
					is.setInt(2, y);
					is.setInt(3, z);
					is.setBytes(4, image);
					is.addBatch();
					if(batch ++ >= BATCH_SIZE) {
						batch = 0;
						is.executeBatch();
					}
				}
				rs.close();
			}
			is.executeBatch();
			is.close();
			ps.close();
		};
		procFile.delete();
	}

	private static void prepareNewHillshadeFile(Connection newFile, boolean bigPlanet, int minZoom, int maxZoom) throws SQLException {
		Statement statement = newFile.createStatement();
		statement.execute("CREATE TABLE tiles (x int, y int, z int, s int, image blob, time long, PRIMARY KEY (x,y,z,s))");
		statement.execute("CREATE INDEX IND on tiles (x,y,z,s)");
		statement.execute("CREATE TABLE info(tilenumbering,minzoom,maxzoom,timecolumn,url,rule,referer)");
		statement.close();


		PreparedStatement pStatement = newFile.prepareStatement("INSERT INTO INFO VALUES(?,?,?,?,?,?,?)");
		String tileNumbering = bigPlanet ? "BigPlanet" : "simple";
		pStatement.setString(1, tileNumbering);
		int minNormalZoom = bigPlanet ? 17 - maxZoom : minZoom;
		int maxNormalZoom = bigPlanet ? 17 - minZoom : maxZoom;
		pStatement.setInt(2, minNormalZoom);
		pStatement.setInt(3, maxNormalZoom);
		pStatement.setString(4, "yes");
		pStatement.setString(5, "");
		pStatement.setString(6, "");
		pStatement.setString(7, "");
		pStatement.execute();
	}
	
	// @Native public static final long MAX_VALUE = 0x7fffffffffffffffL;
	// @Native public static final int  MAX_VALUE = 0x7fffffff         ;
	private static final long MASK1 = 0x00000000ffffff00L;
	private static final long SHIFT_1 = 8;
	private static final long MASK2 = 0x00ffffff00000000L;
	private static final long SHIFT_2 = 32;
	private static final long MASK3 = 0x00000000000000ffl;
	private static final long SHIFT_3 = 0;
	private static long pack(int r1, int r2, int r3) {
		long l = 0;
		l |= ((long)r1 << SHIFT_1) & MASK1;
		l |= ((long)r2 << SHIFT_2) & MASK2;
		l |= ((long)r3 << SHIFT_3) & MASK3;
		if(unpack3(l) != r3  || unpack2(l) != r2  || unpack1(l) != r1 ) {
			throw new IllegalStateException();
		}
		return l;
	}
	
	private static int unpack3(long l) {
		return (int) ((l & MASK3) >> SHIFT_3);
	}
	
	private static int unpack2(long l) {
		return (int) ((l & MASK2) >> SHIFT_2);
	}
	
	private static int unpack1(long l) {
		return (int) ((l & MASK1) >> SHIFT_1);
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
