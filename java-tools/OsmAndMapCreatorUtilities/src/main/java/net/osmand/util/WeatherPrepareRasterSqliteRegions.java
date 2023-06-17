package net.osmand.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

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
import net.osmand.map.WorldRegion;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;

public class WeatherPrepareRasterSqliteRegions {
	private static final Log LOG = PlatformUtil.getLog(WeatherPrepareRasterSqliteRegions.class);
	private static boolean SKIP_EXISTING = false;
	private static String EXTENSION = ".sqlitedb";
	private static boolean GZIP = false;
	private static int ZOOM = 4;
	private static final int BATCH_SIZE = 100;
	private static int GEN_HOURS_BACK = 4;
	private static String REGIONS_FOLDER = "regions";
	private static String SOURCES_FORECAST = "gfs,ecmwf";
	
	public static void main(String[] args) throws IOException {
		File weatherFolder = new File(args[0]);
		boolean dryRun = false;
		String filter = null; // mauritius
		String prefix = "Weather_";
		
		for (int i = 1; i < args.length; i++) {
			if ("--dry-run".equals(args[i])) {
				dryRun = true;
			} else if ("--gzip".equals(args[i])) {
				GZIP = true;
			} else if (args[i].startsWith("--zoom=")) {
				ZOOM = Integer.parseInt(args[i].substring("--zoom=".length()));
			} else if (args[i].startsWith("--include-hours-before-now=")) {
				GEN_HOURS_BACK = Integer.parseInt(args[i].substring("--include-hours-before-now=".length()));
			} else if (args[i].startsWith("--extension=")) {
				EXTENSION = args[i].substring("--extension=".length());
			} else if (args[i].startsWith("--sources=")) {
				SOURCES_FORECAST = args[i].substring("--sources=".length());
			} else if (args[i].startsWith("--filter=")) {
				filter = args[i].substring("--filter=".length());
			} else if (args[i].startsWith("--prefix=")) {
				prefix = args[i].substring("--prefix=".length());
			}
		}
		OsmandRegions or = new OsmandRegions();
		BinaryMapIndexReader fl = or.prepareFile();
		Map<String, LinkedList<BinaryMapDataObject>> allCountries = or.cacheAllCountries(false);
		MapIndex mapIndex = fl.getMapIndexes().get(0);
		int boundaryTag = mapIndex.getRule("osmand_region", "boundary");
		int cnt = 1, proc = 1;
		Set<String> failedCountries = new HashSet<String>();
		for (String fullName : allCountries.keySet()) {
			proc++;
			if (filter != null && !fullName.contains(filter)) {
				continue;
			}
			WorldRegion region = or.getRegionData(fullName);
			BinaryMapDataObject rc = getBoundary(or, allCountries, boundaryTag, region);
			boolean hasParentBoundaries = getBoundary(or, allCountries, boundaryTag, region.getSuperregion()) != null 
					&& !region.getSuperregion().getRegionId().equals("northamerica_us")
					&& !region.getSuperregion().getRegionId().equals("northamerica_canada");
			if (rc == null || hasParentBoundaries) {
				continue;
			}
			String dname = region.getRegionDownloadName();
			if (dname == null) {
				String superRegion = region.getSuperregion().getRegionId();
				dname = fullName.substring(superRegion.length() +1) + "_" + superRegion;
			}
			System.out.println(String.format("\nRegion %s processed %d [%d/%d]", dname, cnt++, proc, allCountries.size()));
			try {
				process(rc, allCountries.get(fullName), dname, weatherFolder, prefix, dryRun);
			} catch (Exception e) {
				failedCountries.add(fullName);
				e.printStackTrace();
			}
		}
		if (!failedCountries.isEmpty()) {
			throw new IllegalStateException("Failed countries " + failedCountries);
		}
	}

	private static BinaryMapDataObject getBoundary(OsmandRegions or, Map<String, LinkedList<BinaryMapDataObject>> allCountries, int boundaryTag, WorldRegion region) {
		if (region == null) {
			return null;
		}
		LinkedList<BinaryMapDataObject> lst = allCountries.get(region.getRegionId());
		BinaryMapDataObject rc = null;
		if (lst == null) {
			return null;
		}
		for (BinaryMapDataObject r : lst) {
			if (!r.containsType(boundaryTag) && r.getPointsLength() > 1) {
				rc = r;
				break;
			}
		}
		return rc;
	}

	private static void process(BinaryMapDataObject country, List<BinaryMapDataObject> boundaries,
			String downloadName, File weatherFolder, String prefix, boolean dryRun) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		String name = country.getName();
		String dwName = prefix + Algorithms.capitalizeFirstLetterAndLowercase(downloadName) + EXTENSION;
		final File targetFile = new File(new File(weatherFolder, REGIONS_FOLDER), dwName);
		final File targetFileGZip = new File(new File(weatherFolder, REGIONS_FOLDER), dwName + ".gz");
		if (!SKIP_EXISTING) {
			targetFile.delete();
			targetFileGZip.delete();
		}
		if (targetFile.exists()) {
			System.out.println("Already processed "+ name);
			return;
		}

		Set<Long> regionTileNames = new TreeSet<>();
		calculateRegionBoundaries(country, boundaries, name, regionTileNames);
		if (dryRun) {
			return;
		}
		procRegion(weatherFolder, regionTileNames, targetFile);
		if (GZIP) {
			FileOutputStream fout = new FileOutputStream(targetFileGZip);
			FileInputStream fin = new FileInputStream(targetFile);
			Algorithms.streamCopy(fin, fout);
			fin.close();
			fout.close();
		}
		
	}

	private static void calculateRegionBoundaries(BinaryMapDataObject country, List<BinaryMapDataObject> boundaries,
			String name, Set<Long> regionTileNames) {
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
					regionTileNames.add(pack(tileX, tileY, ZOOM));
					x = x >> 1;
					y = y >> 1;
				}
			}
		}
		System.out.println();
		System.out.println("-----------------------------");
		System.out.println("PROCESSING " + name + " lon [" + leftLon + " - " + rightLon + "] lat [" + bottomLat + " - "
				+ topLat + "] TOTAL " + regionTileNames.size() + " tiles ");
	}

	private static void procRegion(File weatherFolder, Set<Long> regionTileNames, final File targetFile)
			throws SQLException, IOException, FileNotFoundException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(System.currentTimeMillis());
		cal.add(Calendar.HOUR_OF_DAY, -GEN_HOURS_BACK);
		Date filterTime = cal.getTime();
		
		int batch = 0;
		try (Connection targetConn = DBDialect.SQLITE.getDatabaseConnection(targetFile.getAbsolutePath(), LOG)) {
			prepareNewWeatherFile(targetConn, false, ZOOM, ZOOM);
			PreparedStatement psinsnew = targetConn.prepareStatement(
					"INSERT INTO tiles(x, y, z, s, image, time, source, forecastdate) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
			for (String source : SOURCES_FORECAST.split(",")) {
				File srcFile = new File(weatherFolder, source + "/tiff");
				if (!srcFile.exists()) {
					continue;
				}
				for (File dtFolder : srcFile.listFiles()) {
					if (dtFolder.isDirectory()) {
						Date forecastdate;
						try {
							forecastdate = dateFormat.parse(dtFolder.getName());
							if (forecastdate.before(filterTime)) {
								// ignore early dates
								continue;
							}
						} catch (ParseException e) {
							LOG.info("Error parsing folder name: " + e.getMessage() + " - " + dtFolder.getName());
							continue;
						}

						for (long s : regionTileNames) {
							int x = unpack1(s);
							int y = unpack2(s);
							int z = unpack3(s);
							File imageGzip = new File(dtFolder, z + "_" + x + "_" + y + ".tiff.gz");
							byte[] image = Algorithms
									.readBytesFromInputStream(new GZIPInputStream(new FileInputStream(imageGzip)));
							if (image != null) {
								psinsnew.setInt(1, x);
								psinsnew.setInt(2, y);
								psinsnew.setInt(3, z);
								psinsnew.setString(4, dtFolder.getName() + ":" + source);
								psinsnew.setBytes(5, image);
								psinsnew.setLong(6, imageGzip.lastModified());
								psinsnew.setString(7, source);
								psinsnew.setLong(8, forecastdate.getTime());
								psinsnew.addBatch();

								if (batch++ >= BATCH_SIZE) {
									batch = 0;
									psinsnew.executeBatch();
								}
							}
						}

					}
				}
			}
			psinsnew.executeBatch();
			psinsnew.close();
		}
	}

	

	private static void prepareNewWeatherFile(Connection newFile, boolean bigPlanet, int minZoom, int maxZoom) throws SQLException {
		Statement statement = newFile.createStatement();
		statement.execute("CREATE TABLE tiles (x int, y int, z int, s int, image blob, time long, source, forecastdate long, PRIMARY KEY (x,y,z,s))");
		statement.execute("CREATE INDEX IND on tiles (x,y,z,s)");
		statement.execute("CREATE TABLE info(tilenumbering,minzoom,maxzoom,timecolumn,gentime,url,rule,referer)");
		statement.close();


		PreparedStatement pStatement = newFile.prepareStatement("INSERT INTO INFO(tilenumbering,minzoom,maxzoom,timecolumn,gentime) VALUES(?,?,?,?,?)");
		String tileNumbering = bigPlanet ? "BigPlanet" : "simple";
		pStatement.setString(1, tileNumbering);
		int minNormalZoom = bigPlanet ? 17 - maxZoom : minZoom;
		int maxNormalZoom = bigPlanet ? 17 - minZoom : maxZoom;
		pStatement.setInt(2, minNormalZoom);
		pStatement.setInt(3, maxNormalZoom);
		pStatement.setString(4, "yes");
		pStatement.setLong(5, System.currentTimeMillis());
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
