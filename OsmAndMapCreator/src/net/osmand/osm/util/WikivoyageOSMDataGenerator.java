package net.osmand.osm.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.data.preparation.DBDialect;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.util.MapUtils;


public class WikivoyageOSMDataGenerator {
	
	private static int sleft = MapUtils.get31TileNumberX(-179.9);
	private static int sright = MapUtils.get31TileNumberX(179.9);
	private static int stop = MapUtils.get31TileNumberY(85);
	private static int sbottom = MapUtils.get31TileNumberY(-85);
	
	private static final Log log = PlatformUtil.getLog(WikivoyageOSMDataGenerator.class);
	private static final String[] columns = new String[] {"osm_id long", "city_type text", "population long", "country text", "region text"};
	private static final double DISTANCE_THRESHOLD = 3000000;
	private static final int POPULATION_LIMIT = 1000000;
	private static OsmandRegions regions;
	
	public static void main(String[] args) throws SQLException, IOException {
		String file = "";
		String regionsFile = "";
		String citiesObf = "";
		if(args.length == 0) {	
			file = "/home/user/osmand/wikivoyage/wikivoyage.sqlite";
			regionsFile = "/home/user/repo/tools/OsmAndMapCreator/regions.ocbf";
			citiesObf = "/home/user/Cities.obf";
		}
		if(args.length > 2) {
			file = args[0];
			regionsFile = args[1];
			citiesObf = args[2];
		} else if (file.isEmpty()) {
			System.err.println("You should provide the path to the wikivoyage.sqlite file.");
			System.exit(-1);
		}
		File sqliteFile = new File(file);
		File countriesObfFile = new File(citiesObf);
		if (!sqliteFile.exists()) {
			System.err.println("The path to the wikivoyage.sqlite is incorrect. Exiting...");
			System.exit(-1);
		} else if (!countriesObfFile.exists()) {
			System.err.println("The path to the regions.ocbf is incorrect. Exiting...");
			System.exit(-1);
		}
		regions = new OsmandRegions();
		regions.prepareFile(regionsFile);
		regions.cacheAllCountries();
		processDb(sqliteFile, countriesObfFile);
	}

	private static void processDb(File sqliteFile, File citiesObf) throws SQLException, IOException {
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = (Connection) dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
		addColumns(conn);
		RandomAccessFile raf = new RandomAccessFile(citiesObf, "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, citiesObf);
		PreparedStatement stat = conn.prepareStatement("SELECT title, lat, lon FROM travel_articles");
		int columns = getRowCount(conn);
		int count = 0;
		ResultSet rs = stat.executeQuery();
		Map<String, List<Amenity>> cities = fetchCities(reader);
		while(rs.next()) {
			String title = rs.getString(1);
			String searchTitle = title.replaceAll("\\(.*\\)", "").trim();
			long lat = rs.getLong(2);
			long lon = rs.getLong(3);
			LatLon ll = new LatLon(lat, lon);
			if(lat == 0 && lon == 0) {
				continue;
			}
			List<Amenity> results = cities.get(searchTitle);
			Amenity acceptedResult = (results != null && results.size() == 1) ? results.get(0) : getClosestMatch(results, ll);
			insertData(conn, title, acceptedResult, ll);
			if (count++ % 500 == 0) {
				System.out.format("%.2f", (((double)count / (double)columns) * 100d));
				System.out.println("%");
			}
		}
		createPopularArticlesTable(conn);
		stat.close();
		conn.close();
		raf.close();
	}

	private static void createPopularArticlesTable(Connection conn) throws SQLException {
		conn.createStatement().execute("DROP TABLE IF EXISTS popular_articles;");
		conn.createStatement().execute("CREATE TABLE popular_articles(title text, trip_id long, popularity_index long, lat double, lon double, lang text)");
		conn.createStatement().execute("INSERT INTO popular_articles(title, trip_id , popularity_index, lat, lon, lang) " + 
				"SELECT title, trip_id , population, lat, lon, lang " + 
				"FROM travel_articles WHERE population > " + POPULATION_LIMIT);
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS popular_lang_ind ON popular_articles(lang);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS popular_city_id_ind ON popular_articles(trip_id);");
	}

	private static void insertData(Connection conn, String title, Amenity acceptedResult, LatLon fromDB)
			throws SQLException, IOException {
		boolean hasResult = acceptedResult != null;
		String sqlStatement = hasResult ? "UPDATE travel_articles SET lat = ?, lon = ?, osm_id = ?, city_type = ?, population = ?, "
				+ "country = ?, region = ? WHERE title = ?" : "UPDATE travel_articles SET country = ?, region = ? WHERE title = ?";
		PreparedStatement updateStatement = conn.prepareStatement(sqlStatement);
		int column = 1;
		LatLon coords = hasResult ? acceptedResult.getLocation() : fromDB;
		if (hasResult) {
			updateStatement.setDouble(column++, coords.getLatitude());
			updateStatement.setDouble(column++, coords.getLongitude());
			updateStatement.setLong(column++, acceptedResult.getId() >> 1);
			updateStatement.setString(column++, acceptedResult.getSubType());
			String population = acceptedResult.getAdditionalInfo("population");
			updateStatement.setLong(column++, (population == null || population.isEmpty()) ? 0 : Long.parseLong(population.replaceAll("[-,. \\)\\(]", "")));
		}
		List<String> regionsList = getRegions(coords.getLatitude(), coords.getLongitude());
		WorldRegion country = null;
		WorldRegion region = null;
		for (String reg : regionsList) {
			WorldRegion regionData = regions.getRegionDataByDownloadName(reg);
			if (regionData == null) {
				continue;
			}
			if (regionData.getLevel() == 2) {
				country = regionData;
			} else if (regionData.getLevel() > 2) {
				region = regionData;
			}
		}
		updateStatement.setString(column++, country == null ? null : country.getLocaleName());
		updateStatement.setString(column++, region == null ? null : region.getRegionDownloadName());
		updateStatement.setString(column++, title);
		updateStatement.executeUpdate();
		updateStatement.clearParameters();
	}

	private static Map<String, List<Amenity>> fetchCities(BinaryMapIndexReader reader) throws IOException {
		Map<String, List<Amenity>> res = new HashMap<>();
		SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(sleft, sright, stop, sbottom, -1, BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER, null);
		System.out.println("Start fetching cities");
		long startTime = System.currentTimeMillis();
		List<Amenity> results = reader.searchPoi(req);
		System.out.println("Getting cities list took: " + (System.currentTimeMillis() - startTime) + " ms");
		for (Amenity am : results) {
			for (String name : am.getAllNames()) {
				if (!res.containsKey(name)) {
					List<Amenity> list = new ArrayList<>();
					list.add(am);
					res.put(name, list);
				} else {
					res.get(name).add(am);
				}
			}
		}
		return res;
	}

	private static int getRowCount(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("SELECT Count(*) FROM travel_articles");
		int columns = 0;
		ResultSet res = ps.executeQuery();
		while (res.next()) {
			columns = res.getInt(1);
		}
		return columns;
	}

	private static void addColumns(Connection conn) {
		for (String s : columns) {
			try {
				conn.createStatement().execute("ALTER TABLE travel_articles ADD COLUMN " + s);
			} catch (SQLException ex) {
				// ex.printStackTrace();
				System.out.println("Column alredy exsists");
			}
		}
	}

	private static Amenity getClosestMatch(List<Amenity> results, LatLon fromDB) {
		if (results == null || results.isEmpty()) {
			return null;
		}
		Amenity result = results.get(0);
		double distance = MapUtils.getDistance(fromDB, result.getLocation());;
		for (int i = 1; i < results.size(); i++) {
			Amenity a = results.get(i);
			LatLon thisLoc = a.getLocation();
			double calculatedDistance = MapUtils.getDistance(fromDB, thisLoc);
			if (calculatedDistance < distance) {
				result = a;
			}
		}
		LatLon loc = result.getLocation();
		if ((((int) loc.getLongitude() == (int) fromDB.getLongitude()) && ((int) loc.getLatitude() == (int) fromDB.getLatitude()) 
				|| distance < DISTANCE_THRESHOLD)) {
			return result;
		}
		return null;	
	}
	
	private static List<String> getRegions(double lat, double lon) throws IOException {
		List<String> keyNames = new ArrayList<>();
		int x31 = MapUtils.get31TileNumberX(lon);
		int y31 = MapUtils.get31TileNumberY(lat);
		List<BinaryMapDataObject> cs = regions.query(x31, y31);
		for (BinaryMapDataObject b : cs) {
			if(regions.contain(b, x31, y31)) {
				keyNames.add(regions.getDownloadName(b));
			}
		}
		return keyNames;
	}

}
