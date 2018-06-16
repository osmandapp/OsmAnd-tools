package net.osmand.travel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.osm.io.NetworkUtils;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;


public class WikivoyageOSMDataGenerator {
	
	private static int sleft = MapUtils.get31TileNumberX(-179.9);
	private static int sright = MapUtils.get31TileNumberX(179.9);
	private static int stop = MapUtils.get31TileNumberY(85);
	private static int sbottom = MapUtils.get31TileNumberY(-85);
	
	private static final Log log = PlatformUtil.getLog(WikivoyageOSMDataGenerator.class);
	private static final String[] columns = new String[] {"osm_id long", "city_type text", "population long", "country text", "region text"};
	private static final double DISTANCE_THRESHOLD = 3000000;
	private static final int POPULATION_LIMIT = 1000000;
	private static final int BATCH_INSERT_INTO_SIZE = 100;
	private static OsmandRegions regions;
	
	public static void main(String[] args) throws SQLException, IOException, JSONException {
		String file = "";
		String citiesObf = "";
		if(args.length == 0) {	
			file = "/home/user/osmand/wikivoyage/wikivoyage.sqlite";
			citiesObf = "/home/user/Cities.obf";
		}
		if(args.length > 2) {
			file = args[0];
			citiesObf = args[1];
		} else if (file.isEmpty()) {
			System.err.println("You should provide the path to the wikivoyage.sqlite file.");
			System.exit(-1);
		}
		File sqliteFile = new File(file);
		File citiesObfFile = new File(citiesObf);
		if (!sqliteFile.exists()) {
			System.err.println("The path to the wikivoyage.sqlite is incorrect. Exiting...");
			System.exit(-1);
		} else if (!citiesObfFile.exists()) {
			System.err.println("The path to the cities.ocbf is incorrect. Exiting...");
			System.exit(-1);
		}
		regions = new OsmandRegions();
		regions.prepareFile();
		regions.cacheAllCountries();
		processDb(sqliteFile, citiesObfFile);
		
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = (Connection) dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
		addCitiesData(citiesObfFile, conn);
		createPopularArticlesTable(conn);
		conn.close();
	}

	private static void processDb(File sqliteFile, File citiesObf) throws SQLException, IOException, JSONException {
		
	}

	private static void addCitiesData(File citiesObf, Connection conn) throws FileNotFoundException, IOException,
			SQLException {
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
		stat.close();
		raf.close();
	}
	
	private static TreeSet<String> readMostPopularArticlesFromWikivoyage(TreeSet<String> langs, int limit) throws IOException, JSONException {
		TreeSet<String> articleIds = new TreeSet<>();
		String date = new SimpleDateFormat("yyyy/MM").format(new Date(System.currentTimeMillis() - 24*60*60*1000*30l));// previous month
		for (String lang : langs) {
			String url = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/" + lang + ".wikivoyage/all-access/"
					+ date + "/all-days";
			System.out.println("Loading " + url);
			HttpURLConnection conn = NetworkUtils.getHttpURLConnection(url);
			StringBuilder sb = Algorithms.readFromInputStream(conn.getInputStream());
			// System.out.println("Debug Data " + sb);
			JSONObject object = new JSONObject(new JSONTokener(sb.toString()));
			TreeSet<String> list = new TreeSet<String>();
			JSONArray articles = object.getJSONArray("items").getJSONObject(0).getJSONArray("articles");
			for (int i = 0; i < articles.length() && i < limit; i++) {
				String title = articles.getJSONObject(i).getString("article").toLowerCase();
				list.add(title);
				articleIds.add(title);
			}
		}
		return articleIds;
	}
	

	private static void createPopularArticlesTable(Connection conn) throws SQLException, IOException, JSONException {
		conn.createStatement().execute("DROP TABLE IF EXISTS popular_articles;");
		// Itineraries UNESCO
		System.out.println("Create popular articles");
		conn.createStatement().execute("CREATE TABLE popular_articles(title text, trip_id long,"
				+ " population long, order_index long, popularity_index long, lat double, lon double, lang text)");
		ResultSet rs = conn.createStatement().executeQuery("SELECT DISTINCT lang from travel_articles");
		TreeSet<String> langs = new TreeSet<String>();
		while(rs.next()) {
			langs.add(rs.getString(1));
		}
		rs.close();
		Set<Long> tripIds = new TreeSet<Long>();
		Set<Long> excludeTripIds = new TreeSet<Long>();
		
		System.out.println("Read most popular articles for " + langs);
		TreeSet<String> popularArticleIds = readMostPopularArticlesFromWikivoyage(langs, 100);
		
		System.out.println("Scan articles for big cities");
		rs = conn.createStatement().executeQuery("SELECT title, trip_id, population, lat, lon, lang FROM travel_articles ");
		
		while(rs.next()) {
			String title = rs.getString(1).toLowerCase();
			Long tripId = rs.getLong(2);
			if(title.equals("main page") || title.contains("disambiguation") 
					|| title.contains("значения")) {
				excludeTripIds.add(tripId);
			}
			if(title.contains("itineraries") || title.contains("unesco")) {
				tripIds.add(tripId);
			}
			if(popularArticleIds.contains(title)) {
				tripIds.add(tripId);
			}
			if(rs.getLong(3) > POPULATION_LIMIT) {
				tripIds.add(tripId);
			}
		}
		rs.close();
		tripIds.removeAll(excludeTripIds);
		
		System.out.println("Create popular article refs");
		while(!tripIds.isEmpty()) {
			int batchSize = BATCH_INSERT_INTO_SIZE;
			StringBuilder tripIdsInStr = new StringBuilder();
			Iterator<Long> it = tripIds.iterator();
			while(it.hasNext() && batchSize -- > 0) {
				if(tripIdsInStr.length() > 0) {
					tripIdsInStr.append(", ");
				}
				tripIdsInStr.append(it.next());
				it.remove();
			}
			
			conn.createStatement().execute("INSERT INTO popular_articles(title, trip_id, population, lat, lon, lang) " + 
				"SELECT title, trip_id, population, lat, lon, lang FROM travel_articles WHERE trip_id IN ("+tripIdsInStr+")");
		}
		
		
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
