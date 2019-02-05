package net.osmand.travel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import net.osmand.util.SqlInsertValuesReader;
import net.osmand.util.SqlInsertValuesReader.InsertValueProcessor;

import org.apache.commons.logging.Log;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

public class WikivoyageDataGenerator {

	private static final Log log = PlatformUtil.getLog(WikivoyageDataGenerator.class);
	private static final int BATCH_SIZE = 500;
	
	private static final String[] columns = new String[] {"osm_id long", "city_type text", "population long", "country text", "region text"};
	private static final double DISTANCE_THRESHOLD = 3000000;
	private static final int POPULATION_LIMIT = 1000000;
	private static final int BATCH_INSERT_INTO_SIZE = 100;
	
	private int sleft = MapUtils.get31TileNumberX(-179.9);
	private int sright = MapUtils.get31TileNumberX(179.9);
	private int stop = MapUtils.get31TileNumberY(85);
	private int sbottom = MapUtils.get31TileNumberY(-85);
	private OsmandRegions regions;
	

	public static void main(String[] args) throws SQLException, IOException {
		boolean uncompressed = false;
		File wikivoyageFile = new File(args[0]);
		if(!wikivoyageFile.exists()) {
			throw new IllegalArgumentException("Wikivoyage file doesn't exist: " + args[0]);
		}
		File citiesObfFile = null;
		File workingDir = wikivoyageFile.getParentFile();

		for(int i = 1; i < args.length; i++) {
			String val = args[i].substring(args[i].indexOf('=') + 1);
			if(args[i].startsWith("--uncompressed=")) {
				uncompressed = Boolean.parseBoolean(val);
			} else if(args[i].startsWith("--cities-obf=")) {
				citiesObfFile = new File(val);
			}
		}
		System.out.println("Process " + wikivoyageFile.getName() + " " + (uncompressed ? "uncompressed" : ""));
		
		final File langlinkFolder = new File(workingDir, "langlinks");
		final File langlinkFile = new File(workingDir, "langlink.sqlite");
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = (Connection) dialect.getDatabaseConnection(wikivoyageFile.getAbsolutePath(), log);
		WikivoyageDataGenerator generator = new WikivoyageDataGenerator();
		generator.regions = new OsmandRegions();
		generator.regions.prepareFile();
		generator.regions.cacheAllCountries();
		
		
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_orig_id ON travel_articles(original_id);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_image_title ON travel_articles(image_title);");
		
		printStep("Processing langlink file " + langlinkFile.getAbsolutePath());
		generator.createLangLinksIfMissing(langlinkFile, langlinkFolder, conn);
		printStep("Connect translations ");
		generator.generateSameTripIdForDifferentLang(langlinkFile, conn);
		printStep("Generate missing ids");
		generator.generateIdsIfMissing(conn, langlinkFile);
		printStep("Download/Copy proper headers for articles");
		generator.updateProperHeaderForArticles(conn, workingDir);
		printStep("Copy headers between lang");
		generator.copyHeaders(conn);
		printStep("Generate agg part of");
		generator.generateAggPartOf(conn);
		printStep("Generate search table");
		generator.generateSearchTable(conn);
		if (citiesObfFile != null) {
			printStep("Add osm city data");
		}
		generator.addCitiesData(citiesObfFile, conn);
		printStep("Populate popular articles");
		generator.createPopularArticlesTable(conn);
		
		conn.createStatement().execute("DROP INDEX IF EXISTS index_orig_id");
		conn.createStatement().execute("DROP INDEX IF EXISTS index_image_title ");
		conn.close();
	}

	private static void printStep(String step) {
		System.out.println("########## " + step + " ##########");
	}

	private void updateProperHeaderForArticles(Connection conn, File workingDir) throws SQLException {
		final File imagesMetadata = new File(workingDir, "images.sqlite");
		// delete images to fully recreate db
		// imagesMetadata.delete();
		Connection imagesConn = (Connection) DBDialect.SQLITE.getDatabaseConnection(imagesMetadata.getAbsolutePath(), log);
		imagesConn.createStatement()
				.execute("CREATE TABLE IF NOT EXISTS images(file text, url text, metadata text, sourcefile text)");
		conn.createStatement().execute("DROP TABLE IF EXISTS source_image;");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS source_image(banner_image text, source_image text)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_source_image ON source_image(banner_image);");
		
		Map<String, String> existingImagesMapping = new LinkedHashMap<String, String>();
		TreeSet<String> sourceImages = new TreeSet<String>();
		ResultSet rs1 = imagesConn.createStatement().executeQuery("SELECT file, sourcefile FROM images");
		while(rs1.next()) {
			String sourceFile = rs1.getString(2);
			sourceFile = stripImageName(sourceFile);
			existingImagesMapping.put(rs1.getString(1), sourceFile);
			if(sourceFile != null) {
				sourceImages.add(rs1.getString(2));
			}
		}
		rs1.close();
		
		
		Map<String, String> valuesToUpdate = new LinkedHashMap<String, String>();
//		PreparedStatement pSelect = imagesConn.prepareStatement("SELECT file, url, metadata, sourcefile FROM images WHERE file = ?");
		//PreparedStatement pDelete = imagesConn.prepareStatement("DELETE FROM images WHERE file = ?");
		PreparedStatement pInsert = imagesConn.prepareStatement("INSERT INTO images(file, url, metadata, sourcefile) VALUES (?, ?, ?, ?)");
		ResultSet rs = conn.createStatement().executeQuery("SELECT distinct image_title, title, lang FROM travel_articles where image_title <> ''");
		PreparedStatement pInsertSource = conn.prepareStatement("INSERT INTO source_image(banner_image, source_image) VALUES(?, ?)");
		
		int imagesFetched = 0;
		int imagesProcessed = 0;
		int imagesToUpdate = 0;
		while (rs.next()) {
			String imageTitle = rs.getString(1);
			String name = rs.getString(2);
			String lang = rs.getString(3);
			if(imageTitle == null || imageTitle.length() == 0) {
				continue;
			}
			if (valuesToUpdate.containsKey(imageTitle)) {
				continue;
			}
			if(sourceImages.contains(imageTitle)) {
				// processed before
				continue;
			}
			if (imagesProcessed++ % 5000 == 0) {
				System.out.println("Images metadata processed: " + imagesProcessed);
			}
			if(!existingImagesMapping.containsKey(imageTitle)) {
				existingImagesMapping.put(imageTitle, null);
				String metadataUrl = "https://commons.wikimedia.org/w/index.php?title=File:" + imageTitle + "&action=raw";
				try {
					URL url = new URL(metadataUrl);
					BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
					StringBuilder metadata = new StringBuilder();
					String s;
					String sourceFile = null;
					while ((s = reader.readLine()) != null) {
						if (s.contains("source=") && s.contains("File:")) {
							sourceFile = s.substring(s.indexOf("File:") + "File:".length());
							sourceFile = stripImageName(sourceFile);
						}
						metadata.append(s).append("\n");
					}
					pInsert.setString(1, imageTitle);
					pInsert.setString(2, metadataUrl);
					pInsert.setString(3, metadata.toString());
					pInsert.setString(4, sourceFile);
					pInsert.executeUpdate();
					existingImagesMapping.put(imageTitle, sourceFile);
					if (imagesFetched++ % 100 == 0) {
						System.out.println("Images metadata fetched: " + imagesFetched);
					}
				} catch (IOException e) {
					System.err.println("Error fetching image " + imageTitle + " " + lang + ":" + name + " "
							+ e.getMessage());
				}
			}
			String sourceFile = existingImagesMapping.get(imageTitle);
			
			valuesToUpdate.put(imageTitle, sourceFile);
			if (sourceFile != null && sourceFile.trim().length() > 0) {
				pInsertSource.setString(1, imageTitle);
				pInsertSource.setString(2, sourceFile);
				pInsertSource.executeUpdate();
				imagesToUpdate++;
			}
		}
		rs.close();
		System.out.println("Updating images " + imagesToUpdate + ".");
		int updated = conn.createStatement().executeUpdate("UPDATE travel_articles SET image_title = "
				+ " (SELECT source_image from source_image s where s.banner_image = travel_articles.image_title) "
				+ " WHERE image_title IN (SELECT distinct banner_image from source_image)");
		System.out.println("Update to full size images finished, updated: " + updated);
		
		imagesConn.close();
		
	}

	private String stripImageName(String sourceFile) {
		if(sourceFile == null) {
			return null;
		}
		if (sourceFile.contains("]")) {
			sourceFile = sourceFile.substring(0, sourceFile.indexOf(']'));
		}
		if (sourceFile.contains("}")) {
			sourceFile = sourceFile.substring(0, sourceFile.indexOf('}'));
		}
		if(sourceFile.contains("[[") || sourceFile.contains("|")) {
			return null;
		}
		return sourceFile;
	}

	private void copyHeaders(Connection conn) throws SQLException {
		Statement statement = conn.createStatement();
		boolean update = statement.execute("update or ignore travel_articles set image_title=(SELECT image_title FROM travel_articles t "
				+ "WHERE t.trip_id = travel_articles.trip_id and t.lang = 'en')"
				+ " where travel_articles.image_title='' and travel_articles.lang <>'en'");
        System.out.println("Copy headers from english language to others: " + update);
        statement.close();
        statement = conn.createStatement();
        System.out.println("Articles without banner image:");
        ResultSet rs = statement.executeQuery("select count(*), lang from travel_articles where image_title = '' group by lang");
        while(rs.next()) {
        	System.out.println("\t" + rs.getString(2) + " " + rs.getInt(1));
        }
		rs.close();
		statement.close();
	}

	private void generateAggPartOf(Connection conn) throws SQLException {
		try {
			conn.createStatement().execute("ALTER TABLE travel_articles ADD COLUMN aggregated_part_of");
		} catch (Exception e) {
			System.err.println("Column aggregated_part_of already exists");
		}
		PreparedStatement updatePartOf = conn
				.prepareStatement("UPDATE travel_articles SET aggregated_part_of = ? WHERE title = ? AND lang = ?");
		PreparedStatement data = conn.prepareStatement("SELECT trip_id, title, lang, is_part_of FROM travel_articles");
		ResultSet rs = data.executeQuery();
		int batch = 0;
		while (rs.next()) {
			String title = rs.getString("title");
			String lang = rs.getString("lang");
			updatePartOf.setString(1, getAggregatedPartOf(conn, rs.getString("is_part_of"), lang));
			updatePartOf.setString(2, title);
			updatePartOf.setString(3, lang);
			updatePartOf.addBatch();
			if (batch++ > BATCH_SIZE) {
				updatePartOf.executeBatch();
				batch = 0;
			}
		}
		finishPrep(updatePartOf);
		data.close();
		rs.close();
	}

	public void generateSearchTable(Connection conn) throws SQLException {
		conn.createStatement().execute("DROP TABLE IF EXISTS travel_search;");
		conn.createStatement()
				.execute("CREATE TABLE travel_search(search_term text, trip_id long, article_title text, lang text)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_search_term ON travel_search(search_term);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_search_city ON travel_search(trip_id)");

		PreparedStatement insertSearch = conn.prepareStatement("INSERT INTO travel_search VALUES (?, ?, ?, ?)");
		PreparedStatement data = conn.prepareStatement("SELECT trip_id, title, lang, is_part_of FROM travel_articles");

		ResultSet rs = data.executeQuery();
		int batch = 0;
		while (rs.next()) {
			String title = rs.getString("title");
			String titleToSplit = title.replaceAll("[/\\)\\(-]", " ").replaceAll(" +", " ");
			String lang = rs.getString("lang");
			long id = rs.getLong("trip_id");
			for (String s : titleToSplit.split(" ")) {
				insertSearch.setString(1, s.toLowerCase());
				insertSearch.setLong(2, id);
				insertSearch.setString(3, title);
				insertSearch.setString(4, lang);
				insertSearch.addBatch();
				if (batch++ > 500) {
					insertSearch.executeBatch();
					batch = 0;
				}
			}
		}
		finishPrep(insertSearch);
		data.close();
		rs.close();
	}

	private void generateSameTripIdForDifferentLang(final File langlinkFile, Connection conn)
			throws SQLException {
		DBDialect dialect = DBDialect.SQLITE;
		Connection langlinkConn = (Connection) dialect.getDatabaseConnection(langlinkFile.getAbsolutePath(), log);
		PreparedStatement langlinkStatement = langlinkConn
				.prepareStatement("SELECT id FROM langlinks WHERE title = ? AND lang = ?");
		PreparedStatement updateTripId = conn
				.prepareStatement("UPDATE travel_articles SET trip_id = ? WHERE title = ? AND lang = ?");
		PreparedStatement data = conn.prepareStatement("SELECT trip_id, title, lang, is_part_of FROM travel_articles");

		ResultSet rs = data.executeQuery();
		int batch = 0;
		while (rs.next()) {
			String title = rs.getString("title");
			String lang = rs.getString("lang");
			long id = getCityId(langlinkStatement, title, lang);
			updateTripId.setLong(1, id);
			updateTripId.setString(2, title);
			updateTripId.setString(3, lang);
			updateTripId.addBatch();
			if (batch++ > BATCH_SIZE) {
				updateTripId.executeBatch();
				batch = 0;
			}
		}
		finishPrep(updateTripId);
		langlinkStatement.close();
		langlinkConn.close();
	}

	private long getCityId(PreparedStatement langlinkStatement, String title, String lang) throws SQLException {
		langlinkStatement.setString(1, title);
		langlinkStatement.setString(2, lang);
		ResultSet rs = langlinkStatement.executeQuery();
		if (rs.next()) {
			return rs.getLong("id");
		}
		return 0;
	}

	private void createLangLinksIfMissing(File langlinkFile, File langlinkFolder, Connection conn)
			throws IOException, SQLException {
		if (langlinkFolder.exists() && !langlinkFile.exists()) {
			processLangLinks(langlinkFolder, langlinkFile, conn);
		}
	}

	private void processLangLinks(File langlinkFolder, File langlinkFile, Connection wikivoyageConnection)
			throws IOException, SQLException {
		if (!langlinkFolder.isDirectory()) {
			System.err.println("Specified langlink folder is not a directory");
			System.exit(-1);
		}
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = (Connection) dialect.getDatabaseConnection(langlinkFile.getAbsolutePath(), log);
		conn.createStatement()
				.execute("CREATE TABLE langlinks (id long NOT NULL DEFAULT 0, lang text NOT NULL DEFAULT '', "
						+ "title text NOT NULL DEFAULT '', UNIQUE (lang, title) ON CONFLICT IGNORE)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_title ON langlinks(title);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_lang ON langlinks(lang);");
		PreparedStatement prep = conn.prepareStatement("INSERT OR IGNORE INTO langlinks VALUES (?, ?, ?)");
		PreparedStatement articleQuery = wikivoyageConnection
				.prepareStatement("SELECT title FROM travel_articles WHERE original_id = ? AND lang = ?");
		Set<Long> ids = new HashSet<>();
		Set<Long> currentFileIds = new HashSet<>();
		Map<Long, Long> currMapping = new HashMap<>();
		File[] files = langlinkFolder.listFiles();
		final String[] lang = new String[1];
		InsertValueProcessor p = new InsertValueProcessor() {

			private int batch = 0;
			private long maxId = 0;

			@Override
			public void process(List<String> insValues) {
				long id = Long.valueOf(insValues.get(0));
				try {
					articleQuery.setLong(1, id);
					articleQuery.setString(2, lang[0]);
					ResultSet rs = articleQuery.executeQuery();
					String thisTitle = "";
					while (rs.next()) {
						thisTitle = rs.getString("title");
					}
					articleQuery.clearParameters();
					maxId = Math.max(maxId, id);
					Long genId = currMapping.get(id);
					if (genId == null) {
						if (ids.contains(id)) {
							genId = ++maxId;
							currMapping.put(id, genId);
						}
					}
					id = genId == null ? id : genId;
					currentFileIds.add(id);
					if (!thisTitle.isEmpty()) {
						prep.setLong(1, id);
						prep.setString(2, lang[0]);
						prep.setString(3, thisTitle);
						prep.addBatch();
						batch++;
					}
					prep.setLong(1, id);
					prep.setString(2, insValues.get(1));
					prep.setString(3, insValues.get(2));
					prep.addBatch();
					if (batch++ > BATCH_SIZE) {
						prep.executeBatch();
						batch = 0;
					}
				} catch (SQLException e) {
					System.err.println(e.getMessage());
				}
			}
		};
		for (File f : files) {
			lang[0] = f.getName().replace("wikivoyage-latest-langlinks.sql.gz", "");
			SqlInsertValuesReader.readInsertValuesFile(f.getAbsolutePath(), p);
			ids.addAll(currentFileIds);
			currentFileIds.clear();
			currMapping.clear();
		}
		prep.addBatch();
		prep.executeBatch();
		prep.close();
		conn.createStatement().execute("DROP INDEX IF EXISTS index_orig_id;");
		articleQuery.close();
		conn.close();
	}

	public void finishPrep(PreparedStatement ps) throws SQLException {
		ps.addBatch();
		ps.executeBatch();
		ps.close();
	}

	private String getAggregatedPartOf(Connection conn, String partOf, String lang) throws SQLException {
		if (partOf.isEmpty()) {
			return "";
		}
		StringBuilder res = new StringBuilder();
		res.append(partOf);
		res.append(",");
		PreparedStatement ps = conn
				.prepareStatement("SELECT is_part_of FROM travel_articles WHERE title = ? AND lang = '" + lang + "'");
		String prev = "";
		while (true) {
			ps.setString(1, partOf);
			ResultSet rs = ps.executeQuery();
			String buf = "";
			while (rs.next()) {
				buf = rs.getString(1);
			}
			if (buf.equals("") || buf.equals(partOf) || buf.equals(prev)) {
				ps.close();
				rs.close();
				return res.toString().substring(0, res.length() - 1);
			} else {
				rs.close();
				ps.clearParameters();
				res.append(buf);
				res.append(',');
				prev = partOf;
				partOf = buf;
			}
		}
	}

	private void generateIdsIfMissing(Connection conn, File langlinkfile) throws SQLException {
		long maxId = 0;
		DBDialect dialect = DBDialect.SQLITE;
		Connection langConn = (Connection) dialect.getDatabaseConnection(langlinkfile.getAbsolutePath(), log);
		Statement st = langConn.createStatement();
		ResultSet rs = st.executeQuery("SELECT MAX(id) FROM langlinks");
		if (rs.next()) {
			maxId = rs.getLong(1) + 1;
		}
		st.close();
		rs.close();

		langConn.close();
		if (maxId == 0) {
			System.err.println("MAX ID is 0");
			throw new IllegalStateException();
		}
		int batch = 0;
		Statement ps = conn.createStatement();
		PreparedStatement prep = conn
				.prepareStatement("UPDATE travel_articles SET trip_id = ? WHERE title = ? AND lang = ?");
		ResultSet res = ps.executeQuery("SELECT title, lang FROM travel_articles WHERE trip_id = 0");
		int updated = 0;
		while (res.next()) {
			updated++;
			String title = res.getString("title");
			String lang = res.getString("lang");
			prep.setLong(1, maxId++);
			prep.setString(2, title);
			prep.setString(3, lang);
			prep.addBatch();
			if (batch++ > 500) {
				prep.executeBatch();
				batch = 0;
			}
		}
		prep.addBatch();
		prep.executeBatch();
		prep.close();
		res.close();
		ps.close();

		System.out.println("Updated " + updated + " trip_id with max id " + maxId);
		Statement st2 = conn.createStatement();
		rs = st2.executeQuery("SELECT count(*) FROM travel_articles WHERE trip_id = 0");
		if (rs.next()) {
			System.out.println("Count travel articles with empty trip_id: " + rs.getInt(1));

		}
		rs.close();
		st2.close();

	}
	

	


	private void addCitiesData(File citiesObf, Connection conn) throws FileNotFoundException, IOException,
			SQLException {
		addColumns(conn);
		if (citiesObf != null) {
			RandomAccessFile raf = new RandomAccessFile(citiesObf, "r");
			BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, citiesObf);
			PreparedStatement stat = conn.prepareStatement("SELECT title, lat, lon FROM travel_articles");
			int columns = getRowCount(conn);
			int count = 0;
			ResultSet rs = stat.executeQuery();
			Map<String, List<Amenity>> cities = fetchCities(reader);
			while (rs.next()) {
				String title = rs.getString(1);
				String searchTitle = title.replaceAll("\\(.*\\)", "").trim();
				long lat = rs.getLong(2);
				long lon = rs.getLong(3);
				LatLon ll = new LatLon(lat, lon);
				if (lat == 0 && lon == 0) {
					continue;
				}
				List<Amenity> results = cities.get(searchTitle);
				Amenity acceptedResult = (results != null && results.size() == 1) ? results.get(0) : getClosestMatch(
						results, ll);
				insertData(conn, title, acceptedResult, ll);
				if (count++ % BATCH_SIZE == 0) {
					System.out.format("%.2f", (((double) count / (double) columns) * 100d));
					System.out.println("%");
				}
			}
			stat.close();
			raf.close();
		}
	}
	
	private TreeSet<String> readMostPopularArticlesFromWikivoyage(TreeSet<String> langs, int limit) throws IOException, JSONException {
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
	

	private void createPopularArticlesTable(Connection conn) throws SQLException, IOException, JSONException {
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

	private void insertData(Connection conn, String title, Amenity acceptedResult, LatLon fromDB)
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
			population = population == null ? "" : population.replaceAll("[-,. \\)\\(]", "");
			updateStatement.setLong(column++, (population.isEmpty() || !population.matches("[0-9]+")) ? 0 : Long.parseLong(population));
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

	private Map<String, List<Amenity>> fetchCities(BinaryMapIndexReader reader) throws IOException {
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

	private int getRowCount(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("SELECT Count(*) FROM travel_articles");
		int columns = 0;
		ResultSet res = ps.executeQuery();
		while (res.next()) {
			columns = res.getInt(1);
		}
		return columns;
	}

	private void addColumns(Connection conn) {
		for (String s : columns) {
			try {
				conn.createStatement().execute("ALTER TABLE travel_articles ADD COLUMN " + s);
			} catch (SQLException ex) {
				// ex.printStackTrace();
				System.out.println("Column alredy exsists");
			}
		}
	}

	private Amenity getClosestMatch(List<Amenity> results, LatLon fromDB) {
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
	
	private List<String> getRegions(double lat, double lon) throws IOException {
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
