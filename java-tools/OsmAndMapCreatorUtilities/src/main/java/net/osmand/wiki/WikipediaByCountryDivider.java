package net.osmand.wiki;

import net.osmand.IndexConstants;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.MapZooms;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlSerializer;
import rtree.RTree;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class WikipediaByCountryDivider {
	private static final Log log = PlatformUtil.getLog(WikipediaByCountryDivider.class);
	private static final long BATCH_SIZE = 3500;

	private static class GlobalWikiStructure {

		private Map<PreparedStatement, Long> statements = new LinkedHashMap<PreparedStatement, Long>();
		private Connection c;
		private OsmandRegions regions;
		private List<String> keyNames = new ArrayList<String>();

		

		public GlobalWikiStructure(String fileName, OsmandRegions regions) throws SQLException {
			this.regions = regions;
			c = DBDialect.SQLITE.getDatabaseConnection(fileName, log);
			createTables();
			createIndexes();
		}

		public void updateRegions() throws SQLException, IOException {
			c.createStatement().execute("DELETE FROM wiki_region");
			c.createStatement().execute("VACUUM");
			PreparedStatement insertWikiRegion = c.prepareStatement("INSERT INTO wiki_region VALUES(?, ? )");
			ResultSet rs = c.createStatement().executeQuery("SELECT id, lat, lon from wiki_content order by id");
			long pid = -1;
			while(rs.next()) {
				long id = rs.getLong(1);
				if(id != pid) {
					pid = id;
					List<String> rgs = getRegions(rs.getDouble(2), rs.getDouble(3));
					for (String reg : rgs) {
						insertWikiRegion.setLong(1, id);
						insertWikiRegion.setString(2, reg);
						insertWikiRegion.setString(2, reg);
						addBatch(insertWikiRegion);
					}
				}
			}

		}


		public void addBatch(PreparedStatement p) throws SQLException {
			p.addBatch();
			Long l = statements.get(p);
			if (l == null) {
				l = new Long(0);
			}
			long lt = l + 1;
			if (lt > BATCH_SIZE) {
				p.executeBatch();
				lt = 0;
			}
			statements.put(p, lt);

		}

		private List<String> getRegions(double lat, double lon) throws IOException {
			keyNames.clear();
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

		public void closeConnnection() throws SQLException {
			for(PreparedStatement s : statements.keySet()) {
				s.executeBatch();
				s.close();
			}
			c.close();
		}

		public void createTables() throws SQLException {
			c.createStatement().execute("CREATE TABLE wiki_region(id long, regionName text)");
		}

		public void createIndexes() throws SQLException {
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS CONTENTID_INDEX ON wiki_content(id)");
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS REGIONID_INDEX ON wiki_region(id)");
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS REGIONNAME_INDEX ON wiki_region(regionName)");
		}
	}

	public static void main(String[] args) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		String cmd = args[0];
		String folder = args[1];
		boolean skip = false;
		if (args.length > 2) {
			skip = args[2].equals("--skip-existing");
		}
		if(cmd.equals("inspect")) {
			inspectWikiFile(folder);
		} else if(cmd.equals("update_countries")) {
			updateCountries(folder);
		} else if(cmd.equals("generate_country_sqlite")) {
			generateCountrySqlite(folder, skip);
		}
	}

	protected static void updateCountries(String folder) throws IOException, SQLException {
		OsmandRegions regions = new OsmandRegions();
		regions.prepareFile();
		regions.cacheAllCountries();
		final GlobalWikiStructure wikiStructure = new GlobalWikiStructure(folder + "wiki.sqlite", regions);
		wikiStructure.updateRegions();
		wikiStructure.closeConnnection();
		System.out.println("Generation finished");
	}



	protected static void generateCountrySqlite(String folder, boolean skip) throws SQLException, IOException, InterruptedException, XmlPullParserException {
		Connection conn = (Connection) DBDialect.SQLITE.getDatabaseConnection(folder + "wiki.sqlite", log);
		OsmandRegions regs = new OsmandRegions();
		regs.prepareFile();
		Map<String, LinkedList<BinaryMapDataObject>> mapObjects = regs.cacheAllCountries();
		File rgns = new File(folder, "regions");
		rgns.mkdirs();
		Map<String, String> preferredRegionLanguages = new LinkedHashMap<>();
		for(String key : mapObjects.keySet()) {
			if(key == null) {
				continue;
			}
			WorldRegion wr = regs.getRegionDataByDownloadName(key);
			if(wr == null) {
				System.out.println("Missing language for world region '" + key + "'!");
			} else {
				String regionLang = wr.getParams().getRegionLang();
				preferredRegionLanguages.put(key.toLowerCase(), regionLang);
			}
		}
		
		ResultSet rs = conn.createStatement().executeQuery("SELECT DISTINCT regionName  FROM wiki_region");
		while (rs.next()) {
			String lcRegionName = rs.getString(1);
			if(lcRegionName == null) {
				continue;
			}
			String regionName = Algorithms.capitalizeFirstLetterAndLowercase(lcRegionName);
			String preferredLang = preferredRegionLanguages.get(lcRegionName);
			if(preferredLang == null) {
				preferredLang = "";
			}
			LinkedList<BinaryMapDataObject> list = mapObjects.get(lcRegionName.toLowerCase());
			boolean hasWiki = false;
			if(list != null) {
				for(BinaryMapDataObject o : list) {
					Integer rl = o.getMapIndex().getRule("region_wiki", "yes");
					if(o.containsAdditionalType(rl)) {
						hasWiki = true;
						break;
					}
				}
			}
			if(!hasWiki) {
				System.out.println("Skip " + lcRegionName.toLowerCase() + " doesn't generate wiki");
				continue;
			}
			File sqliteFile = new File(rgns, regionName + ".sqlite");
			File osmBz2 = new File(rgns, regionName + "_" + IndexConstants.BINARY_MAP_VERSION + ".wiki.osm.bz2");
			File obfFile = new File(rgns, regionName + "_" + IndexConstants.BINARY_MAP_VERSION + ".wiki.obf");
			if(obfFile.exists() && skip) {
				continue;
			}
			if(!skip) {
				sqliteFile.delete();
				osmBz2.delete();
			}
			if(!osmBz2.exists() || !sqliteFile.exists()) {
				System.out.println("Generate " +sqliteFile.getName());
				generateOsmFileAndLocalSqlite(conn, rs, preferredLang, sqliteFile, osmBz2);
			}
			
			obfFile.delete();
			generateObf(osmBz2, obfFile);
		}
		conn.close();
	}

	private static void generateOsmFileAndLocalSqlite(Connection conn, ResultSet rs, String preferredLang,
			File sqliteFile, File osmBz2) throws SQLException, FileNotFoundException, IOException {
		Connection loc = (Connection) DBDialect.SQLITE.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
		loc.createStatement()
				.execute(
						"CREATE TABLE wiki_content(id long, lat double, lon double, lang text, wikiId long, title text, zipContent blob)");
		PreparedStatement insertWikiContentCountry = loc
				.prepareStatement("INSERT INTO wiki_content VALUES(?, ?, ?, ?, ?, ?, ?)");
		ResultSet rps = conn.createStatement().executeQuery(
				"SELECT WC.id, WC.lat, WC.lon, WC.lang, WC.title, WC.zipContent "
						+ " FROM wiki_content WC INNER JOIN wiki_region WR "
						+ " ON WC.id = WR.id AND WR.regionName = '" + rs.getString(1) + "' ORDER BY WC.id");

		FileOutputStream out = new FileOutputStream(osmBz2);
		BZip2CompressorOutputStream bzipStream = new BZip2CompressorOutputStream(out);
		XmlSerializer serializer = new org.kxml2.io.KXmlSerializer();
		serializer.setOutput(bzipStream, "UTF-8");
		serializer.startDocument("UTF-8", true);
		serializer.startTag(null, "osm");
		serializer.attribute(null, "version", "0.6");
		serializer.attribute(null, "generator", "OsmAnd");
		serializer.setFeature("http://xmlpull.org/v1/doc/features.html#indent-output", true);
		// indentation as 3 spaces
		// serializer.setProperty(
		// "http://xmlpull.org/v1/doc/properties.html#serializer-indentation", "   ");
		// // also set the line separator
		// serializer.setProperty(
		// "http://xmlpull.org/v1/doc/properties.html#serializer-line-separator", "\n");
		int cnt = 1;
		long prevOsmId = -1;
		StringBuilder content = new StringBuilder();
		String nameUnique = null;
		boolean preferredAdded = false;
		boolean nameAdded = false;
		while (rps.next()) {
			long osmId = -rps.getLong(1);
			double lat = rps.getDouble(2);
			double lon = rps.getDouble(3);
			long wikiId = rps.getLong(1);
			String wikiLang = rps.getString(4);
			String title = rps.getString(5);
			byte[] bytes = rps.getBytes(6);
			GZIPInputStream gzin = new GZIPInputStream(new ByteArrayInputStream(bytes));
			BufferedReader br = new BufferedReader(new InputStreamReader(gzin));
			content.setLength(0);
			String s;
			while ((s = br.readLine()) != null) {
				content.append(s);
			}
			String contentStr = content.toString();
			contentStr = contentStr.replace((char) 9, ' ');
			contentStr = contentStr.replace((char) 0, ' ');
			contentStr = contentStr.replace((char) 22, ' ');
			contentStr = contentStr.replace((char) 27, ' ');
			insertWikiContentCountry.setLong(1, osmId);
			insertWikiContentCountry.setDouble(2, lat);
			insertWikiContentCountry.setDouble(3, lon);
			insertWikiContentCountry.setString(4, wikiLang);
			insertWikiContentCountry.setLong(5, wikiId);
			insertWikiContentCountry.setString(6, title);
			insertWikiContentCountry.setBytes(7, bytes);
			insertWikiContentCountry.addBatch();
			if (cnt++ % BATCH_SIZE == 0) {
				insertWikiContentCountry.executeBatch();
			}
			if (osmId != prevOsmId) {
				if (prevOsmId != -1) {
					closeOsmWikiNode(serializer, nameUnique, nameAdded);
				}
				prevOsmId = osmId;
				nameAdded = false;
				nameUnique = null;
				preferredAdded = false;
				serializer.startTag(null, "node");
				serializer.attribute(null, "visible", "true");
				serializer.attribute(null, "id", (osmId) + "");
				serializer.attribute(null, "lat", lat + "");
				serializer.attribute(null, "lon", lon + "");

			}
			if (wikiLang.equals("en")) {
				nameAdded = true;
				addTag(serializer, "name", title);
				addTag(serializer, "wiki_id", wikiId + "");
				addTag(serializer, "content", contentStr);
				addTag(serializer, "wiki_lang:en", "yes");
			} else {
				addTag(serializer, "name:" + wikiLang, title);
				addTag(serializer, "wiki_id:" + wikiLang, wikiId + "");
				addTag(serializer, "wiki_lang:" + wikiLang, "yes");
				if (!preferredAdded) {
					nameUnique = title;
					preferredAdded = preferredLang.contains(wikiLang);
				}
				addTag(serializer, "content:" + wikiLang, contentStr);
			}
		}
		if (prevOsmId != -1) {
			closeOsmWikiNode(serializer, nameUnique, nameAdded);
		}
		insertWikiContentCountry.executeBatch();
		loc.close();
		serializer.endDocument();
		serializer.flush();
		bzipStream.close();
		System.out.println("Processed " + cnt + " pois");
	}

	private static void closeOsmWikiNode(XmlSerializer serializer, String nameUnique, boolean nameAdded)
			throws IOException {
		if(!nameAdded && nameUnique != null) {
			addTag(serializer, "name", nameUnique);
		}
		if(nameAdded || nameUnique != null) {
			addTag(serializer, "osmwiki", "wiki_place");
		}
		serializer.endTag(null, "node");
	}

	private static void generateObf(File osmBz2, File obf) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		// be independent of previous results
		RTree.clearCache();
		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexMap = false;
		settings.indexAddress = false;
		settings.indexPOI = true;
		settings.indexTransport = false;
		settings.indexRouting = false;
		settings.poiZipLongStrings = true;
		
		IndexCreator creator = new IndexCreator(obf.getParentFile(), settings); //$NON-NLS-1$
		new File(obf.getParentFile(), IndexCreator.TEMP_NODES_DB).delete();
		creator.setMapFileName(obf.getName());
		creator.generateIndexes(osmBz2,
				new ConsoleProgressImplementation(1), null, MapZooms.getDefault(),
				new MapRenderingTypesEncoder(obf.getName()), log);


	}

	private static void addTag(XmlSerializer serializer, String key, String value) throws IOException {
		serializer.startTag(null, "tag");
		serializer.attribute(null, "k", key);
		serializer.attribute(null, "v", value);
		serializer.endTag(null, "tag");
	}

	protected static void inspectWikiFile(String folder) throws SQLException {
		Connection conn = DBDialect.SQLITE.getDatabaseConnection(folder + "wiki.sqlite", log);
		ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) cnt, regionName  FROM wiki_region GROUP BY regionName ORDER BY cnt desc");
		System.out.println("POI by countries");
		while(rs.next()) {
			System.out.println(rs.getString(1) + " " + rs.getString(2));
		}
		System.out.println("POI by languages: ");
		rs = conn.createStatement().executeQuery("SELECT COUNT(*) cnt, lang  FROM wiki_content GROUP BY lang ORDER BY cnt desc");
		while(rs.next()) {
			System.out.println(rs.getString(1) + " " + rs.getString(2));
		}
		System.out.println();
		rs = conn.createStatement().executeQuery("SELECT id, title, lang  FROM wiki_content ORDER BY id ");
		int maxcnt = 100;
		while(rs.next() && maxcnt-- > 0) {
			System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
		}
	}

}
