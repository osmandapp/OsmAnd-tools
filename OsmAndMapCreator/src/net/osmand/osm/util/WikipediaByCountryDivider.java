package net.osmand.osm.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import net.osmand.IndexConstants;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.MapZooms;
import net.osmand.data.preparation.DBDialect;
import net.osmand.data.preparation.IndexCreator;
import net.osmand.data.preparation.IndexPoiCreator;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.osmand.util.sql.SqlInsertValuesReader;
import net.osmand.util.sql.SqlInsertValuesReader.InsertValueProcessor;

import org.apache.commons.logging.Log;
import org.apache.tools.bzip2.CBZip2OutputStream;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlSerializer;

import rtree.RTree;

public class WikipediaByCountryDivider {
	private static final Log log = PlatformUtil.getLog(WikipediaByCountryDivider.class);
	private static final long BATCH_SIZE = 3500;

	private static class LanguageSqliteFile {
		private Connection conn;
		private ResultSet rs;

		public LanguageSqliteFile(String fileName) throws SQLException {
			conn = (Connection) DBDialect.SQLITE.getDatabaseConnection(fileName, log);
//			conn.createStatement().execute("CREATE INDEX IF NOT EXISTS ID_INDEX ON WIKI(ID)");
		}

		public long getId() throws SQLException {
			return rs.getLong(1);
		}

		public double getLat() throws SQLException {
			return rs.getDouble(2);
		}

		public double getLon() throws SQLException {
			return rs.getDouble(3);
		}

		public String getTitle() throws SQLException {
			return rs.getString(4);
		}

		public byte[] getContent() throws SQLException {
			return rs.getBytes(5);
		}

		public boolean next() throws SQLException {
			if(rs == null) {
				rs = conn.createStatement().executeQuery("SELECT id, lat, lon, title, zipContent FROM wiki");
			}
			return rs.next();
		}

		public void closeConnnection() throws SQLException {
			conn.close();
		}
	}

	private static class GlobalWikiStructure {


		private PreparedStatement getWikidataById;
		private PreparedStatement insertWikiContent;
		private PreparedStatement insertWikidata;
		private Map<PreparedStatement, Long> statements = new LinkedHashMap<PreparedStatement, Long>();
		private Connection c;
		private OsmandRegions regions;
		private List<String> keyNames = new ArrayList<String>();

		

		public GlobalWikiStructure(String fileName, OsmandRegions regions, boolean regenerate) throws SQLException {
			this.regions = regions;
			if(regenerate) {
				File fl = new File(fileName);
				fl.delete();
			}
			c = (Connection) DBDialect.SQLITE.getDatabaseConnection(fileName, log);
			if(regenerate) {
				prepareToInsert();
			}
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

		public void prepareToInsert() throws SQLException {
			createTables();
			prepareStatetements();
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

		public void prepareStatetements() throws SQLException {
			getWikidataById = c.prepareStatement("SELECT wikidata FROM wikidata WHERE lang = ? and id = ? ");
			insertWikiContent = c.prepareStatement("INSERT INTO wiki_content VALUES(?, ?, ?, ?, ?, ?, ?)");
			insertWikidata = c.prepareStatement("INSERT INTO wikidata(lang, id, wikidata) VALUES(?, ?, ?)");
		}

		public String getIdForArticle(String lang, long wikiId) throws SQLException {
			getWikidataById.setString(1, lang);
			getWikidataById.setLong(2, wikiId);
			ResultSet rs = getWikidataById.executeQuery();
			if(rs.next()) {
				return rs.getString(1);
			}
			return null;
		}

		public void insertWikiTranslation(String lang, long id, String wikidata) throws SQLException {
			insertWikidata.setString(1, lang);
			insertWikidata.setLong(2, id);
			insertWikidata.setString(3, wikidata);
			addBatch(insertWikidata);
		}

		public void commitTranslationInsert() throws SQLException {
			insertWikidata.executeBatch();
		}

		public void insertArticle(long id, double lat, double lon, String lang, long wikiId, String title, byte[] zipContent)
				throws SQLException, IOException {
			insertWikiContent.setLong(1, id);
			insertWikiContent.setDouble(2, lat);
			insertWikiContent.setDouble(3, lon);
			insertWikiContent.setString(4, lang);
			insertWikiContent.setLong(5, wikiId);
			insertWikiContent.setString(6, title);
			insertWikiContent.setBytes(7, zipContent);
			addBatch(insertWikiContent);

		}

		public void createTables() throws SQLException {
			c.createStatement()
					.execute(
							"CREATE TABLE wiki_content(id long, lat double, lon double, lang text, wikiId long, title text, zipContent blob)");
			c.createStatement().execute("CREATE TABLE wiki_region(id long, regionName text)");
			c.createStatement().execute("CREATE TABLE wikidata(lang text, id long, wikidata text)");
		}

		public void createIndexes() throws SQLException {
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS WIKIID_INDEX ON wiki_content(lang, wikiId)");
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS CONTENTID_INDEX ON wiki_content(ID)");
			
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS TRANS_INDEX ON wikidata(lang,id)");
			
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS REGIONID_INDEX ON wiki_region(ID)");
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS REGIONNAME_INDEX ON wiki_region(regionName)");
		}


	}

	public static void main(String[] args) throws IOException, SQLException, InterruptedException, XmlPullParserException {
//		String folder = "/home/victor/projects/osmand/wiki/";
//		String regionsFile = "/home/victor/projects/osmand/repo/resources/countries-info/regions.ocbf";
//		String cmd = "regenerate";
		String cmd = args[0];
		String regionsFile = args[1];
		String folder = args[2];
		boolean skip = false;
		if (args.length > 3) {
			skip = args[3].equals("-skip-existing");
		}
		if(cmd.equals("inspect")) {
			inspectWikiFile(folder);
		} else if(cmd.equals("update_countries")) {
			updateCountries(folder, regionsFile);
		} else if(cmd.equals("generate_country_sqlite")) {
			generateCountrySqlite(folder, skip);
		} else if(cmd.equals("regenerate")) {
			generateGlobalWikiFile(folder, regionsFile);
		}
	}

	protected static void updateCountries(String folder, String regionsFile) throws IOException, SQLException {
		OsmandRegions regions = new OsmandRegions();
		regions.prepareFile(regionsFile);
		regions.cacheAllCountries();
		final GlobalWikiStructure wikiStructure = new GlobalWikiStructure(folder + "wiki.sqlite", regions, false);
		wikiStructure.updateRegions();
		wikiStructure.closeConnnection();
		System.out.println("Generation finished");
	}



	protected static void generateCountrySqlite(String folder, boolean skip) throws SQLException, IOException, InterruptedException, XmlPullParserException {
		Connection conn = (Connection) DBDialect.SQLITE.getDatabaseConnection(folder + "wiki.sqlite", log);
		OsmandRegions regs = new OsmandRegions();
		regs.prepareFile(new File("resources/countries-info/regions.ocbf").getAbsolutePath());
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
			File fl = new File(rgns, regionName + ".sqlite");
			File osmBz2 = new File(rgns, regionName + "_" + IndexConstants.BINARY_MAP_VERSION + ".wiki.osm.bz2");
			File obfFile = new File(rgns, regionName + "_" + IndexConstants.BINARY_MAP_VERSION + ".wiki.obf");
			if(obfFile.exists() && skip) {
				continue;
			}

			fl.delete();
			osmBz2.delete();
			obfFile.delete();
			System.out.println("Generate " +fl.getName());
			Connection loc = (Connection) DBDialect.SQLITE.getDatabaseConnection(fl.getAbsolutePath(), log);
			loc.createStatement()
					.execute(
							"CREATE TABLE wiki_content(id long, lat double, lon double, lang text, wikiId long, title text, zipContent blob)");
			PreparedStatement insertWikiContentCountry = loc
					.prepareStatement("INSERT INTO wiki_content VALUES(?, ?, ?, ?, ?, ?, ?)");
			ResultSet rps = conn.createStatement().executeQuery(
					"SELECT WC.id, WC.lat, WC.lon, WC.lang, WC.wikiId, WC.title, WC.zipContent "
							+ " FROM wiki_content WC INNER JOIN wiki_region WR "
							+ " ON WC.id = WR.id AND WR.regionName = '" + rs.getString(1) + "' ORDER BY WC.id");

			FileOutputStream out = new FileOutputStream(osmBz2);
			out.write('B');
			out.write('Z');
			CBZip2OutputStream bzipStream = new CBZip2OutputStream(out);
			XmlSerializer serializer = new org.kxml2.io.KXmlSerializer();
			serializer.setOutput(bzipStream, "UTF-8");
			serializer.startDocument("UTF-8", true);
			serializer.startTag(null, "osm");
			serializer.attribute(null, "version", "0.6");
			serializer.attribute(null, "generator", "OsmAnd");
			serializer.setFeature(
					"http://xmlpull.org/v1/doc/features.html#indent-output", true);
			// indentation as 3 spaces
//			serializer.setProperty(
//			   "http://xmlpull.org/v1/doc/properties.html#serializer-indentation", "   ");
//			// also set the line separator
//			serializer.setProperty(
//			   "http://xmlpull.org/v1/doc/properties.html#serializer-line-separator", "\n");
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
				long wikiId = rps.getLong(5);
				String wikiLang = rps.getString(4);
				String title = rps.getString(6);
				byte[] bytes = rps.getBytes(7);
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
				if(osmId != prevOsmId) {
					if(prevOsmId != -1) {
						closeOsmWikiNode(serializer, nameUnique, nameAdded);
					}
					prevOsmId = osmId;
					nameAdded = false;
					nameUnique = null;
					preferredAdded = false;
					serializer.startTag(null, "node");
					serializer.attribute(null, "visible", "true");
					serializer.attribute(null, "id", (osmId)+"");
					serializer.attribute(null, "lat", lat+"");
					serializer.attribute(null, "lon", lon+"");

				}
				if(wikiLang.equals("en")) {
					nameAdded = true;
					addTag(serializer, "name", title);
					addTag(serializer, "wiki_id", wikiId +"");
					addTag(serializer, "content", contentStr);
					addTag(serializer, "wiki_lang:en", "yes");
				} else {
					addTag(serializer, "name:"+wikiLang, title);
					addTag(serializer, "wiki_id:"+wikiLang, wikiId +"");
					addTag(serializer, "wiki_lang:"+wikiLang, "yes");
					if(!preferredAdded) {
						nameUnique = title;
						preferredAdded = preferredLang.contains(wikiLang);
					}
					addTag(serializer, "content:"+wikiLang, contentStr);
				}
			}
			if(prevOsmId != -1) {
				closeOsmWikiNode(serializer, nameUnique, nameAdded);
			}
			insertWikiContentCountry.executeBatch();
			loc.close();
			serializer.endDocument();
			serializer.flush();
			bzipStream.close();
			System.out.println("Processed " + cnt + " pois");
			generateObf(osmBz2, obfFile);
		}
		conn.close();
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
		IndexPoiCreator.ZIP_LONG_STRINGS = true;
		// be independent of previous results
		RTree.clearCache();
		IndexCreator creator = new IndexCreator(obf.getParentFile()); //$NON-NLS-1$
		new File(obf.getParentFile(), IndexCreator.TEMP_NODES_DB).delete();
		creator.setIndexMap(false);
		creator.setIndexAddress(false);
		creator.setIndexPOI(true);
		creator.setIndexTransport(false);
		creator.setIndexRouting(false);
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
		Connection conn = (Connection) DBDialect.SQLITE.getDatabaseConnection(folder + "wiki.sqlite", log);
		ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) cnt, regionName  FROM wiki_region GROUP BY regionName ORDER BY cnt desc");
		System.out.println("POI by countries");
		while(rs.next()) {
			System.out.println(rs.getString(1) + " " + rs.getString(2));
		}
//		rs = conn.createStatement().executeQuery("SELECT COUNT(distinct id) FROM wiki_content");
//		rs.next();
//		System.out.println("POI in total: " + rs.getLong(1)) ;
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

	protected static void generateGlobalWikiFile(String folder, String regionsFile) throws IOException, SQLException,
			FileNotFoundException, UnsupportedEncodingException {
		OsmandRegions regions = new OsmandRegions();
		regions.prepareFile(regionsFile);
		regions.cacheAllCountries();
		final GlobalWikiStructure wikiStructure = new GlobalWikiStructure(folder + "wiki.sqlite", regions, true);
		File fl = new File(folder);
		List<String> langs = new ArrayList<String>();
		for (File f : fl.listFiles()) {
			if (f.getName().endsWith("wiki.sqlite") && f.getName().length() > "wiki.sqlite".length()) {
				langs.add(f.getName().substring(0, f.getName().length() - "wiki.sqlite".length()));
			}
		}
		// insert translation
		for (String lang : langs) {
			String langLinks = folder + lang + "wiki-latest-page_props.sql.gz";
			System.out.println("Insert wikidata " + lang + " " + new Date());
			insertWikidata(wikiStructure, lang, langLinks);
		}
		wikiStructure.createIndexes();
		for (String lang : langs) {
			processLang(lang, folder, wikiStructure);
		}
		wikiStructure.closeConnnection();
		System.out.println("Generation finished");
	}

	protected static void processLang(String lang, String folder, final GlobalWikiStructure wikiStructure)
			throws SQLException, IOException, FileNotFoundException, UnsupportedEncodingException {
		System.out.println("Copy articles for " + lang + " " + new Date());
		LanguageSqliteFile ifl = new LanguageSqliteFile(folder + lang + "wiki.sqlite");
		while (ifl.next()) {
			final long wikiId = ifl.getId();
			String id = wikiStructure.getIdForArticle(lang, wikiId);
			if(id == null) {
				System.err.println("ERROR: Skip article " + lang + " " + ifl.getTitle() + " no wikidata id" ) ;
			} else {
				try {
					wikiStructure.insertArticle(Long.parseLong(id.substring(1)), ifl.getLat(), ifl.getLon(), lang, wikiId, ifl.getTitle(),
						ifl.getContent());
				} catch (Exception e) {
					System.err.println("ERROR: Skip article " + lang + " " + ifl.getTitle() + " wrong wikidata id: " +  id) ;
				}
			}
		}
		ifl.closeConnnection();
		
	}

	protected static void insertWikidata(final GlobalWikiStructure wikiStructure, final String lang,
			String langLinks) throws IOException, SQLException {

		SqlInsertValuesReader.readInsertValuesFile(langLinks, new InsertValueProcessor() {
			@Override
			public void process(List<String> vs) {
				try {
					if(!vs.get(1).equals("wikibase_item")) {
						return;
					}
					final long wikiId = Long.parseLong(vs.get(0));
					wikiStructure.insertWikiTranslation(lang, wikiId, vs.get(2));
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
		});
		wikiStructure.commitTranslationInsert();
	}


	
}
