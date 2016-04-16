package net.osmand.osm.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import net.osmand.IndexConstants;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.data.preparation.DBDialect;
import net.osmand.data.preparation.IndexCreator;
import net.osmand.data.preparation.IndexPoiCreator;
import net.osmand.data.preparation.MapZooms;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.regions.CountryOcbfGeneration;
import net.osmand.regions.CountryOcbfGeneration.CountryRegion;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.tools.bzip2.CBZip2OutputStream;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlSerializer;

import rtree.RTree;

public class WikipediaByCountryDivider {
	private static final Log log = PlatformUtil.getLog(WikipediaByCountryDivider.class);
	private static final long BATCH_SIZE = 500;

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

		private long idGen = 100;

		private PreparedStatement getIdByTitle;
		private PreparedStatement insertWikiContent;
		private PreparedStatement insertWikiTranslation;
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
			c.createStatement().execute("DELETE FROM wiki_translation"); // not needed any more
			c.createStatement().execute("VACUUM");
			PreparedStatement insertWikiRegion = c.prepareStatement("INSERT INTO wiki_region VALUES(?, ?)");
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
			List<BinaryMapDataObject> cs = regions.query(MapUtils.get31TileNumberX(lon), MapUtils.get31TileNumberY(lat));
			for (BinaryMapDataObject b : cs) {
				keyNames.add(regions.getDownloadName(b));
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
			getIdByTitle = c.prepareStatement("SELECT ID FROM wiki_translation WHERE lang = ? and title = ? ");
			insertWikiContent = c.prepareStatement("INSERT INTO wiki_content VALUES(?, ?, ?, ?, ?, ?, ?)");
			insertWikiTranslation = c.prepareStatement("INSERT INTO wiki_translation VALUES(?, ?, ?)");
		}

		public long getIdForArticle(String lang, String title) throws SQLException {
			getIdByTitle.setString(1, lang);
			getIdByTitle.setString(2, title);
			ResultSet rs = getIdByTitle.executeQuery();
			if(rs.next()) {
				return rs.getLong(1);
			}
			return -1;
		}

		public void insertWikiTranslation(long id, String lang, String title) throws SQLException {
			insertWikiTranslation.setLong(1, id);
			insertWikiTranslation.setString(2, lang);
			insertWikiTranslation.setString(3, title);
			addBatch(insertWikiTranslation);
		}

		public void commitTranslationInsert() throws SQLException {
			insertWikiTranslation.executeBatch();
		}

		public long insertArticle(double lat, double lon, String lang, long wikiId, String title, byte[] zipContent)
				throws SQLException, IOException {
			long id = getIdForArticle(lang, title);
			boolean genId = id < 0;
			if (genId) {
				id = idGen++;
			}
			insertWikiContent.setLong(1, id);
			insertWikiContent.setDouble(2, lat);
			insertWikiContent.setDouble(3, lon);
			insertWikiContent.setString(4, lang);
			insertWikiContent.setLong(5, wikiId);
			insertWikiContent.setString(6, title);
			insertWikiContent.setBytes(7, zipContent);
			addBatch(insertWikiContent);
			return id;

		}

		public void createTables() throws SQLException {
			c.createStatement()
					.execute(
							"CREATE TABLE wiki_content(id long, lat double, lon double, lang text, wikiId long, title text, zipContent blob)");
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS WIKIID_INDEX ON wiki_content(lang, wikiId)");
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS CONTENTID_INDEX ON wiki_content(ID)");

			c.createStatement().execute("CREATE TABLE wiki_region(id long, regionName text)");
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS REGIONID_INDEX ON wiki_region(ID)");
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS REGIONNAME_INDEX ON wiki_region(regionName)");

			c.createStatement().execute("CREATE TABLE wiki_translation(id long, lang text, title text)");
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS TRANSID_INDEX ON wiki_translation(ID)");
			c.createStatement().execute("CREATE INDEX IF NOT EXISTS TRANS_INDEX ON wiki_translation(lang,title)");
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
		ResultSet rs = conn.createStatement().executeQuery("SELECT DISTINCT regionName  FROM wiki_region");
		while (rs.next()) {
			String lcRegionName = rs.getString(1);
			if(lcRegionName == null) {
				continue;
			}
			String regionName = Algorithms.capitalizeFirstLetterAndLowercase(lcRegionName);
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
			PreparedStatement insertWikiContent = loc
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
				insertWikiContent.setLong(1, osmId);
				insertWikiContent.setDouble(2, lat);
				insertWikiContent.setDouble(3, lon);
				insertWikiContent.setString(4, wikiLang);
				insertWikiContent.setLong(5, wikiId);
				insertWikiContent.setString(6, title);
				insertWikiContent.setBytes(7, bytes);
				insertWikiContent.addBatch();
				if (cnt++ % BATCH_SIZE == 0) {
					insertWikiContent.executeBatch();
				}
				if(osmId != prevOsmId) {
					if(prevOsmId != -1) {
						closeOsmWikiNode(serializer, nameUnique, nameAdded);
					}
					prevOsmId = osmId;
					nameAdded = false;
					nameUnique = null;
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
					nameUnique = title;
					addTag(serializer, "content:"+wikiLang, contentStr);
				}
			}
			if(prevOsmId != -1) {
				closeOsmWikiNode(serializer, nameUnique, nameAdded);
			}
			insertWikiContent.executeBatch();
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
		for (String lang : langs) {
			processLang(lang, folder, wikiStructure);
		}
		wikiStructure.closeConnnection();
		System.out.println("Generation finished");
	}

	protected static void processLang(String lang, String folder, final GlobalWikiStructure wikiStructure)
			throws SQLException, IOException, FileNotFoundException, UnsupportedEncodingException {
		System.out.println("Copy articles for " + lang);
		String langLinks = folder + lang + "wiki-latest-langlinks.sql.gz";
		LanguageSqliteFile ifl = new LanguageSqliteFile(folder + lang + "wiki.sqlite");
		final Map<Long, Long> idMapping = new LinkedHashMap<Long, Long>();
		while (ifl.next()) {
			final long wikiId = ifl.getId();
			long articleId = wikiStructure.insertArticle(ifl.getLat(), ifl.getLon(), lang, wikiId, ifl.getTitle(),
					ifl.getContent());
			idMapping.put(wikiId, articleId);
		}
		ifl.closeConnnection();
		System.out.println("Insert translation " + lang);
		insertTranslationMapping(wikiStructure, langLinks, idMapping);
	}

	protected static void insertTranslationMapping(final GlobalWikiStructure wikiStructure, String langLinks,
			final Map<Long, Long> idMapping) throws FileNotFoundException, UnsupportedEncodingException, IOException,
			SQLException {
		readInsertValuesFile(langLinks, new InsertValueProcessor() {
			@Override
			public void process(List<String> vs) {
				final long wikiId = Long.parseLong(vs.get(0));
				Long lid = idMapping.get(wikiId);
				if (lid != null) {
					try {
						wikiStructure.insertWikiTranslation(lid, vs.get(1), vs.get(2));
					} catch (SQLException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}
			}
		});
		wikiStructure.commitTranslationInsert();
	}


	public interface InsertValueProcessor {
    	public void process(List<String> vs);
    }

	protected static void readInsertValuesFile(final String fileName, InsertValueProcessor p)
			throws FileNotFoundException, UnsupportedEncodingException, IOException {
		InputStream fis = new FileInputStream(fileName);
		if(fileName.endsWith("gz")) {
			fis = new GZIPInputStream(fis);
		}
    	InputStreamReader read = new InputStreamReader(fis, "UTF-8");
    	char[] cbuf = new char[1000];
    	int cnt;
    	boolean values = false;
    	String buf = ""	;
    	List<String> insValues = new ArrayList<String>();
    	while((cnt = read.read(cbuf)) >= 0) {
    		String str = new String(cbuf, 0, cnt);
    		buf += str;
    		if(!values) {
    			if(buf.contains("VALUES")) {
    				buf = buf.substring(buf.indexOf("VALUES") + "VALUES".length());
    				values = true;
    			}
    		} else {
    			boolean openString = false;
    			int word = -1;
    			int last = 0;
    			for(int k = 0; k < buf.length(); k++) {
    				if(openString ) {
						if (buf.charAt(k) == '\'' && (buf.charAt(k - 1) != '\\'
								|| buf.charAt(k - 2) == '\\')) {
    						openString = false;
    					}
    				} else if(buf.charAt(k) == ',' && word == -1) {
    					continue;
    				} else if(buf.charAt(k) == '(') {
    					word = k;
    					insValues.clear();
    				} else if(buf.charAt(k) == ')' || buf.charAt(k) == ',') {
    					String vl = buf.substring(word + 1, k).trim();
    					if(vl.startsWith("'")) {
    						vl = vl.substring(1, vl.length() - 1);
    					}
    					insValues.add(vl);
    					if(buf.charAt(k) == ')') {
//    						if(insValues.size() < 4) {
//    							System.err.println(insValues);
//    							System.err.println(buf);
//    						}
    						try {
								p.process(insValues);
							} catch (Exception e) {
								System.err.println(e.getMessage() + " " +insValues);
								e.printStackTrace();
							}
    						last = k + 1;
    						word = -1;
    					} else {
    						word = k;
    					}
    				} else if(buf.charAt(k) == '\'') {
    					openString = true;
    				}
    			}
    			buf = buf.substring(last);


    		}

    	}
    	read.close();
	}
}
