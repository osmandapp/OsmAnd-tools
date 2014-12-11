package net.osmand.osm.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.data.preparation.DBDialect;
import net.osmand.map.OsmandRegions;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;

public class WikipediaByCountryDivider {
	private static final Log log = PlatformUtil.getLog(WikipediaByCountryDivider.class);
	
	
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
		private static final long BATCH_SIZE = 500;

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
				keyNames.add(regions.getKeyName(b));
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

	public static void main(String[] args) throws IOException, SQLException {
//		String folder = "/home/victor/projects/osmand/wiki/";
//		String regionsFile = "/home/victor/projects/osmand/repo/resources/countries-info/regions.ocbf";
//		String cmd = "regenerate";
		String cmd = args[0];
		String regionsFile = args[1];
		String folder = args[2];
		if(cmd.equals("inspect")) {
			inspectWikiFile(folder);
		} else if(cmd.equals("update_countries")) {
			updateCountries(folder, regionsFile);
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
