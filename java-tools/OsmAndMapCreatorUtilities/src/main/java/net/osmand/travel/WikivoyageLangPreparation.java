package net.osmand.travel;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import net.osmand.wiki.WikivoyageOSMTags;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.gpx.GPXUtilities.WptPt;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.util.Algorithms;
import net.osmand.util.SqlInsertValuesReader;
import net.osmand.util.SqlInsertValuesReader.InsertValueProcessor;
import net.osmand.wiki.CustomWikiModel;
import net.osmand.wiki.WikiDatabasePreparation;
import net.osmand.wiki.WikiDatabasePreparation.PoiFieldCategory;
import net.osmand.wiki.WikiDatabasePreparation.PoiFieldType;
import net.osmand.wiki.WikiDatabasePreparation.WikiDBBrowser;
import net.osmand.wiki.WikiImageUrlStorage;

public class WikivoyageLangPreparation {
	private static final Log log = PlatformUtil.getLog(WikivoyageLangPreparation.class);	
	private static boolean uncompressed;
	private final static Map<Long, Long> KNOWN_WIKIVOYAGE_MAIN = new HashMap<Long, Long>();
	private static final long WID_TRAVEL_TOPICS = 14199938l;
	public static final long WID_DESTINATIONS = 1200957l;
	private static final long WID_ITINERARIES = 1322323l;
	private static final long WID_PHRASEBOOKS = 1599788l;
	private static final long WID_CULTURAL_ATTRACTIONS = 11042l;
	private static final long WID_WORLD_HERITAGE_SITE = 9259l;
	private static final long WID_OTHER_DESTINATIONS = 14201351l;
	static {
		KNOWN_WIKIVOYAGE_MAIN.put(WID_TRAVEL_TOPICS, 0l); // Travel topics
		KNOWN_WIKIVOYAGE_MAIN.put(WID_PHRASEBOOKS, WID_TRAVEL_TOPICS); // Phrasebooks
		KNOWN_WIKIVOYAGE_MAIN.put(14208553l, WID_TRAVEL_TOPICS); // Discover
		KNOWN_WIKIVOYAGE_MAIN.put(WID_ITINERARIES, WID_TRAVEL_TOPICS); // Itineraries
		KNOWN_WIKIVOYAGE_MAIN.put(5056668l, WID_TRAVEL_TOPICS); // Sleep
		///  
		KNOWN_WIKIVOYAGE_MAIN.put(WID_DESTINATIONS, 0l); // Destinations
		KNOWN_WIKIVOYAGE_MAIN.put(15l, WID_DESTINATIONS); // Africa
		KNOWN_WIKIVOYAGE_MAIN.put(51l, WID_DESTINATIONS); // Antarctica
		KNOWN_WIKIVOYAGE_MAIN.put(48l, WID_DESTINATIONS); // Asia
		KNOWN_WIKIVOYAGE_MAIN.put(49l, WID_DESTINATIONS); // North America
		KNOWN_WIKIVOYAGE_MAIN.put(55643l, WID_DESTINATIONS); // Oceania
		KNOWN_WIKIVOYAGE_MAIN.put(18l, WID_DESTINATIONS); // South America
		KNOWN_WIKIVOYAGE_MAIN.put(46l, WID_DESTINATIONS); // Europe
		KNOWN_WIKIVOYAGE_MAIN.put(WID_OTHER_DESTINATIONS, WID_DESTINATIONS); // Other destinations
	}
	private static final boolean DEBUG = false;
	private static final String SUFFIX_EN_REDIRECT = "en:";
	public static final String EMAIL = "Email";
	public static final String DIRECTIONS = "Directions";
	public static final String WORKING_HOURS = "Working hours";
	public static final String PRICE = "Price";
	public static final String PHONE = "Phone";
	
	private enum INS_POI_COLUMN {
		TRIP_ID, ORIGINAL_ID, LANG, TITLE, //
		LAT, LON, CATEGORY, NAME, WIKIPEDIA, WIKIDATA_ID, DESCRIPTION, //
		EMAIL, FAX, WEBSITE, PRICE, OPENING_HOURS, ADDRESS, PHONE, DIRECTIONS, 
	}
	public enum WikivoyageTemplates {
		LOCATION("geo"),
		POI("poi"),
		PART_OF("part_of"),
		QUICK_FOOTER("quickfooter"),
		BANNER("pagebanner"),
		REGION_LIST("regionlist"), 
		WARNING("warningbox"),
		CITATION("citation"),
		TWO_PART ("two"),
		STATION ("station"),
		METRIC_DATA("metric"),
		TRANSLATION("translation"),
		PHRASEBOOK("phrasebook"),
		MONUMENT_TITLE("monument-title"),
		DISAMB("disamb");
		
		private final String type;
		WikivoyageTemplates(String s) {
			type = s;
		}
		
		public String getType() {
			return type;
		}
	}

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, SQLException,
			XmlPullParserException, InterruptedException {
		String lang = "";
		String wikivoyageFolderName = "";
		String wikidataSqliteName = "";
		boolean help = false;
		for (String arg : args) {
			String val = arg.substring(arg.indexOf("=") + 1);
			if (arg.startsWith("--lang=")) {
				lang = val;
			} else if (arg.startsWith("--wikivoyageDir=")) {
				wikivoyageFolderName = val;
			} else if (arg.startsWith("--compress=")) {
				uncompressed = "no".equals(val) || "false".equals(val);
			} else if (arg.startsWith("--wikidataDB=")) {
				wikidataSqliteName = val;
			} else if (arg.equals("--h")) {
				help = true;
			}
		}
		if (args.length == 0 || help || wikivoyageFolderName.isEmpty() || wikidataSqliteName.isEmpty()) {
			printHelp();
			return;
		}
		final File wikiArticles = new File(wikivoyageFolderName, lang + "wikivoyage-latest-pages-articles.xml.bz2");
		final File wikiProps = new File(wikivoyageFolderName, lang + "wikivoyage-latest-page_props.sql.gz");
		final File wikiTitles = new File(wikivoyageFolderName, lang + "wikivoyage-latest-all-titles.gz");
		
		if (!wikiArticles.exists()) {
			System.out.println("Wikivoyage dump for " + lang + " doesn't exist" + wikiArticles.getName());
			return;
		}
		if (!wikiProps.exists()) {
			System.out.println("Wikivoyage page props for " + lang + " doesn't exist" + wikiProps.getName());
			return;
		}
		File wikivoyageSqlite = new File(wikivoyageFolderName, (uncompressed ? "full_" : "") + "wikivoyage.sqlite");
		File wikidataDB = new File(wikidataSqliteName);
		processWikivoyage(wikiArticles, wikiProps, wikiTitles, lang, wikivoyageSqlite, wikidataDB);
		System.out.println("Successfully generated.");
	}

	private static void printHelp() {
		System.out.printf("Usage: --lang=<lang> --wikivoyageDir=<wikivoyage folder> --compress=<true|false> " +
				"--wikidataDB=<wikidata sqlite>%n" +
				"--h - print this help%n" +
				"<lang> - language of wikivoyage%n" +
				"<wikivoyage folder> - work folder%n" +
				"<compress> - with the \"false\" or \"no\" argument, uncompressed content and gpx fields are added in the full_wikivoyage.sqlite,%n" +
				"\t\tby default only gziped fields are present in the wikivoyage.sqlite%n" +
				"<wikidata sqlite> - database file with data from wikidatawiki-latest-pages-articles and commonswiki-latest-pages-articles.xml.gz%n" +
				"\t\tThe file is in the folder which has osm_wiki_*.gz files. This files with osm elements with wikidata=*, wikipedia=* tags,%n" +
				"\t\talso folder has wikidatawiki-latest-pages-articles.xml.gz file ");
	}
	
	protected static class PageInfo {
		public long id;
		public String banner;
		public String image;
		public long wikidataId;
		
		public String title; // only for calculated
		public String partOf; // only for calculated 
		public double lat;
		public double lon;
	}
	
	private static class PageInfos {
		private Map<Long, PageInfo> byId = new HashMap<Long, PageInfo>();
		private Map<Long, PageInfo> byWikidataId = new HashMap<Long, PageInfo>();
		private Map<String, PageInfo> byTitle = new HashMap<String, PageInfo>(); // only published pages
		private Map<String, String> titlesLc = new HashMap<String, String>();
		public Set<String> missingParentsInfo = new HashSet<>();
		
		public String getCorrectTitle(String titleRef) {
			String lc = titleRef.toLowerCase();
			if (titlesLc.containsKey(lc)) {
				return titlesLc.get(lc);
			}
			lc = lc.replace('_', ' ');
			if (titlesLc.containsKey(lc)) {
				return titlesLc.get(lc);
			}
			if (titlesLc.containsKey(lc.replace("__", "_"))) {
				return titlesLc.get(lc);
			}
			if (lc.contains("%")) {
				try {
					lc = URLDecoder.decode(lc, "UTF-8");
				} catch (Exception e) {
					// ignore
				}
			}
			if (titlesLc.containsKey(lc)) {
				return titlesLc.get(lc);
			}
			return null;
		}
	}

	protected static void processWikivoyage(final File wikiArticles, final File wikiProps, 
			final File wikiTitles, String lang, File wikivoyageSqlite, File wikidataSqlite)
			throws ParserConfigurationException, SAXException, IOException, SQLException {
		PageInfos pageInfos = readPageInfos(wikiProps, wikiTitles);
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		InputStream articlesStream = new BufferedInputStream(new FileInputStream(wikiArticles), 8192 * 4);
		BZip2CompressorInputStream zis = new BZip2CompressorInputStream(articlesStream);
		Reader reader = new InputStreamReader(zis, "UTF-8");
		InputSource articlesSource = new InputSource(reader);
		articlesSource.setEncoding("UTF-8");
		Connection wikidataConn = DBDialect.SQLITE.getDatabaseConnection(wikidataSqlite.getAbsolutePath(), log);
		final PreparedStatement selectCoordsByID = wikidataConn.prepareStatement("SELECT lat, lon FROM wiki_coords where id = ?");
		final PreparedStatement selectTitle = wikidataConn.prepareStatement("select title from wiki_mapping where id = ? and lang = ?");
		final PreparedStatement selectWid = wikidataConn.prepareStatement("select id from wiki_mapping where title = ? and lang = ?");
		WikiDBBrowser browser = new WikiDBBrowser() {
			
			@Override
			public String getWikipediaTitleByWid(String lang, long wikidataId) throws SQLException {
				if (wikidataId > 0) {
					selectTitle.setLong(1, wikidataId);
					selectTitle.setString(2, lang);
					ResultSet rs = selectTitle.executeQuery();
					if (rs.next()) {
						return rs.getString(1);
					}
				}
				return null;

			}
			
			@Override
			public LatLon getLocation(String lang, String wikiLink, long wikidataId) throws SQLException {
				LatLon r = getLocationWid(wikidataId);
				if (r != null) {
					return r;
				}
				if (!Algorithms.isEmpty(wikiLink)) {
					selectWid.setString(1, wikiLink);
					selectWid.setString(2, lang);
					ResultSet rs = selectWid.executeQuery();
					if (rs.next()) {
						long wid = rs.getLong(1);
						if(wid > 0) {
							return getLocationWid(wid);
						}
					}
				}
				return null;
			}

			private LatLon getLocationWid(long wikidataId) throws SQLException {
				if (wikidataId > 0) {
					selectCoordsByID.setLong(1, wikidataId);
					ResultSet rs = selectCoordsByID.executeQuery();
					if (rs.next()) {
						LatLon l = new LatLon(rs.getDouble(1), rs.getDouble(2));
						if (l.getLatitude() != 0 || l.getLongitude() != 0) {
							return l;
						}
					}
				}
				return null;
			}
		};
		WikivoyageHandler handler = new WikivoyageHandler(sx, articlesStream, lang, wikivoyageSqlite, pageInfos,
				browser);
		sx.parse(articlesSource, handler);
		handler.finish();
		wikidataConn.close();
	}

	private static PageInfos readPageInfos(final File wikiProps, File wikiTitles) throws IOException {
		PageInfos pageInfos = new PageInfos();
		InputStream fis = new FileInputStream(wikiTitles);
		if (wikiTitles.getName().endsWith("gz")) {
			fis = new GZIPInputStream(fis);
		}		
		BufferedReader r = new BufferedReader(new InputStreamReader(fis));
		String s = null;
		while ((s = r.readLine()) != null) {
			int i = s.indexOf('\t');
			if (i > 0 && s.substring(0, i).trim().equals("0")) {
				String title = s.substring(i + 1).trim().replace('_', ' ');
				pageInfos.titlesLc.put(title.toLowerCase(), title);
			}
		}
		System.out.printf("Read %d page info titles\n", pageInfos.titlesLc.size());
		fis.close();
		
		
		SqlInsertValuesReader.readInsertValuesFile(wikiProps.getAbsolutePath(), new InsertValueProcessor() {
			
			@Override
			public void process(List<String> vs) {
				long pageId = Long.parseLong(vs.get(0));
				String property = vs.get(1);
				String value = vs.get(2);
				PageInfo pi = getPageInfo(pageId);
				if(property.equals("wpb_banner")) {
					pi.banner = value;
				} else if(property.equals("wikibase_item")) {
					pi.wikidataId = Long.parseLong(value.substring(1));
					pageInfos.byWikidataId.put(pi.wikidataId, pi);
				} else if(property.equals("page_image_free")) {
					pi.image = value;
					
				}
			}
			private PageInfo getPageInfo(long pageId) {
				PageInfo p = pageInfos.byId.get(pageId);
				if (p == null) {
					p = new PageInfo();
					p.id = pageId;
					pageInfos.byId.put(pageId, p);
				}
				return p;
			}
		});
		return pageInfos;
	}
	

	public static void createInitialDbStructure(Connection conn, String lang, boolean uncompressed) throws SQLException {
		conn.createStatement()
				.execute("CREATE TABLE IF NOT EXISTS travel_articles(title text, content_gz blob"
						+ (uncompressed ? ", content text" : "") + ", is_part_of text, is_part_of_wid bigint, lat double, lon double"
								+ ", image_title text, banner_title text, src_banner_title text,  gpx_gz blob"
						+ (uncompressed ? ", gpx text" : "") + ", trip_id bigint, original_id bigint, lang text, contents_json text)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_title ON travel_articles(title);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_id ON travel_articles(trip_id);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_local_id ON travel_articles(lang, original_id);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_part_of ON travel_articles(is_part_of);");
		StringBuilder b = new StringBuilder("CREATE TABLE IF NOT EXISTS travel_points(");
		boolean f = true;
		for (INS_POI_COLUMN col : INS_POI_COLUMN.values()) {
			if (!f) {
				b.append(", ");
			}
			b.append(col.name().toLowerCase());
			if (col == INS_POI_COLUMN.TRIP_ID || col == INS_POI_COLUMN.ORIGINAL_ID
					|| col == INS_POI_COLUMN.WIKIDATA_ID) {
				b.append(" bigint");
			} else if (col == INS_POI_COLUMN.LAT || col == INS_POI_COLUMN.LON) {
				b.append(" double");
			} else {
				b.append(" text");
			}
			f = false;
		}
		b.append(");");
		conn.createStatement().execute(b.toString());
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS indexp_trip_id ON travel_points(trip_id);");
		conn.createStatement().execute("DELETE FROM travel_articles WHERE lang = '" + lang + "';");
		conn.createStatement().execute("DELETE FROM travel_points WHERE lang = '" + lang + "';");
	}

	public static PreparedStatement generateInsertPrep(Connection conn, boolean uncompressed) throws SQLException {
		return conn.prepareStatement("INSERT INTO travel_articles(title, content_gz"
				+ (uncompressed ? ", content" : "") + ", is_part_of, lat, lon, image_title, banner_title, gpx_gz"
				+ (uncompressed ? ", gpx" : "") + ", trip_id , original_id , lang, contents_json)"
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" + (uncompressed ? ", ?, ?": "") + ")");
	}
	
	

	public static class WikivoyageHandler extends DefaultHandler {
		long id = 1;
		private final SAXParser saxParser;
		private boolean page = false;
		private boolean revision = false;
		
		private StringBuilder ctext = null;
		private String title;
		private long cid;
		private long cns;
		private PageInfo cInfo;
		
		private final InputStream progIS;
		private ConsoleProgressImplementation progress = new ConsoleProgressImplementation();
		private DBDialect dialect = DBDialect.SQLITE;
		private Connection wikiVoyageConn;
		private final WikiImageUrlStorage imageUrlStorage;
		private WikiDBBrowser dbBrowser;
		private PreparedStatement prepInsert;
		private PreparedStatement prepInsertPOI;
		private int batch = 0;
		private final static int BATCH_SIZE = 500;
		final ByteArrayOutputStream bous = new ByteArrayOutputStream(64000);
		private String lang;
		
		private PageInfos pageInfos;
		private PageInfos enPageInfos;
		private Map<String, String> redirects = new HashMap<>();
		private int skippedArticle;
//		private Map<String, String> parentStructure = new HashMap<>();
		
		WikivoyageHandler(SAXParser saxParser, InputStream progIS, String lang, File wikivoyageSqlite, PageInfos pageInfos,
				WikiDBBrowser dbBrowser) throws IOException, SQLException {
			this.lang = lang;
			this.saxParser = saxParser;
			this.progIS = progIS;
			this.pageInfos = pageInfos;
			this.dbBrowser = dbBrowser;
			progress.startTask("Parse wikivoyage xml", progIS.available());
			wikiVoyageConn = dialect.getDatabaseConnection(wikivoyageSqlite.getAbsolutePath(), log);
			imageUrlStorage = new WikiImageUrlStorage(wikiVoyageConn, wikivoyageSqlite.getParent(), lang);
			createInitialDbStructure(wikiVoyageConn, lang, uncompressed);
			prepInsert = generateInsertPrep(wikiVoyageConn, uncompressed);
			enPageInfos = readEnPageInfo(wikiVoyageConn);
			redirects.put("Q" + WID_TRAVEL_TOPICS, "");
			redirects.put("Q" + WID_ITINERARIES, "");
			redirects.put("Q" + WID_DESTINATIONS, "");
			redirects.put("Q" + WID_CULTURAL_ATTRACTIONS, "");
			redirects.put("Q" + WID_WORLD_HERITAGE_SITE, ""); 
			prepInsertPOI = prepareInsertPoi();
		}

		private PreparedStatement prepareInsertPoi() throws SQLException {
			StringBuilder b = new StringBuilder("INSERT INTO travel_points(");
			boolean f = true;
			for (INS_POI_COLUMN col : INS_POI_COLUMN.values()) {
				if (!f) {
					b.append(", ");
				}
				b.append(col.name().toLowerCase());
				f = false;
			}
			b.append(") VALUES (");
			f = true;
			for (int i = 0; i < INS_POI_COLUMN.values().length; i++) {
				if (!f) {
					b.append(", ");
				}
				b.append("?");
				f = false;
			}
			b.append(")");
			return wikiVoyageConn.prepareStatement(b.toString());
		}


		private PageInfos readEnPageInfo(Connection c) throws SQLException {
			Statement s = c.createStatement();
			PageInfos en = new PageInfos();
			ResultSet rs = s.executeQuery("select lang, title, trip_id, original_id, is_part_of, image_title, banner_title, lat, lon from travel_articles where lang='en' ");
			while (rs.next()) {
				PageInfo pi = new PageInfo();
//				pi.lang = rs.getString(1);
				pi.id = rs.getLong(4);
				pi.wikidataId = rs.getLong(3);
				pi.title = rs.getString(2);
				pi.partOf = rs.getString(5);
				pi.image = rs.getString(6);
				pi.banner = rs.getString(7);
				pi.lat = rs.getDouble(8);
				pi.lon = rs.getDouble(9);
				en.byWikidataId.put(pi.wikidataId, pi);
				en.byTitle.put(pi.title, pi);
				en.byId.put(pi.id, pi);
			}
			return en;
		}

		public void addBatch() throws SQLException {
			prepInsert.addBatch();
			if (batch++ > BATCH_SIZE) {
				prepInsert.executeBatch();
				prepInsertPOI.executeBatch();
				batch = 0;
			}
		}

		public void finish() throws SQLException {
			prepInsertPOI.executeBatch();
			prepInsert.executeBatch();
			if (!wikiVoyageConn.getAutoCommit()) {
				wikiVoyageConn.commit();
			}
			prepInsert.close();
			assignDefaultPartOfAndValidate();
			wikiVoyageConn.close();
			final Map<String, Integer> mp = WikiDatabasePreparation.POI_OTHER_TYPES;
			List<String> keys = new ArrayList<>(mp.keySet());
			Collections.sort(keys, new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					return -Integer.compare(mp.get(o1), mp.get(o2));
				}
			});
			System.out.println("-----------------");
			System.out.println("Other category types (top 15) could be assigned to default categories:");
			for (int i = 0; i < 15 && i < keys.size(); i++) {
				System.out.printf("Key: %s - %d\n", keys.get(i), mp.get(keys.get(i)));
			}
			mp.clear();
			
		}

		private void assignDefaultPartOfAndValidate() throws SQLException {
			// fix non existing parent to english
			PreparedStatement upd = wikiVoyageConn.prepareStatement("UPDATE travel_articles SET is_part_of = ?, is_part_of_wid = ? WHERE original_id =  ? and lang = ?");
			PreparedStatement del = wikiVoyageConn.prepareStatement("DELETE FROM travel_articles WHERE original_id =  ? and lang = ?");
			int batch = 0;
			int articles = 0, articlesParentWid = 0;
			PageInfo otherDest = pageInfos.byWikidataId.get(WID_OTHER_DESTINATIONS);
			PageInfo enOtherDest = enPageInfos.byWikidataId.get(WID_OTHER_DESTINATIONS);
			String otherDestTitle = otherDest == null ? (SUFFIX_EN_REDIRECT + enOtherDest.title) : otherDest.title;
			for (PageInfo p : pageInfos.byId.values()) {
				String partOf = p.partOf;
				long partOfWid = 0;
				if (partOf == null) {
					continue;
				}
				boolean delete = false;
				if (redirects.containsKey(partOf)) {
					String target = redirects.get(partOf);
					// Calculate redirects (by wikidata id)
					if (partOf.startsWith("Q") && target.isEmpty()) {
						long wid = Long.parseLong(partOf.substring(1));
						PageInfo pageRedirect = pageInfos.byWikidataId.get(wid);
						PageInfo enPage = enPageInfos.byWikidataId.get(wid);
						if (pageRedirect != null && pageRedirect.title != null) {
							partOf = pageRedirect.title;
						} else if (enPage != null && enPage.title != null) {
							System.out.printf("Warning redirect to 'en' from '%s' (not exist) '%s'  -> '%s'\n", lang, partOf, SUFFIX_EN_REDIRECT + enPage.title);
							partOf = SUFFIX_EN_REDIRECT + enPage.title;
						} else {
							skipArticle(lang, pageRedirect.title, "n", "parent no redirect to " + partOf);
							delete = true;
						}
					} else {
						partOf = target;
					}
				}
				
				// if part of doesn't exist check partOf of english wikipedia
				if (!Algorithms.isEmpty(partOf) && !pageInfos.byTitle.containsKey(partOf) && !partOf.startsWith(SUFFIX_EN_REDIRECT) && 
						p.wikidataId > 0) {
					PageInfo enPage = enPageInfos.byWikidataId.get(p.wikidataId);
					if (enPage != null && enPageInfos.byTitle.get(enPage.partOf) != null) {
						PageInfo enParent = enPageInfos.byTitle.get(enPage.partOf);
						PageInfo locParent = pageInfos.byWikidataId.get(enParent.wikidataId);
						if (locParent != null) {
							System.out.printf("Info correct parent %s '%s': '%s' -> '%s' \n", lang, p.title, partOf, locParent.title);
							partOf = locParent.title;
						} else {
							String enPartOf = SUFFIX_EN_REDIRECT + enParent.title;
							System.out.printf("Warning redirect parent %s '%s': '%s' -> '%s' \n", lang, p.title, partOf, enPartOf);
							partOf = enPartOf;
						}
					}
				}
				
				if (partOf.equalsIgnoreCase(title)) {
					skipArticle(lang, title, "l", String.format("Loop reference is detected!"));
					delete = true;
				} else if (partOf.startsWith(SUFFIX_EN_REDIRECT)) {
					// skip redirect
					PageInfo parentEnPage = enPageInfos.byTitle.get(partOf.substring(SUFFIX_EN_REDIRECT.length()));
					if (parentEnPage != null) {
						partOfWid = parentEnPage.wikidataId;
					} else {
						skipArticle(lang, p.title, "e", "en parent doesn't exist " + partOf);
						delete = true;
					}
				} else if (!Algorithms.isEmpty(partOf)) {
					PageInfo parentPage = pageInfos.byTitle.get(partOf);
					if (parentPage != null) {
						partOfWid = parentPage.wikidataId;
					} else {
						skipArticle(lang, p.title, "p", "parent doesn't exist " + partOf);
						delete = true;
					}
				} else if (p.wikidataId != WID_DESTINATIONS && p.wikidataId != WID_TRAVEL_TOPICS) {
					skipArticle(lang, p.title, "r", "no parent");
					delete = true;
				}
				if (delete) {
					del.setLong(1, p.id);
					del.setString(2, lang);
					del.addBatch();
					// here we should delete all siblings recursively in theory
				} else {
					if (partOfWid == WID_DESTINATIONS && !KNOWN_WIKIVOYAGE_MAIN.containsKey(p.wikidataId)) {
						// reassign to other destinations (oceans, ...)
						partOfWid = WID_OTHER_DESTINATIONS;
						partOf = otherDestTitle;
					} 
						
					articles++;
					if (partOfWid > 0) {
						articlesParentWid++;
					} else {
						System.out.printf("Warning parent no wid %s '%s' -> '%s' \n", lang, p.title, partOf);
					}
					/// update redirects in tables
					upd.setString(1, partOf);
					upd.setLong(2, partOfWid);
					upd.setLong(3, p.id);
					upd.setString(4, lang);
					upd.addBatch();
					if (++batch % 500 == 0) {
						System.out.println("Update parent wikidata id batch: " + batch);
						upd.executeBatch();
					}
				}
			}
			upd.executeBatch();
			del.executeBatch();
			System.out.printf("Total %s: %d articles (%d - parent wikidata, %d - skipped)\n",
					lang, articles, articlesParentWid, skippedArticle);
		}
		
		public String getStandardPartOf(Map<WikivoyageTemplates, List<String>> macroBlocks, PageInfo enPage) {
			long wid = 0;
			if (macroBlocks.containsKey(WikivoyageTemplates.PHRASEBOOK)) {
				wid = WID_PHRASEBOOKS; // Q1599788 -- Phrasebooks
			}
			if (KNOWN_WIKIVOYAGE_MAIN.containsKey(cInfo.wikidataId)) {
				wid = KNOWN_WIKIVOYAGE_MAIN.get(cInfo.wikidataId);
			} else if(enPage != null && enPageInfos.byTitle.containsKey(enPage.partOf)){
				PageInfo enParent = enPageInfos.byTitle.get(enPage.partOf);
				wid = enParent.wikidataId;
			}
			PageInfo p = pageInfos.byWikidataId.get(wid);
			if (wid > 0) {
				if (p != null && p.title != null) {
					return p.title;
				} else {
					String parent = "Q" + wid;
					redirects.put(parent, "");
					return parent;
				}
			}
			return "";
		}

		public int getCount() {
			return (int) (id - 1);
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			String name = saxParser.isNamespaceAware() ? localName : qName;
			if (!page) {
				page = name.equals("page");
				if(page) {
					cid = 0;
					cns = -1;
					cInfo = null;
					title = null;
				}
			} else {
				if (name.equals("title")) {
					ctext = new StringBuilder();
				} else if (name.equals("text")) {
					ctext = null;
					if (cid > 0) {
						if (cns == 0 || (cInfo != null && KNOWN_WIKIVOYAGE_MAIN.containsKey(cInfo.wikidataId))) {
							ctext = new StringBuilder();
						}
					}
				} else if (name.equals("revision")) {
					revision = true;
				} else if (name.equals("id") && !revision) {
					ctext = new StringBuilder();
				} else if (name.equals("ns")) {
					ctext = new StringBuilder();
				}
			}
		}

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			if (page) {
				if (ctext != null) {
					ctext.append(ch, start, length);
				}
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			String name = saxParser.isNamespaceAware() ? localName : qName;
			
			try {
				if (page) {
					if (name.equals("page")) {
						page = false;
						progress.remaining(progIS.available());
					} else if (name.equals("title")) {
						title = trim(ctext.toString());
						ctext = null;
					} else if (name.equals("revision")) {
						revision = false;
					} else if (name.equals("ns")) {
						cns = Long.parseLong(ctext.toString());
						ctext = null;
					} else if (name.equals("id") && !revision) {
						cid = Long.parseLong(ctext.toString());
						cInfo = pageInfos.byId.get(cid);
						ctext = null;
					} else if (name.equals("text")) {
						if (ctext != null) {
							String red = WikiDatabasePreparation.getRedirect(ctext);
							if (red != null) {
								redirects.put(title, trim(red));
							} else if (cInfo == null) {
								// debug https://de.wikivoyage.org/wiki/Special:Export/Frankfurt_am_Main/Nordwesten
								// possibly no wikidata id, no banner (so no properties) - de 116081
								System.err.printf("Error page %d %s - empty info\n", cid, title);
							} else {
								parseText(ctext);
							}
						}
						ctext = null;
					}
				}
			} catch (IOException | SQLException e) {
				throw new SAXException(e);
			}
		}

		private void parseText(StringBuilder cont) throws IOException, SQLException, SAXException {
			Map<WikivoyageTemplates, List<String>> macroBlocks = new HashMap<>();
			List<Map<PoiFieldType, Object>> pois = new ArrayList<Map<PoiFieldType, Object>>();
			cInfo.title = title;
			String wikiText = WikiDatabasePreparation.removeMacroBlocks(cont, null, macroBlocks, pois, lang, title, dbBrowser, null);
			if (macroBlocks.containsKey(WikivoyageTemplates.DISAMB) || 
					macroBlocks.containsKey(WikivoyageTemplates.MONUMENT_TITLE)) {
//				System.out.println("Skip disambiguation " + title); 
				return;
			}
			PageInfo enPage = null;
			if (cInfo.wikidataId > 0) {
				enPage = enPageInfos.byWikidataId.get(cInfo.wikidataId);
			}
			try {
				LatLon ll = dbBrowser.getLocation(lang, null, cInfo.wikidataId);
				// keep same coordinates
				if (ll == null) {
					if (enPage != null && enPage.lat != 0) {
						ll = new LatLon(enPage.lat, enPage.lon);
					} else {
						ll = WikiDatabasePreparation.getLatLonFromGeoBlock(macroBlocks.get(WikivoyageTemplates.LOCATION), lang, title);
					}
				} 
				
				boolean accepted = true;// filtered by namespace !title.toString().contains(":");
				if (accepted) {
					int column = 1;
					String filename = getFileName(macroBlocks.get(WikivoyageTemplates.BANNER));
					if (id++ % 500 == 0) {
						log.debug(String.format("Article accepted %d %s %s free: %s\n", cid, title, ll,
								Runtime.getRuntime().freeMemory() / (1024 * 1024)));
					}
					CustomWikiModel wikiModel = new CustomWikiModel(
							// Images could be different compare...
							// https://upload.wikimedia.org/wikivoyage/de/thumb/d/d4/Museo_in_San_Juan_verkl.jpg/600px-Museo_in_San_Juan_verkl.jpg
							// https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Percys_and_Kruis2.png/1300px-Percys_and_Kruis2.png
							"https://upload.wikimedia.org/wikipedia/commons/${image}",
							"https://" + lang + ".wikivoyage.org/wiki/${title}", imageUrlStorage, false);

					String plainStr = WikiDatabasePreparation.generateHtmlArticle(wikiText, wikiModel);
					String contentsJson = wikiModel.getContentsJson();
					String shortDescr = WikiDatabasePreparation.getShortDescr(wikiText, wikiModel);
					if (DEBUG) {
						String savePath = "/Users/plotva/Documents";
						File myFile = new File(savePath, "page.html");
						BufferedWriter htmlFileWriter = new BufferedWriter(new FileWriter(myFile, false));
						htmlFileWriter.write(plainStr);
						htmlFileWriter.close();
					}
					// part_of
					String partOf = parsePartOf(macroBlocks.get(WikivoyageTemplates.PART_OF), title, lang);
					if ("Voyages culturels".equals(partOf)) {
						partOf = "Q" + WID_CULTURAL_ATTRACTIONS;
					}
					if (partOf == null) {
						// this disamb or redirection
						return;
					}
					if (partOf.length() == 0) {
						partOf = getStandardPartOf(macroBlocks, enPage).trim();
					}
					if (partOf.length() == 0) {
						List<String> possiblePartOf = parsePartOfFromQuickFooter(macroBlocks.get(WikivoyageTemplates.QUICK_FOOTER), title, lang); // for it
						if (possiblePartOf != null) {
							for (String s : possiblePartOf) {
								// if the order is not correct then by title will produce wrong results
								// (it's slightly corrected by english hierarchy)
								if (s.startsWith("WIKIDATAQ")) {
									partOf = s.substring("WIKIDATA".length());
									break;
								} else if (pageInfos.getCorrectTitle(s) != null) {
									partOf = pageInfos.getCorrectTitle(s);
									break;
								} else if (pageInfos.missingParentsInfo.add(s)) {
									System.out.printf("Info missing parent '%s' in %s '%s' \n", s, lang, title);
								}
							}
						}
					}
					if (partOf.length() == 0 && title.contains("/")) {
						String parent = pageInfos.getCorrectTitle(title.substring(0, title.lastIndexOf('/')));
						if (parent != null) {
							partOf = parent;
						}
					}
					String corrPartOf = pageInfos.getCorrectTitle(partOf);
					if (!partOf.equals(corrPartOf) && corrPartOf != null) {
						System.out.printf("Info correct parent %s '%s': '%s' -> '%s' \n", lang, title, partOf, corrPartOf);
						partOf = corrPartOf;
					}
					if (KNOWN_WIKIVOYAGE_MAIN.containsKey(cInfo.wikidataId)) {
						partOf = getStandardPartOf(macroBlocks, enPage).trim();
					}
					partOf = trim(partOf);
					if (Algorithms.isEmpty(partOf)) {
						long wid = cInfo == null ? 0 : cInfo.wikidataId;
						if (wid != WID_DESTINATIONS && wid != WID_TRAVEL_TOPICS) {
							skipArticle(lang, title, "s", String.format("wid=%s no parent attached", "Q" + wid));
							return;
						}
					}
					if (partOf.equalsIgnoreCase(title)) {
						skipArticle(lang, title, "l", String.format("Loop reference is detected!"));
						return;
					}
					// prep.setString(column++, Encoder.encodeUrl(title.toString()));
					prepInsert.setString(column++, title.toString());
					prepInsert.setBytes(column++, stringToCompressedByteArray(bous, plainStr));
					if (uncompressed) {
						prepInsert.setString(column++, plainStr);
					}
					prepInsert.setString(column++, partOf);
					
					
					cInfo.partOf = partOf; 
					pageInfos.byTitle.put(title, cInfo); // Publish by title
					if (ll == null) {
						prepInsert.setNull(column++, Types.DOUBLE);
						prepInsert.setNull(column++, Types.DOUBLE);
					} else {
						prepInsert.setDouble(column++, ll.getLatitude());
						prepInsert.setDouble(column++, ll.getLongitude());
					}
					// banner
					if (cInfo.image == null && enPage != null) {
						cInfo.image = enPage.image;
					}
					if (cInfo.image != null) {
						prepInsert.setString(column++, cInfo.image);
					} else {
						column++;
					}
					if (cInfo.banner == null && enPage != null) {
						cInfo.banner = enPage.banner;
					}
					if (cInfo.banner != null) {
						prepInsert.setString(column++, cInfo.banner);
					} else if (cInfo.image != null) {
						prepInsert.setString(column++, cInfo.image);
					} else {
						prepInsert.setString(column++, filename);
					}

					// gpx_gz
					String gpx = generateGpx(pois, title.toString(), lang, shortDescr, cInfo.wikidataId,
							cid, ll);
					prepInsert.setBytes(column++, stringToCompressedByteArray(bous, gpx));
					if (uncompressed) {
						prepInsert.setString(column++, gpx);
					}
					// trip id equals to wikidata id
					prepInsert.setLong(column++, cInfo.wikidataId);
					prepInsert.setLong(column++, cid);
					prepInsert.setString(column++, lang);
					prepInsert.setString(column++, contentsJson);
					
					addBatch();

				}
			} catch (SQLException e) {
				throw new SAXException(e);
			}
		}
		
		
		private void skipArticle(String lang, String title, String sh, String msg) {
			skippedArticle++;
			String titleUrl = title.replace(" ", "%20");
			System.out.printf("Skip article -%s- %s https://%s.wikivoyage.org/wiki/%s: %s \n", sh, lang, lang, titleUrl,
					msg);
		}

		

		public static String capitalizeFirstLetterAndLowercase(String s) {
			if (s != null && s.length() > 1) {
				// not very efficient algorithm
				return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
			} else {
				return s;
			}
		}
		
		private String generateGpx(List<Map<PoiFieldType, Object>> list, String title, String lang, String descr, long tripId, long id,
				LatLon ll) throws SQLException {
			GPXFile f = new GPXFile(title, lang, descr);
			List<WptPt> points = new ArrayList<>();
			Map<INS_POI_COLUMN, Object> insParams = new TreeMap<>();
			for (Map<PoiFieldType, Object> s : list) {
				insParams.clear();
				WptPt point = new WptPt();
				Iterator<Entry<PoiFieldType, Object>> tags = s.entrySet().iterator();
				Map<String, String> extraValues = new LinkedHashMap<String, String>();
				insParams.put(INS_POI_COLUMN.TITLE, title);
				insParams.put(INS_POI_COLUMN.LANG, lang);
				insParams.put(INS_POI_COLUMN.ORIGINAL_ID, id);
				insParams.put(INS_POI_COLUMN.TRIP_ID, tripId);
				while (tags.hasNext()) {
					Entry<PoiFieldType, Object> e = tags.next();
					PoiFieldType fieldType = e.getKey();
					String value = String.valueOf(e.getValue());
					if (fieldType == PoiFieldType.CATEGORY) {
						PoiFieldCategory cat = (PoiFieldCategory) e.getValue();
						point.category = cat.name().toLowerCase();
						point.setColor(cat.color);
						if (!Algorithms.isEmpty(cat.icon)) {
							point.setIconName(cat.icon);
						}
						insParams.put(INS_POI_COLUMN.CATEGORY, cat.name().toLowerCase());
					} else if (fieldType == PoiFieldType.PHONE) {
						extraValues.put(PHONE, value);
						point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_PHONE.tag(), value);
						insParams.put(INS_POI_COLUMN.PHONE, value);
					} else if (fieldType == PoiFieldType.WORK_HOURS) {
						extraValues.put(WORKING_HOURS, value);
						point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_OPENING_HOURS.tag(), value);
						insParams.put(INS_POI_COLUMN.OPENING_HOURS, value);
					} else if (fieldType == PoiFieldType.PRICE) {
						extraValues.put(PRICE, value);
						point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_PRICE.tag(), value);
						insParams.put(INS_POI_COLUMN.PRICE, value);
					} else if (fieldType == PoiFieldType.DIRECTIONS) {
						extraValues.put(DIRECTIONS, value);
						point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_DIRECTIONS.tag(), value);
						insParams.put(INS_POI_COLUMN.DIRECTIONS, value);
					} else if (fieldType == PoiFieldType.ADDRESS) {
						point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_ADDRESS.tag(), value);
						insParams.put(INS_POI_COLUMN.ADDRESS, value);
					} else if (fieldType == PoiFieldType.WIKIDATA) {
						point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_WIKIDATA.tag(), value);
						if (value.length() > 1) {
							try {
								insParams.put(INS_POI_COLUMN.WIKIDATA_ID,
										Long.parseLong(value.substring(1)));
							} catch (NumberFormatException e1) {
								System.out.println("Parsing wikidata long: '" + value + "'");
							}
						}
					} else if (fieldType == PoiFieldType.WIKIPEDIA) {
						point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_WIKIPEDIA.tag(), value);
						insParams.put(INS_POI_COLUMN.WIKIPEDIA, value);
					} else if (fieldType == PoiFieldType.EMAIL) {
						extraValues.put(EMAIL, value);
						point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_EMAIL.tag(), value);
						insParams.put(INS_POI_COLUMN.EMAIL, value);
					} else if (fieldType == PoiFieldType.FAX) {
						extraValues.put("Fax", value);
						point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_FAX.tag(), value);
						insParams.put(INS_POI_COLUMN.FAX, value);
					} else if (fieldType == PoiFieldType.NAME) {
						point.name = value;
						insParams.put(INS_POI_COLUMN.NAME, value);
					} else if (fieldType == PoiFieldType.WEBSITE) {
						point.link = value;
						insParams.put(INS_POI_COLUMN.WEBSITE, value);
					} else if (fieldType == PoiFieldType.DESCRIPTION) {
						point.desc = value;
					} else if (fieldType == PoiFieldType.LATLON) {
						point.lat = ((LatLon) e.getValue()).getLatitude();
						point.lon = ((LatLon) e.getValue()).getLongitude();
						insParams.put(INS_POI_COLUMN.LAT, point.lat);
						insParams.put(INS_POI_COLUMN.LON, point.lon);
					}
				}
				for (String key : extraValues.keySet()) {
					if (point.desc == null) {
						point.desc = "";
					} else {
						point.desc += "\n ";
					}
					point.desc += key + ": " + extraValues.get(key); // ". " backward compatible
				}
				insParams.put(INS_POI_COLUMN.DESCRIPTION, point.desc);
				if (point.hasLocation() && !Algorithms.isEmpty(point.name)) {
					for (INS_POI_COLUMN i : INS_POI_COLUMN.values()) {
						Object obj = insParams.get(i);
						if (obj instanceof Long) {
							prepInsertPOI.setLong(i.ordinal() + 1, (long) obj);
						} else if (obj instanceof Double) {
							prepInsertPOI.setDouble(i.ordinal() + 1, (double) obj);
						} else if (obj != null) {
							prepInsertPOI.setString(i.ordinal() + 1, String.valueOf(obj));
						} else {
							prepInsertPOI.setObject(i.ordinal() + 1, null);
						}
					}
					prepInsertPOI.addBatch();
					points.add(point);
				} else {
//					System.out.printf("Missing point loc %s %s in %s:%s\n", point.name, point.link, lang, title);	
				}
			}	
			if (!points.isEmpty()) {
				f.addPoints(points);
				return GPXUtilities.asString(f);
			}
			return "";
		}
		
		private byte[] stringToCompressedByteArray(ByteArrayOutputStream baos, String toCompress) {
			baos.reset();
			try {
				GZIPOutputStream gzout = new GZIPOutputStream(baos);
				gzout.write(toCompress.getBytes("UTF-8"));
				gzout.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return baos.toByteArray();
		}

		private String getFileName(List<String> list) {
			if (list != null && !list.isEmpty()) {
				String bannerInfo = list.get(0);
				String[] infoSplit = bannerInfo.split("\\|");
				for (String s : infoSplit) {
					String toCompare = s.toLowerCase();
					if (toCompare.contains(".jpg") || toCompare.contains(".jpeg") 
							|| toCompare.contains(".png") || toCompare.contains(".gif")) {
						s = s.replace("https:", "");
						int equalInd = s.indexOf("=");
						int columnInd = s.indexOf(":");
						int index = (equalInd == -1 && columnInd == -1) ? -1 : (columnInd > equalInd ? columnInd : equalInd);
						if (index != -1) {
							return s.substring(index + 1, s.length()).trim();
						}
						return s.trim();
					}
				}
			}
			return "";
		}
		
		public static String parsePartOf(List<String> list, String lang, String title) {
			if (list != null && !list.isEmpty()) {
				String partOf = list.get(0);
				String lowerCasePartOf = partOf.toLowerCase();
				if (lowerCasePartOf.startsWith("navigation")) {
					// to fix it's incorrect for many languagues 
					String[] splitPartOf = partOf.split(" ");
					if (splitPartOf.length > 1) {
						return splitPartOf[1];
					} else {
						System.out.printf("Error structure the partof: %s (%s %s)", partOf, lang, title);
						return "";
					}
				} else if (lowerCasePartOf.startsWith("footer|")) {
					String part = "";
					String type = "";
					String[] splitPartOf = partOf.split("\\|");
					for (String s : splitPartOf) {
						String[] vls = s.split("=");
						String key = vls[0].trim().toLowerCase();
						if (vls.length > 1 && key.equals("ispartof")) {
							part = vls[1].trim();
							break;
						} else if (vls.length > 1 && key.equals("type")) {
							type = vls[1].trim();
						}
					}
					if (part.length() == 0) {
						if (type.equalsIgnoreCase("маршрут")) {
							return "Q" + WID_ITINERARIES; // Itineraries
						} else if (type.equalsIgnoreCase("континент")) {
							return "Q" + WID_DESTINATIONS; // Destinations
						} else if (type.equalsIgnoreCase("сводная") || type.equalsIgnoreCase("природа")
								|| type.equalsIgnoreCase("наследие")) {
							return null;
						} else {
							System.out.printf("Error structure the partof: %s (%s) in the article: %s %s\n", partOf, type, lang, title);
						}
					}
					return trim(part).replaceAll("_", " ");
				} else if (lowerCasePartOf.contains("קטגוריה")) {
					return partOf.substring(partOf.indexOf(":") + 1).trim().replaceAll("[_\\|\\*]", "");
				} else {
					String[] splitPartOf = partOf.split("\\|");
					if (splitPartOf.length > 1) {
						return trim(splitPartOf[1]).replaceAll("_", " ");
					} else {
						System.out.printf("Error structure the partof: %s in the article: %s %s\n", partOf, lang, title);
						return "";
					}
				}
			}
			return "";
		}

		public static List<String> parsePartOfFromQuickFooter(List<String> list, String lang, String title) {
			List<String> l = null;
			if (list != null && !list.isEmpty()) {
				String partOf = list.get(0);
				String[] info = partOf.split("\\|");
				for (String s : info) {
					String value = null;
					int i = s.indexOf("=");
					if (i > 0) {
						String key = s.substring(0, i).trim().toLowerCase();
						String v = s.substring(i + 1).trim();
						if (!key.equals("livello") && !key.equals("w") && 
								!key.equals("commons") && v.length() > 0) {
							value = v;
						}
					} else if (s.trim().toUpperCase().equals("UNESCO")) {
						value = "WIKIDATAQ" + WID_WORLD_HERITAGE_SITE;
					}
					if (value != null) {
						if (l == null) {
							l = new ArrayList<String>();
						}
						l.add(0, value);
					}
				}
			}
			return l;
		}
	}

	private static String trim(String s) {
		return s.trim().replaceAll("[\\p{Cf}]", "");
	}	
}
