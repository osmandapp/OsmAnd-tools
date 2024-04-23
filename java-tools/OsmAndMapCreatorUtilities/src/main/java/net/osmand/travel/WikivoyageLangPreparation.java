package net.osmand.travel;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xmlpull.v1.XmlPullParserException;

import info.bliki.wiki.filter.HTMLConverter;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.gpx.GPXUtilities.WptPt;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.util.Algorithms;
import net.osmand.util.LocationParser;
import net.osmand.util.SqlInsertValuesReader;
import net.osmand.util.SqlInsertValuesReader.InsertValueProcessor;
import net.osmand.wiki.CustomWikiModel;
import net.osmand.wiki.WikiDatabasePreparation;
import net.osmand.wiki.WikiDatabasePreparation.WikiDBBrowser;
import net.osmand.wiki.WikiImageUrlStorage;

public class WikivoyageLangPreparation {
	private static final Log log = PlatformUtil.getLog(WikivoyageLangPreparation.class);	
	private static boolean uncompressed;
	private final static Map<Long, Long> KNOWN_WIKIVOYAGE_MAIN = new HashMap<Long, Long>();
	private static final long WID_TRAVEL_TOPICS = 14199938l;
	private static final long WID_DESTINATIONS = 1200957l;
	static {
		KNOWN_WIKIVOYAGE_MAIN.put(WID_DESTINATIONS, 0l); // Travel topics
		KNOWN_WIKIVOYAGE_MAIN.put(WID_TRAVEL_TOPICS, 0l); // Destinations
		KNOWN_WIKIVOYAGE_MAIN.put(1599788l, WID_TRAVEL_TOPICS); // Phrasebooks
		KNOWN_WIKIVOYAGE_MAIN.put(14208553l, WID_TRAVEL_TOPICS); // Discover
		KNOWN_WIKIVOYAGE_MAIN.put(1322323l, WID_TRAVEL_TOPICS); // Itineraries
		KNOWN_WIKIVOYAGE_MAIN.put(5056668l, WID_TRAVEL_TOPICS); // Sleep
		KNOWN_WIKIVOYAGE_MAIN.put(15l, WID_DESTINATIONS); // Africa
		KNOWN_WIKIVOYAGE_MAIN.put(51l, WID_DESTINATIONS); // Antarctica
		KNOWN_WIKIVOYAGE_MAIN.put(48l, WID_DESTINATIONS); // Asia
		KNOWN_WIKIVOYAGE_MAIN.put(49l, WID_DESTINATIONS); // North America
		KNOWN_WIKIVOYAGE_MAIN.put(55643l, WID_DESTINATIONS); // Oceania
		KNOWN_WIKIVOYAGE_MAIN.put(14201351l, WID_DESTINATIONS); // Other destinations
		KNOWN_WIKIVOYAGE_MAIN.put(18l, WID_DESTINATIONS); // South America
		KNOWN_WIKIVOYAGE_MAIN.put(46l, WID_DESTINATIONS); // Europe
	}
	private static final boolean DEBUG = false;
	private static final String SUFFIX_EN_REDIRECT = "en:";
	public enum WikivoyageOSMTags {
		TAG_WIKIDATA ("wikidata"),
		TAG_WIKIPEDIA ("wikipedia"),
		TAG_OPENING_HOURS ("opening_hours"),
		TAG_ADDRESS ("address"),
		TAG_EMAIL ("email"),
		TAG_DIRECTIONS ("directions"),
		TAG_PRICE ("price"),
		TAG_PHONE ("phone");

		private final String tg;

		private WikivoyageOSMTags(String tg) {
			this.tg = tg;
		}
		
		public String tag() {
			return tg;
		}
	}
	
	public static final String EMAIL = "Email";
	public static final String DIRECTIONS = "Directions";
	public static final String WORKING_HOURS = "Working hours";
	public static final String PRICE = "Price";
	public static final String PHONE = "Phone";
	
	public enum WikivoyageTemplates {
		LOCATION("geo"),
		POI("poi"),
		PART_OF("part_of"),
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
		processWikivoyage(wikiArticles, wikiProps, lang, wikivoyageSqlite, wikidataDB);
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
	}
	
	private static class PageInfos {
		private Map<Long, PageInfo> byId = new HashMap<Long, PageInfo>();
		private Map<Long, PageInfo> byWikidataId = new HashMap<Long, PageInfo>();
		private Map<String, PageInfo> byTitle = new HashMap<String, PageInfo>(); // only published pages
	}

	protected static void processWikivoyage(final File wikiArticles, final File wikiProps, 
			String lang, File wikivoyageSqlite, File wikidataSqlite)
			throws ParserConfigurationException, SAXException, IOException, SQLException {
		PageInfos pageInfos = readPageInfos(wikiProps);
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

	private static PageInfos readPageInfos(final File wikiProps) throws IOException {
		PageInfos pageInfos = new PageInfos();
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
	

	public static void createInitialDbStructure(Connection conn, boolean uncompressed) throws SQLException {
		conn.createStatement()
				.execute("CREATE TABLE IF NOT EXISTS travel_articles(title text, content_gz blob"
						+ (uncompressed ? ", content text" : "") + ", is_part_of text, lat double, lon double, image_title text, banner_title text, gpx_gz blob"
						+ (uncompressed ? ", gpx text" : "") + ", trip_id long, original_id long, lang text, contents_json text)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_title ON travel_articles(title);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_id ON travel_articles(trip_id);");
		conn.createStatement()
				.execute("CREATE INDEX IF NOT EXISTS index_part_of ON travel_articles(is_part_of);");
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
		private int batch = 0;
		private final static int BATCH_SIZE = 500;
		private static final String P_OPENED = "<p>";
		private static final String P_CLOSED = "</p>";
		final ByteArrayOutputStream bous = new ByteArrayOutputStream(64000);
		private String lang;
		
		private PageInfos pageInfos;
		private PageInfos enPageInfos;
		private Map<String, String> redirects = new HashMap<>();
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
			createInitialDbStructure(wikiVoyageConn, uncompressed);
			prepInsert = generateInsertPrep(wikiVoyageConn, uncompressed);
			enPageInfos = readEnPageInfo(wikiVoyageConn);
		}

		private PageInfos readEnPageInfo(Connection c) throws SQLException {
			Statement s = c.createStatement();
			PageInfos en = new PageInfos();
			ResultSet rs = s.executeQuery("select lang, title, trip_id, original_id, is_part_of, image_title, banner_title from travel_articles where lang='en' ");
			while (rs.next()) {
				PageInfo pi = new PageInfo();
//				pi.lang = rs.getString(1);
				pi.id = rs.getLong(4);
				pi.wikidataId = rs.getLong(3);
				pi.title = rs.getString(2);
				pi.partOf = rs.getString(5);
				pi.image = rs.getString(6);
				pi.banner = rs.getString(7);
				en.byWikidataId.put(pi.wikidataId, pi);
				en.byTitle.put(pi.title, pi);
				en.byId.put(pi.id, pi);
			}
			return en;
		}

		public void addBatch() throws SQLException {
			prepInsert.addBatch();
			if(batch++ > BATCH_SIZE) {
				prepInsert.executeBatch();
				batch = 0;
			}
		}

		public void finish() throws SQLException {
			prepInsert.executeBatch();
			if (!wikiVoyageConn.getAutoCommit()) {
				wikiVoyageConn.commit();
			}
			prepInsert.close();
			assignDefaultPartOfAndValidate();
			wikiVoyageConn.close();
			
		}

		private void assignDefaultPartOfAndValidate() throws SQLException {
//			System.out.println("Parent: " + parentStructure.keySet());
			ArrayList<String> redirectKeys = new ArrayList<String>(redirects.keySet());
			// 1. update wikidata to titles
			for (String s : redirectKeys) {
				if (s.startsWith("Q") && redirects.get(s).isEmpty()) {
					long wid = Long.parseLong(s.substring(1));
					PageInfo parentPage = pageInfos.byWikidataId.get(wid);
					PageInfo enPage = enPageInfos.byWikidataId.get(wid);
					if (parentPage != null && parentPage.title != null) {
						redirects.put(s, parentPage.title);
					} else if (enPage != null && enPage.title != null) {
						System.out.printf("Warning redirect to en %s '%s' -> '%s' redirect to %s \n", lang, s, SUFFIX_EN_REDIRECT + parentPage.title);
						redirects.put(s, SUFFIX_EN_REDIRECT + parentPage.title);
					} else {
						System.out.printf("Error parent redirect %s to %s is not \n", lang, s);

					}
				} else if(redirects.get(s).isEmpty()) {
					System.out.printf("Error parent redirect %s to %s is not \n", lang, s);
				}
			}
			// fix non existing parent to en
			for (PageInfo p : pageInfos.byId.values()) {
				String partOf = p.partOf;
				if (redirects.containsKey(partOf)) {
					partOf = redirects.get(partOf);
				}
				if (!Algorithms.isEmpty(partOf) && !pageInfos.byTitle.containsKey(partOf)) {
					PageInfo enPage = enPageInfos.byWikidataId.get(p.wikidataId);
					if(enPage != null && enPageInfos.byTitle.get(enPage.partOf) != null){
						PageInfo enParent = enPageInfos.byTitle.get(enPage.partOf);
						String enPartOf = SUFFIX_EN_REDIRECT + enParent.title;
						System.out.printf("Warning parent structure %s '%s' -> '%s' redirect to %s \n", lang, p.title, partOf, enPartOf);
						redirects.put(p.partOf, enPartOf);
					}
				}
			}
			/// update redirects in tables
			PreparedStatement ps = wikiVoyageConn.prepareStatement("UPDATE travel_articles SET is_part_of = ? WHERE lang = ? and is_part_of =  ?");
			TreeSet<String> redirectKey = new TreeSet<>(redirects.keySet());
			for (String redirectFrom : redirectKey) {
				String redirectTo = redirects.get(redirectFrom);
				ps.setString(1, redirectTo);
				ps.setString(2, lang);
				ps.setString(3, redirectFrom);
				ps.execute();
//				System.out.println("Redirect from " + valueParent+ " to " + actualTarget);
			}
			for (PageInfo p : pageInfos.byId.values()) {
				String partOf = p.partOf;
				if (partOf == null) {
					continue;
				}
				if (redirects.containsKey(partOf)) {
					partOf = redirects.get(partOf);
				}
				if (partOf.startsWith(SUFFIX_EN_REDIRECT)) {
					// skip redirect
					if (!enPageInfos.byTitle.containsKey(partOf.substring(SUFFIX_EN_REDIRECT.length()))) {
						System.out.printf("Error parent structure %s '%s' -> '%s' \n", lang, p.title, partOf);
					}
				} else if (!Algorithms.isEmpty(partOf) && !pageInfos.byTitle.containsKey(partOf)) {
					System.out.printf("Error parent structure %s '%s' -> '%s' \n", lang, p.title, partOf);
				}
			}
		}
		
		public String getStandardPartOf(Map<WikivoyageTemplates, List<String>> macroBlocks, PageInfo enPage) {
			long wid = 0;
			if (macroBlocks.containsKey(WikivoyageTemplates.PHRASEBOOK)) {
				wid = 1599788; // Q1599788 -- Phrasebooks
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
							String textStr = ctext.toString().trim().toLowerCase();
							if (textStr.startsWith("#redirect") || textStr.startsWith("#weiterleitung") ||
									textStr.startsWith("#перенаправление") || textStr.startsWith("#patrz") ||
									textStr.startsWith("#перенаправлення") || textStr.startsWith("#doorverwijzing")
									) {
								// redirect
								int l = ctext.indexOf("[[");
								int e = ctext.indexOf("]]");
								if (l > 0 && e > 0) {
									redirects.put(title, trim(ctext.substring(l + 2, e)));
								}
							} else if (cInfo == null) {
								// debug https://de.wikivoyage.org/wiki/Special:Export/Frankfurt_am_Main/Nordwesten
								// possibly no wikidata id, no banner (so no properties) - de 116081
								System.err.printf("Error with page %d %s - empty info\n", cid, title);
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
			cInfo.title = title;
			String text = WikiDatabasePreparation.removeMacroBlocks(cont, macroBlocks, lang, title, dbBrowser);
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
				if (ll == null) {
					ll = getLatLonFromGeoBlock(macroBlocks.get(WikivoyageTemplates.LOCATION), lang, title);
				}
				boolean accepted = true;// filtered by namespace !title.toString().contains(":");
				if (accepted) {
					int column = 1;
					String filename = getFileName(macroBlocks.get(WikivoyageTemplates.BANNER));
					if (id++ % 500 == 0) {
						log.debug(String.format("Article accepted %d %s %s free: %s\n", cid, title, ll,
								Runtime.getRuntime().freeMemory() / (1024 * 1024)));
					}
					final HTMLConverter converter = new HTMLConverter(false);
					CustomWikiModel wikiModel = new CustomWikiModel(
							// Images could be different compare... 
							// https://upload.wikimedia.org/wikivoyage/de/thumb/d/d4/Museo_in_San_Juan_verkl.jpg/600px-Museo_in_San_Juan_verkl.jpg
							// https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Percys_and_Kruis2.png/1300px-Percys_and_Kruis2.png
							"https://upload.wikimedia.org/wikipedia/commons/${image}",
							"https://" + lang + ".wikivoyage.org/wiki/${title}", imageUrlStorage, false);
					String plainStr = wikiModel.render(converter, text);
					plainStr = plainStr.replaceAll("<p>div class=&#34;content&#34;", "<div class=\"content\">\n<p>")
							.replaceAll("<p>/div\n</p>", "</div>");

					if (DEBUG) {
						String savePath = "/Users/plotva/Documents";
						File myFile = new File(savePath, "page.html");
						BufferedWriter htmlFileWriter = new BufferedWriter(new FileWriter(myFile, false));
						htmlFileWriter.write(plainStr);
						htmlFileWriter.close();
					}

					// part_of
					String partOf = parsePartOf(macroBlocks.get(WikivoyageTemplates.PART_OF));
					if (partOf == null) {
						// this disamb or redirection
						return;
					}
					
					if (partOf.length() == 0 || KNOWN_WIKIVOYAGE_MAIN.containsKey(cInfo.wikidataId)) {
						partOf = getStandardPartOf(macroBlocks, enPage).trim();
					}
					partOf = trim(partOf);

					// prep.setString(column++, Encoder.encodeUrl(title.toString()));
					prepInsert.setString(column++, title.toString());
					prepInsert.setBytes(column++, stringToCompressedByteArray(bous, plainStr));
					if (uncompressed) {
						prepInsert.setString(column++, plainStr);
					}
					prepInsert.setString(column++, partOf);
					if (Algorithms.isEmpty(partOf)) {
						long wid = cInfo == null ? 0 : cInfo.wikidataId;
						if (wid != WID_DESTINATIONS && wid != WID_TRAVEL_TOPICS) {
							System.out.println("Warning ignore root article: " + lang + " Q" + wid + " " + " " + title);
						}
					}
					
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
					String gpx = generateGpx(macroBlocks.get(WikivoyageTemplates.POI), title.toString(), lang,
							getShortDescr(plainStr), ll);
					prepInsert.setBytes(column++, stringToCompressedByteArray(bous, gpx));
					if (uncompressed) {
						prepInsert.setString(column++, gpx);
					}
					// trip id equals to wikidata id
					prepInsert.setLong(column++, cInfo.wikidataId);
					prepInsert.setLong(column++, cid);
					prepInsert.setString(column++, lang);
					prepInsert.setString(column++, wikiModel.getContentsJson());
					addBatch();

				}
			} catch (SQLException e) {
				throw new SAXException(e);
			}
		}
		
		
		private String getShortDescr(String content) {
			if (content == null) {
				return null;
			}

			int firstParagraphStart = content.indexOf(P_OPENED);
			int firstParagraphEnd = content.indexOf(P_CLOSED);
			firstParagraphEnd = firstParagraphEnd < firstParagraphStart ? content.indexOf(P_CLOSED, firstParagraphStart) : firstParagraphEnd;
			if (firstParagraphStart == -1 || firstParagraphEnd == -1
					|| firstParagraphEnd < firstParagraphStart) {
				return null;
			}
			String firstParagraphHtml = content.substring(firstParagraphStart, firstParagraphEnd + P_CLOSED.length());
			while (firstParagraphHtml.length() == (P_OPENED.length() + P_CLOSED.length())
					&& (firstParagraphEnd + P_CLOSED.length()) < content.length()) {
				firstParagraphStart = content.indexOf(P_OPENED, firstParagraphEnd);
				firstParagraphEnd = firstParagraphStart == -1 ? -1 : content.indexOf(P_CLOSED, firstParagraphStart);
				if (firstParagraphStart != -1 && firstParagraphEnd != -1) {
					firstParagraphHtml = content.substring(firstParagraphStart, firstParagraphEnd + P_CLOSED.length());
				} else {
					break;
				}
			}
			return firstParagraphHtml;
		}

		public static String capitalizeFirstLetterAndLowercase(String s) {
			if (s != null && s.length() > 1) {
				// not very efficient algorithm
				return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
			} else {
				return s;
			}
		}
		
		private String generateGpx(List<String> list, String title, String lang, String descr, LatLon ll) {
			if (list != null && !list.isEmpty()) {
				GPXFile f = new GPXFile(title, lang, descr);
				List<WptPt> points = new ArrayList<>(); 
				for (String s : list) {
					String[] info = s.split("\\|");
					WptPt point = new WptPt();
					String category = trim(info[0].replaceAll("\n", ""));
					point.category = category;
					if (category.equalsIgnoreCase("vcard") || category.equalsIgnoreCase("listing")) {
						point.category = transformCategory(info);
					}
					if (!Algorithms.isEmpty(point.category)) {
						point.category = capitalizeFirstLetterAndLowercase(trim(point.category));
					}
					String areaCode = "";
					Map<String, String> extraValues = new LinkedHashMap<String, String>();
					for (int i = 1; i < info.length; i++) {
						String field = trim(info[i]);
						String value = "";
						int index = field.indexOf("=");
						if (index != -1) {
							value = WikiDatabasePreparation.appendSqareBracketsIfNeeded(i, info, field.substring(index + 1, field.length()).trim());
							value = value.replaceAll("[\\]\\[]", "");
							field = field.substring(0, index).trim();
						}
						
						if (!value.isEmpty() && !value.contains("{{")) {
							String lat = "";
							String lon = "";
							try {
								if (field.equalsIgnoreCase("name") || field.equalsIgnoreCase("nome") || field.equalsIgnoreCase("nom")
										|| field.equalsIgnoreCase("שם") || field.equalsIgnoreCase("نام")) {
									point.name = value;
								} else if (field.equalsIgnoreCase("url") || field.equalsIgnoreCase("sito") || field.equalsIgnoreCase("האתר הרשמי")
										|| field.equalsIgnoreCase("نشانی اینترنتی")) {
									point.link = value;
								} else if (field.equalsIgnoreCase("intl-area-code")) {
									areaCode = value;
								} else if (field.equalsIgnoreCase("lat") || field.equalsIgnoreCase("latitude") || field.equalsIgnoreCase("عرض جغرافیایی")) {
									lat = value.trim();
								} else if (field.equalsIgnoreCase("long") || field.equalsIgnoreCase("longitude") || field.equalsIgnoreCase("طول جغرافیایی")) {
									lon = value.trim();
								} else if (field.equalsIgnoreCase("content") || field.equalsIgnoreCase("descrizione") 
										|| field.equalsIgnoreCase("description") || field.equalsIgnoreCase("sobre") || field.equalsIgnoreCase("תיאור")
										|| field.equalsIgnoreCase("متن")) {
									point.desc = value;
								} else if (field.equalsIgnoreCase("wikidata")) {
									point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_WIKIDATA.tag(), value);
								} else if (field.equalsIgnoreCase("wikipedia")) {
									point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_WIKIPEDIA.tag(), value);
								} else if (field.equalsIgnoreCase("address")) {
									point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_ADDRESS.tag(), value);
								} else if (field.equalsIgnoreCase("email") || field.equalsIgnoreCase("מייל") || field.equalsIgnoreCase("پست الکترونیکی")) {
									extraValues.put(EMAIL, value);
									point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_EMAIL.tag(), value);
								} else if (field.equalsIgnoreCase("phone") || field.equalsIgnoreCase("tel") || field.equalsIgnoreCase("téléphone")
										|| field.equalsIgnoreCase("טלפון") || field.equalsIgnoreCase("تلفن")) {
									extraValues.put(PHONE, value);
									point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_PHONE.tag(), value);
								} else if (field.equalsIgnoreCase("price") || field.equalsIgnoreCase("prezzo") || field.equalsIgnoreCase("prix") 
										|| field.equalsIgnoreCase("מחיר") || field.equalsIgnoreCase("بها")) {
									extraValues.put(PRICE, value);
									point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_PRICE.tag(), value);
								} else if (field.equalsIgnoreCase("hours") || field.equalsIgnoreCase("orari") || field.equalsIgnoreCase("horaire") 
										|| field.equalsIgnoreCase("funcionamento") || field.equalsIgnoreCase("שעות") || field.equalsIgnoreCase("ساعت‌ها")) {
									extraValues.put(WORKING_HOURS, value);
									point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_OPENING_HOURS.tag(), value);
								} else if (field.equalsIgnoreCase("directions") || field.equalsIgnoreCase("direction") || field.equalsIgnoreCase("הוראות")
										|| field.equalsIgnoreCase("مسیرها") || field.equalsIgnoreCase("address")) {
									extraValues.put(DIRECTIONS, value);
									point.getExtensionsToWrite().put(WikivoyageOSMTags.TAG_DIRECTIONS.tag(), value);
								}
								if (isEmpty(lat) || isEmpty(lon)) {
									// skip empty
								} else {
									LatLon loct = parseLocation(lat, lon);
									if (loct == null) {
										System.out.printf("Error parsing (%s %s): %s %s\n", lang, title, lat, lon);
									} else {
										point.lat = loct.getLatitude();
										point.lon = loct.getLongitude();
									}
								}
							} catch (RuntimeException e) {
								System.out.printf("Error parsing (%s %s): %s\n", lang, title, e.getMessage());
							}
						}
					}
					for (String key : extraValues.keySet()) {
						if (point.desc == null) {
							point.desc = "";
						} else {
							point.desc += "\n\r";
						}
						String value = extraValues.get(key);
						if (areaCode.length() > 0 && key.equals(PHONE)) {
							value = areaCode + " " + value;
						}
						point.desc += key + ": " + value; // ". " backward compatible
					}
					if (!point.hasLocation() && ll != null) {
						// coordinates of article
						point.lat = ll.getLatitude();
						point.lon = ll.getLongitude();
					}
					if (point.hasLocation() && point.name != null && !point.name.isEmpty()) {
						if (point.category != null) {
							if (point.category.equalsIgnoreCase("see") || point.category.equalsIgnoreCase("do")) {
								point.setColor(0xCC10A37E);
								point.setIconName("special_photo_camera");
							} else if (point.category.equalsIgnoreCase("eat") || point.category.equalsIgnoreCase("drink")) {
								point.setColor(0xCCCA2D1D);
								point.setIconName("restaurants");
							} else if (point.category.equalsIgnoreCase("sleep")) {
								point.setColor(0xCC0E53C9);
								point.setIconName("tourism_hotel");
							} else if (point.category.equalsIgnoreCase("buy") || point.category.equalsIgnoreCase("listing")) {
								point.setColor(0xCC8F2BAB);
								point.setIconName("shop_department_store");
							} else if (point.category.equalsIgnoreCase("go")) {
								point.setColor(0xCC0F5FFF);
								point.setIconName("public_transport_stop_position");
							}
						}
						points.add(point);
					}
				}
				if (!points.isEmpty()) {
					f.addPoints(points);
					return GPXUtilities.asString(f);
				}
			}
			return "";
		}
		
		private String transformCategory(String[] info) {
			String type = "";
			for (int i = 1; i < info.length; i++) {
				if (info[i].trim().startsWith("type")) {
					type = info[i].substring(info[i].indexOf("=") + 1, info[i].length()).trim();
				}
			}
			return type;
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
		
		private static LatLon getLatLonFromGeoBlock(List<String> list, String lang, String title) {
			if (list != null && !list.isEmpty()) {
				String location = list.get(0) + " "; // to parse "geo||"
				String[] parts = location.split("\\|");
				LatLon ll = null;
				if (parts.length >= 3) {
					String lat = null;
					String lon = null;
					if (parts[0].trim().equalsIgnoreCase("geo")) {
						lat = parts[1];
						lon = parts[2];
					} else {// if(parts[0].trim().equalsIgnoreCase("geodata")) {
						for (String part : parts) {
							String p = part.trim();
							if (p.startsWith("lat=")) {
								lat = p.substring("lat=".length());
							} else if (p.startsWith("long=")) {
								lon = p.substring("long=".length());
							} else if (p.startsWith("latitude=")) {
								lat = p.substring("latitude=".length());
							} else if (p.startsWith("longitude=")) {
								lon = p.substring("longitude=".length());
							}
						}
					}
					if (isEmpty(lat) || isEmpty(lon)) {
						return null;
					}
					if (lat != null && lon != null) {
						ll = parseLocation(lat, lon);
					}
				}
				if (ll == null) {
					System.err.printf("Error geo (%s %s): %s \n", lang, title, location);
				}
				return ll;
			}
			return null;
		}

		private String parsePartOf(List<String> list) {
			if (list != null && !list.isEmpty()) {
				String partOf = list.get(0);
				String lowerCasePartOf = partOf.toLowerCase();
				if (lowerCasePartOf.contains("quickfooter")) {
					return parsePartOfFromQuickBar(partOf);
				} if (lowerCasePartOf.startsWith("navigation")) {
					String[] splitPartOf = partOf.split(" ");
					if (splitPartOf.length > 1) {
						return splitPartOf[1];
					} else {
						System.out.println("Error parsing the partof: " + partOf + " in the article: " + title);
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
							return "Q1322323";
						} else if (type.equalsIgnoreCase("континент")) {
							return "Q1200957";
						} else if (type.equalsIgnoreCase("сводная") || type.equalsIgnoreCase("природа")
								|| type.equalsIgnoreCase("наследие")) {
							return null;
						} else {
							System.out.printf("Error parsing the partof: %s (%s) in the article: %s\n", partOf, type, title);
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
						System.out.println("Error parsing the partof (else): " + partOf + " in the article: " + title);
						return "";
					}
				}
			}
			return "";
		}

		private String parsePartOfFromQuickBar(String partOf) {
			String[] info = partOf.split("\\|");
			String region = "";
			for (String s : info) {
				if (s.contains("=")) {
					if (!s.toLowerCase().contains("livello")) {
						region = s.substring(s.indexOf("=") + 1).trim();
					}
				}
			}
			return region;
		}
	}

	public static boolean isEmpty(String lat) {
		return "".equals(lat) || "NA".equals(lat)  || "N/A".equals(lat) ;
	}

	public static LatLon parseLocation(String lat, String lon) {
		String loc = lat + " " + lon;
		if (!loc.contains(".") && loc.contains(",")) {
			loc = loc.replace(',', '.');
		}
		return LocationParser.parseLocation(loc);
	}

	private static String trim(String s) {
		return s.trim().replaceAll("[\\p{Cf}]", "");
	}	
}
