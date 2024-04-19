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
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import net.osmand.data.LatLon;
import net.osmand.util.Algorithms;
import net.osmand.wiki.*;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import info.bliki.wiki.filter.Encoder;
import info.bliki.wiki.filter.HTMLConverter;
import net.osmand.PlatformUtil;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.gpx.GPXUtilities.WptPt;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.util.SqlInsertValuesReader;
import net.osmand.util.SqlInsertValuesReader.InsertValueProcessor;
import org.xmlpull.v1.XmlPullParserException;

public class WikivoyageLangPreparation {
	private static final Log log = PlatformUtil.getLog(WikivoyageLangPreparation.class);	
	private static boolean uncompressed;
	
	private static final boolean DEBUG = false;
	
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
		TRANSLATION("translation");

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
		File commonsWikiSqlite = new File(wikidataSqliteName);
		processWikivoyage(wikiArticles, wikiProps, lang, wikivoyageSqlite, commonsWikiSqlite);
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
		public String wikidataId;
	}

	protected static void processWikivoyage(final File wikiArticles, final File wikiProps, String lang,
	                                        File wikivoyageSqlite, File wikidataSqlite)
			throws ParserConfigurationException, SAXException, IOException, SQLException {

		Map<Long, PageInfo> pageInfos = new LinkedHashMap<Long, WikivoyageLangPreparation.PageInfo>();
		SqlInsertValuesReader.readInsertValuesFile(wikiProps.getAbsolutePath(), new InsertValueProcessor() {
			
			@Override
			public void process(List<String> vs) {
				if(vs.get(1).equals("wpb_banner")) {
					getPageInfo(vs).banner = vs.get(2);
				} else if(vs.get(1).equals("wikibase_item")) {
					getPageInfo(vs).wikidataId = vs.get(2);
				} else if(vs.get(1).equals("page_image_free")) {
					getPageInfo(vs).image = vs.get(2);
					
				}
			}
			private PageInfo getPageInfo(List<String> vs) {
				long id = Long.parseLong(vs.get(0));
				PageInfo p = pageInfos.get(id);
				if ( p == null) {
					p = new PageInfo();
					p.id = id;
					pageInfos.put(id, p);
				}
				return p;
			}
		});
		
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		InputStream articlesStream = new BufferedInputStream(new FileInputStream(wikiArticles), 8192 * 4);
		BZip2CompressorInputStream zis = new BZip2CompressorInputStream(articlesStream);
		Reader reader = new InputStreamReader(zis, "UTF-8");
		InputSource articlesSource = new InputSource(reader);
		articlesSource.setEncoding("UTF-8");
		OsmCoordinatesByTag osmCoordinates = new OsmCoordinatesByTag(new String[]{"wikipedia", "wikidata"},
				new String[]{"wikidata"}).parse(wikidataSqlite, false);
		WikivoyageHandler handler = new WikivoyageHandler(sx, articlesStream, lang, wikivoyageSqlite, pageInfos,
				osmCoordinates);
		sx.parse(articlesSource, handler);
		handler.finish();
		osmCoordinates.closeConnection();
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
		private long cid;
		private PageInfo cInfo;
		private StringBuilder title = new StringBuilder();
		private StringBuilder text = new StringBuilder();
		private StringBuilder pageId = new StringBuilder();
		
		private boolean parseText = false;

		private final InputStream progIS;
		private ConsoleProgressImplementation progress = new ConsoleProgressImplementation();
		private DBDialect dialect = DBDialect.SQLITE;
		private Connection conn;
		private final WikiImageUrlStorage imageUrlStorage;
		private OsmCoordinatesByTag osmCoordinates;
		private PreparedStatement prep;
		private int batch = 0;
		private final static int BATCH_SIZE = 500;
		private static final String P_OPENED = "<p>";
		private static final String P_CLOSED = "</p>";
		final ByteArrayOutputStream bous = new ByteArrayOutputStream(64000);
		private String lang;
		private WikidataConnection wikidataconn;
		private Map<Long, PageInfo> pageInfos;

		WikivoyageHandler(SAXParser saxParser, InputStream progIS, String lang, File wikivoyageSqlite, Map<Long,
				PageInfo> pageInfos, OsmCoordinatesByTag osmCoordinates)
				throws IOException, SQLException {
			this.lang = lang;
			this.saxParser = saxParser;
			this.progIS = progIS;
			this.pageInfos = pageInfos;
			this.osmCoordinates = osmCoordinates;
			progress.startTask("Parse wikivoyage xml", progIS.available());
			conn = dialect.getDatabaseConnection(wikivoyageSqlite.getAbsolutePath(), log);
			imageUrlStorage = new WikiImageUrlStorage(conn, wikivoyageSqlite.getParent(), lang);
			createInitialDbStructure(conn, uncompressed);
			prep = generateInsertPrep(conn, uncompressed);
			wikidataconn = new WikidataConnection(new File(wikivoyageSqlite.getParentFile(), "wikidata.sqlite"));
		}

		public void addBatch() throws SQLException {
			prep.addBatch();
			if(batch++ > BATCH_SIZE) {
				prep.executeBatch();
				batch = 0;
			}
		}

		public void finish() throws SQLException {
			prep.executeBatch();
			if(!conn.getAutoCommit()) {
				conn.commit();
			}
			prep.close();
			conn.close();
			wikidataconn.close();
		}

		public int getCount() {
			return (int) (id - 1);
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			String name = saxParser.isNamespaceAware() ? localName : qName;
			if (!page) {
				page = name.equals("page");
			} else {
				if (name.equals("title")) {
					title.setLength(0);
					ctext = title;
				} else if (name.equals("text")) {
					if(parseText) {
						text.setLength(0);
						ctext = text;
					}
				} else if (name.equals("revision")) {
					revision  = true;
				} else if (name.equals("id") && !revision) {
					pageId.setLength(0);
					ctext = pageId;
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
						parseText = false;
						progress.remaining(progIS.available());
					} else if (name.equals("title")) {
						ctext = null;
					} else if (name.equals("revision")) {
						revision = false;
					} else if (name.equals("id") && !revision) {
						ctext = null;
						cid = Long.parseLong(pageId.toString());
						cInfo = pageInfos.get(cid);
						parseText = true;
					} else if (name.equals("text")) {
						if (parseText ) {
							if (cInfo == null) {
								// System.out.println("Empty page prop: " + title + " " + cid);
							} else if(cInfo.wikidataId == null) {
								System.out.println("Empty wikidata id prop: " + title + " " + cid);
							} else {
								parseText(ctext.toString());
							}
						}
						ctext = null;
					}
				}
			} catch (IOException | SQLException e) {
				throw new SAXException(e);
			}
		}

		private void parseText(String cont) throws IOException, SQLException, SAXException {
			Map<String, List<String>> macroBlocks = new HashMap<>();
			String text = WikiDatabasePreparation.removeMacroBlocks(cont, macroBlocks, lang, wikidataconn, osmCoordinates);
			try {
				if (!macroBlocks.isEmpty()) {
					LatLon ll = osmCoordinates.getCoordinatesFromCommonsWikiDB(
							pageInfos.get(Long.parseLong(pageId.toString())).wikidataId);
					if (ll == null) {
						ll = getLatLonFromGeoBlock(macroBlocks.get(WikivoyageTemplates.LOCATION.getType()));
					}
					boolean accepted = !title.toString().contains(":");
					if (accepted) {
						int column = 1;
						String filename = getFileName(macroBlocks.get(WikivoyageTemplates.BANNER.getType()));
						filename = filename.startsWith("<!--") ? "" : filename;
						if (id++ % 500 == 0) {
							log.debug("Article accepted " + cid + " " + title.toString() + " " + ll.getLatitude() + " "
									+ ll.getLongitude() + " free: "
									+ (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
						}
						final HTMLConverter converter = new HTMLConverter(false);
						CustomWikiModel wikiModel = new CustomWikiModel(
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
						
						// prep.setString(column++, Encoder.encodeUrl(title.toString()));
						prep.setString(column++, title.toString());
						prep.setBytes(column++, stringToCompressedByteArray(bous, plainStr));
						if (uncompressed) {
							prep.setString(column++, plainStr);
						}
						// part_of
						prep.setString(column++, parsePartOf(macroBlocks.get(WikivoyageTemplates.PART_OF.getType())));
						if (ll.getLongitude() == 0 && ll.getLatitude() == 0) {
							prep.setNull(column++, Types.DOUBLE);
							prep.setNull(column++, Types.DOUBLE);
						} else {
							prep.setDouble(column++, ll.getLatitude());
							prep.setDouble(column++, ll.getLongitude());
						}
						// banner
						if (cInfo.image != null) {
							prep.setString(column++, cInfo.image);
						} else {
							column++;
						}
						if (cInfo.banner != null) {
							prep.setString(column++, cInfo.banner);
						} else {
							prep.setString(column++,
									Encoder.encodeUrl(filename).replaceAll("\\(", "%28").replaceAll("\\)", "%29"));
						}

						// gpx_gz
						String gpx = generateGpx(macroBlocks.get(WikivoyageTemplates.POI.getType()), title.toString(),
								lang, getShortDescr(plainStr));
						prep.setBytes(column++, stringToCompressedByteArray(bous, gpx));
						if (uncompressed) {
							prep.setString(column++, gpx);
						}
						// trip id equals to wikidata id
						prep.setLong(column++, Long.parseLong(cInfo.wikidataId.substring(1)));
						prep.setLong(column++, cid);
						prep.setString(column++, lang);
						prep.setString(column++, wikiModel.getContentsJson());
						addBatch();

					}
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
		
		private String generateGpx(List<String> list, String title, String lang, String descr) {
			if (list != null && !list.isEmpty()) {
				GPXFile f = new GPXFile(title, lang, descr);
				List<WptPt> points = new ArrayList<>(); 
				for (String s : list) {
					String[] info = s.split("\\|");
					WptPt point = new WptPt();
					String category = info[0].replaceAll("\n", "").trim();
					point.category = category;
					if (category.equalsIgnoreCase("vcard") || category.equalsIgnoreCase("listing")) {
						point.category = transformCategory(info);
					}
					if (!Algorithms.isEmpty(point.category)) {
						point.category = capitalizeFirstLetterAndLowercase(point.category.trim());
					}
					String areaCode = "";
					Map<String, String> extraValues = new LinkedHashMap<String, String>();
					for (int i = 1; i < info.length; i++) {
						String field = info[i].trim();
						String value = "";
						int index = field.indexOf("=");
						if (index != -1) {
							value = WikiDatabasePreparation.appendSqareBracketsIfNeeded(i, info, field.substring(index + 1, field.length()).trim());
							value = value.replaceAll("[\\]\\[]", "");
							field = field.substring(0, index).trim();
						}
						
						if (!value.isEmpty() && !value.contains("{{")) {
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
									point.lat = Double.valueOf(value);
								} else if (field.equalsIgnoreCase("long") || field.equalsIgnoreCase("longitude") || field.equalsIgnoreCase("طول جغرافیایی")) {
									point.lon = Double.valueOf(value);
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
							} catch (Exception e) {}
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
		
		private LatLon getLatLonFromGeoBlock(List<String> list) {
			
			if (list != null && !list.isEmpty()) {
				String location = list.get(0);
				return parseLatLon(location);
			}
			return new LatLon(0, 0);
		}

		private static LatLon parseLatLon(String location) {
			double lat = 0d;
			double lon = 0d;
			

			String[] parts = location.split("\\|");
			// skip malformed location blocks
			String regex_pl = "(\\d+)°.+?(\\d+).+?(\\d*).*?";
			if (location.toLowerCase().contains("geo|")) {
				if (parts.length >= 3) {
					if (parts[1].matches(regex_pl) && parts[2].matches(regex_pl)) {
						lat = toDecimalDegrees(parts[1], regex_pl);
						lon = toDecimalDegrees(parts[2], regex_pl);
					} else {
						try {
							lat = Double.valueOf(parts[1]);
							lon = Double.valueOf(parts[2]);
						} catch (Exception e) {	}
					}
				}
			} else {
				String latStr = "";
				String lonStr = "";
				String regex = "(\\d+).+?(\\d+).+?(\\d*).*?([N|E|W|S|n|e|w|s]+)";
				for (String part : parts) {
					part = part.replaceAll(" ", "").toLowerCase();
					if (part.startsWith("lat=") || part.startsWith("latitude=")) {
						latStr = part.substring(part.indexOf("=") + 1, part.length()).replaceAll("\n", "");
					} else if (part.startsWith("lon=") || part.startsWith("long=") || part.startsWith("longitude=")) {
						lonStr = part.substring(part.indexOf("=") + 1, part.length()).replaceAll("\n", "");
					}
				}
				if (latStr.matches(regex) && lonStr.matches(regex)) {
					lat = toDecimalDegrees(latStr, regex);
					lon = toDecimalDegrees(lonStr, regex);
				} else {
					try {
						lat = Double.valueOf(latStr.replaceAll("°", ""));
						lon = Double.valueOf(lonStr.replaceAll("°", ""));
					} catch (Exception e) {}
				}
			}
			return new LatLon(lat, lon);
		}

		private static double toDecimalDegrees(String str, String regex) {
			Pattern p = Pattern.compile(regex);
			Matcher m = p.matcher(str);
			m.find();
			double res = 0;
			double signe = 1.0;
			double degrees = 0;
			double minutes = 0;
			double seconds = 0;
			String hemisphereOUmeridien = "";
			try {
				degrees = Double.parseDouble(m.group(1));
				minutes = Double.parseDouble(m.group(2));
				seconds = m.group(3).isEmpty() ? 0 : Double.parseDouble(m.group(3));
				hemisphereOUmeridien = m.group(4);
			} catch (Exception e) {
				// Skip malformed strings
			}
			if ((hemisphereOUmeridien.equalsIgnoreCase("W")) || (hemisphereOUmeridien.equalsIgnoreCase("S"))) {
				signe = -1.0;
			}
			res = signe * (Math.floor(degrees) + Math.floor(minutes) / 60.0 + seconds / 3600.0);
			return res;
		}

		private String parsePartOf(List<String> list) {
			if (list != null && !list.isEmpty()) {
				String partOf = list.get(0);
				String lowerCasePartOf = partOf.toLowerCase();
				if (lowerCasePartOf.contains("quickfooter")) {
					return parsePartOfFromQuickBar(partOf);
				} else if (lowerCasePartOf.startsWith("footer|")) {
					String part = "";
					try {
						int index = partOf.indexOf('|', partOf.indexOf('|') + 1);
						part = partOf.substring(partOf.indexOf("=") + 1, 
								index == -1 ? partOf.length() : index);
					} catch (Exception e) {
						System.out.println("Error parsing the partof: " + partOf  + " in the article: " + title);
					}
					return part.trim().replaceAll("_", " ");
				} else if (lowerCasePartOf.contains("קטגוריה")) {
					return partOf.substring(partOf.indexOf(":") + 1).trim().replaceAll("[_\\|\\*]", "");
				} else {
					String[] splitPartOf = partOf.split("\\|");
					if (splitPartOf.length > 1) {
						return splitPartOf[1].trim().replaceAll("_", " ");
					} else {
						System.out.println("Error parsing the partof: " + partOf + " in the article: " + title);
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
}
