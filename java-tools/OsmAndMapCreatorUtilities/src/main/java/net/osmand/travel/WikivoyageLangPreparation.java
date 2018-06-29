package net.osmand.travel;

import info.bliki.wiki.filter.Encoder;
import info.bliki.wiki.filter.HTMLConverter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import net.osmand.PlatformUtil;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.travel.GPXUtils.GPXFile;
import net.osmand.travel.GPXUtils.WptPt;
import net.osmand.wiki.CustomWikiModel;
import net.osmand.wiki.WikiDatabasePreparation;
import net.osmand.wiki.WikiDatabasePreparation.LatLon;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class WikivoyageLangPreparation {
	private static final Log log = PlatformUtil.getLog(WikiDatabasePreparation.class);	
	private static boolean uncompressed;
	
	
	public enum WikivoyageTemplates {
		LOCATION("geo"),
		POI("poi"),
		PART_OF("part_of"),
		BANNER("pagebanner"),
		REGION_LIST("regionlist"), 
		WARNING("warningbox");
		
		private String type;
		WikivoyageTemplates(String s) {
			type = s;
		}
		
		public String getType() {
			return type;
		}
	}
	
	
	private static void testLatLonParse() {
		// {{}}
		System.out.println(WikiOsmHandler.parseLatLon("geo|-34.60|-58.38|zoom=4"));
		System.out.println(WikiOsmHandler.parseLatLon("geo|48.856|2.351"));
		System.out.println(WikiOsmHandler.parseLatLon("geo|7.4|14.5|zoom=6"));

		

	}
	
	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, SQLException {
		String lang = "";
		String folder = "";
		if(args.length == 0) {
			lang = "en";
			folder = "/home/user/osmand/wikivoyage/";
			uncompressed = true;
		}
		if(args.length > 0) {
			lang = args[0];
		}
		if(args.length > 1){
			folder = args[1];
		}
		if(args.length > 2){
			uncompressed = args[2].equals("uncompressed");
		}
		final String wikiPg = folder + lang + "wikivoyage-latest-pages-articles.xml.bz2";
		
		if (!new File(wikiPg).exists()) {
			System.out.println("Dump for " + lang + " doesn't exist");
			return;
		}
		final String sqliteFileName = folder + (uncompressed ? "full_" : "") + "wikivoyage.sqlite";	
		processWikivoyage(wikiPg, lang, sqliteFileName);
		System.out.println("Successfully generated.");
    }
	
	protected static void processWikivoyage(final String wikiPg, String lang, String sqliteFileName)
			throws ParserConfigurationException, SAXException, FileNotFoundException, IOException, SQLException {
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		InputStream streamFile = new BufferedInputStream(new FileInputStream(wikiPg), 8192 * 4);
		BZip2CompressorInputStream zis = new BZip2CompressorInputStream(streamFile);
		Reader reader = new InputStreamReader(zis,"UTF-8");
		InputSource is = new InputSource(reader);
		is.setEncoding("UTF-8");
		final WikiOsmHandler handler = new WikiOsmHandler(sx, streamFile, lang, new File(sqliteFileName));
		sx.parse(is, handler);
		handler.finish();
	}

	public static void createInitialDbStructure(Connection conn, boolean uncompressed) throws SQLException {
		conn.createStatement()
				.execute("CREATE TABLE IF NOT EXISTS travel_articles(title text, content_gz blob"
						+ (uncompressed ? ", content text" : "") + ", is_part_of text, lat double, lon double, image_title text not null, gpx_gz blob"
						+ (uncompressed ? ", gpx text" : "") + ", trip_id long, original_id long, lang text, contents_json text)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_title ON travel_articles(title);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_id ON travel_articles(trip_id);");
		conn.createStatement()
				.execute("CREATE INDEX IF NOT EXISTS index_part_of ON travel_articles(is_part_of);");
	}

	public static PreparedStatement generateInsertPrep(Connection conn, boolean uncompressed) throws SQLException {
		return conn.prepareStatement("INSERT INTO travel_articles(title, content_gz"
				+ (uncompressed ? ", content" : "") + ", is_part_of, lat, lon, image_title, gpx_gz"
				+ (uncompressed ? ", gpx" : "") + ", trip_id , original_id , lang, contents_json)"
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" + (uncompressed ? ", ?, ?": "") + ")");
	}
	
	public static class WikiOsmHandler extends DefaultHandler {
		long id = 1;
		private final SAXParser saxParser;
		private boolean page = false;
		private boolean revision = false;
		
		private StringBuilder ctext = null;
		private long cid;
		private StringBuilder title = new StringBuilder();
		private StringBuilder text = new StringBuilder();
		private StringBuilder pageId = new StringBuilder();
		
		private boolean parseText = false;

		private final InputStream progIS;
		private ConsoleProgressImplementation progress = new ConsoleProgressImplementation();
		private DBDialect dialect = DBDialect.SQLITE;
		private Connection conn;
		private PreparedStatement prep;
		private int batch = 0;
		private final static int BATCH_SIZE = 500;
		private static final String P_OPENED = "<p>";
		private static final String P_CLOSED = "</p>";
		final ByteArrayOutputStream bous = new ByteArrayOutputStream(64000);
		private String lang;
		private WikidataConnection wikidataconn;
			
		WikiOsmHandler(SAXParser saxParser, InputStream progIS, String lang, File sqliteFile)
				throws IOException, SQLException {
			this.lang = lang;
			this.saxParser = saxParser;
			this.progIS = progIS;		
			progress.startTask("Parse wiki xml", progIS.available());

			conn = (Connection) dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
			createInitialDbStructure(conn, uncompressed);
			prep = generateInsertPrep(conn, uncompressed);
			
			wikidataconn  = new WikidataConnection(new File(sqliteFile.getParentFile(), "wikidata.sqlite"));
			
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
						parseText = true;
					} else if (name.equals("text")) {
						if (parseText) {
							Map<String, List<String>> macroBlocks = new HashMap<>();
							String text = WikiDatabasePreparation.removeMacroBlocks(ctext.toString(), macroBlocks,
									lang, wikidataconn);
							try {
								if (!macroBlocks.isEmpty()) {
									LatLon ll = getLatLonFromGeoBlock(
											macroBlocks.get(WikivoyageTemplates.LOCATION.getType()));
									boolean accepted = !title.toString().contains(":");
									if(accepted) {
										int column = 1;
										String filename = getFileName(macroBlocks.get(WikivoyageTemplates.BANNER.getType()));
										filename = filename.startsWith("<!--") ? "" : filename;
										if (id++ % 500 == 0) {
											log.debug("Article accepted " + cid + " " + title.toString() + " " + ll.getLatitude()
													+ " " + ll.getLongitude() + " free: "
													+ (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
										}
										final HTMLConverter converter = new HTMLConverter(false);
										CustomWikiModel wikiModel = new CustomWikiModel("https://upload.wikimedia.org/wikipedia/commons/${image}", 
												"https://"+lang+".wikivoyage.org/wiki/${title}", false);
										String plainStr = wikiModel.render(converter, text);
										plainStr = plainStr.replaceAll("<p>div class=&#34;content&#34;", "<div class=\"content\">\n<p>").replaceAll("<p>/div\n</p>", "</div>");
										//prep.setString(column++, Encoder.encodeUrl(title.toString()));
										prep.setString(column++, title.toString());
										prep.setBytes(column++, stringToCompressedByteArray(bous, plainStr));
										if (uncompressed) {
											prep.setString(column++, plainStr);
										}
										// part_of
										prep.setString(column++, parsePartOf(macroBlocks.get(WikivoyageTemplates.PART_OF.getType())));
										if(ll.isZero()) {
											prep.setNull(column++, Types.DOUBLE);
											prep.setNull(column++, Types.DOUBLE); 
										} else {
											prep.setDouble(column++, ll.getLatitude());
											prep.setDouble(column++, ll.getLongitude());
										}
										// banner
										prep.setString(column++, Encoder.encodeUrl(filename).replaceAll("\\(", "%28")
												.replaceAll("\\)", "%29"));
										// gpx_gz
										String gpx = generateGpx(macroBlocks.get(WikivoyageTemplates.POI.getType()), title.toString(), lang, getShortDescr(plainStr));
										prep.setBytes(column++, stringToCompressedByteArray(bous, gpx));
										if (uncompressed) {
											prep.setString(column++, gpx);
										}
										// skip trip_id column
										column++;
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
						ctext = null;
					}
				}
			} catch (IOException | SQLException e) {
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

		public static boolean isEmpty(String s) {
			return s == null || s.length() == 0;
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
					point.category = (category.equalsIgnoreCase("vcard") 
							|| category.equalsIgnoreCase("listing")) ? transformCategory(info) : category;
					if(!isEmpty(point.category)) {
						point.category = capitalizeFirstLetterAndLowercase(point.category.trim());
					}
					String areaCode = "";
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
									point.desc = point.desc = point.desc == null ? value : 
										point.desc + ". " + value;
								} else if (field.equalsIgnoreCase("email") || field.equalsIgnoreCase("מייל") || field.equalsIgnoreCase("پست الکترونیکی")) {
									point.desc = point.desc == null ? "Email: " + value : 
										point.desc + ". Email: " + value;
								} else if (field.equalsIgnoreCase("phone") || field.equalsIgnoreCase("tel") || field.equalsIgnoreCase("téléphone")
										|| field.equalsIgnoreCase("טלפון") || field.equalsIgnoreCase("تلفن")) {
									point.desc = point.desc == null ? "Phone: " + areaCode + value : 
										point.desc + ". Phone: " + areaCode + value;
								} else if (field.equalsIgnoreCase("price") || field.equalsIgnoreCase("prezzo") || field.equalsIgnoreCase("prix") 
										|| field.equalsIgnoreCase("מחיר") || field.equalsIgnoreCase("بها")) {
									point.desc = point.desc == null ? "Price: " + value : 
										point.desc + ". Price: " + value;
								} else if (field.equalsIgnoreCase("hours") || field.equalsIgnoreCase("orari") || field.equalsIgnoreCase("horaire") 
										|| field.equalsIgnoreCase("funcionamento") || field.equalsIgnoreCase("שעות") || field.equalsIgnoreCase("ساعت‌ها")) {
									point.desc = point.desc == null ? "Working hours: " + value : 
										point.desc + ". Working hours: " + value;
								} else if (field.equalsIgnoreCase("directions") || field.equalsIgnoreCase("direction") || field.equalsIgnoreCase("הוראות")
										|| field.equalsIgnoreCase("مسیرها")) {
									point.desc = point.desc == null ? "Directions: " + value : 
										point.desc + " Directions: " + value;
								}
							} catch (Exception e) {}
						}
					}
					if (point.hasLocation() && point.name != null && !point.name.isEmpty()) {
						point.setColor();
						points.add(point);
					}
				}
				if (!points.isEmpty()) {
					f.addPoints(points);
					return GPXUtils.asString(f);
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
		        seconds = m.group(3).isEmpty() ? 0 :Double.parseDouble(m.group(3));
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
						System.out.println("Error parsing the partof: " + partOf);
					}
					return part.trim().replaceAll("_", " ");
				} else if (lowerCasePartOf.contains("קטגוריה")) {
					return partOf.substring(partOf.indexOf(":") + 1).trim().replaceAll("[_\\|\\*]", "");
				} else {
					return partOf.substring(partOf.indexOf("|") + 1).trim().replaceAll("_", " ");
				}
			}
			return "";
		}

		private String parsePartOfFromQuickBar(String partOf) {
			String[] info = partOf.split("\\|");
			String region = "";
			for (String s : info) {
				if (s.indexOf("=") != -1) {
					if (!s.toLowerCase().contains("livello")) {
						region = s.substring(s.indexOf("=") + 1, s.length()).trim();
					}
				}
			}
			return region;
		}
	}
	
	public static class WikidataConnection {
		private Connection conn;
		private PreparedStatement pselect;
		private PreparedStatement pinsert;
		private int downloadMetadata = 0;

		public WikidataConnection(File f) throws SQLException {
			conn = (Connection) DBDialect.SQLITE.getDatabaseConnection(f.getAbsolutePath(), log);
			conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wikidata(wikidataid text, metadata text)");
			pselect = conn.prepareStatement("SELECT metadata FROM wikidata where wikidataid = ? ");
			pinsert = conn.prepareStatement("INSERT INTO wikidata( wikidataid, metadata) VALUES(?, ?) ");
		}
		
		public JsonObject downloadMetadata(String id) {
			JsonObject obj = null;
			try {
				if(++downloadMetadata % 50 == 0) {
					System.out.println("Download wiki metadata " + downloadMetadata);
				}
				StringBuilder metadata = new StringBuilder();
				String metadataUrl = "https://www.wikidata.org/wiki/Special:EntityData/"+id+".json";
				URL url = new URL(metadataUrl);
				BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
				String s;
				while ((s = reader.readLine()) != null) {
					metadata.append(s).append("\n");
				}
				obj = new JsonParser().parse(metadata.toString()).getAsJsonObject();
				pinsert.setString(1, id);
				pinsert.setString(2, metadata.toString());
				pinsert.execute();
			} catch (Exception e) {
				System.err.println("Error downloading wikidata " + id + " " + e.getMessage());
			}
			return obj;
		}
		
		
		public JsonObject getMetadata(String id) throws SQLException {
			pselect.setString(1, id);
			ResultSet rs = pselect.executeQuery();
			try {
				if(rs.next()){
					return new JsonParser().parse(rs.getString(1)).getAsJsonObject();
				}
			} catch (JsonSyntaxException e) {
				e.printStackTrace();
				return null;
			} finally {
				rs.close();
			}
			return null;
		}
		
		public void close() throws SQLException {
			conn.close();
		}
	}
	
}
