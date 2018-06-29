package net.osmand.wiki;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import info.bliki.wiki.filter.HTMLConverter;
import info.bliki.wiki.model.WikiModel;
import net.osmand.PlatformUtil;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.travel.WikivoyageLangPreparation.WikidataConnection;
import net.osmand.travel.WikivoyageLangPreparation.WikivoyageTemplates;
import net.osmand.wiki.wikidata.WikiDataHandler;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xwiki.component.embed.EmbeddableComponentManager;
import org.xwiki.component.manager.ComponentLookupException;
import org.xwiki.rendering.block.Block;
import org.xwiki.rendering.block.FormatBlock;
import org.xwiki.rendering.block.LinkBlock;
import org.xwiki.rendering.block.XDOM;
import org.xwiki.rendering.block.match.ClassBlockMatcher;
import org.xwiki.rendering.converter.ConversionException;
import org.xwiki.rendering.converter.Converter;
import org.xwiki.rendering.listener.Format;
import org.xwiki.rendering.parser.ParseException;
import org.xwiki.rendering.parser.Parser;
import org.xwiki.rendering.renderer.printer.DefaultWikiPrinter;
import org.xwiki.rendering.renderer.printer.WikiPrinter;
import org.xwiki.rendering.syntax.Syntax;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class WikiDatabasePreparation {
	private static final Log log = PlatformUtil.getLog(WikiDatabasePreparation.class);
	
	public static class LatLon {
		private final double longitude;
		private final double latitude;

		public LatLon(double latitude, double longitude) {
			this.latitude = latitude;
			this.longitude = longitude;
		}
		
		public double getLatitude() {
			return latitude;
		}
		
		public double getLongitude() {
			return longitude;
		}
		
		public boolean isZero() {
			return (latitude == 0 && longitude == 0);
		}
		
		@Override
		public String toString() {
			return "lat: " + latitude + " lon:" + longitude;
		}

	}
    
	public static String removeMacroBlocks(String text, Map<String, List<String>> blocksMap,
			String lang, WikidataConnection wikidata) throws IOException, SQLException {
		StringBuilder bld = new StringBuilder();
		int openCnt = 0;
		int beginInd = 0;
		int endInd = 0;
		int headerCount = 0;
		boolean hebrew = false;
		for (int i = 0; i < text.length(); i++) {
			int nt = text.length() - i - 1;
			if (nt > 0 && ((text.charAt(i) == '{' && text.charAt(i + 1) == '{') || (text.charAt(i) == '[' && text.charAt(i + 1) == '[' && text.charAt(i + 2) == 'ק') 
					|| (text.charAt(i) == '<' && text.charAt(i + 1) == 'm' && text.charAt(i + 2) == 'a' && text.charAt(i + 3) == 'p' 
					&& text.charAt(i + 4) == 'l' && text.charAt(i + 5) == 'i') 
					|| (text.charAt(i) == '<' && text.charAt(i + 1) == 'g' && text.charAt(i + 2) == 'a' && text.charAt(i + 3) == 'l')
					|| (text.charAt(i) == '<' && text.charAt(i + 1) == '!' && text.charAt(i + 2) == '-' && text.charAt(i + 3) == '-'))) {
				hebrew = text.length() > 2 && text.charAt(i + 2) == 'ק';
				beginInd = beginInd == 0 ? i + 2 : beginInd;
				openCnt++;
				i++;
			} else if (nt > 0 && ((text.charAt(i) == '}' && text.charAt(i + 1) == '}') || (hebrew && text.charAt(i) == ']' && text.charAt(i + 1) == ']')
					|| (text.charAt(i) == '>' && text.charAt(i - 1) == 'k' && text.charAt(i - 2) == 'n' && text.charAt(i - 3) == 'i' 
					&& text.charAt(i - 4) == 'l' && text.charAt(i - 5) == 'p') 
					|| (text.charAt(i) == '>' && text.charAt(i - 1) == 'y' && text.charAt(i - 2) == 'r' && text.charAt(i - 3) == 'e')
					|| (text.charAt(i) == '>' && text.charAt(i - 1) == '-' && text.charAt(i - 2) == '-'))) {
				if (openCnt > 1) {
					openCnt--;
					i++;
					continue;
				}
				openCnt--;
				endInd = i;
				String val = text.substring(beginInd, endInd);
				if (val.startsWith("allery")) {
					bld.append(parseGallery(val));
				}
				String key = getKey(val.toLowerCase());
				if (key.equals(WikivoyageTemplates.POI.getType())) {
					bld.append(parseListing(val, wikidata, lang));
				} else if (key.equals(WikivoyageTemplates.REGION_LIST.getType())) {
					bld.append((parseRegionList(val)));
				} else if (key.equals(WikivoyageTemplates.WARNING.getType())) {
					appendWarning(bld, val);
				}
				if (!key.isEmpty()) {
					if (key.contains("|")) {
						for (String str : key.split("\\|")) {
							addToMap(blocksMap, str, val);
						}
					} else {
						addToMap(blocksMap, key, val);
					}
				}
				i++;
			} else {
				if (openCnt == 0) {
					int headerLvl = 0;
					int indexCopy = i;
					if (i > 0 && text.charAt(i - 1) != '=') {
						headerLvl = calculateHeaderLevel(text, i);
						indexCopy = indexCopy + headerLvl;
					}
					if (text.charAt(indexCopy) != '\n' && headerLvl > 1) {
						int indEnd = text.indexOf("=", indexCopy);
						if (indEnd != -1) {
							indEnd = indEnd + calculateHeaderLevel(text, indEnd);
							char ch = indEnd < text.length() - 2 ? text.charAt(indEnd + 1) : text.charAt(indEnd);
							int nextHeader = calculateHeaderLevel(text, ch == '\n' ? indEnd + 2 : indEnd + 1);
							if (nextHeader > 1 && headerLvl >= nextHeader ) {
								i = indEnd;
								continue;
							} else if (headerLvl == 2) {
								if (headerCount != 0) {
									bld.append("\n/div\n");
								}
								bld.append(text.substring(i, indEnd));
								bld.append("\ndiv class=\"content\"\n");
								headerCount++;
								i = indEnd;
							} else {
								bld.append(text.charAt(i));
							}
						}						
					} else {
						bld.append(text.charAt(i));
					}
					beginInd = 0;
					endInd = 0;
				}
			}
		}
		return bld.toString();
	}

	private static int calculateHeaderLevel(String s, int index) {
		int res = 0;
		while (index < s.length() - 1 && s.charAt(index) == '=') {
			index++;
			res++;
		}
		return res;
	}

	private static void appendWarning(StringBuilder bld, String val) {
		int ind = val.indexOf("|");
		ind = ind == -1 ? 0 : ind + 1;
		bld.append("<p class=\"warning\"><b>Warning: </b>");
		String[] parts = val.split("\\|");
		val = parts.length > 1 ? parts[1] : "";
		val = !val.isEmpty() ? appendSqareBracketsIfNeeded(1, parts, val) : val;
		bld.append(val);
		bld.append("</p>");
	}

	private static String parseGallery(String val) {
		String[] parts = val.split("\n");
		StringBuilder bld = new StringBuilder();
		for (String part : parts) {
			String toCompare = part.toLowerCase();
			if (toCompare.contains(".jpg") || toCompare.contains(".jpeg") 
					|| toCompare.contains(".png") || toCompare.contains(".gif")) {
				bld.append("[[");
				bld.append(part);
				bld.append("]]");
			}
		}
		return bld.toString();
	}

	private static String parseRegionList(String val) {
		StringBuilder bld = new StringBuilder();
		String[] parts = val.split("\\|");
		for (int i = 0; i < parts.length; i++) {
			String part = parts[i].trim();
			int ind = part.indexOf("=");
			if (ind != -1) {
				String partname = part.trim().substring(0, ind).trim();
				if (partname.matches("region\\d+name")) {
					String value = appendSqareBracketsIfNeeded(i, parts, part.substring(ind + 1, part.length()));
					bld.append("*");
					bld.append(value);
					bld.append("\n");
				} else if (partname.matches("region\\d+description")) {
					String desc = part.substring(ind + 1, part.length());
					int startInd = i;
					while (i < parts.length - 1 && !parts[++i].contains("=")) {
						desc += "|" + parts[i];
					}
					i = i == startInd++ ? i : i - 1;
					bld.append(desc);
					bld.append("\n");
				}
			}
		}
		return bld.toString();
	}

	private static void addToMap(Map<String, List<String>> blocksMap, String key, String val) {
		if (blocksMap.containsKey(key)) {
			blocksMap.get(key).add(val);
		} else {
			List<String> tmp = new ArrayList<>();
			tmp.add(val);
			blocksMap.put(key, tmp);
		}
	}
	
	private static String parseListing(String val, WikidataConnection wikiDataconn, String wikiLang) throws IOException, SQLException {
		StringBuilder bld = new StringBuilder();
		String[] parts = val.split("\\|");
		String lat = null;
		String lon = null;
		String areaCode = "";
		String wikiLink = "";
		String wikiData = "";
		for (int i = 1; i < parts.length; i++) {
			String field = parts[i].trim();
			String value = "";
			int index = field.indexOf("=");
			if (index != -1) {
				value = appendSqareBracketsIfNeeded(i, parts, field.substring(index + 1, field.length()).trim()).replaceAll("\n", "");
				field = field.substring(0, index).trim();
			}
			if (!value.isEmpty() && !value.contains("{{")) {
				try {
					if (field.equalsIgnoreCase(("name")) || field.equalsIgnoreCase("nome") || field.equalsIgnoreCase("nom") 
							|| field.equalsIgnoreCase("שם") || field.equalsIgnoreCase("نام")) {
						bld.append("'''").append(value).append("'''").append(", ");
					} else if (field.equalsIgnoreCase("url") || field.equalsIgnoreCase("sito") || field.equalsIgnoreCase("האתר הרשמי")
							|| field.equalsIgnoreCase("نشانی اینترنتی")) {
						bld.append("Website: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("intl-area-code")) {
						areaCode = value;
					} else if (field.equalsIgnoreCase("address") || field.equalsIgnoreCase("addresse") || field.equalsIgnoreCase("כתובת")
							|| field.equalsIgnoreCase("نشانی")) {
						bld.append(value).append(", ");
					} else if (field.equalsIgnoreCase("lat") || field.equalsIgnoreCase("latitude") || field.equalsIgnoreCase("عرض جغرافیایی")) {
						lat = value;
					} else if (field.equalsIgnoreCase("long") || field.equalsIgnoreCase("longitude") || field.equalsIgnoreCase("طول جغرافیایی")) {
						lon = value;
					} else if (field.equalsIgnoreCase("content") || field.equalsIgnoreCase("descrizione") || field.equalsIgnoreCase("description")
							|| field.equalsIgnoreCase("sobre") || field.equalsIgnoreCase("תיאור") || field.equalsIgnoreCase("متن")) {
						bld.append(value).append(" ");
					} else if (field.equalsIgnoreCase("email") || field.equalsIgnoreCase("מייל") || field.equalsIgnoreCase("پست الکترونیکی")) {
						bld.append("e-mail: " + "mailto:").append(value).append(", ");
					} else if (field.equalsIgnoreCase("fax") || field.equalsIgnoreCase("פקס")
							|| field.equalsIgnoreCase("دورنگار")) {
						bld.append("fax: ").append(value).append(", ");
					} else if (field.equalsIgnoreCase("wdid") || field.equalsIgnoreCase("wikidata")) {
						wikiData = value;
					} else if (field.equalsIgnoreCase("phone") || field.equalsIgnoreCase("tel")
							|| field.equalsIgnoreCase("téléphone") || field.equalsIgnoreCase("טלפון") || field.equalsIgnoreCase("تلفن")) {
						String tel = areaCode.replaceAll("[ -]", "/") + "/" + value.replaceAll("[ -]", "/")
							.replaceAll("[^\\d\\+\\)\\(,]", "");
						tel = tel.replaceAll("\\(", "o").replaceAll("\\)", "c");
						bld.append("☎ " + "tel:").append(tel).append(". ");
					} else if (field.equalsIgnoreCase("price") || field.equalsIgnoreCase("prezzo") || field.equalsIgnoreCase("מחיר")
							|| field.equalsIgnoreCase("prix") || field.equalsIgnoreCase("بها")) {
						bld.append(value).append(". ");
					} else if (field.equalsIgnoreCase("hours") || field.equalsIgnoreCase("שעות") || field.equalsIgnoreCase("ساعت‌ها")) {
						bld.append("Working hours: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("directions") || field.equalsIgnoreCase("direction") 
							|| field.equalsIgnoreCase("הוראות") || field.equalsIgnoreCase("مسیرها")) {
						bld.append(value).append(". ");
					} else if (field.equalsIgnoreCase("indicazioni")) {
						bld.append("Indicazioni: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("orari")) {
						bld.append("Orari: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("horaire")) {
						bld.append("Horaire: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("funcionamento")) {
						bld.append("Funcionamento: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("wikipedia") && !value.equals("undefined")
							&& !value.isEmpty()) {
						wikiLink = value;
					}
				} catch (Exception e) {}
			}
		}
		if (wikiLink.isEmpty() && !wikiData.isEmpty()) {
			wikiLink = getWikidata(wikiLang, wikiData, wikiDataconn);
		}
		if (!wikiLink.isEmpty()) {
			bld.append(addWikiLink(wikiLang, wikiLink, lat, lon));
			bld.append(" ");
		}
		if (lat != null && lon != null) {
			bld.append(" geo:").append(lat).append(",").append(lon);
		}
		return bld.toString() + "\n";
	}
	

	private static String getWikidata(String wikiLang, String wikiData, WikidataConnection wikiDataconn) throws SQLException {
		if(wikiDataconn == null) {
			return "";
		}
		JsonObject metadata = wikiDataconn.getMetadata(wikiData);
		if(metadata == null) {
			metadata = wikiDataconn.downloadMetadata(wikiData);
		}
		if(metadata == null) {
			return "";
		}
		try {
			JsonObject ks = metadata.get("entities").getAsJsonObject();
			JsonElement siteLinksElement = ks.get(ks.keySet().iterator().next()).getAsJsonObject().get("sitelinks");
			if(siteLinksElement.isJsonObject() &&  siteLinksElement.getAsJsonObject().has(wikiLang + "wiki")) {

				return siteLinksElement.getAsJsonObject().get(wikiLang + "wiki").getAsJsonObject().get("title").getAsString();
			}
		} catch (IllegalStateException e) {
			System.err.println("Error parsing wikidata Json " + wikiData + " "  + metadata);
		}
		return "";
	}

	public static String appendSqareBracketsIfNeeded(int i, String[] parts, String value) {
		while (StringUtils.countMatches(value, "[[") > StringUtils.countMatches(value, "]]") && i + 1 < parts.length) {
			value += "|" + parts[++i];
		}
		return value;
	}

	private static String addWikiLink(String lang, String value, String lat, String lon) throws UnsupportedEncodingException {
		return "[https://" + lang + 
				".wikipedia.org/wiki/" + URLEncoder.encode(value.trim().replaceAll(" ", "_"), "UTF-8") 
				+ ((lat != null && lon != null && !lat.isEmpty() && !lon.isEmpty()) ? "?lat=" + lat + "&lon=" + lon : "") +  " Wikipedia]";
	}

	private static String getKey(String str) {
		if (str.startsWith("geo|") || str.startsWith("geodata")) {
			return WikivoyageTemplates.LOCATION.getType();
		} else if (str.startsWith("ispartof|") || str.startsWith("istinkat") || str.startsWith("isin")
				|| str.startsWith("quickfooter") || str.startsWith("dans") || str.startsWith("footer|")
				|| str.startsWith("fica em") || str.startsWith("estáen") || str.startsWith("קטגוריה") 
				|| str.startsWith("είναιΤμήμαΤου") || str.startsWith("commonscat") || str.startsWith("jest w")
				|| str.startsWith("istinkat")  || str.startsWith("partoftopic") || str.startsWith("theme") || str.startsWith("categoría")
				|| str.startsWith("بخشی")) {
			return WikivoyageTemplates.PART_OF.getType();
		} else if (str.startsWith("do") || str.startsWith("see") 
				|| str.startsWith("eat") || str.startsWith("drink") 
				|| str.startsWith("sleep") || str.startsWith("buy") 
				|| str.startsWith("listing") || str.startsWith("vcard") || str.startsWith("se loger") 
				|| str.startsWith("destination") || str.startsWith("voir") || str.startsWith("aller") 
				|| str.startsWith("manger") || str.startsWith("durma") || str.startsWith("veja") 
				|| str.startsWith("coma") || str.startsWith("אוכל") || str.startsWith("שתייה") 
				|| str.startsWith("לינה") || str.startsWith("מוקדי") || str.startsWith("רשימה")
				|| str.startsWith("marker") || str.startsWith("خوابیدن") || str.startsWith("دیدن")
				|| str.startsWith("انجام‌دادن") || str.startsWith("نوشیدن")) {
			return WikivoyageTemplates.POI.getType();
		} else if (str.startsWith("pagebanner") || str.startsWith("citybar") 
				|| str.startsWith("quickbar ") || str.startsWith("banner") || str.startsWith("באנר")
				|| str.startsWith("سرصفحه")) {
			return WikivoyageTemplates.BANNER.getType();
		} else if ((str.startsWith("quickbar") && (str.contains("lat=") || str.contains("lon=") || str.contains("long=")
				|| str.contains("longitude=")))
				|| str.startsWith("info ")) {
			return "geo|pagebanner";
		} else if (str.startsWith("regionlist")) {
			return WikivoyageTemplates.REGION_LIST.getType();
		} else if (str.startsWith("warningbox")) {
			return WikivoyageTemplates.WARNING.getType();
		}
		return "";
	}

	public static void mainTest(String[] args) throws ConversionException, ComponentLookupException, ParseException, IOException, SQLException {
		EmbeddableComponentManager cm = new EmbeddableComponentManager();
		cm.initialize(WikiDatabasePreparation.class.getClassLoader());
		Parser parser = cm.getInstance(Parser.class, Syntax.MEDIAWIKI_1_0.toIdString());
		FileReader fr = new FileReader(new File("/Users/victorshcherb/Documents/b.src.html"));
		BufferedReader br = new BufferedReader(fr);
		String content = "";
		String s;
		while((s = br.readLine()) != null) {
			content += s;
		}
		content = removeMacroBlocks(content, new HashMap<>(), "en", null);
		
		XDOM xdom = parser.parse(new StringReader(content));
		        
		// Find all links and make them italic
		for (Block block : xdom.getBlocks(new ClassBlockMatcher(LinkBlock.class), Block.Axes.DESCENDANT)) {
		    Block parentBlock = block.getParent();
		    Block newBlock = new FormatBlock(Collections.<Block>singletonList(block), Format.ITALIC);
		    parentBlock.replaceChild(newBlock, block);
		}
//		for (Block block : xdom.getBlocks(new ClassBlockMatcher(ParagraphBlock.class), Block.Axes.DESCENDANT)) {
//			ParagraphBlock b = (ParagraphBlock) block;
//			block.getParent().removeBlock(block);
//		}
		WikiPrinter printer = new DefaultWikiPrinter();
//		BlockRenderer renderer = cm.getInstance(BlockRenderer.class, Syntax.XHTML_1_0.toIdString());
//		renderer.render(xdom, printer);
//		System.out.println(printer.toString());
		
		Converter converter = cm.getInstance(Converter.class);

		// Convert input in XWiki Syntax 2.1 into XHTML. The result is stored in the printer.
		printer = new DefaultWikiPrinter();
		converter.convert(new FileReader(new File("/Users/victorshcherb/Documents/a.src.html")), Syntax.MEDIAWIKI_1_0, Syntax.XHTML_1_0, printer);

		System.out.println(printer.toString());
		
		final HTMLConverter nconverter = new HTMLConverter(false);
		String lang = "be";
		WikiModel wikiModel = new WikiModel("http://"+lang+".wikipedia.com/wiki/${image}", "http://"+lang+".wikipedia.com/wiki/${title}");
//		String plainStr = wikiModel.render(nconverter, content);
//		System.out.println(plainStr);
//		downloadPage("https://be.m.wikipedia.org/wiki/%D0%93%D0%BE%D1%80%D0%B0%D0%B4_%D0%9C%D1%96%D0%BD%D1%81%D0%BA",
//		"/Users/victorshcherb/Documents/a.wiki.html");

	}

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, SQLException, ComponentLookupException {
		String lang = "";
		String folder = "";
		if(args.length == 0) {
			lang = "en";
			folder = "/home/user/osmand/wikivoyage/";
		}
		if(args.length > 0) {
			lang = args[0];
		}
		if(args.length > 1){
			folder = args[1];
		}
		final String wikiPg = folder + lang + "wiki-latest-pages-articles.xml.bz2";
		final String sqliteFileName = folder + "wiki.sqlite";
		final String pathToWikiData = folder + "wikidatawiki-latest-pages-articles.xml.bz2";
		final String wikidataSqlite = folder + "wikidata.sqlite";
		if (!new File(wikidataSqlite).exists() && new File(pathToWikiData).exists()) {
			log.info("Processing wikidata...");
			processDump(pathToWikiData, wikidataSqlite);
		} else {
			log.error("Wikidata dump doesn't exist. Exiting.");
			System.exit(1);
		}
		log.info("Processing Wikipedia articles");
		processDump(wikiPg, sqliteFileName, lang, wikidataSqlite);
    }
	
	public static void downloadPage(String page, String fl) throws IOException {
		URL url = new URL(page);
		FileOutputStream fout = new FileOutputStream(new File(fl));
		InputStream in = url.openStream();
		byte[] buf = new byte[1024];
		int read;
		while((read = in.read(buf)) != -1) {
			fout.write(buf, 0, read);
		}
		in.close();
		fout.close();
		
	}

	protected static void testContent(String lang, String folder) throws SQLException, IOException {
		Connection conn = DBDialect.SQLITE.getDatabaseConnection(folder + lang + "wiki.sqlite", log);
		ResultSet rs = conn.createStatement().executeQuery("SELECT * from wiki");
		while(rs.next()) {
			double lat = rs.getDouble("lat");
			double lon = rs.getDouble("lon");
			byte[] zp = rs.getBytes("zipContent");
			String title = rs.getString("title");
			BufferedReader rd = new BufferedReader(new InputStreamReader(
					new GZIPInputStream(new ByteArrayInputStream(zp))));
			System.out.println(title + " " + lat + " " + lon + " " + zp.length);
			String s ;
			while((s = rd.readLine()) != null) {
				System.out.println(s);
			}
		}
	}

	private static void processDump(final String pathToDump, String sqliteFileName)
			throws ParserConfigurationException, SAXException, IOException, SQLException, ComponentLookupException {
		processDump(pathToDump, sqliteFileName, null, null);
	}

	private static void processDump(final String wikiPg, String sqliteFileName, String lang, String pathToWikiData)
			throws ParserConfigurationException, SAXException, IOException, SQLException, ComponentLookupException {
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		InputStream streamFile = new BufferedInputStream(new FileInputStream(wikiPg), 8192 * 4);
		InputSource is = getInputSource(streamFile);
		if (lang != null && pathToWikiData != null) {
			final WikiOsmHandler handler = new WikiOsmHandler(sx, streamFile, lang, pathToWikiData,  new File(sqliteFileName));
			sx.parse(is, handler);
			handler.finish();
		} else {
			final WikiDataHandler handler = new WikiDataHandler(sx, streamFile, new File(sqliteFileName));
			sx.parse(is, handler);
			handler.finish();
		}
	}

	private static InputSource getInputSource(InputStream streamFile) throws IOException {
		BZip2CompressorInputStream zis = new BZip2CompressorInputStream(streamFile);
		Reader reader = new InputStreamReader(zis,"UTF-8");
		InputSource is = new InputSource(reader);
		is.setEncoding("UTF-8");
		return is;
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

		private final InputStream progIS;
		private ConsoleProgressImplementation progress = new ConsoleProgressImplementation();
		private DBDialect dialect = DBDialect.SQLITE;
		private Connection languageConn;
		private Connection wikidataConn;
		private PreparedStatement insertPrep;
		private PreparedStatement selectPrep;
		private int batch = 0;
		private final static int BATCH_SIZE = 500;
		final ByteArrayOutputStream bous = new ByteArrayOutputStream(64000);
		private String lang;
		final String[] wikiJunkArray = new String[]{
				".jpg",".JPG",".jpeg",".png",".gif",".svg","/doc","틀:","위키프로젝트:","แม่แบบ:","위키백과:","แม่แบบ:","Àdàkọ:","Aide:","Aiuto:","Andoza:","Anexo:","Bản:","mẫu:","Batakan:","Categoría:","Categoria:","Catégorie:","Category:","Cithakan:","Datei:","Draft:","Endrika:","Fájl:","Fichier:","File:","Format:","Formula:","Help:","Hjælp:","Kategori:","Kategoria:","Kategorie:","Kigezo:","モジュール:","Mal:","Mall:","Malline:","Modèle:","Modèl:","Modello:","Modelo:","Modèl:","Moduł:","Module:","Modulis:","Modul:","Mô:","đun:","Nodyn:","Padron:","Patrom:","Pilt:","Plantía:","Plantilla:","Plantilya:","Portaal:","Portail:","Portal:","Portál:","Predefinição:","Predloga:","Predložak:","Progetto:","Proiect:","Projet:","Sablon:","Šablon:","Şablon:","Šablona:","Šablóna:","Šablonas:","Ŝablono:","Sjabloon:","Schabloun:","Skabelon:","Snið:","Stampa:","Szablon:","Templat:","Txantiloi:","Veidne:","Vikipedio:","Vikipediya:","Vikipeedia:","Viquipèdia:","Viquiprojecte:","Viquiprojecte:","Vörlaag:","Vorlage:","Vorlog:","วิกิพีเดีย:","Wikipedia:","Wikipedie:","Wikipedija:","Wîkîpediya:","Wikipédia:","Wikiproiektu:","Wikiprojekt:","Wikiproyecto:","الگو:","سانچ:","قالب:","وکیپیڈیا:","ויקיפדיה:","תבנית","Βικιπαίδεια:","Πρότυπο:","Википедиа:","Википедија:","Википедия:","Вікіпедія:","Довідка:","Загвар:","Инкубатор:","Калып:","Ҡалып:","Кеп:","Категорія:","Портал:","Проект:","Уикипедия:","Үлгі:","Файл:","Хуызæг:","Шаблон:","Կաղապար:","Մոդուլ:","Վիքիպեդիա:","ვიკიპედია:","თარგი:","ढाँचा:","विकिपीडिया:","साचा:","साँचा:","ઢાંચો:","વિકિપીડિયા:","మూస:","வார்ப்புரு:","ഫലകം:","വിക്കിപീഡിയ:","টেমপ্লেট:","プロジェクト:","উইকিপিডিয়া:","মডেল:","پرونده:","模块:","ماڈیول:"
				};

		WikiOsmHandler(SAXParser saxParser, InputStream progIS, String lang, String pathToWikiData, File sqliteFile)
				throws IOException, SQLException {
			this.lang = lang;
			this.saxParser = saxParser;
			this.progIS = progIS;
			languageConn = dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
			wikidataConn = dialect.getDatabaseConnection(pathToWikiData, log);
			languageConn.createStatement().execute("CREATE TABLE IF NOT EXISTS wiki_content(id long, lat double, lon double, title text, lang text, zipContent blob)");
			languageConn.createStatement().execute("CREATE INDEX WIKIID_INDEX ON wiki_content(lang, id)");
			insertPrep = languageConn.prepareStatement("INSERT INTO wiki_content VALUES (?, ?, ?, ?, ?, ?)");
			selectPrep = wikidataConn.prepareStatement("SELECT wiki_coords.lat, wiki_coords.lon, wiki_coords.id " +
					"FROM wiki_coords JOIN wiki_mapping ON " +
					"wiki_coords.id = wiki_mapping.id WHERE wiki_mapping.title = ? AND wiki_mapping.lang = ?");
			progress.startTask("Parse wiki xml", progIS.available());
		}

		public void addBatch() throws SQLException {
			insertPrep.addBatch();
			if(batch++ > BATCH_SIZE) {
				insertPrep.executeBatch();
				batch = 0;
			}
		}
		
		public void finish() throws SQLException {
			insertPrep.executeBatch();
			if(!languageConn.getAutoCommit()) {
				languageConn.commit();
			}
			insertPrep.close();
			selectPrep.close();
			languageConn.close();
			wikidataConn.close();
		}

		public int getCount() {
			return (int) (id - 1);
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) {
			String name = saxParser.isNamespaceAware() ? localName : qName;
			if (!page) {
				page = name.equals("page");
			} else {
				if (name.equals("title")) {
					title.setLength(0);
					ctext = title;
				} else if (name.equals("text")) {
					text.setLength(0);
					ctext = text;
				} else if (name.equals("revision")) {
					revision  = true;
				} else if (name.equals("id") && !revision) {
					pageId.setLength(0);
					ctext = pageId;
				}
			}
		}

		@Override
		public void characters(char[] ch, int start, int length) {
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
						ctext = null;
					} else if (name.equals("revision")) {
						revision = false;
					} else if (name.equals("id") && !revision) {
						ctext = null;
						cid = Long.parseLong(pageId.toString());
					} else if (name.equals("text")) {
						boolean isJunk = false;
						for(String wikiJunk : wikiJunkArray) {
							if(title.toString().contains(wikiJunk)) {
								isJunk = true;
								break;
							}
						}
						if (!isJunk) {
							selectPrep.setString(1, title.toString());
							selectPrep.setString(2, lang);
							ResultSet rs = selectPrep.executeQuery();
							String id = null;
							double lat = 0;
							double lon = 0;
							while (rs.next()) {
								lat = rs.getDouble(1);
								lon = rs.getDouble(2);
								id = rs.getString(3);
							}
							selectPrep.clearParameters();
							if (id != null && lat != 0 && lon != 0) {
								LatLon ll = new LatLon(lat, lon);
								long wikiId = Long.parseLong(id.substring(1));
								String text = removeMacroBlocks(ctext.toString(), new HashMap<>(),
										lang, null);
								final HTMLConverter converter = new HTMLConverter(false);
								CustomWikiModel wikiModel = new CustomWikiModel("http://"+lang+".wikipedia.org/wiki/${image}", "http://"+lang+".wikipedia.org/wiki/${title}", true);
								String plainStr = wikiModel.render(converter, text);
								plainStr = plainStr.replaceAll("<p>div class=&#34;content&#34;", "<div class=\"content\">\n<p>").replaceAll("<p>/div\n</p>", "</div>");
								if (this.id++ % 500 == 0) {
									log.debug("Article accepted " + cid + " " + title.toString() + " " + ll.getLatitude()
											+ " " + ll.getLongitude() + " free: "
											+ (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
								}
								try {
									insertPrep.setLong(1, wikiId);
									insertPrep.setDouble(2, ll.getLatitude());
									insertPrep.setDouble(3, ll.getLongitude());
									insertPrep.setString(4, title.toString());
									insertPrep.setString(5, lang);
									bous.reset();
									GZIPOutputStream gzout = new GZIPOutputStream(bous);
									gzout.write(plainStr.getBytes("UTF-8"));
									gzout.close();
									final byte[] byteArray = bous.toByteArray();
									insertPrep.setBytes(6, byteArray);
									addBatch();
								} catch (SQLException e) {
									throw new SAXException(e);
								}
							}
						}
						ctext = null;
					}
				}
			} catch (IOException | SQLException e) {
				throw new SAXException(e);
			}
		}


	}

	/**
	 * Gets distance in meters
	 */
	public static double getDistance(double lat1, double lon1, double lat2, double lon2){
		double R = 6372.8; // for haversine use R = 6372.8 km instead of 6371 km
		double dLat = toRadians(lat2-lat1);
		double dLon = toRadians(lon2-lon1); 
		double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
		        Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * 
		        Math.sin(dLon/2) * Math.sin(dLon/2); 
		//double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		//return R * c * 1000;
		// simplyfy haversine:
		return (2 * R * 1000 * Math.asin(Math.sqrt(a)));
	}
	
	private static double toRadians(double angdeg) {
//		return Math.toRadians(angdeg);
		return angdeg / 180.0 * Math.PI;
	}
		
}
