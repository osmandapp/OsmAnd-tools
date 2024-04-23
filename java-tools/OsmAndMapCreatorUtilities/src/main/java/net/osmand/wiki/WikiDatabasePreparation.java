package net.osmand.wiki;

import static java.util.EnumSet.of;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xmlpull.v1.XmlPullParserException;
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

import gnu.trove.map.hash.TIntObjectHashMap;
import info.bliki.wiki.filter.HTMLConverter;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.travel.WikivoyageLangPreparation.WikivoyageTemplates;
import net.osmand.util.Algorithms;
import net.osmand.util.LocationParser;
import net.osmand.wiki.OsmCoordinatesByTag.OsmLatLonId;
import net.osmand.wiki.wikidata.WikiDataHandler;

public class WikiDatabasePreparation {
	private static final Log log = PlatformUtil.getLog(WikiDatabasePreparation.class);

	private static final Set<String> unitsOfDistance = new HashSet<>(Arrays.asList("mm", "cm", "m", "km", "in", "ft", "yd", "mi", "nmi", "m2"));
	public static final String WIKIPEDIA_SQLITE = "wikipedia.sqlite";
	public static final String WIKIDATA_ARTICLES_GZ = "wikidatawiki-latest-pages-articles.xml.gz";
	public static final String WIKI_ARTICLES_GZ = "wiki-latest-pages-articles.xml.gz";
	public static final String OSM_WIKI_FILE_PREFIX = "osm_wiki_";
	
	
	public enum PoiFieldType {
		NAME, PHONE, WEBSITE, WORK_HOURS, PRICE, DIRECTIONS, WIKIPEDIA, WIKIDATA, FAX, EMAIL, DESCRIPTION, LON, LAT, ADDRESS, AREA_CODE,
	}
	
	public interface WikiDBBrowser {
		
		public LatLon getLocation(String lang, String wikiLink, long wikiDataQId) throws SQLException;

		public String getWikipediaTitleByWid(String lang, long wikiDataQId) throws SQLException;
	}
	
	public static LatLon parseLatLon(String lat, String lon) {
		if (lat.equals("") || lat.equals("NA") || lat.equals("N/A")) {
			return null;
		}
		if (lon.equals("") || lon.equals("NA") || lon.equals("N/A")) {
			return null;
		}
		try {
			String loc = lat + " " + lon;
			if (!loc.contains(".") && loc.contains(",")) {
				loc = loc.replace(',', '.');
			}
			LatLon res = LocationParser.parseLocation(loc);
			if (res != null) {
				return res;
			}
			return new LatLon(Double.parseDouble(lat), Double.parseDouble(lon));
		} catch (RuntimeException e) {
			System.err.printf("Error parsing lat=%s lon=%s (%s)\n", lat, lon,
					//LocationParser.parseLocation(lat + " " + lon), 
					e.getMessage());
			return null;
		}
	}

	public static String removeMacroBlocks(StringBuilder text, Map<WikivoyageTemplates, List<String>> blockResults, String lang, String title,
			WikiDBBrowser browser) throws IOException, SQLException {
		StringBuilder bld = new StringBuilder();
		int openCnt = 0;
		int beginInd = 0;
		int headerCount = 0;
		Map<PoiFieldType, String> poiFields = new HashMap<>();
		for (int i = 0; i < text.length();) {
			int leftChars = text.length() - i - 1;
			if (isCommentOpen(text, leftChars, i)) {
				int endI = skipComment(text, i + 3);
				text.replace(i, endI + 1, "");
			} else {
				i++;
			}
		}
		Set<Integer> errorBracesCnt = new TreeSet<Integer>();
		String[] tagsRetrieve = {"maplink", "ref", "gallery"};
		for (int i = 0; ; i++) {
			if(i == text.length()) {
				if (openCnt > 0) {
					System.out.println("Error braces {{ }}: " + lang + " " + title + " ..."
							+ text.substring(beginInd, Math.min(text.length() - 1, beginInd + 10)));
					// start over again
					errorBracesCnt.add(beginInd);
					beginInd = openCnt = i = 0;
					blockResults.clear();
				} else {
					break;
				}
			}
			int leftChars = text.length() - i - 1;
			
			if (openCnt == 0 && text.charAt(i) == '<') {
				boolean found = false;
				for (String tag : tagsRetrieve) {
					if (leftChars > tag.length() && text.substring(i + 1, i + 1 + tag.length()).equals(tag)) {
						found = true;
						StringBuilder val = new StringBuilder();
						i = parseTag(text, val, tag, i, lang, title);
						if (tag.equals("ref")) {
							parseAndAppendCitation(val.toString(), bld);
						} else if (tag.equals("gallery")) {
							String res = parseGalleryString(val.toString());
							bld.append(res);
						}
						break;
					}
				}
				if (found) {
					continue;
				}
			}
			if ((leftChars > 0 && text.charAt(i) == '{' && text.charAt(i + 1) == '{')) {
				if (!errorBracesCnt.contains(i + 2)) {
					beginInd = beginInd == 0 ? i + 2 : beginInd;
					openCnt++;	
				}
				i++;
			} else if (leftChars > 0 && text.charAt(i) == '}' && text.charAt(i + 1) == '}') {
				// Macroblock
				if (openCnt == 0) {
					continue;
				}
				if (openCnt > 1) {
					openCnt--;
					continue;
				}
				openCnt--;
				int endInd = i;
				String val = text.substring(beginInd, endInd);
				beginInd = 0;
				if (val.startsWith("gallery")) {
					bld.append(parseGalleryString(val));
				} else if (val.toLowerCase().startsWith("weather box")) {
					parseAndAppendWeatherTable(val, bld);
				} else if (val.startsWith("wide image") || val.startsWith("תמונה רחבה")) {
					bld.append(parseWideImageString(val));
				}
				EnumSet<WikivoyageTemplates> key = getKey(val.toLowerCase());
				if (key.contains(WikivoyageTemplates.POI)) {
					val = val.replaceAll("\\{\\{.*}}", "");
					bld.append(getWikivoyagePoiHtmlDescription(lang, browser, poiFields, val));
				} else if (key.contains(WikivoyageTemplates.REGION_LIST)) {
					bld.append((parseRegionList(val)));
				} else if (key.contains(WikivoyageTemplates.WARNING)) {
					appendWarning(bld, val);
				} else if (key.contains(WikivoyageTemplates.CITATION)) {
					parseAndAppendCitation(val, bld);
				} else if (key.contains(WikivoyageTemplates.TWO_PART)) {
					parseAndAppendTwoPartFormat(val, bld);
				} else if (key.contains(WikivoyageTemplates.STATION)) {
					parseAndAppendStation(val, bld);
				} else if (key.contains(WikivoyageTemplates.METRIC_DATA)) {
					parseAndAppendMetricData(val, bld);
				} else if (key.contains(WikivoyageTemplates.TRANSLATION)) {
					parseAndAppendTranslation(val, bld);
				}
				if (!key.isEmpty() && blockResults != null) {
					for (WikivoyageTemplates w : key) {
						addToMap(blockResults, w, val);
					}
				}
				if (text.charAt(i + 1) != ' ' && text.charAt(i) != '}') {
					i--;
				}
				i++;
			} else if (openCnt == 0) {
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
						if (nextHeader > 1 && headerLvl >= nextHeader) {
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
			}
		}
		return bld.toString();
	}

	private static StringBuilder getWikivoyagePoiHtmlDescription(String lang, WikiDBBrowser browser,
			Map<PoiFieldType, String> poiFields, String val)
			throws IOException, SQLException, UnsupportedEncodingException {
		StringBuilder poiShortDescription = parsePoiWithAddLatLon(val, poiFields);
		String wikiLink = poiFields.get(PoiFieldType.WIKIPEDIA);
		String wikiDataQId = poiFields.get(PoiFieldType.WIKIDATA);
		long wikidataId = 0;
		if (!Algorithms.isEmpty(wikiDataQId)) {
			try {
				if (wikiDataQId.indexOf(' ') > 0) {
					wikidataId = Long.parseLong(wikiDataQId.substring(1, wikiDataQId.indexOf(' ')));
				} else {
					wikidataId = Long.parseLong(wikiDataQId.substring(1));
				}
			} catch (NumberFormatException e) {
				System.err.println("Error wid - " + wikiDataQId);
			}
		}
		if (Algorithms.isEmpty(wikiLink) && wikidataId > 0 && browser != null) {
			wikiLink = browser.getWikipediaTitleByWid(lang, wikidataId);
		}
		LatLon latLon = browser == null ? null : browser.getLocation(lang, wikiLink, wikidataId);
		if (latLon == null && poiFields.containsKey(PoiFieldType.LAT)
				&& poiFields.containsKey(PoiFieldType.LON)) {
			latLon = parseLatLon(poiFields.get(PoiFieldType.LAT), poiFields.get(PoiFieldType.LON));
		}
		if (!Algorithms.isEmpty(wikiLink)) {
			poiShortDescription.append(addWikiLink(lang, wikiLink, latLon));
			poiShortDescription.append(" ");
		}
		if (latLon != null) {
			poiShortDescription.append(String.format(" geo:%.5f,%.5f,", latLon.getLatitude(), latLon.getLongitude()));
		}
		return poiShortDescription;
	}

	private static String addWikiLink(String lang, String value, LatLon latLon) throws UnsupportedEncodingException {
		String attr = "";
		if (latLon != null) {
			attr = String.format("?lat=%.5f&lon=%.5f", latLon.getLatitude(), latLon.getLongitude());
		}
		return "[https://" + lang + ".wikipedia.org/wiki/"
				+ URLEncoder.encode(value.trim().replaceAll(" ", "_"), "UTF-8") + attr + " Wikipedia]";
	}
	
	private static int skipComment(StringBuilder text, int i) {
		while (i < text.length() && !isCommentClosed(text, i)) {
			i++;
		}
		return i;
	}

	private static boolean isCommentClosed(StringBuilder text, int i) {
		return i > 1 && text.charAt(i - 2) == '-' && text.charAt(i - 1) == '-' && text.charAt(i) == '>';
	}

	private static boolean isCommentOpen(StringBuilder text, int nt, int i) {
		return nt > 2 && text.charAt(i) == '<' && text.charAt(i + 1) == '!' && text.charAt(i + 2) == '-' && text.charAt(i + 3) == '-';
	}

	
	private static void parseAndAppendWeatherTable(String val, StringBuilder bld) {
		String[] parts = val.split("\\|");
		Map<String, String> headerMappings = new HashMap<>();
		headerMappings.put("record high", "Record high °C");
		headerMappings.put("high", "Average high °C");
		headerMappings.put("mean", "Daily mean °C");
		headerMappings.put("low", "Average low °C");
		headerMappings.put("record low", "Record low °C");
		headerMappings.put("precipitation", "Average rainfall mm");
		headerMappings.put("rain", "Average rainy days (≥ 1.0 mm)");

		bld.append("{| class=\"wikitable sortable\"\n");
		bld.append("!Month\n");
		bld.append("!Jan\n");
		bld.append("!Feb\n");
		bld.append("!Mar\n");
		bld.append("!Apr\n");
		bld.append("!May\n");
		bld.append("!Jun\n");
		bld.append("!Jul\n");
		bld.append("!Aug\n");
		bld.append("!Sep\n");
		bld.append("!Oct\n");
		bld.append("!Nov\n");
		bld.append("!Dec\n");
		bld.append("!Year\n");
		Map<String, TIntObjectHashMap<String>> data = new LinkedHashMap<>();
		for (String part : parts) {
			String header = getHeader(part, headerMappings);
			if (part.contains("colour") || header == null) {
				continue;
			}
			if (data.get(header) == null) {
				TIntObjectHashMap<String> vals = new TIntObjectHashMap<>();
				vals.put(getIndex(part), part.substring(part.indexOf("=") + 1));
				data.put(header, vals);
			} else {
				data.get(header).put(getIndex(part), part.substring(part.indexOf("=") + 1));
			}
		}
		for (String header : data.keySet()) {
			bld.append("|-\n");
			bld.append("|").append(header).append("\n");
			TIntObjectHashMap<String> values = data.get(header);
			for (int i = 1; i < 14; i++) {
				String valueToAppend = values.get(i);
				valueToAppend = valueToAppend == null ? "" : valueToAppend;
				bld.append("|").append(valueToAppend).append("\n");
			}
		}
		bld.append("|}");
	}

	private static void parseAndAppendMetricData(String val, StringBuilder bld) {
		String[] parts = val.split("\\|");
		String value = "";
		String units = "";
		for (String part : parts) {
			if (value.isEmpty() && StringUtils.isNumeric(part)) {
				value = part;
			}
			if (units.isEmpty() && stringIsMetricUnit(part)) {
				units = part;
			}
		}
		if (!value.isEmpty() && !units.isEmpty())
			bld.append(String.format("%s %s", value, units));
	}

	private static boolean stringIsMetricUnit(String part) {
		return unitsOfDistance.contains(part);
	}

	private static int getIndex(String part) {
		if (part.contains("Jan")) {
			return 1;
		} else if (part.contains("Feb")) {
			return 2;
		} else if (part.contains("Mar")) {
			return 3;
		} else if (part.contains("Apr")) {
			return 4;
		} else if (part.contains("May")) {
			return 5;
		} else if (part.contains("Jun")) {
			return 6;
		} else if (part.contains("Jul")) {
			return 7;
		} else if (part.contains("Aug")) {
			return 8;
		} else if (part.contains("Sep")) {
			return 9;
		} else if (part.contains("Oct")) {
			return 10;
		} else if (part.contains("Nov")) {
			return 11;
		} else if (part.contains("Dec")) {
			return 12;
		} else if (part.contains("year")) {
			return 13;
		} else {
			return -1;
		}
	}

	private static String getHeader(String part, Map<String, String> mapping) {
		if (part.contains("high")) {
			return part.contains("record") ? mapping.get("record high") : mapping.get("high");
		} else if (part.contains("low")) {
			return part.contains("record") ? mapping.get("record low") : mapping.get("low");
		} else if (part.contains("mean")) {
			return mapping.get("mean");
		} else if (part.contains("precipitation")) {
			return mapping.get("precipitation");
		} else if (part.contains("rain")) {
			return mapping.get("rain");
		}
		return null;
	}

	
	private static int parseTag(StringBuilder text, StringBuilder bld, String tag, int indOpen, String lang, String title) {
		int selfClosed = text.indexOf("/>", indOpen);
		int nextTag = text.indexOf("<", indOpen+1);
		if (selfClosed > 0 && (selfClosed < nextTag || nextTag == -1)) {
			bld.append(text.substring(indOpen + 1, selfClosed));
			return selfClosed + 1;
		}
		int ind = text.indexOf("</" +tag, indOpen);
		int l2 = text.indexOf("</ " +tag, indOpen);
		if (l2 > 0) {
			ind = ind == -1 ? l2 : Math.min(l2, ind);
		} else if (ind == -1) {
			System.out.printf("Error tag (not closed) %s %s: %s\n", lang, title,
					text.substring(indOpen + 1, Math.min(text.length() - 1, indOpen + 1 + 10)));
			return indOpen + 1;
		}
		
		int lastChar = text.indexOf(">", ind);
		bld.append(text.substring(indOpen + 1, ind));
//		System.out.println(" ...." + bld + "....");
		return lastChar;
	}

	private static void parseAndAppendCitation(String ref, StringBuilder bld) {
		String[] parts = ref.split("\\|");
		String url = "";
		for (String part : parts) {
			part = part.trim().toLowerCase();
			if (part.startsWith("url=")) {
				url = part.substring(part.indexOf("=") + 1);
			}
		}
		if (!url.isEmpty()) {
			bld.append("[").append(url).append("]");
		}
	}
	
	private static void parseAndAppendTwoPartFormat(String ref, StringBuilder bld) {
		String[] parts = ref.split("\\|+|:");
		if (parts.length > 1) {
			bld.append(parts[1]);
		}
	}
	
	private static void parseAndAppendStation(String ref, StringBuilder bld) {
		String[] parts = ref.split("\\|");
		if (parts.length <= 2) {
			return;
		}
		String[] stations = Arrays.copyOfRange(parts, 2, parts.length);
		if (stations.length > 0) {
			String st = "|";
			for (String station : stations) {
				st = st + station + "|";
			}
			bld.append(parts[0]).append(" ").append(parts[1]).append(" ").append(st);
		} else
			bld.append(parts[0]).append(" ").append(parts[1]);
	}
	
	private static void parseAndAppendTranslation(String ref, StringBuilder bld) {
		String[] parts = ref.split("\\|");
		String lang = "";
		if (parts.length > 1) {
			for (String part : parts) {
				if (part.startsWith("lang-")) {
					lang = part.split("-")[1] + " = ";
				}
			}
			if (!lang.isEmpty()) {
				bld.append(lang).append(parts[1]);
			} else
				bld.append("[").append(parts[1]).append("]");
		}
	}
	
	private static int calculateHeaderLevel(StringBuilder s, int index) {
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

	private static String parseWideImageString(String val) {
		String[] parts = val.split("\\|");
		StringBuilder bld = new StringBuilder();
		bld.append("[[");
		for (int i = 1; i < parts.length; i++) {
			String part = parts[i];
			String toCompare = part.toLowerCase();
			if (toCompare.contains(".jpg") || toCompare.contains(".jpeg")
					|| toCompare.contains(".png") || toCompare.contains(".gif")) {
				if (!toCompare.contains(":")) {
					bld.append("File:");
				}
			}
			bld.append(part);
			if (i < parts.length - 1) {
				bld.append("|");
			}
		}
		bld.append("]]");
		return bld.toString();
	}

	private static String parseGalleryString(String val) {
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

	private static void addToMap(Map<WikivoyageTemplates, List<String>> blocksMap, WikivoyageTemplates key, String val) {
		if (blocksMap.containsKey(key)) {
			blocksMap.get(key).add(val);
		} else {
			List<String> tmp = new ArrayList<>();
			tmp.add(val);
			blocksMap.put(key, tmp);
		}
	}

	private static StringBuilder parsePoiWithAddLatLon(String val, Map<PoiFieldType, String> poiFields)
			throws IOException, SQLException {
		StringBuilder poiShortDescription = new StringBuilder();
		String[] parts = val.split("\\|");
		String areaCode = "";
		poiFields.clear();
		
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
						poiShortDescription.append("'''").append(value).append("'''").append(", ");
						poiFields.put(PoiFieldType.NAME, value);
					} else if (field.equalsIgnoreCase("url") || field.equalsIgnoreCase("sito") || field.equalsIgnoreCase("האתר הרשמי")
							|| field.equalsIgnoreCase("نشانی اینترنتی")) {
						poiShortDescription.append("Website: ").append(value).append(". ");
						poiFields.put(PoiFieldType.WEBSITE, value);
					} else if (field.equalsIgnoreCase("intl-area-code")) {
						areaCode = value;
						poiFields.put(PoiFieldType.AREA_CODE, value);
					} else if (field.equalsIgnoreCase("address") || field.equalsIgnoreCase("addresse") || field.equalsIgnoreCase("כתובת")
							|| field.equalsIgnoreCase("نشانی")) {
						poiShortDescription.append(value).append(", ");
						poiFields.put(PoiFieldType.ADDRESS, value);
					} else if (field.equalsIgnoreCase("lat") || field.equalsIgnoreCase("latitude") || field.equalsIgnoreCase("عرض جغرافیایی")) {
//						lat = value;
						poiFields.put(PoiFieldType.LAT, value);
					} else if (field.equalsIgnoreCase("long") || field.equalsIgnoreCase("longitude") || field.equalsIgnoreCase("طول جغرافیایی")) {
//						lon = value;
						poiFields.put(PoiFieldType.LON, value);
					} else if (field.equalsIgnoreCase("content") || field.equalsIgnoreCase("descrizione") || field.equalsIgnoreCase("description")
							|| field.equalsIgnoreCase("sobre") || field.equalsIgnoreCase("תיאור") || field.equalsIgnoreCase("متن")) {
						poiShortDescription.append(value).append(" ");
						poiFields.put(PoiFieldType.DESCRIPTION, value);
					} else if (field.equalsIgnoreCase("email") || field.equalsIgnoreCase("מייל") || field.equalsIgnoreCase("پست الکترونیکی")) {
						poiShortDescription.append("e-mail: " + "mailto:").append(value).append(", ");
						poiFields.put(PoiFieldType.EMAIL, value);
					} else if (field.equalsIgnoreCase("fax") || field.equalsIgnoreCase("פקס")
							|| field.equalsIgnoreCase("دورنگار")) {
						poiFields.put(PoiFieldType.FAX, value);
						poiShortDescription.append("fax: ").append(value).append(", ");
					} else if (field.equalsIgnoreCase("wdid") || field.equalsIgnoreCase("wikidata")) {
//						wikiDataQId = value;
						poiFields.put(PoiFieldType.WIKIDATA, value);
					} else if (field.equalsIgnoreCase("phone") || field.equalsIgnoreCase("tel")
							|| field.equalsIgnoreCase("téléphone") || field.equalsIgnoreCase("טלפון") || field.equalsIgnoreCase("تلفن")) {
						String tel = areaCode.replaceAll("[ -]", "/") + "/" + value.replaceAll("[ -]", "/")
							.replaceAll("[^\\d\\+\\)\\(,]", "");
						tel = tel.replaceAll("\\(", "o").replaceAll("\\)", "c");
						poiFields.put(PoiFieldType.PHONE, tel);
						poiShortDescription.append("☎ " + "tel:").append(tel).append(". ");
					} else if (field.equalsIgnoreCase("price") || field.equalsIgnoreCase("prezzo") || field.equalsIgnoreCase("מחיר")
							|| field.equalsIgnoreCase("prix") || field.equalsIgnoreCase("بها")) {
						poiFields.put(PoiFieldType.PRICE, value);
						poiShortDescription.append(value).append(". ");
					} else if (field.equalsIgnoreCase("hours") || field.equalsIgnoreCase("שעות") || field.equalsIgnoreCase("ساعت‌ها")) {
						poiFields.put(PoiFieldType.WORK_HOURS, value);
						poiShortDescription.append("Working hours: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("directions") || field.equalsIgnoreCase("direction") 
							|| field.equalsIgnoreCase("הוראות") || field.equalsIgnoreCase("مسیرها")) {
						poiFields.put(PoiFieldType.DIRECTIONS, value);
						poiShortDescription.append(value).append(". ");
					} else if (field.equalsIgnoreCase("indicazioni")) {
						poiFields.put(PoiFieldType.DIRECTIONS, value);
						poiShortDescription.append("Indicazioni: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("orari")) {
						poiFields.put(PoiFieldType.WORK_HOURS, value);
						poiShortDescription.append("Orari: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("horaire")) {
						poiFields.put(PoiFieldType.WORK_HOURS, value);
						poiShortDescription.append("Horaire: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("funcionamento")) {
						poiFields.put(PoiFieldType.WORK_HOURS, value);
						poiShortDescription.append("Funcionamento: ").append(value).append(". ");
					} else if (field.equalsIgnoreCase("wikipedia") && !value.equals("undefined")
							&& !value.isEmpty()) {
						poiFields.put(PoiFieldType.WIKIPEDIA, value);
//						wikiLink = value;
					}
				} catch (Exception e) {}
			}
		}
		return poiShortDescription;
	}

	public static String appendSqareBracketsIfNeeded(int i, String[] parts, String value) {
		while (StringUtils.countMatches(value, "[[") > StringUtils.countMatches(value, "]]") && i + 1 < parts.length) {
			value += "|" + parts[++i];
		}
		return value;
	}

	
	private static EnumSet<WikivoyageTemplates> getKey(String str) {
		if (str.startsWith("geo|") || str.startsWith("geodata")) {
			return of(WikivoyageTemplates.LOCATION);
		} else if (str.startsWith("ispartof") || str.startsWith("partofitinerary") || str.startsWith("isin")
				|| str.startsWith("quickfooter") || str.startsWith("dans") || str.startsWith("footer|")
				|| str.startsWith("istinkat") || str.startsWith("istin|") || str.startsWith("istin ") 
				// || str.startsWith("navigation ") -- incorect
				 
				|| str.startsWith("fica em") || str.startsWith("estáen") || str.startsWith("קטגוריה") 
				|| str.startsWith("είναιτμήματου") || str.startsWith("είναιτμήματης")
				//|| str.startsWith("commonscat") 
				|| str.startsWith("jest w")
				|| str.startsWith("partoftopic") || str.startsWith("theme") || str.startsWith("categoría")
				|| str.startsWith("بخشی")) {
			return of(WikivoyageTemplates.PART_OF);
		} else if (str.startsWith("do") || str.startsWith("see") || str.startsWith("go")
				|| str.startsWith("eat") || str.startsWith("drink") 
				|| str.startsWith("sleep") || str.startsWith("buy") 
				|| str.startsWith("listing") || str.startsWith("vcard") || str.startsWith("se loger") 
				|| str.startsWith("destination") || str.startsWith("voir") || str.startsWith("aller") 
				|| str.startsWith("manger") || str.startsWith("durma") || str.startsWith("veja") 
				|| str.startsWith("coma") || str.startsWith("אוכל") || str.startsWith("שתייה") 
				|| str.startsWith("לינה") || str.startsWith("מוקדי") || str.startsWith("רשימה")
				|| str.startsWith("marker") || str.startsWith("خوابیدن") || str.startsWith("دیدن")
				|| str.startsWith("انجام‌دادن") || str.startsWith("نوشیدن")
				|| str.startsWith("event")) {
			return of(WikivoyageTemplates.POI);
		} else if (str.startsWith("pagebanner") || str.startsWith("citybar") 
				|| str.startsWith("quickbar ") || str.startsWith("banner") || str.startsWith("באנר")
				|| str.startsWith("سرصفحه")) {
			return of(WikivoyageTemplates.BANNER);
		} else if ((str.startsWith("quickbar") && (str.contains("lat=") || str.contains("lon=") || str.contains("long=")
				|| str.contains("longitude=")))
				|| str.startsWith("info ")) {
			return of(WikivoyageTemplates.LOCATION, WikivoyageTemplates.BANNER);
		} else if (str.startsWith("regionlist")) {
			return of(WikivoyageTemplates.REGION_LIST);
		} else if (str.startsWith("warningbox")) {
			return of(WikivoyageTemplates.WARNING);
		} else if (str.startsWith("cite")) {
			return of(WikivoyageTemplates.CITATION);
		} else if (str.startsWith("iata")|| str.startsWith("formatnum")) {
			return of(WikivoyageTemplates.TWO_PART);
		} else if (str.startsWith("station") || str.startsWith("rint")) {
			return of(WikivoyageTemplates.STATION);
		} else if (str.startsWith("ipa") || str.startsWith("lang-")) {
			return of(WikivoyageTemplates.TRANSLATION);
		} else if (str.startsWith("disamb") || str.startsWith("disambiguation") ||
				str.startsWith("неоднозначность") ||
				str.startsWith("ujednoznacznienie") ||
				str.startsWith("msg:disamb") || str.startsWith("wegweiser") || str.startsWith("begriffsklärung")) {
			return of(WikivoyageTemplates.DISAMB);
		} else if (str.startsWith("guidephrasebook") || str.startsWith("partofphrasebook") || 
				str.startsWith("phrasebookguide")) {
			return of(WikivoyageTemplates.PHRASEBOOK);
		} else if (str.startsWith("monument-title")) {
			return of(WikivoyageTemplates.MONUMENT_TITLE);
		} else if (str.startsWith("geo") ) {
			return of(WikivoyageTemplates.LOCATION);
		} else {
			Set<String> parts = new HashSet<>(Arrays.asList(str.split("\\|")));
			if (parts.contains("convert") || parts.contains("unité")) {
				return of(WikivoyageTemplates.METRIC_DATA);
			} else {
				parts.retainAll(unitsOfDistance);
				if (!parts.isEmpty())
					return of(WikivoyageTemplates.METRIC_DATA);
			}
		}
		return EnumSet.noneOf(WikivoyageTemplates.class);
	}

	public static void mainTest(String[] args) throws ConversionException, ComponentLookupException, ParseException, IOException, SQLException {
		EmbeddableComponentManager cm = new EmbeddableComponentManager();
		cm.initialize(WikiDatabasePreparation.class.getClassLoader());
		Parser parser = cm.getInstance(Parser.class, Syntax.MEDIAWIKI_1_0.toIdString());
		FileReader fr = new FileReader(new File("/Users/victorshcherb/Documents/b.src.html"));
		StringBuilder content = new StringBuilder();
		String s;
		BufferedReader br = new BufferedReader(fr);
		while((s = br.readLine()) != null) {
			content.append(s);
		}
		br.close();
		//content = removeMacroBlocks(content, null, "en", null);
		//for testing file.html after removeMacroBlocks and generating new file.html
		String resContent = generateHtmlArticle(content, "en", "b", null);
		String savePath = "/Users/plotva/Documents";
		File myFile = new File(savePath, "page.html");
		BufferedWriter htmlFileWriter = new BufferedWriter(new FileWriter(myFile, false));
		htmlFileWriter.write(resContent);
        htmlFileWriter.close();
		
		XDOM xdom = parser.parse(new StringReader(content.toString()));
		        
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
		
//		final HTMLConverter nconverter = new HTMLConverter(false);
//		String lang = "be";
//		WikiModel wikiModel = new WikiModel("http://"+lang+".wikipedia.com/wiki/${image}", "http://"+lang+".wikipedia.com/wiki/${title}");
//		String plainStr = wikiModel.render(nconverter, content);
//		System.out.println(plainStr);
//		downloadPage("https://be.m.wikipedia.org/wiki/%D0%93%D0%BE%D1%80%D0%B0%D0%B4_%D0%9C%D1%96%D0%BD%D1%81%D0%BA",
//		"/Users/victorshcherb/Documents/a.wiki.html");
	}
	
	public static void mainTestPage(String[] args) throws IOException, SQLException {
		StringBuilder rs = Algorithms
				.readFromInputStream(WikiDatabasePreparation.class.getResourceAsStream("/page.txt"));
		TreeMap<WikivoyageTemplates, List<String>> macros = new TreeMap<WikivoyageTemplates, List<String>>();
		String text = WikiDatabasePreparation.removeMacroBlocks(rs, macros, null, null, null);
		System.out.println(text);
		System.out.println(macros);
	}

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, SQLException, ComponentLookupException, XmlPullParserException, InterruptedException {
		if (args.length == 1 && args[0].equals("testRun")) {
			mainTestPage(args);
			return;
		}
		String lang = "";
		String wikipediaFolder = "";
		String wikidataFolder = "";
		String mode = "";
		long testArticleID = 0;
		String resultDB = "";
		String wikipediaSqliteName = "";
		String wikidataSqliteName = "";

		for (String arg : args) {
			String val = arg.substring(arg.indexOf("=") + 1);
			if (arg.startsWith("--lang=")) {
				lang = val;
			} else if (arg.startsWith("--dir=")) {
				wikipediaFolder = val;
				wikidataFolder = val;
			} else if (arg.startsWith("--mode=")) {
				mode = val;
			} else if (arg.startsWith("--testID=")) {
				testArticleID = Long.parseLong(val);
			} else if (arg.startsWith("--result_db=")) {
				resultDB = val;
			}
		}

		if (mode.isEmpty()) {
			throw new RuntimeException("Correct arguments weren't supplied. --mode= is not set");
		}
		if (mode.equals("process-wikipedia") || mode.equals("test-wikipedia")) {
			if (wikipediaFolder.isEmpty()) {
				throw new RuntimeException("Correct arguments weren't supplied. --dir= is not set");
			}
			if (lang.isEmpty()) {
				throw new RuntimeException("Correct arguments weren't supplied. --lang= is not set");
			}
			wikipediaSqliteName = resultDB.isEmpty() ? wikipediaFolder + WIKIPEDIA_SQLITE : resultDB;
		}
		if (mode.equals("create-wikidata") || mode.equals("update-wikidata") || 
				mode.equals("create-osm-wikidata")) {
			if (resultDB.isEmpty()) {
				throw new RuntimeException("Correct arguments weren't supplied. --result_db= is not set");
			}
			if (wikidataFolder.isEmpty()) {
				throw new RuntimeException("Correct arguments weren't supplied. --dir= is not set");
			}
			wikidataSqliteName = resultDB;
		}

		final String pathToWikiData = wikidataFolder + WIKIDATA_ARTICLES_GZ;
		OsmCoordinatesByTag osmCoordinates = new OsmCoordinatesByTag(new String[]{"wikipedia", "wikidata"},
				new String[] { "wikipedia:" });
		File wikidataDB;

		switch (mode) {
			case "process-wikidata-regions":
				processWikidataRegions(wikidataSqliteName);
				break;
			case "create-wikidata":
				File wikiDB = new File(wikidataSqliteName);
				if (!new File(pathToWikiData).exists()) {
					throw new RuntimeException("Wikidata dump doesn't exist:" + pathToWikiData);
				}
				if (wikiDB.exists()) {
					wikiDB.delete();
				}
				String wikidataFile = wikidataFolder + WIKIDATA_ARTICLES_GZ;
				wikidataDB = new File(wikidataSqliteName);
				log.info("Process OSM coordinates...");
				osmCoordinates.parse(wikidataDB.getParentFile());
				log.info("Create wikidata...");
				processWikidata(wikidataDB, wikidataFile, osmCoordinates,0);
				createOSMWikidataTable(wikidataDB, osmCoordinates);
				break;
			case "create-osm-wikidata":
				wikidataDB = new File(wikidataSqliteName);
				log.info("Process OSM coordinates...");
				osmCoordinates.parse(wikidataDB.getParentFile());
				log.info("Create table mapping osm to wikidata...");
				createOSMWikidataTable(wikidataDB, osmCoordinates);
				break;
			case "update-wikidata":
				wikidataDB = new File(wikidataSqliteName);
				log.info("Process OSM coordinates...");
				osmCoordinates.parse(wikidataDB.getParentFile());
				WikiDatabaseUpdater wdu = new WikiDatabaseUpdater(wikidataDB);
				List<String> downloadedPages = wdu.getDownloadedPages();
				long maxQId = wdu.getMaxQId();
				log.info("Updating wikidata...");
				for (String f : downloadedPages) {
					log.info("Updating " + f);
					processWikidata(wikidataDB, f, osmCoordinates, maxQId);
				}
				wdu.removeDownloadedPages();
				createOSMWikidataTable(wikidataDB, osmCoordinates);
				break;
			case "process-wikipedia":
				log.info("Processing wikipedia...");
				processWikipedia(wikipediaFolder, wikipediaSqliteName, lang, 0);
				break;
			case "test-wikipedia":
				processWikipedia(wikipediaFolder, wikipediaSqliteName, lang, testArticleID);
				break;
		}
	}

	private static void processWikidataRegions(final String sqliteFileName) throws SQLException, IOException {
		File wikiDB = new File(sqliteFileName);
		log.info("Processing wikidata regions...");
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = dialect.getDatabaseConnection(wikiDB.getAbsolutePath(), log);
		OsmandRegions regions = new OsmandRegions();
		regions.prepareFile();
		regions.cacheAllCountries();
		PreparedStatement wikiRegionPrep = conn
				.prepareStatement("INSERT INTO wiki_region(id, regionName) VALUES(?, ? )");
		ResultSet rs = conn.createStatement().executeQuery("SELECT id, lat, lon from wiki_coords");
		int batch = 0;
		List<String> rgs = new ArrayList<String>();
		while (rs.next()) {
			rgs = regions.getRegionsToDownload(rs.getDouble(2), rs.getDouble(3), rgs);
			for (String reg : rgs) {
				wikiRegionPrep.setLong(1, rs.getLong(1));
				wikiRegionPrep.setString(2, reg);
				wikiRegionPrep.addBatch();
				if (batch++ > WikiDataHandler.BATCH_SIZE) {
					wikiRegionPrep.executeBatch();
					batch = 0;
				}
			}
		}
		wikiRegionPrep.executeBatch();
		rs.close();
		wikiRegionPrep.close();
		conn.close();
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
		Connection conn = DBDialect.SQLITE.getDatabaseConnection(folder + lang + WIKIPEDIA_SQLITE, log);
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

	
	public static void createOSMWikidataTable(File wikidataDB, OsmCoordinatesByTag c) throws SQLException {
		Connection commonsWikiConn = DBDialect.SQLITE.getDatabaseConnection(wikidataDB.getAbsolutePath(), log);
		Statement st = commonsWikiConn.createStatement();
		ResultSet rs = st.executeQuery("SELECT C.id,M.lang,M.title FROM wiki_coords C LEFT JOIN wiki_mapping M ON M.id=C.id");
		Map<Long, OsmLatLonId> res = new TreeMap<>();
		int scan = 0;
		long time = System.currentTimeMillis();
		while (rs.next()) {
			long wid = rs.getLong(1);
			String articleLang = rs.getString(2);
			String articleTitle = rs.getString(3);
			if (++scan % 500000 == 0) {
				System.out.println("Scanning wiki to merge with OSM... " + scan + " " + (System.currentTimeMillis() - time) + " ms");
				time = System.currentTimeMillis();
			}
			if (articleLang != null) {
				setWikidataId(res, c.getCoordinates("wikipedia:" + articleLang, articleTitle), wid);
				setWikidataId(res, c.getCoordinates("wikipedia", articleLang + ":" + articleTitle), wid);
				setWikidataId(res, c.getCoordinates("wikipedia", articleTitle), wid);
				setWikidataId(res, c.getCoordinates("wikidata", "Q" + wid), wid);
			}
			setWikidataId(res, c.getCoordinates("wikidata", "Q" + wid), wid);
		}
		try {
			commonsWikiConn.createStatement().executeQuery("DROP TABLE osm_wikidata");
		} catch (SQLException e) {
			// ignore
			System.err.println("Table osm_wikidata doesn't exist");
		}
		st.execute("CREATE TABLE osm_wikidata(osmid bigint, osmtype int, wikidataid bigint, lat double, long double, tags string, poitype string, poisubtype string)");
		st.close();
		PreparedStatement ps = commonsWikiConn.prepareStatement("INSERT INTO osm_wikidata VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
		int batch = 0;
		for (OsmLatLonId o : res.values()) {
			ps.setLong(1, o.id);
			ps.setInt(2, o.type);
			ps.setLong(3, o.wikidataId);
			ps.setDouble(4, o.lat);
			ps.setDouble(5, o.lon);
			ps.setString(6, o.tagsJson);
			ps.setString(7, o.amenity == null ? null : o.amenity.getType().getKeyName());
			ps.setString(8, o.amenity == null ? null : o.amenity.getSubType());
			ps.addBatch();
			if (batch++ >= 1000) {
				ps.executeBatch();
			}
		}
		ps.executeBatch();
		ps.close();
		commonsWikiConn.close();
	}
	
	private static void setWikidataId(Map<Long, OsmLatLonId> mp, OsmLatLonId c, long wid) {
		if (c != null) {
			c.wikidataId = wid;
			setWikidataId(mp, c.next, wid);
			mp.put(c.type + (c.id << 2l), c);
		}
	}
	
	public static void processWikipedia(final String wikipediaFolder, final String wikipediaSqliteFileName, String lang, long testArticleId)
			throws ParserConfigurationException, SAXException, IOException, SQLException {
		File wikipediaSqlite = new File(wikipediaSqliteFileName);
		String wikiFile = wikipediaFolder + lang + WIKI_ARTICLES_GZ;
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		FileProgressImplementation progress = new FileProgressImplementation("Read wikipedia file", new File(wikiFile));
		InputStream streamFile = progress.openFileInputStream();
		InputSource is = getInputSource(streamFile);
		final WikipediaHandler handler = new WikipediaHandler(sx, progress, lang, wikipediaSqlite, testArticleId);
		sx.parse(is, handler);
		handler.finish();
	}

	public static void processWikidata(File wikidataSqlite, final String wikidataFile, OsmCoordinatesByTag osmCoordinates, long lastProcessedId)
			throws ParserConfigurationException, SAXException, IOException, SQLException {
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		FileProgressImplementation progress = new FileProgressImplementation("Read wikidata file", new File(wikidataFile));
		InputStream streamFile = progress.openFileInputStream();
		InputSource is = getInputSource(streamFile);
		OsmandRegions regions = new OsmandRegions();
		regions.prepareFile();
		regions.cacheAllCountries();
		final WikiDataHandler handler = new WikiDataHandler(sx, progress, wikidataSqlite, osmCoordinates, regions, lastProcessedId);
		sx.parse(is, handler);
		handler.finish();
		osmCoordinates.closeConnection();
	}

	private static InputSource getInputSource(InputStream streamFile) throws IOException {
		GZIPInputStream zis = new GZIPInputStream(streamFile);
		Reader reader = new InputStreamReader(zis,"UTF-8");
		InputSource is = new InputSource(reader);
		is.setEncoding("UTF-8");
		return is;
	}

	public static class WikipediaHandler extends DefaultHandler {
		long counter = 1;
		private final SAXParser saxParser;
		private boolean page = false;
		private boolean revision = false;
		private StringBuilder ctext = null;

		private StringBuilder title = new StringBuilder();
		private StringBuilder text = new StringBuilder();
		private StringBuilder pageId = new StringBuilder();

		private DBDialect dialect = DBDialect.SQLITE;
		private Connection conn;
		private final WikiImageUrlStorage imageUrlStorage;
		private PreparedStatement insertPrep;
		private PreparedStatement selectPrep;
		private int batch = 0;
		private final static int BATCH_SIZE = 1500;
		private static final long ARTICLES_BATCH = 1000;
		private long testArticleId;

		final ByteArrayOutputStream bous = new ByteArrayOutputStream(64000);
		private String lang;
		final String[] wikiJunkArray = new String[] { ".jpg", ".JPG", ".jpeg", ".png", ".gif", ".svg", "/doc", "틀:",
				"위키프로젝트:", "แม่แบบ:", "위키백과:", "แม่แบบ:", "Àdàkọ:", "Aide:", "Aiuto:", "Andoza:", "Anexo:", "Bản:",
				"mẫu:", "Batakan:", "Categoría:", "Categoria:", "Catégorie:", "Category:", "Cithakan:", "Datei:",
				"Draft:", "Endrika:", "Fájl:", "Fichier:", "File:", "Format:", "Formula:", "Help:", "Hjælp:",
				"Kategori:", "Kategoria:", "Kategorie:", "Kigezo:", "モジュール:", "Mal:", "Mall:", "Malline:", "Modèle:",
				"Modèl:", "Modello:", "Modelo:", "Modèl:", "Moduł:", "Module:", "Modulis:", "Modul:", "Mô:", "đun:",
				"Nodyn:", "Padron:", "Patrom:", "Pilt:", "Plantía:", "Plantilla:", "Plantilya:", "Portaal:", "Portail:",
				"Portal:", "Portál:", "Predefinição:", "Predloga:", "Predložak:", "Progetto:", "Proiect:", "Projet:",
				"Sablon:", "Šablon:", "Şablon:", "Šablona:", "Šablóna:", "Šablonas:", "Ŝablono:", "Sjabloon:",
				"Schabloun:", "Skabelon:", "Snið:", "Stampa:", "Szablon:", "Templat:", "Txantiloi:", "Veidne:",
				"Vikipedio:", "Vikipediya:", "Vikipeedia:", "Viquipèdia:", "Viquiprojecte:", "Viquiprojecte:",
				"Vörlaag:", "Vorlage:", "Vorlog:", "วิกิพีเดีย:", "Wikipedia:", "Wikipedie:", "Wikipedija:",
				"Wîkîpediya:", "Wikipédia:", "Wikiproiektu:", "Wikiprojekt:", "Wikiproyecto:", "الگو:", "سانچ:",
				"قالب:", "وکیپیڈیا:", "ויקיפדיה:", "תבנית", "Βικιπαίδεια:", "Πρότυπο:", "Википедиа:", "Википедија:",
				"Википедия:", "Вікіпедія:", "Довідка:", "Загвар:", "Инкубатор:", "Калып:", "Ҡалып:", "Кеп:",
				"Категорія:", "Портал:", "Проект:", "Уикипедия:", "Үлгі:", "Файл:", "Хуызæг:", "Шаблон:", "Կաղապար:",
				"Մոդուլ:", "Վիքիպեդիա:", "ვიკიპედია:", "თარგი:", "ढाँचा:", "विकिपीडिया:", "साचा:", "साँचा:", "ઢાંચો:",
				"વિકિપીડિયા:", "మూస:", "வார்ப்புரு:", "ഫലകം:", "വിക്കിപീഡിയ:", "টেমপ্লেট:", "プロジェクト:", "উইকিপিডিয়া:",
				"মডেল:", "پرونده:", "模块:", "ماڈیول:" };
		private FileProgressImplementation progIS;
		private long cid;

		WikipediaHandler(SAXParser saxParser, FileProgressImplementation progIS, String lang, File wikipediaSqlite,
		                 long testArticleId) throws SQLException {
			this.lang = lang;
			this.saxParser = saxParser;
			this.progIS = progIS;
			this.testArticleId = testArticleId;
			conn = dialect.getDatabaseConnection(wikipediaSqlite.getAbsolutePath(), log);
			log.info("Prepare wiki_content table");
			conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wiki_content(id long, title text, lang text, zipContent blob)");
			conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_wiki_content ON wiki_content(id)");
			conn.createStatement().execute("CREATE INDEX IF NOT EXISTS lang_title_wiki_content ON wiki_content (lang, title)");
			conn.createStatement().execute("DELETE FROM wiki_content WHERE lang = '" + lang + "'");
			insertPrep = conn.prepareStatement("INSERT INTO wiki_content(id, title, lang, zipContent) VALUES (?, ?, ?, ?)");
			if (this.testArticleId == 0) {
				selectPrep = conn.prepareStatement("SELECT id FROM wiki_mapping WHERE wiki_mapping.title = ? AND wiki_mapping.lang = ?");
			}
			imageUrlStorage = new WikiImageUrlStorage(conn, wikipediaSqlite.getParent(), lang);
			log.info("Tables are prepared");
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
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			insertPrep.close();
			if (testArticleId == 0) {
				selectPrep.close();
			}
			conn.close();
		}

		public int getCount() {
			return (int) (counter - 1);
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
					revision = true;
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
					progIS.update();
					if (name.equals("page")) {
						page = false;
					} else if (name.equals("title")) {
						ctext = null;
					} else if (name.equals("revision")) {
						revision = false;
					} else if (name.equals("id") && !revision) {
						ctext = null;
						cid = Long.parseLong(pageId.toString());
					} else if (name.equals("text")) {
						boolean isJunk = false;
						long wikiId = 0;
						String plainStr = null;
						for (String wikiJunk : wikiJunkArray) {
							if (title.toString().contains(wikiJunk)) {
								isJunk = true;
								break;
							}
						}
						if (testArticleId == 0) {
							if (!isJunk) {
								selectPrep.setString(1, title.toString());
								selectPrep.setString(2, lang);
								ResultSet rs = selectPrep.executeQuery();
								if (rs.next()) {
									wikiId = rs.getLong(1);
								}
								selectPrep.clearParameters();
							}
						} else {
							wikiId = testArticleId;
							testArticleId++;
						}
						if (wikiId != 0) {
							try {
								plainStr = generateHtmlArticle(ctext, lang, title.toString(), imageUrlStorage);
							} catch (RuntimeException e) {
								log.error(String.format("Error with article %d - %s : %s", cid, title, e.getMessage()), e);
							}
						}
						if (plainStr != null) {
							if (++counter % ARTICLES_BATCH == 0) {
								log.info("Article accepted " + cid + " " + title.toString());
							}
							try {
								insertPrep.setLong(1, wikiId);
								insertPrep.setString(2, title.toString());
								insertPrep.setString(3, lang);
								bous.reset();
								GZIPOutputStream gzout = new GZIPOutputStream(bous);
								gzout.write(plainStr.getBytes("UTF-8"));
								gzout.close();
								final byte[] byteArray = bous.toByteArray();
								insertPrep.setBytes(4, byteArray);
								addBatch();
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
	}

	private static String generateHtmlArticle(StringBuilder contentText, String lang, String title, WikiImageUrlStorage imageUrlStorage)
			throws IOException, SQLException {
		String text = removeMacroBlocks(contentText, new HashMap<>(), lang, title, null);
		final HTMLConverter converter = new HTMLConverter(false);
		CustomWikiModel wikiModel = new CustomWikiModel("http://" + lang + ".wikipedia.org/wiki/${image}",
				"http://" + lang + ".wikipedia.org/wiki/${title}", imageUrlStorage, true);
		String plainStr = wikiModel.render(converter, text);
		plainStr = plainStr.replaceAll("<p>div class=&#34;content&#34;", "<div class=\"content\">\n<p>")
				.replaceAll("<p>/div\n</p>", "</div>");
		return plainStr;
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
