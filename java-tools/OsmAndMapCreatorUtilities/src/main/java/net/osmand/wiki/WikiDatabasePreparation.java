package net.osmand.wiki;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import info.bliki.wiki.model.WikiModel;
import net.osmand.PlatformUtil;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.travel.WikivoyageLangPreparation.WikivoyageTemplates;
import net.osmand.wiki.wikidata.WikiDataHandler;

public class WikiDatabasePreparation {
	private static final Log log = PlatformUtil.getLog(WikiDatabasePreparation.class);

	private static final Set<String> unitsOfDistance = new HashSet<>(Arrays.asList("mm", "cm", "m", "km", "in", "ft", "yd", "mi", "nmi", "m2"));
	
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

	public static String removeMacroBlocks(String text, Map<String, List<String>> blockResults, String lang, WikidataConnection wikidata) throws IOException, SQLException {
		StringBuilder bld = new StringBuilder();
		int openCnt = 0;
		int beginInd = 0;
		int endInd = 0;
		int headerCount = 0;
		for (int i = 0; i < text.length(); i++) {
			int nt = text.length() - i - 1;
			if ((nt > 0 && text.charAt(i) == '{' && text.charAt(i + 1) == '{')
					|| (nt > 4 && text.charAt(i) == '<' && text.charAt(i + 1) == 'm' && text.charAt(i + 2) == 'a' && text.charAt(i + 3) == 'p'
					&& text.charAt(i + 4) == 'l' && text.charAt(i + 5) == 'i')
					|| (nt > 2 && text.charAt(i) == '<' && text.charAt(i + 1) == '!' && text.charAt(i + 2) == '-' && text.charAt(i + 3) == '-')) {
				beginInd = beginInd == 0 ? i + 2 : beginInd;
				openCnt++;
				i++;
			} else if (nt > 0 && ((text.charAt(i) == '}' && text.charAt(i + 1) == '}')
					|| (i > 4 && text.charAt(i) == '>' && text.charAt(i - 1) == 'k' && text.charAt(i - 2) == 'n' && text.charAt(i - 3) == 'i'
					&& text.charAt(i - 4) == 'l' && text.charAt(i - 5) == 'p')
					|| (i > 1 && text.charAt(i) == '>' && text.charAt(i - 1) == '-' && text.charAt(i - 2) == '-'))) {
				if (openCnt > 1) {
					openCnt--;
					i++;
					continue;
				}
				openCnt--;
				endInd = i;
				String val = text.substring(beginInd, endInd);
				if (val.startsWith("gallery")) {
					bld.append(parseGalleryString(val));
				} else if (val.toLowerCase().startsWith("weather box")) {
					parseAndAppendWeatherTable(val, bld);
				} else if (val.startsWith("wide image") || val.startsWith("תמונה רחבה")) {
					bld.append(parseWideImageString(val));
				}
				String key = getKey(val.toLowerCase());
				if (key.equals(WikivoyageTemplates.POI.getType())) {
					bld.append(parseListing(val, wikidata, lang));
				} else if (key.equals(WikivoyageTemplates.REGION_LIST.getType())) {
					bld.append((parseRegionList(val)));
				} else if (key.equals(WikivoyageTemplates.WARNING.getType())) {
					appendWarning(bld, val);
				} else if (key.equals(WikivoyageTemplates.CITATION.getType())) {
					parseAndAppendCitation(val, bld);
				} else if (key.equals(WikivoyageTemplates.TWO_PART.getType())) {
					parseAndAppendTwoPartFormat(val, bld);
				} else if (key.equals(WikivoyageTemplates.STATION.getType())) {
					parseAndAppendStation(val, bld);
				} else if (key.equals(WikivoyageTemplates.METRIC_DATA.getType())) {
					parseAndAppendMetricData(val, bld);
				} else if (key.equals(WikivoyageTemplates.TRANSLATION.getType())) {
					parseAndAppendTranslation(val, bld);
				}
				if (!key.isEmpty() && blockResults != null) {
					if (key.contains("|")) {
						for (String str : key.split("\\|")) {
							addToMap(blockResults, str, val);
						}
					} else {
						addToMap(blockResults, key, val);
					}
				}
				if (text.charAt(i + 1) != ' ' && text.charAt(i) != '}') {
					i--;
				}
				i++;
			} else if (nt > 2 && text.charAt(i) == '<' && text.charAt(i + 1) == 'r' && text.charAt(i + 2) == 'e'
					&& text.charAt(i + 3) == 'f' && openCnt == 0) {
				i = parseRef(text, bld, i);
			} else if (nt > 2 && text.charAt(i) == '<' && text.charAt(i + 1) == 'g' && text.charAt(i + 2) == 'a' && text.charAt(i + 3) == 'l') {
				i = parseGallery(text, bld, i);
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
					beginInd = 0;
					endInd = 0;
				}
			}
		}
		return bld.toString();
	}

	private static int parseGallery(String text, StringBuilder bld, int i) {
		int closeTag = text.indexOf("</gallery>", i);
		int endInd = closeTag + "</gallery>".length();
		if (endInd > text.length() - 1 || closeTag < i) {
			return i;
		}
		String val = text.substring(i, closeTag);
		bld.append(parseGalleryString(val));
		return --endInd;
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

	private static int parseRef(String text, StringBuilder bld, int i) {
		int closingTag = text.indexOf("</", i);
		int selfClosed = text.indexOf("/>", i);
		closingTag = closingTag < 0 ? Integer.MAX_VALUE : closingTag;
		selfClosed = selfClosed < 0 ? Integer.MAX_VALUE : selfClosed;
		int endInd = Math.min(closingTag, selfClosed);
		endInd = endInd == closingTag ? endInd + "</ref>".length() : endInd + "/>".length();
		if (endInd > text.length() - 1 || endInd < 0) {
			return i;
		}
		parseAndAppendCitation(text.substring(i, endInd), bld);
		// return index of the last char in the reference
		return --endInd;
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
		val = val.replaceAll("\\{\\{.*}}", "");
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
		if (wikiLink.isEmpty() && !wikiData.isEmpty() && wikiDataconn != null) {
			wikiLink = wikiDataconn.getWikipediaTitleByWid(wikiLang, wikiData); 
		}
		if (!wikiLink.isEmpty()) {
			bld.append(addWikiLink(wikiLang, wikiLink, lat, lon));
			bld.append(" ");
		}
		if (lat != null && lon != null) {
			bld.append(" geo:").append(lat).append(",").append(lon);
		}
		return bld.toString();
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
				|| str.startsWith("انجام‌دادن") || str.startsWith("نوشیدن")
				|| str.startsWith("event")) {
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
		} else if (str.startsWith("cite")) {
			return WikivoyageTemplates.CITATION.getType();
		} else if (str.startsWith("iata")|| str.startsWith("formatnum")) {
			return WikivoyageTemplates.TWO_PART.getType();
		} else if (str.startsWith("station") || str.startsWith("rint")) {
			return WikivoyageTemplates.STATION.getType();
		} else if (str.startsWith("ipa") || str.startsWith("lang-")) {
			return WikivoyageTemplates.TRANSLATION.getType();
		} else {
			Set<String> parts = new HashSet<>(Arrays.asList(str.split("\\|")));
			if (parts.contains("convert") || parts.contains("unité")) {
				return WikivoyageTemplates.METRIC_DATA.getType();
			} else {
				parts.retainAll(unitsOfDistance);
				if (!parts.isEmpty())
					return WikivoyageTemplates.METRIC_DATA.getType();
			}
		}
		return "";
	}

	public static void mainTest(String[] args) throws ConversionException, ComponentLookupException, ParseException, IOException, SQLException {
		EmbeddableComponentManager cm = new EmbeddableComponentManager();
		cm.initialize(WikiDatabasePreparation.class.getClassLoader());
		Parser parser = cm.getInstance(Parser.class, Syntax.MEDIAWIKI_1_0.toIdString());
		FileReader fr = new FileReader(new File("/Users/victorshcherb/Documents/b.src.html"));
		String content = "";
		String s;
		BufferedReader br = new BufferedReader(fr);
		while((s = br.readLine()) != null) {
			content += s;
		}
		br.close();
		//content = removeMacroBlocks(content, null, "en", null);
		//for testing file.html after removeMacroBlocks and generating new file.html
		content = generateHtmlArticle(content, "en", null);
		String savePath = "/Users/plotva/Documents";
		File myFile = new File(savePath, "page.html");
		BufferedWriter htmlFileWriter = new BufferedWriter(new FileWriter(myFile, false));
		htmlFileWriter.write(content);
        htmlFileWriter.close();
		
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
		String mode = "";
		long testArticleID = 0;

		for (String arg : args) {
			String val = arg.substring(arg.indexOf("=") + 1);
			if (arg.startsWith("--lang=")) {
				lang = val;
			} else if (arg.startsWith("--dir=")) {
				folder = val;
			} else if (arg.startsWith("--mode=")) {
				mode = val;
			} else if (arg.startsWith("--testID=")) {
				testArticleID = Long.parseLong(val);
			}
		}
		if (mode.isEmpty() || folder.isEmpty()
				|| ((mode.equals("process-wikipedia") || mode.equals("test-wikipedia")) && lang.isEmpty())) {
			throw new RuntimeException("Correct arguments weren't supplied");
		}

		final String wikiPg = folder + lang + "wiki-latest-pages-articles.xml.gz";
		final String sqliteFileName = folder + "wiki.sqlite";
		final String pathToWikiData = folder + "wikidatawiki-latest-pages-articles.xml.gz";

		switch (mode) {
			case "process-wikidata-regions":
				processWikidataRegions(sqliteFileName);
				break;
			case "process-wikidata":
				File wikiDB = new File(sqliteFileName);
				if (!new File(pathToWikiData).exists()) {
					throw new RuntimeException("Wikidata dump doesn't exist. Exiting.");
				}
				if (wikiDB.exists()) {
					wikiDB.delete();
				}
				log.info("Processing wikidata...");
				processDump(pathToWikiData, sqliteFileName, null);
				break;
			case "process-wikipedia":
				processDump(wikiPg, sqliteFileName, lang);
				break;
			case "test-wikipedia":
				processDump(wikiPg, sqliteFileName, lang, testArticleID);
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

	private static void processDump(final String wikiPg, String sqliteFileName, String lang)
			throws SQLException, ParserConfigurationException, IOException, SAXException {
		processDump(wikiPg, sqliteFileName,lang,0);
	}

	public static void  processDump(final String wikiPg, String sqliteFileName, String lang, long testArticleId)
			throws ParserConfigurationException, SAXException, IOException, SQLException {
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		FileProgressImplementation prog = new FileProgressImplementation("Read wikidata file", new File(wikiPg));
		InputStream streamFile = prog.openFileInputStream();
		InputSource is = getInputSource(streamFile);
		if (lang != null) {
			final WikiOsmHandler handler = new WikiOsmHandler(sx, prog, lang, new File(sqliteFileName), testArticleId);
			sx.parse(is, handler);
			handler.finish();
		} else {
			OsmandRegions regions = new OsmandRegions();
			regions.prepareFile();
			regions.cacheAllCountries();
			final WikiDataHandler handler = new WikiDataHandler(sx, prog, new File(sqliteFileName), regions);
			sx.parse(is, handler);
			handler.finish();
		}
	}

	private static InputSource getInputSource(InputStream streamFile) throws IOException {
		GZIPInputStream zis = new GZIPInputStream(streamFile);
		Reader reader = new InputStreamReader(zis,"UTF-8");
		InputSource is = new InputSource(reader);
		is.setEncoding("UTF-8");
		return is;
	}

	public static class WikiOsmHandler extends DefaultHandler {
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
		private final long testArticleId;

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

		WikiOsmHandler(SAXParser saxParser, FileProgressImplementation progIS, String lang, File sqliteFile,
		               long testArticleId) throws IOException, SQLException {
			this.lang = lang;
			this.saxParser = saxParser;
			this.progIS = progIS;
			this.testArticleId = testArticleId;
			conn = dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
			log.info("Prepare wiki_content table");
			conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wiki_content(id long, title text, lang text, zipContent blob)");
			conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_wiki_content ON wiki_content(id)");
			conn.createStatement().execute("CREATE INDEX IF NOT EXISTS lang_title_wiki_content ON wiki_content (lang, title)");
			conn.createStatement().execute("DELETE FROM wiki_content WHERE lang = '" + lang + "'");
			insertPrep = conn.prepareStatement("INSERT INTO wiki_content(id, title, lang, zipContent) VALUES (?, ?, ?, ?)");
			if (this.testArticleId == 0) {
				selectPrep = conn.prepareStatement("SELECT id FROM wiki_mapping WHERE wiki_mapping.title = ? AND wiki_mapping.lang = ?");
			}
			imageUrlStorage = new WikiImageUrlStorage(conn);
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
						}
						if (wikiId != 0) {
							try {
								imageUrlStorage.setArticleTitle(title.toString());
								imageUrlStorage.setLang(lang);
								plainStr = generateHtmlArticle(ctext.toString(), lang, imageUrlStorage);
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

	private static String generateHtmlArticle(String contentText, String lang, WikiImageUrlStorage imageUrlStorage)
			throws IOException, SQLException {
		String text = removeMacroBlocks(contentText, new HashMap<>(), lang, null);
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
