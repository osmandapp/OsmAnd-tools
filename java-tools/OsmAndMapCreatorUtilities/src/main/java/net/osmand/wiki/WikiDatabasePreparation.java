package net.osmand.wiki;

import static java.util.EnumSet.of;
import static net.osmand.util.Algorithms.stringsEqual;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
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
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicLong;
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

import javax.annotation.Nullable;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xmlpull.v1.XmlPullParserException;
import org.xwiki.component.manager.ComponentLookupException;

import gnu.trove.map.hash.TIntObjectHashMap;
import info.bliki.wiki.filter.HTMLConverter;
import info.bliki.wiki.filter.PlainTextConverter;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.travel.WikivoyageLangPreparation.WikivoyageTemplates;
import net.osmand.util.Algorithms;
import net.osmand.util.LocationParser;
import net.osmand.wiki.OsmCoordinatesByTag.OsmLatLonId;
import net.osmand.wiki.commonswiki.parser.AuthorParser;
import net.osmand.wiki.commonswiki.parser.DateParser;
import net.osmand.wiki.commonswiki.parser.DescriptionParser;
import net.osmand.wiki.commonswiki.parser.LicenseParser;
import net.osmand.wiki.commonswiki.parser.ParserUtils;
import net.osmand.wiki.wikidata.WikiDataHandler;

public class WikiDatabasePreparation {
	private static final Log log = PlatformUtil.getLog(WikiDatabasePreparation.class);

	private static final Set<String> unitsOfDistance = new HashSet<>(
			Arrays.asList("mm", "cm", "m", "km", "in", "ft", "yd", "mi", "nmi", "m2"));
	public static final String WIKIPEDIA_SQLITE = "wikipedia.sqlite";
	public static final String WIKIRATING_SQLITE = "wiki_rating.sqlite";
	public static final String WIKIDATA_MAPPING_SQLITE = "wikidata_mapping.sqlitedb";
	public static final String WIKIDATA_ARTICLES_GZ = "wikidatawiki-latest-pages-articles.xml.gz";
	public static final String WIKI_ARTICLES_GZ = "wiki-latest-pages-articles.xml.gz";
	public static final String OSM_WIKI_FILE_PREFIX = "osm_wiki_";

	private static final int OPTIMAL_SHORT_DESCR = 250;
	private static final int OPTIMAL_LONG_DESCR = 500;
	private static final int SHORT_PARAGRAPH = 10;

	public static final String DEFAULT_LANG = "en";
	

	public enum PoiFieldType {
		NAME, PHONE, WEBSITE, WORK_HOURS, PRICE, DIRECTIONS, WIKIPEDIA, WIKIDATA, FAX, EMAIL, DESCRIPTION, LON, LAT,
		ADDRESS, AREA_CODE,
		// Object categories
		CATEGORY, LATLON
	}

	public enum PoiFieldCategory {
		SEE("special_photo_camera", 0xCC10A37E, new String[] { "see", "voir", "veja", "מוקדי", "دیدن" }, "church",
				"mosque", "square", "town_hall", "building", "veja", "voir", "temple", "mosque", "synagogue",
				"monastery", "palace", "château", "memorial", "archaeological", "fort", "monument", "castle", "دیدن",
				"tower", "cathedral", "arts centre", "mill", "house", "ruins"),
		DO("special_photo_camera", 0xCC10A37E, new String[] { "do", "event", "פעילויות", "انجام‌دادن" }, "museum",
				"zoo", "theater", "fair", "faire", "cinema", "disco", "sauna", "aquarium", "swimming", "amusement",
				"golf", "club", "sports", "music", "spa", "انجام‌دادن", "festival"),
		EAT("restaurants", 0xCCCA2D1D, new String[] { "eat", "manger", "coma", "אוכל", "خوردن" }, "restaurant", "cafe",
				"bistro"),
		DRINK("restaurants", 0xCCCA2D1D, new String[] { "drink", "boire", "beba", "שתייה", "نوشیدن" }, "bar", "pub"),
		SLEEP("tourism_hotel", 0xCC0E53C9, new String[] { "sleep", "se loger", "durma", "לינה", "خوابیدن" }, "hotel",
				"hostel", "habitat", "campsite"),
		BUY("shop_department_store", 0xCC8F2BAB, new String[] { "buy", "קניות", "فهرست‌بندی" }, "shop", "market",
				"mall"),
		GO("public_transport_stop_position", 0xCC0F5FFF,
				new String[] { "go", "destination", "aller", "circuler", "sortir", "רשימה" }, "airport", "train",
				"station", "bus"),
		NATURAL("special_photo_camera", 0xCC10A37E, new String[] { "landscape", "island", "nature", "island" }, "park",
				"cemetery", "garden", "lake", "beach", "landmark", "cemetery", "cave", "garden", "waterfall",
				"viewpoint", "mountain"),
		OTHER("", 0xCC0F5FFF, new String[] { "other", "marker", "ville", "item", "רשימה", "دیدن", "יעד מרכזי",
				"יישוב מרכזי", "représentation diplomatique" });

		public final String[] names;
		public final String[] types;
		public final String icon;
		public final int color;

		private PoiFieldCategory(String icon, int color, String[] names, String... types) {
			this.icon = icon;
			this.color = color;
			this.names = names;
			this.types = types;
		}

	}

	public interface WikiDBBrowser {

		public LatLon getLocation(String lang, String wikiLink, long wikiDataQId) throws SQLException;

		public String getWikipediaTitleByWid(String lang, long wikiDataQId) throws SQLException;
	}

	public static LatLon getLatLonFromGeoBlock(List<String> list, String lang, String title) {
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
						int eq = part.indexOf('=');
						if (eq == -1) {
							continue;
						}
						String key = part.substring(0, eq).trim().toLowerCase();
						String val = part.substring(eq + 1).trim();
						if (key.equals("lat") || key.equals("latitude")) {
							lat = val;
						} else if (key.equals("long") || key.equals("longitude")) {
							lon = val;
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
				System.err.printf("Error structure geo (%s %s): %s \n", lang, title,
						location.substring(0, Math.min(location.length(), 10)));
			}
			return ll;
		}
		return null;
	}

	public static boolean isEmpty(String lat) {
		return "".equals(lat) || "NA".equals(lat) || "N/A".equals(lat);
	}

	public static LatLon parseLocation(String lat, String lon) {
		String loc = lat + " " + lon;
		if (!loc.contains(".") && loc.contains(",")) {
			loc = loc.replace(',', '.');
		}
		return LocationParser.parseLocation(loc);
	}

	public static LatLon parseLatLon(String lat, String lon) {
		if (isEmpty(lat) || isEmpty(lon)) {
			return null;
		}
		try {
			LatLon res = parseLocation(lat, lon);
			if (res != null) {
				return res;
			}
			return new LatLon(Double.parseDouble(lat), Double.parseDouble(lon));
		} catch (RuntimeException e) {
			System.err.printf("Error point lat=%s lon=%s (%s)\n", lat, lon,
					// LocationParser.parseLocation(lat + " " + lon),
					e.getMessage());
			return null;
		}
	}
	
	public static String removeMacroBlocks(StringBuilder text,
	                                       @Nullable Map<String, String> webBlockResults,
	                                       Map<WikivoyageTemplates, List<String>> blockResults,
	                                       @Nullable List<Map<PoiFieldType, Object>> pois,
	                                       String lang,
	                                       String title,
	                                       @Nullable WikiDBBrowser browser,
	                                       @Nullable Boolean allLangs)
			throws IOException, SQLException {
		return removeMacroBlocks(text, webBlockResults, blockResults, pois, lang, title, browser, allLangs, null, true);
	}

	public static String removeMacroBlocks(StringBuilder text,
	                                       @Nullable Map<String, String> webBlockResults,
	                                       Map<WikivoyageTemplates, List<String>> blockResults,
	                                       @Nullable List<Map<PoiFieldType, Object>> pois,
	                                       String lang,
	                                       String title,
	                                       @Nullable WikiDBBrowser browser,
	                                       @Nullable Boolean allLangs,
	                                       @Nullable AtomicLong errorContentBracesCounter,
	                                       boolean logErrorContentBraces)
			throws IOException, SQLException {
		StringBuilder bld = new StringBuilder();
		int openCnt = 0;
		int beginInd = 0;
		int headerCount = 0;
		
		final String TAG_GALLERY = "gallery";
		final String TAG_WEATHER_BOX = "weather box";
		final String TAG_WIDE_IMAGE = "wide image";
		final String TAG_REF = "ref";
		final String LICENSE_HEADER = "=={{int:license-header}}==";
		
		boolean isLicenseBlock = false;
		List<String> licenseBlock = new ArrayList<>();
		
		for (int i = 0; i < text.length(); ) {
			int leftChars = text.length() - i - 1;
			if (isCommentOpen(text, leftChars, i)) {
				int endI = skipComment(text, i + 3);
				text.replace(i, endI + 1, "");
			} else if (isNowikiOpen(text, i)) {
				int endI = skipNowiki(text, i);
				text.replace(i, endI + 1, "");
			} else {
				i++;
			}
		}
		Set<Integer> errorBracesCnt = new TreeSet<>();
		String[] tagsRetrieve = {TAG_GALLERY, TAG_WEATHER_BOX, TAG_WIDE_IMAGE, TAG_REF};
		int cursor = -1;
		for (int i = 0; ; i++) {
			if (cursor >= i) {
				log.error(String.format("BUG ! %d -> %d content parsing: %s %s", cursor, i, lang, title));
				i = cursor + 1; // loop detected
			}
			cursor = i;
			if (i >= text.length()) {
				if (openCnt > 0) {
					if (logErrorContentBraces) {
						String contentPreview = text.substring(beginInd, Math.min(text.length() - 1, beginInd + 20));
						log.error(String.format("Error content braces {{ }}: %s %s ...%s", lang, title, contentPreview));
					}
					if (errorContentBracesCounter != null) {
						errorContentBracesCounter.incrementAndGet();
					}
					// start over again
					errorBracesCnt.add(beginInd);
					beginInd = openCnt = i = 0;
					cursor = -1;
					blockResults.clear();
				} else {
					break;
				}
			}
			int leftChars = text.length() - i - 1;
			if (openCnt == 0 && text.charAt(i) == '<') {
				boolean found = false;
				for (String tag : tagsRetrieve) {
					if (leftChars > tag.length()
							&& text.substring(i + 1, i + 1 + tag.length()).toLowerCase().equals(tag)) {
						found = true;
						StringBuilder val = new StringBuilder();
						i = parseTag(text, val, tag, i, lang, title);
						if (tag.equals(TAG_REF)) {
							parseAndAppendCitation(val.toString(), bld);
						} else if (tag.equals(TAG_GALLERY)) {
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
					i++;
					continue;
				}
				openCnt--;
				if (openCnt > 0) {
					i++;
					continue;
				}
				int endInd = i;
				String val = text.substring(beginInd, endInd);
				if (isLicenseBlock) {
					licenseBlock.add(val);
				}
				beginInd = 0;
				String vallc = val.toLowerCase().trim();
				if (val.startsWith(TAG_GALLERY)) {
					bld.append(parseGalleryString(val));
				} else if (vallc.startsWith(TAG_WEATHER_BOX)) {
					parseAndAppendWeatherTable(val, bld);
				} else if (vallc.startsWith(TAG_WIDE_IMAGE) || vallc.startsWith("תמונה רחבה")) {
					bld.append(parseWideImageString(val));
				} else if (ParserUtils.isInformationLikeTemplate(vallc)) {
					parseInformationBlock(val, lang, webBlockResults, allLangs);
				}
				PoiFieldCategory pc = isPOIKey(vallc, lang);
				EnumSet<WikivoyageTemplates> key = pc != null ? of(WikivoyageTemplates.POI) : getKey(vallc, lang);
				if (pc != null) {
					val = val.replaceAll("\\{\\{.*}}", "");
					Map<PoiFieldType, Object> poiFields = new HashMap<>();
					poiFields.put(PoiFieldType.CATEGORY, pc);
					bld.append(getWikivoyagePoiHtmlDescription(lang, browser, poiFields, val));
					if (pois != null && !poiFields.isEmpty()) {
						pois.add(poiFields);
					}
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
				i++;
			} else if (openCnt == 0) {
				int headerLvl = 0;
				int indexCopy = i;
				if (isLicenseBlock) {
					String license = LicenseParser.parse(licenseBlock);
					if (license != null && webBlockResults != null) {
							webBlockResults.put(ParserUtils.FIELD_LICENSE, license);
						}

					isLicenseBlock = false;
				}
				if (i > 0 && text.charAt(i - 1) != '=') {
					headerLvl = calculateHeaderLevel(text, i);
					indexCopy = indexCopy + headerLvl;
				}
				if (text.charAt(indexCopy) != '\n' && headerLvl > 1) {
					int indEnd = text.indexOf("=", indexCopy);
					if (indEnd != -1) {
						indEnd = indEnd + calculateHeaderLevel(text, indEnd);
						char ch = indEnd < text.length() - 2 ? text.charAt(indEnd + 1) : text.charAt(indEnd);
						if (text.substring(i, indEnd).replace(" ", "").contains(LICENSE_HEADER)) {
							isLicenseBlock = true;
						}
						int nextHeader = calculateHeaderLevel(text, ch == '\n' ? indEnd + 2 : indEnd + 1);
						if (nextHeader > 1 && headerLvl >= nextHeader) {
							i = indEnd;
						} else if (headerLvl == 2) {
							if (headerCount != 0) {
								bld.append("\n/div\n");
							}
							bld.append(text, i, indEnd);
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

	public static void prepareMetaData(Map<String, String> metaData) {
		metaData.computeIfPresent(ParserUtils.FIELD_LICENSE, (k, v) -> LicenseParser.process(v));
	}


	private static void parseInformationBlock(String val, String lang, Map<String, String> webBlockResults, Boolean allLangs) throws IOException {
		
		if (webBlockResults == null || val == null) {
			return;
		}
		
		String author = null;
		String date = null;
		String license = null;
		Map<String, String> description = new HashMap<>();
		
		// Clean up the input string by removing extra spaces and newlines
		val = ParserUtils.normalizeWhitespace(val);
		
		List<String> parts = ParserUtils.splitByPipeOutsideBraces(val, true);
		
		// Process all parts looking for metadata fields (no need to check template name)
		for (String line : parts) {
			line = line.trim();
			String lineLc = line.toLowerCase();
			
			if (lineLc.startsWith(ParserUtils.FIELD_AUTHOR) || lineLc.startsWith(ParserUtils.FIELD_PHOTOGRAPHER)) {
				author = AuthorParser.parse(line);
			}
			if (lineLc.startsWith(ParserUtils.FIELD_DATE)) {
				date = DateParser.parse(line);
			}
			if (lineLc.startsWith(ParserUtils.FIELD_DESCRIPTION)) {
				description = DescriptionParser.parse(line);
			}
			if (lineLc.startsWith(ParserUtils.FIELD_LICENSE) || lineLc.startsWith("permission")) {
				String licenseValue = ParserUtils.extractFieldValue(line, ParserUtils.FIELD_LICENSE);
				if (licenseValue == null) {
					licenseValue = ParserUtils.extractFieldValue(line, "permission");
				}
				if (licenseValue != null) {
					license = LicenseParser.parseFromInformationBlock(licenseValue);
				}
			}
		}

		if (author != null) {
			webBlockResults.put(ParserUtils.FIELD_AUTHOR, author);
		}
		if (date != null) {
			webBlockResults.put(ParserUtils.FIELD_DATE, date);
		}
		if (license != null) {
			webBlockResults.put(ParserUtils.FIELD_LICENSE, license);
		}
		if (!description.isEmpty()) {
			if (Boolean.TRUE.equals(allLangs)) {
				webBlockResults.put(ParserUtils.FIELD_DESCRIPTION, new Gson().toJson(description));
			} else {
				for (Map.Entry<String, String> entry : description.entrySet()) {
					if (entry.getKey().equals(lang)) {
						webBlockResults.put(ParserUtils.FIELD_DESCRIPTION, entry.getValue());
					}
				}

				// If no description was found in the target language, fallback to English description (default language)
				if (!webBlockResults.containsKey(ParserUtils.FIELD_DESCRIPTION) && description.containsKey(DEFAULT_LANG)) {
					webBlockResults.put(ParserUtils.FIELD_DESCRIPTION, description.get(DEFAULT_LANG));
				}

				// If no description for English either, fallback to the first available description
				if (!webBlockResults.containsKey(ParserUtils.FIELD_DESCRIPTION) && !description.isEmpty()) {
					webBlockResults.put(ParserUtils.FIELD_DESCRIPTION, description.values().iterator().next());
				}
			}
		}
    }
	
	private static StringBuilder getWikivoyagePoiHtmlDescription(String lang, WikiDBBrowser browser,
			Map<PoiFieldType, Object> poiFields, String val)
			throws IOException, SQLException, UnsupportedEncodingException {
		StringBuilder poiShortDescription = parsePoiWithAddLatLon(val, poiFields);
		String wikiLink = (String) poiFields.get(PoiFieldType.WIKIPEDIA);
		String wikiDataQId = (String) poiFields.get(PoiFieldType.WIKIDATA);
		long wikidataId = 0;
		if (!Algorithms.isEmpty(wikiDataQId)) {
			try {
				if (wikiDataQId.indexOf(' ') > 0) {
					wikidataId = Long.parseLong(wikiDataQId.substring(1, wikiDataQId.indexOf(' ')));
				} else {
					wikidataId = Long.parseLong(wikiDataQId.substring(1));
				}
			} catch (NumberFormatException e) {
				System.err.println("Error point wid - " + wikiDataQId);
			}
		}
		if (Algorithms.isEmpty(wikiLink) && wikidataId > 0 && browser != null) {
			wikiLink = browser.getWikipediaTitleByWid(lang, wikidataId);
		}
		LatLon latLon = browser == null ? null : browser.getLocation(lang, wikiLink, wikidataId);
		if (!Algorithms.isEmpty(wikiLink) && !stringsEqual((String) poiFields.get(PoiFieldType.WIKIPEDIA), wikiLink)) {
			poiFields.put(PoiFieldType.WIKIPEDIA, wikiLink);
		}
		if (wikidataId > 0 && !stringsEqual((String) poiFields.get(PoiFieldType.WIKIDATA), "Q" + wikidataId)) {
			poiFields.put(PoiFieldType.WIKIDATA, "Q" + wikidataId);
		}
		if (latLon == null && poiFields.containsKey(PoiFieldType.LAT) && poiFields.containsKey(PoiFieldType.LON)) {
			latLon = parseLatLon((String) poiFields.get(PoiFieldType.LAT), (String) poiFields.get(PoiFieldType.LON));
		}
		if (!Algorithms.isEmpty(wikiLink)) {
			poiShortDescription.append(addWikiLink(lang, wikiLink, latLon));
			poiShortDescription.append(" ");
		}
		if (latLon != null) {
			poiFields.put(PoiFieldType.LATLON, latLon);
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

	private static boolean isNowikiOpen(StringBuilder text, int i) {
		int len = text.length();
		if (i + 7 >= len) {
			return false;
		}
		String maybeTag = text.substring(i, i + 8);
		return maybeTag.equalsIgnoreCase("<nowiki>");
	}

	private static int skipNowiki(StringBuilder text, int i) {
		int len = text.length();
		int pos = i + 8; // after "<nowiki>"
		while (pos < len) {
			if (pos + 8 < len) {
				String maybeClose = text.substring(pos, pos + 9);
				if (maybeClose.equalsIgnoreCase("</nowiki>")) {
					return pos + 8;
				}
			}
			pos++;
		}
		// If no closing tag found, skip to end
		return len - 1;
	}

	private static boolean isCommentClosed(StringBuilder text, int i) {
		return i > 1 && text.charAt(i - 2) == '-' && text.charAt(i - 1) == '-' && text.charAt(i) == '>';
	}

	private static boolean isCommentOpen(StringBuilder text, int nt, int i) {
		return nt > 2 && text.charAt(i) == '<' && text.charAt(i + 1) == '!' && text.charAt(i + 2) == '-'
				&& text.charAt(i + 3) == '-';
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

	private static int parseTag(StringBuilder text, StringBuilder bld, String tag, int indOpen, String lang,
			String title) {
		int selfClosed = text.indexOf("/>", indOpen);
		int nextTag = text.indexOf("<", indOpen + 1);
		if (selfClosed > 0 && (selfClosed < nextTag || nextTag == -1)) {
			bld.append(text.substring(indOpen + 1, selfClosed));
			return selfClosed + 1;
		}
		int ind = text.indexOf("</" + tag, indOpen);
		int ind2 = text.indexOf("</ " + tag, indOpen);
		if (ind == -1 && ind2 == -1) {
			String lc = text.toString().toLowerCase();
			ind = lc.indexOf("</" + tag, indOpen);
			ind2 = lc.indexOf("</ " + tag, indOpen);
		}
		if (ind2 > 0) {
			ind = ind == -1 ? ind2 : Math.min(ind2, ind);
		}

		int lastChar = text.indexOf(">", ind);
		if (ind == -1 || lastChar == -1) {
			String contentPreview = text.substring(indOpen + 1, Math.min(text.length() - 1, indOpen + 1 + 10));
			log.error(String.format("Error content tag (not closed) %s %s: %s", lang, title, contentPreview));
			return indOpen + 1;
		}
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
			if (toCompare.contains(".jpg") || toCompare.contains(".jpeg") || toCompare.contains(".png")
					|| toCompare.contains(".gif")) {
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
			if (toCompare.contains(".jpg") || toCompare.contains(".jpeg") || toCompare.contains(".png")
					|| toCompare.contains(".gif")) {
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

	private static void addToMap(Map<WikivoyageTemplates, List<String>> blocksMap, WikivoyageTemplates key,
			String val) {
		if (blocksMap.containsKey(key)) {
			blocksMap.get(key).add(val);
		} else {
			List<String> tmp = new ArrayList<>();
			tmp.add(val);
			blocksMap.put(key, tmp);
		}
	}

	private static StringBuilder parsePoiWithAddLatLon(String val, Map<PoiFieldType, Object> poiFields)
			throws IOException, SQLException {
		StringBuilder poiShortDescription = new StringBuilder();
		String[] parts = val.split("\\|");
		for (int i = 1; i < parts.length; i++) {
			String field = parts[i].trim();
			String value = "";
			int index = field.indexOf("=");
			if (index != -1) {
				value = appendSqareBracketsIfNeeded(i, parts, field.substring(index + 1, field.length()).trim())
						.replaceAll("\n", " ").trim();
				field = field.substring(0, index).trim();
			}
			if (!value.isEmpty() && !value.contains("{{")) {
				try {
					if (field.equalsIgnoreCase(("name")) || field.equalsIgnoreCase("nome")
							|| field.equalsIgnoreCase("nom") || field.equalsIgnoreCase("שם")
							|| field.equalsIgnoreCase("نام")) {
						poiShortDescription.append("'''").append(value).append("'''").append(", ");
						int l = value.indexOf("[[");
						int e = value.indexOf("]]");
						if (l >= 0 && e >= 0) {
							poiFields.put(PoiFieldType.NAME, value.substring(l + 2, e).trim());
						} else {
							poiFields.put(PoiFieldType.NAME, value);
						}
					} else if (field.equalsIgnoreCase("url") || field.equalsIgnoreCase("sito")
							|| field.equalsIgnoreCase("האתר הרשמי") || field.equalsIgnoreCase("نشانی اینترنتی")) {
						poiShortDescription.append("Website: ").append(value).append(". ");
						poiFields.put(PoiFieldType.WEBSITE, value);
					} else if (field.equalsIgnoreCase("intl-area-code")) {
						poiFields.put(PoiFieldType.AREA_CODE, value);
					} else if (field.equalsIgnoreCase("address") || field.equalsIgnoreCase("addresse")
							|| field.equalsIgnoreCase("כתובת") || field.equalsIgnoreCase("نشانی")) {
						poiShortDescription.append(value).append(", ");
						poiFields.put(PoiFieldType.ADDRESS, value);
					} else if (field.equalsIgnoreCase("lat") || field.equalsIgnoreCase("latitude")
							|| field.equalsIgnoreCase("عرض جغرافیایی")) {
//						lat = value;
						poiFields.put(PoiFieldType.LAT, value);
					} else if (field.equalsIgnoreCase("long") || field.equalsIgnoreCase("longitude")
							|| field.equalsIgnoreCase("طول جغرافیایی")) {
//						lon = value;
						poiFields.put(PoiFieldType.LON, value);
					} else if (field.equalsIgnoreCase("content") || field.equalsIgnoreCase("descrizione")
							|| field.equalsIgnoreCase("description") || field.equalsIgnoreCase("sobre")
							|| field.equalsIgnoreCase("תיאור") || field.equalsIgnoreCase("متن")) {
						poiShortDescription.append(value).append(" ");
						poiFields.put(PoiFieldType.DESCRIPTION, value);
					} else if (field.equalsIgnoreCase("email") || field.equalsIgnoreCase("מייל")
							|| field.equalsIgnoreCase("پست الکترونیکی")) {
						poiShortDescription.append("e-mail: " + "mailto:").append(value).append(", ");
						poiFields.put(PoiFieldType.EMAIL, value);
					} else if (field.equalsIgnoreCase("fax") || field.equalsIgnoreCase("פקס")
							|| field.equalsIgnoreCase("دورنگار")) {
						poiFields.put(PoiFieldType.FAX, value);
						poiShortDescription.append("fax: ").append(value).append(", ");
					} else if (field.equalsIgnoreCase("wdid") || field.equalsIgnoreCase("wikidata")
							|| field.equalsIgnoreCase("ויקינתונים")) {
//						wikiDataQId = value;
						poiFields.put(PoiFieldType.WIKIDATA, value);
					} else if (field.equalsIgnoreCase("phone") || field.equalsIgnoreCase("tel")
							|| field.equalsIgnoreCase("téléphone") || field.equalsIgnoreCase("טלפון")
							|| field.equalsIgnoreCase("تلفن")) {
//						String tel = areaCode.replaceAll("[ -]", "/") + "/" + value.replaceAll("[ -]", "/")
//							.replaceAll("[^\\d\\+\\)\\(,]", "");
//						tel = tel.replaceAll("\\(", "o").replaceAll("\\)", "c");
						poiFields.put(PoiFieldType.PHONE, value);
						poiShortDescription.append("☎ " + "tel:").append(value).append(". ");
					} else if (field.equalsIgnoreCase("price") || field.equalsIgnoreCase("prezzo")
							|| field.equalsIgnoreCase("מחיר") || field.equalsIgnoreCase("prix")
							|| field.equalsIgnoreCase("بها")) {
						poiFields.put(PoiFieldType.PRICE, value);
						poiShortDescription.append(value).append(". ");
					} else if (field.equalsIgnoreCase("hours") || field.equalsIgnoreCase("שעות")
							|| field.equalsIgnoreCase("ساعت‌ها")) {
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
					} else if (field.equalsIgnoreCase("wikipedia") && !value.equals("undefined") && !value.isEmpty()) {
						poiFields.put(PoiFieldType.WIKIPEDIA, value);
//						wikiLink = value;
					}
				} catch (Exception e) {
				}
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

	public static Map<String, Integer> POI_OTHER_TYPES = new HashMap<>();

	private static PoiFieldCategory transformCategory(String[] info) {
		// {{listing | type=go}
		// en: type, pt: tipo, fr: group,
		PoiFieldCategory res = PoiFieldCategory.OTHER;
		for (int i = 0; i < info.length; i++) {
			int ind = info[i].indexOf('=');
			if (ind >= 0) {
				String key = info[i].substring(0, ind).trim();
				if (key.equals("type") || key.equals("tipo") || key.equals("group")) {
					String val = info[i].substring(ind + 1).toLowerCase().trim();
					for (PoiFieldCategory p : PoiFieldCategory.values()) {
						for (String s : p.names) {
							if (val.contains(s)) {
								res = p;
								break;
							}
						}
						for (String s : p.types) {
							if (val.contains(s)) {
								res = p;
								break;
							}
						}
					}
					if (res != PoiFieldCategory.OTHER) {
						return res;
					}
					Integer it = POI_OTHER_TYPES.get(val);
					POI_OTHER_TYPES.put(val, it == null ? 1 : it.intValue() + 1);
				}
			}
		}
		return res;
	}

	private static PoiFieldCategory isPOIKey(String str, String lang) {
		if (str.startsWith("listing") || str.startsWith("vcard")) {
			return transformCategory(str.split("\\|"));
		}
		for (PoiFieldCategory p : PoiFieldCategory.values()) {
			for (String s : p.names) {
				if (str.startsWith(s)) {
					return p;
				}
			}
		}
		return null;
	}

	private static EnumSet<WikivoyageTemplates> getKey(String str, String lang) {
		if (str.startsWith("geo|") || str.startsWith("geo ") || str.startsWith("geodata")) {
			return of(WikivoyageTemplates.LOCATION);
		} else if (str.startsWith("ispartof") || str.startsWith("partofitinerary") || str.startsWith("isin")
				|| str.startsWith("dans") || str.startsWith("footer|") || str.startsWith("istinkat")
				|| str.startsWith("istin|") || str.startsWith("istin ") || str.startsWith("estaen")
				|| str.startsWith("estáen") || str.startsWith("sijainti|") || str.startsWith("sijainti ")
				|| str.startsWith("thème|") || str.startsWith("thème ") || str.startsWith("fica em")
				|| str.startsWith("estáen") || str.startsWith("קטגוריה") || str.startsWith("είναιτμήματου")
				|| str.startsWith("είναιτμήματης")
				// || str.startsWith("commonscat")
				|| str.startsWith("jest w") || str.startsWith("itinerário em") || str.startsWith("partoftopic")
				|| str.startsWith("theme ") || str.startsWith("theme|") || str.startsWith("categoría")
				|| str.startsWith("بخشی") || str.startsWith("位于|") || str.startsWith("位于 ")) {
			return of(WikivoyageTemplates.PART_OF);
		} else if (str.startsWith("quickfooter")) {
			return of(WikivoyageTemplates.QUICK_FOOTER);
		} else if (str.startsWith("navigation ") && lang.equals("de")) {
			// -- incorrect istinkat correct version comparing to
			// https://de.wikivoyage.org/wiki/Kurtinig?action=raw
			// + navigation doesn't space
			return EnumSet.noneOf(WikivoyageTemplates.class);
		} else if (str.startsWith("info guide linguistique") || str.startsWith("conversação")
				|| str.startsWith("frasario")) {
			return of(WikivoyageTemplates.PHRASEBOOK);
		} else if (str.startsWith("info maladie")) {
			return EnumSet.noneOf(WikivoyageTemplates.class);
		} else if (str.startsWith("info ")) {
			return of(WikivoyageTemplates.LOCATION, WikivoyageTemplates.BANNER);
		} else if (str.startsWith("quickbar") && (str.contains("lat=") || str.contains("lon=") || str.contains("long=")
				|| str.contains("longitude="))) {
			return of(WikivoyageTemplates.LOCATION, WikivoyageTemplates.BANNER);
		} else if (str.startsWith("pagebanner") || str.startsWith("citybar") || str.startsWith("quickbar ")
				|| str.startsWith("banner") || str.startsWith("באנר") || str.startsWith("سرصفحه")) {
			return of(WikivoyageTemplates.BANNER);
		} else if (str.startsWith("regionlist")) {
			return of(WikivoyageTemplates.REGION_LIST);
		} else if (str.startsWith("warningbox")) {
			return of(WikivoyageTemplates.WARNING);
		} else if (str.startsWith("cite")) {
			return of(WikivoyageTemplates.CITATION);
		} else if (str.startsWith("iata") || str.startsWith("formatnum")) {
			return of(WikivoyageTemplates.TWO_PART);
		} else if (str.startsWith("station") || str.startsWith("rint")) {
			return of(WikivoyageTemplates.STATION);
		} else if (str.startsWith("ipa") || str.startsWith("lang-")) {
			return of(WikivoyageTemplates.TRANSLATION);
		} else if (str.startsWith("disamb") || str.startsWith("disambiguation") || str.trim().equals("dp")
				|| str.startsWith("неоднозначность") || str.startsWith("desambiguación")
				|| str.startsWith("ujednoznacznienie") || str.startsWith("homonymie") || str.startsWith("desamb")
				|| str.startsWith("ابهام زدایی") || str.startsWith("消歧义") || str.startsWith("消歧義")
				|| str.startsWith("täsmennys") || str.startsWith("förgrening") || str.startsWith("msg:disamb")
				|| str.startsWith("wegweiser") || str.startsWith("begriffsklärung")) {
			return of(WikivoyageTemplates.DISAMB);
		} else if (str.startsWith("guidephrasebook") || str.startsWith("partofphrasebook")
				|| str.startsWith("phrasebookguide")) {
			return of(WikivoyageTemplates.PHRASEBOOK);
		} else if (str.startsWith("monument-title")) {
			return of(WikivoyageTemplates.MONUMENT_TITLE);
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
	
	public static String getRedirect(StringBuilder ctext) {
		String textStr = ctext.toString().trim().toLowerCase();
		if (textStr.startsWith("#redirect") || textStr.startsWith("#weiterleitung")
				|| textStr.startsWith("#перенаправление") || textStr.startsWith("#patrz")
				|| textStr.startsWith("#перенаправлення") || textStr.startsWith("#doorverwijzing")
				|| textStr.startsWith("__disambig_") || textStr.startsWith("#redirecionamento")
				|| textStr.startsWith("#rinvia") || textStr.startsWith("#uudelleenohjaus")
				|| textStr.startsWith("#redirección") || textStr.startsWith("#omdirigering")
				|| textStr.startsWith("#ohjaus") || textStr.startsWith("#ανακατευθυνση")
				|| textStr.startsWith("#تغییر_مسیر") || textStr.startsWith("#הפניה") || textStr.startsWith("#تغییرمسیر")
				|| textStr.startsWith("#đổi") || textStr.startsWith("#重定向") || textStr.startsWith("#पुनर्प्रेषित")
				|| textStr.startsWith("#अनुप्रेषित")) {
			int l = textStr.indexOf("[[");
			int e = textStr.indexOf("]]");
			if (l > 0 && e > 0) {
				return ctext.substring(l + 2, e).trim();
			}
		}
		return null;
	}

	public static void mainTestPage(String[] args) throws IOException, SQLException {
		StringBuilder input = Algorithms
				.readFromInputStream(WikiDatabasePreparation.class.getResourceAsStream("/page.txt"));
		TreeMap<WikivoyageTemplates, List<String>> macros = new TreeMap<WikivoyageTemplates, List<String>>();
		List<Map<PoiFieldType, Object>> pois = new ArrayList<Map<PoiFieldType, Object>>();
		String lang = "de";
		String title = "page";
		CustomWikiModel wikiModel = new CustomWikiModel("https://" + lang + ".wikipedia.org/wiki/${image}",
				"https://" + lang + ".wikipedia.org/wiki/${title}", null, true);
		String rawWikiText = WikiDatabasePreparation.removeMacroBlocks(input, null, macros, pois, lang, title, null, null);

//		System.out.println(text);

//		System.out.println(generateHtmlArticle(rawWikiText, wikiModel));
		System.out.println(getShortDescr(rawWikiText, wikiModel));
//		System.out.println(getLatLonFromGeoBlock(macros.get(WikivoyageTemplates.LOCATION), "", ""));
//		System.out.println(WikivoyageHandler.parsePartOfFromQuickFooter(macros.get(WikivoyageTemplates.QUICK_FOOTER), "", ""));
		List<String> lst = macros.get(WikivoyageTemplates.POI);
//		for(Map<PoiFieldType, Object> poi : pois) {
//			System.out.println(poi);
//			System.out.println("----");
//		}
		if (lst != null) {
//			for (String l : lst) {
//				System.out.println(l.trim());
//				Map<PoiFieldType, Object> poiFields  = new LinkedHashMap<>();
//				parsePoiWithAddLatLon(l, poiFields);
//				System.out.println(poiFields);
//			}

		}
	}

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, SQLException,
			ComponentLookupException, XmlPullParserException, InterruptedException {
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
		if (mode.equals("create-wikidata") || mode.equals("update-wikidata") || mode.equals("create-osm-wikidata")) {
			if (resultDB.isEmpty()) {
				throw new RuntimeException("Correct arguments weren't supplied. --result_db= is not set");
			}
			if (wikidataFolder.isEmpty()) {
				throw new RuntimeException("Correct arguments weren't supplied. --dir= is not set");
			}
			wikidataSqliteName = resultDB;
		}

		if (mode.equals("create-wikidata-mapping")) {
			if (wikidataFolder.isEmpty()) {
				throw new RuntimeException("Correct arguments weren't supplied. --dir= is not set");
			}
			if (resultDB.isEmpty()) {
				throw new RuntimeException("Correct arguments weren't supplied. --result_db= is not set");
			}
			wikidataSqliteName = resultDB;
		}

		final String pathToWikiData = wikidataFolder + WIKIDATA_ARTICLES_GZ;
		OsmCoordinatesByTag osmCoordinates = new OsmCoordinatesByTag(new String[] { "wikipedia", "wikidata" },
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
			processWikidata(wikidataDB, wikidataFile, osmCoordinates, 0);
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
			updateWikidata(osmCoordinates, wikidataDB, false);
			break; 
		case "update-wikidata-daily":
			wikidataDB = new File(wikidataSqliteName);
			updateWikidata(osmCoordinates, wikidataDB, true);
			break;
		case "process-wikipedia":
			log.info("Processing wikipedia...");
			processWikipedia(wikipediaFolder, wikipediaSqliteName, lang, 0);
			break;
		case "test-wikipedia":
			processWikipedia(wikipediaFolder, wikipediaSqliteName, lang, testArticleID);
			break;
		case "create-wikidata-mapping":
			wikidataDB = new File(wikidataSqliteName);
			log.info("Create wikidata mapping DB.");
			createWikidataMapping(wikidataDB, wikidataFolder);
			break;
		}
	}

	private static void updateWikidata(OsmCoordinatesByTag osmCoordinates, File wikidataDB, boolean dailyUpdate) 
			throws SQLException, ParserConfigurationException, SAXException, IOException {
		log.info("Process OSM coordinates...");
		osmCoordinates.parse(wikidataDB.getParentFile());
		WikidataFilesDownloader wfd = new WikidataFilesDownloader(wikidataDB, dailyUpdate);
		List<String> downloadedPageFiles = wfd.getDownloadedPageFiles();
		long maxQId = wfd.getMaxQId();
		log.info("Updating wikidata...");
		for (String fileName : downloadedPageFiles) {
			log.info("Updating from " + fileName);
			processWikidata(wikidataDB, fileName, osmCoordinates, maxQId);
		}
		wfd.removeDownloadedPages();
		createOSMWikidataTable(wikidataDB, osmCoordinates);
	}

	protected static void processWikidataRegions(final String sqliteFileName) throws SQLException, IOException {
		File wikiDB = new File(sqliteFileName);
		log.info("Processing wikidata regions...");
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = dialect.getDatabaseConnection(wikiDB.getAbsolutePath(), log);
		
		OsmandRegions regions = new OsmandRegions();
		regions.prepareFile();
		regions.cacheAllCountries();
		PreparedStatement wikiRegionPrep = conn
				.prepareStatement("INSERT OR IGNORE INTO wiki_region(id, regionName) VALUES(?, ? )");
		ResultSet rs = conn.createStatement().executeQuery("SELECT id from wiki_region");
		Set<Long> existingIds = new TreeSet<>();
		while (rs.next()) {
			existingIds.add(rs.getLong(1));
		}
		rs.close();
		rs = conn.createStatement().executeQuery("SELECT id, lat, lon from wiki_coords");
		int batch = 0;
		List<String> rgs = new ArrayList<String>();
		while (rs.next()) {
			if (existingIds.contains(rs.getLong(1))) {
				continue;
			}
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
		while ((read = in.read(buf)) != -1) {
			fout.write(buf, 0, read);
		}
		in.close();
		fout.close();

	}

	protected static void testContent(String lang, String folder) throws SQLException, IOException {
		Connection conn = DBDialect.SQLITE.getDatabaseConnection(folder + lang + WIKIPEDIA_SQLITE, log);
		ResultSet rs = conn.createStatement().executeQuery("SELECT * from wiki");
		while (rs.next()) {
			double lat = rs.getDouble("lat");
			double lon = rs.getDouble("lon");
			byte[] zp = rs.getBytes("zipContent");
			String title = rs.getString("title");
			BufferedReader rd = new BufferedReader(
					new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(zp))));
			System.out.println(title + " " + lat + " " + lon + " " + zp.length);
			String s;
			while ((s = rd.readLine()) != null) {
				System.out.println(s);
			}
		}
	}

	public static void createOSMWikidataTable(File wikidataDB, OsmCoordinatesByTag c) throws SQLException {
		Connection commonsWikiConn = DBDialect.SQLITE.getDatabaseConnection(wikidataDB.getAbsolutePath(), log);
		Statement st = commonsWikiConn.createStatement();
		ResultSet rs = st
				.executeQuery("SELECT C.id,M.lang,M.title FROM wiki_coords C LEFT JOIN wiki_mapping M ON M.id=C.id");
		Map<Long, OsmLatLonId> res = new TreeMap<>();
		int scan = 0;
		long time = System.currentTimeMillis();
		while (rs.next()) {
			long wid = rs.getLong(1);
			String articleLang = rs.getString(2);
			String articleTitle = rs.getString(3);
			if (++scan % 500000 == 0) {
				System.out.println("Scanning wiki to merge with OSM... " + scan + " "
						+ (System.currentTimeMillis() - time) + " ms");
				time = System.currentTimeMillis();
			}
			if (articleLang != null) {
				setWikidataId(res, c.getCoordinates("wikipedia:" + articleLang, articleTitle), wid);
				setWikidataId(res, c.getCoordinates("wikipedia", articleLang + ":" + articleTitle), wid);
				setWikidataId(res, c.getCoordinates("wikipedia", articleTitle), wid);
			}
			setWikidataId(res, c.getCoordinates("wikidata", "Q" + wid), wid);
		}
		try {
			commonsWikiConn.createStatement().executeQuery("DROP TABLE osm_wikidata");
		} catch (SQLException e) {
			// ignore
			System.err.println("Table osm_wikidata doesn't exist");
		}
		st.execute("""
				CREATE TABLE osm_wikidata(osmid bigint, osmtype int, wikidataid bigint, lat double, long double,
				tags string, poitype string, poisubtype string, wikiCommonsImg string, wikiCommonsCat string)""");
		st.close();
		PreparedStatement ps = commonsWikiConn
				.prepareStatement("INSERT INTO osm_wikidata VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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
			ps.setString(9, o.wikiCommonsImg);
			ps.setString(10, o.wikiCommonsCat);
			ps.addBatch();
			if (batch++ >= 1000) {
				ps.executeBatch();
			}
		}
		ps.executeBatch();
		ps.close();
		commonsWikiConn.close();
	}

	public static void createWikidataMapping(File sourceDb, String wikidataFolder) throws SQLException {
		Connection srcConn = DBDialect.SQLITE.getDatabaseConnection(sourceDb.getAbsolutePath(), log);
		Statement srcSt = srcConn.createStatement();
		ResultSet rs = srcSt.executeQuery("SELECT id, lang, title FROM wiki_mapping");

		File mappingFile = new File(wikidataFolder, WIKIDATA_MAPPING_SQLITE);
		if (mappingFile.exists()) {
			mappingFile.delete();
		}
		Connection mappingConn = DBDialect.SQLITE.getDatabaseConnection(mappingFile.getAbsolutePath(), log);

		Statement mapSt = mappingConn.createStatement();
		mapSt.execute("DROP TABLE IF EXISTS wiki_mapping");
		mapSt.execute("CREATE TABLE wiki_mapping(id LONG, lang TEXT, title TEXT)");
		mapSt.execute("CREATE INDEX IF NOT EXISTS idx_wm_id ON wiki_mapping(id)");
		mapSt.execute("CREATE INDEX IF NOT EXISTS idx_wm_lang_title ON wiki_mapping(lang, title)");

		PreparedStatement ps = mappingConn
				.prepareStatement("INSERT INTO wiki_mapping(id, lang, title) VALUES(?, ?, ?)");
		int batch = 0;
		while (rs.next()) {
			ps.setLong(1, rs.getLong("id"));
			ps.setString(2, rs.getString("lang"));
			ps.setString(3, rs.getString("title"));
			ps.addBatch();
			if (++batch % 1000 == 0) {
				ps.executeBatch();
			}
		}
		ps.executeBatch();

		ps.close();
		mapSt.close();
		mappingConn.close();
		srcSt.close();
		srcConn.close();
	}

	private static void setWikidataId(Map<Long, OsmLatLonId> mp, OsmLatLonId c, long wid) {
		if (c != null) {
			c.wikidataId = wid;
			setWikidataId(mp, c.next, wid);
			mp.put(c.type + (c.id << 2l), c);
		}
	}

	public static void processWikipedia(final String wikipediaFolder, final String wikipediaSqliteFileName, String lang,
			long testArticleId) throws ParserConfigurationException, SAXException, IOException, SQLException {
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

	public static void processWikidata(File wikidataSqlite, final String wikidataFile,
			OsmCoordinatesByTag osmCoordinates, long lastProcessedId)
			throws ParserConfigurationException, SAXException, IOException, SQLException {
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		FileProgressImplementation progress = new FileProgressImplementation("Read wikidata file",
				new File(wikidataFile));
		InputStream streamFile = progress.openFileInputStream();
		InputSource is = getInputSource(streamFile);
		OsmandRegions regions = new OsmandRegions();
		regions.prepareFile();
		regions.cacheAllCountries();
		final WikiDataHandler handler = new WikiDataHandler(sx, progress, wikidataSqlite, osmCoordinates, regions,
				lastProcessedId);
		sx.parse(is, handler);
		handler.finish();
		osmCoordinates.closeConnection();
	}

	private static InputSource getInputSource(InputStream streamFile) throws IOException {
		GZIPInputStream zis = new GZIPInputStream(streamFile);
		Reader reader = new InputStreamReader(zis, "UTF-8");
		InputSource is = new InputSource(reader);
		is.setEncoding("UTF-8");
		return is;
	}

	public static class WikipediaHandler extends DefaultHandler {
		long counter = 1;
		private final SAXParser saxParser;
		private boolean page = false;
		private boolean revision = false;
		private long namespace = 0;
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
		private final static int BATCH_SIZE = 1000;
		private static final long ARTICLES_BATCH = 1000;

		final ByteArrayOutputStream bous = new ByteArrayOutputStream(64000);
		private String lang;
		private FileProgressImplementation progIS;
		private long cid;

		WikipediaHandler(SAXParser saxParser, FileProgressImplementation progIS, String lang, File wikipediaSqlite,
				long testArticleId) throws SQLException {
			this.lang = lang;
			this.saxParser = saxParser;
			this.progIS = progIS;
			conn = dialect.getDatabaseConnection(wikipediaSqlite.getAbsolutePath(), log);
			log.info("Prepare wiki_content table");
			conn.createStatement().execute(
					"CREATE TABLE IF NOT EXISTS wiki_content(id long, title text, lang text, shortDescription text, redirect text, zipContent blob)");
			conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_wiki_content ON wiki_content(id)");
			conn.createStatement()
					.execute("CREATE INDEX IF NOT EXISTS lang_title_wiki_content ON wiki_content (lang, title)");
			conn.createStatement().execute("DELETE FROM wiki_content WHERE lang = '" + lang + "'");
			insertPrep = conn.prepareStatement(
					"INSERT INTO wiki_content(id, title, lang, shortDescription, redirect, zipContent) VALUES (?, ?, ?, ?, ?, ?)");
			selectPrep = conn.prepareStatement(
					"SELECT id FROM wiki_mapping WHERE wiki_mapping.title = ? AND wiki_mapping.lang = ?");
			imageUrlStorage = new WikiImageUrlStorage(conn, wikipediaSqlite.getParent(), lang);
			log.info("Tables are prepared");
		}

		public void addBatch() throws SQLException {
			insertPrep.addBatch();
			if (batch++ > BATCH_SIZE) {
				insertPrep.executeBatch();
				batch = 0;
			}
		}

		public void finish() throws SQLException {
			insertPrep.executeBatch();
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			selectPrep.close();
			insertPrep.close();
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
				} else if (name.equals("ns")) {
					ctext = new StringBuilder();
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
					} else if (name.equals("ns")) {
						namespace = Long.parseLong(ctext.toString());
						ctext = null;
					} else if (name.equals("id") && !revision) {
						ctext = null;
						cid = Long.parseLong(pageId.toString());
					} else if (name.equals("text")) {
						long wikiId = 0;
						String plainStr = null;
						String shortDescr = null;
						if (namespace == 0) {
							selectPrep.setString(1, title.toString());
							selectPrep.setString(2, lang);
							ResultSet rs = selectPrep.executeQuery();
							if (rs.next()) {
								wikiId = rs.getLong(1);
							}
							rs.close();
							selectPrep.clearParameters();
						}
						if (wikiId != 0) {
							try {
								CustomWikiModel wikiModel = new CustomWikiModel(
										"https://" + lang + ".wikipedia.org/wiki/${image}",
										"https://" + lang + ".wikipedia.org/wiki/${title}", imageUrlStorage, true);
								String rawWikiText = removeMacroBlocks(ctext, null, new HashMap<>(), null, lang,
										title.toString(), null, null);
								plainStr = generateHtmlArticle(rawWikiText, wikiModel);
								shortDescr = getShortDescr(rawWikiText, wikiModel);
							} catch (RuntimeException e) {
								log.error(String.format("Error with article %d - %s : %s", cid, title, e.getMessage()), e);
							}
						}
						if (plainStr != null) {
							String redirect = getRedirect(ctext);
							if (++counter % ARTICLES_BATCH == 0) {
								log.info("Article accepted " + cid + " " + title.toString());
//								double GB = (1l << 30);
//								log.info(String.format("Memory used : free %.2f GB of %.2f GB",
//										Runtime.getRuntime().freeMemory() / GB, Runtime.getRuntime().totalMemory() / GB));
							}
							try {
								insertPrep.setLong(1, wikiId);
								insertPrep.setString(2, title.toString());
								insertPrep.setString(3, lang);
								insertPrep.setString(4, shortDescr);
								insertPrep.setString(5, redirect);
								insertPrep.setBytes(6, gzip(plainStr));
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

		private byte[] gzip(String plainStr) throws IOException, UnsupportedEncodingException {
			bous.reset();
			GZIPOutputStream gzout = new GZIPOutputStream(bous);
			gzout.write((plainStr == null ? "" : plainStr).getBytes("UTF-8"));
			gzout.close();
			return bous.toByteArray();
		}
	}

	public static String getShortDescr(String rawWikiText, CustomWikiModel wikiModel) throws IOException {
		final PlainTextConverter converter = new PlainTextConverter(false);
		// clean up photos links [[.... ]] in the beginning only!
		rawWikiText = rawWikiText.trim();
		while (rawWikiText.startsWith("[")) {
			int last = -1;
			int cnt = 0;
			for (int i = 0; i < rawWikiText.length(); i++) {
				if (rawWikiText.charAt(i) == '[') {
					cnt++;
				} else if (rawWikiText.charAt(i) == ']') {
					cnt--;
					if (cnt == 0) {
						last = i + 1;
						break;
					}
				}
			}
			if (last == -1) {
				return "";
			}
			rawWikiText = rawWikiText.substring(last).trim();
		}
		String plainStr = wikiModel.render(converter, rawWikiText);
		if (plainStr != null) {
			plainStr = plainStr.trim();
			int si = plainStr.indexOf('\n');
			while (si < OPTIMAL_SHORT_DESCR && si >= 0) {
				if (si < SHORT_PARAGRAPH) {
					// remove first short paragraphs
					plainStr = plainStr.substring(si).trim();
					si = -1;
				} 
				int ni = plainStr.indexOf('\n', si + 1);
				if (ni > OPTIMAL_LONG_DESCR && si > 0) {
					break;
				}
				si = ni;
			}
			if (si > 0) {
				plainStr = plainStr.substring(0, si).trim();
			}
			plainStr = plainStr.replace("[ˈ]", "");
			plainStr = plainStr.replace("[]", "");
			plainStr = plainStr.replace("()", "");
		}
		return plainStr;
	}

	public static String generateHtmlArticle(String text, CustomWikiModel wikiModel) throws IOException {
		final HTMLConverter converter = new HTMLConverter(false);
		String plainStr = wikiModel.render(converter, text);
		plainStr = plainStr.replaceAll("<p>div class=&#34;content&#34;", "<div class=\"content\">\n<p>")
				.replaceAll("<p>/div\n</p>", "</div>");
		return plainStr;
	}

	/**
	 * Gets distance in meters
	 */
	public static double getDistance(double lat1, double lon1, double lat2, double lon2) {
		double R = 6372.8; // for haversine use R = 6372.8 km instead of 6371 km
		double dLat = toRadians(lat2 - lat1);
		double dLon = toRadians(lon2 - lon1);
		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
				+ Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
		// double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		// return R * c * 1000;
		// simplyfy haversine:
		return (2 * R * 1000 * Math.asin(Math.sqrt(a)));
	}

	private static double toRadians(double angdeg) {
//		return Math.toRadians(angdeg);
		return angdeg / 180.0 * Math.PI;
	}

}

