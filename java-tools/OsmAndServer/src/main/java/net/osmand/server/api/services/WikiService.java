package net.osmand.server.api.services;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.clickhouse.data.value.UnsignedLong;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiType;
import net.osmand.shared.wiki.WikiImage;
import net.osmand.shared.wiki.WikiMetadata;
import net.osmand.util.MapUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import net.osmand.data.LatLon;
import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.server.controllers.pub.GeojsonClasses.FeatureCollection;
import net.osmand.server.controllers.pub.GeojsonClasses.Geometry;
import net.osmand.util.Algorithms;

import jakarta.annotation.PostConstruct;

import static net.osmand.wiki.WikiDatabasePreparation.*;

@Service
public class WikiService {
	
	protected static final Log log = LogFactory.getLog(WikiService.class);

	public static final String USER_AGENT = "OsmAnd-Bot/1.0 (+https://osmand.net; support@osmand.net) OsmAndJavaServer/1.0";
	public static final String WIKIMEDIA_COMMON_SPECIAL_FILE_PATH = "https://commons.wikimedia.org/wiki/Special:FilePath/";
	private static final String IMAGE_ROOT_URL = "https://upload.wikimedia.org/wikipedia/commons/";
	private static final String OSMAND_IMAGE_ROOT_URL = "https://data.osmand.net/wikimedia/images-1280/";
	private static final String THUMB_PREFIX = "320px-";
	protected static int FILE_URL_TO_USE = 1;

	private static final int LIMIT_OBJS_QUERY = 1000;
	private static final int LIMIT_PHOTOS_QUERY = 100;
	private static final String SIMILARITY_CF = "0.975";

	private static final Pattern DIGITS = Pattern.compile("\\d+");


	private final Map<String, String> licenseMap = new HashMap<>();
	
	@Value("${osmand.wiki.location}")
	private String pathToWikiSqlite;
	
	@Autowired
	@Qualifier("wikiJdbcTemplate")
	JdbcTemplate jdbcTemplate;
	
	@Autowired
	DatasourceConfiguration config;

	@PostConstruct
	public void init() {
		parseLicenseFile();
	}
	
    
	public FeatureCollection getImages(String northWest, String southEast) {
		return getPoiData(northWest, southEast, " SELECT id, mediaId, namespace, imageTitle, imgLat, imgLon "
				+ " FROM wikigeoimages WHERE namespace = 6 AND imgLat BETWEEN ? AND ? AND imgLon BETWEEN ? AND ? "
				+ " ORDER BY views desc LIMIT " + LIMIT_OBJS_QUERY,"imgLat", "imgLon", null);
	}
	
	public FeatureCollection getImagesById(long id, double lat, double lon) {
		String query = String.format("SELECT wikidata_id, mediaId, imageTitle, date, author, license " +
				"FROM top_images_final WHERE wikidata_id = %d and dup_sim < " + SIMILARITY_CF +
				"ORDER BY score DESC LIMIT " + LIMIT_PHOTOS_QUERY, id);
		
		List<Feature> features = jdbcTemplate.query(query, (rs, rowNum) -> {
			Feature f = new Feature(Geometry.point(new LatLon(lat, lon)));
			f.properties.put("id", rs.getLong("wikidata_id"));
			f.properties.put("mediaId", rs.getLong("mediaId"));
			f.properties.put("imageTitle", rs.getString("imageTitle"));
			f.properties.put("date", rs.getString("date"));
			f.properties.put("author", rs.getString("author"));
			f.properties.put("license", getLicense(rs.getString("license")));
			return f;
		});
		
		return new FeatureCollection(features.toArray(new Feature[0]));
	}
	
	public String getWikiRawDataFromCache(String imageTitle, Long pageId) {
		String query = "SELECT data FROM wiki.imagesrawdata WHERE title = ? AND pageId = ? LIMIT 1";
		return jdbcTemplate.query(query, ps -> {
			ps.setString(1, imageTitle);
			ps.setLong(2, pageId);
		}, rs -> {
			if (rs.next()) {
				return rs.getString("data");
			}
			return null;
		});
	}
	
	
	public void saveWikiRawDataToCache(String imageTitle, Long pageId, String rawData) {
		String query = "INSERT INTO wiki.imagesrawdata (pageId, title, data) VALUES (?, ?, ?)";
		jdbcTemplate.update(query, ps -> {
			ps.setLong(1, pageId);
			ps.setString(2, imageTitle);
			ps.setString(3, rawData);
		});
	}
	
	
	public String parseRawImageInfo(String imageTitle) {
		// https://dumps.wikimedia.org/commonswiki/
		if (!isValidImageTitle(imageTitle)) {
			logError("Invalid image title", -1, imageTitle);
			return null;
		}
		HttpURLConnection connection = null;
		String encodedImageTitle = URLEncoder.encode(imageTitle, StandardCharsets.UTF_8);
		String urlStr = "https://commons.wikimedia.org/wiki/File:" + encodedImageTitle + "?action=raw";
		try {
			URL url = new URL(urlStr);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty("User-Agent", USER_AGENT);
			
			int responseCode = connection.getResponseCode();
			String rawData;
			BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String inputLine;
			StringBuilder content = new StringBuilder();
			
			while ((inputLine = in.readLine()) != null) {
				content.append(inputLine).append("\n");
			}
			in.close();
			rawData = content.toString();
			
			if (responseCode != HttpURLConnection.HTTP_OK) {
				logError(urlStr, responseCode, rawData);
				return null;
			}
			return rawData;
		} catch (IOException e) {
			log.error("Error while reading url: " + urlStr
					+ " ex=" + e.getClass().getSimpleName()
					+ " msg=" + e.getMessage(), e);
			return null;
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}
	
	private boolean isValidImageTitle(String imageTitle) {
		if (imageTitle.length() > 255) {
			return false;
		}
		String regex = "^[^\\\\/:*?\"<>|]+$"; // Disallow invalid characters
		return imageTitle.matches(regex);
	}
	
	private void logError(String url, int code, String content) {
		String shortenedContent = content != null && content.length() > 80 ? content.substring(0, 80) + "..." : content;
		log.error("Error (" + code + ") reading url: " + url + " content: " + shortenedContent);
	}
	
	public Map<String, String> parseImageInfo(String rawData) throws SQLException, IOException {
		Map<String, String> result = new HashMap<>();
		removeMacroBlocks(new StringBuilder(rawData), result, new HashMap<>(), null, "en", "Old_parsing_without_title", null, null);
		prepareMetaData(result);
		return result;
	}
	
	public Map<String, String> parseImageInfo(String rawData, String title, String lang) throws SQLException, IOException {
		Map<String, String> result = new HashMap<>();
		removeMacroBlocks(new StringBuilder(rawData), result, new HashMap<>(), null, lang, title, null, null);
		prepareMetaData(result);
		return result;
	}

	public FeatureCollection getWikidataData(String northWest, String southEast, String lang, Set<String> filters, Integer zoom) {
		List<Integer> topics = prepareTopics(filters);
		boolean showAll = topics.contains(0);

		String filterQuery = "";

		if (!showAll && !topics.isEmpty()) {
			String inList = topics.stream()
					.map(String::valueOf)
					.collect(Collectors.joining(", "));
			filterQuery = " AND w.topic IN (" + inList + ") ";
		}

		int z = zoom != null ? zoom : calculateOptimalZoom(northWest, southEast);

		List<String> langPriority = buildLangPriorityList(lang);

		String query = buildWikidataQuery(langPriority, showAll, filterQuery, z);

		return getPoiData(northWest, southEast, query, "lat", "lon", langPriority);
	}

	private List<Integer> prepareTopics(Set<String> filters) {
		// if contains 0 or only empty string - show all
		if (filters.contains("0") || (filters.size() == 1 && filters.contains(""))) {
			return List.of(0);
		}

		return filters.stream()
				.filter(Objects::nonNull)
				.map(String::trim)
				.filter(s -> DIGITS.matcher(s).matches())
				.map(Integer::valueOf)
				.toList();
	}

	private String buildWikidataQuery(List<String> langPriority, boolean showAll, String filterQuery, int zoom) {

		String langList = getLangListQuery(langPriority);

		String table;
		if (zoom <= 5)      table = "wiki.top1000_by_quad_z5";
		else if (zoom <= 10) table = "wiki.top1000_by_quad_z10";
		else if (zoom <= 16) table = "wiki.top1000_by_quad_z16";
		else                table = "wiki.wikidata";

		return "SELECT w.id, w.photoId, w.wikiTitle, w.wikiLang, w.wikiDesc, w.photoTitle, " +
				"w.osmid, w.osmtype, w.poitype, w.poisubtype, " +
				"w.search_lat AS lat, w.search_lon AS lon, " +
				"arrayFirst(x -> has(w.wikiArticleLangs, x), " + langList + ") AS lang, " +
				"indexOf(w.wikiArticleLangs, lang) AS ind, " +
				"w.wikiArticleContents[ind] AS content, " +
				"w.wvLinks, w.elo AS elo, w.topic AS topic, w.categories AS categories, w.qrank " +
				"FROM " + table + " AS w " +
				"PREWHERE (w.search_lat BETWEEN ? AND ? AND w.search_lon BETWEEN ? AND ?) " +
				(showAll ? "" : filterQuery) +
				" ORDER BY w.elo DESC, w.qrank DESC " +
				"LIMIT " + LIMIT_OBJS_QUERY;
	}

	public String getLangListQuery(List<String> langPriority) {
		return langPriority.stream()
				.map(String::trim)
				.filter(l -> l.matches("^[a-z]{1,3}$"))
				.map(l -> "'" + l + "'")
				.collect(Collectors.joining(", ", "[", "]"));
	}

	public String getWikipediaContent(String title, String lang) {
		String query = "SELECT hex(zipContent) AS ziphex FROM wiki.wiki_content WHERE title = ? AND lang = ?";
		return jdbcTemplate.query(query, ps -> {
			ps.setString(1, title);
			ps.setString(2, lang);
		}, rs -> {
			if (rs.next()) {
				String contentHex = rs.getString("ziphex");
				byte[] contentBytes = HexFormat.of().parseHex(contentHex);
				if (contentBytes != null) {
					return unzipContent(contentBytes);
				}
			}
			return null;
		});
	}
	
	public String getWikivoyageContent(String title, String lang) {
		String query = "SELECT hex(content_gz) AS ziphex FROM wiki.wikivoyage_articles WHERE title = ? AND lang = ?";
		return jdbcTemplate.query(query, ps -> {
			ps.setString(1, title);
			ps.setString(2, lang);
		}, rs -> {
			if (rs.next()) {
				String contentHex = rs.getString("ziphex");
				byte[] contentBytes = HexFormat.of().parseHex(contentHex);
				if (contentBytes != null) {
					return unzipContent(contentBytes);
				}
			}
			return null;
		});
	}
	
	public String unzipContent(byte[] compressedContent) {
		if (compressedContent == null || compressedContent.length == 0) {
			return null;
		}
		
		try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedContent);
		     GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
		     InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
		     BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
			
			StringBuilder output = new StringBuilder();
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				output.append(line);
			}
			return output.toString();
		} catch (IOException e) {
			return null;
		}
	}

	public FeatureCollection getPoiData(
			String northWest,
			String southEast,
			String query,
			String lat,
			String lon,
			List<String> langPriority
	) {
		if (!config.wikiInitialized()) {
			return new FeatureCollection();
		}

		double[] bbox = parseBoundingBox(northWest, southEast);

		RowMapper<Feature> rowMapper = new RowMapper<>() {
			List<String> columnNames = null;

			@Override
			public Feature mapRow(ResultSet rs, int rowNum) throws SQLException {
				Feature f = new Feature(Geometry.point(new LatLon(rs.getDouble(lat), rs.getDouble(lon))));
				f.properties.put("rowNum", rowNum);

				if (columnNames == null) {
					columnNames = new ArrayList<>();
					ResultSetMetaData rsmd = rs.getMetaData();
					for (int i = 1; i <= rsmd.getColumnCount(); i++) {
						columnNames.add(rsmd.getColumnName(i));
					}
				}

				for (int i = 1; i <= columnNames.size(); i++) {
					String col = columnNames.get(i - 1);
					if (col.equals(lat) || col.equals(lon)) continue;

					switch (col) {
						case "wikiArticles" -> fillWikiArticles(rs, i, f, langPriority);
						case "availableLangs" -> f.properties.put("wikiLangs", rs.getString(i));
						case "wvLinks" -> fillWvLinks(rs, i, f, langPriority);
						case "wikiTitle" -> f.properties.putIfAbsent("wikiTitle", rs.getString(i));
						case "wikiDesc" ->  f.properties.putIfAbsent("wikiDesc", rs.getString(i));
						case "wikiLang" ->  f.properties.putIfAbsent("wikiLang", rs.getString(i));
						case "poisubtype" -> fillPoiSubtype(rs.getString(i), f);
						default -> f.properties.put(col, rs.getString(i));
					}
				}
				return f;
			}
		};

		List<Feature> features = jdbcTemplate.query(query, ps -> {
			if (northWest != null && southEast != null) {
				ps.setDouble(1, bbox[2]); // south
				ps.setDouble(2, bbox[0]); // north
				ps.setDouble(3, bbox[1]); // west
				ps.setDouble(4, bbox[3]); // east
			}
		}, rowMapper);

		return new FeatureCollection(features.toArray(new Feature[0]));
	}

	private int calculateOptimalZoom(String northWest, String southEast) {
		String[] nw = northWest.split(",");
		String[] se = southEast.split(",");
		double lat1 = Double.parseDouble(nw[0]);
		double lon1 = Double.parseDouble(nw[1]);
		double lat2 = Double.parseDouble(se[0]);
		double lon2 = Double.parseDouble(se[1]);

		double minLat = Math.min(lat1, lat2);
		double maxLat = Math.max(lat1, lat2);
		double minLon = Math.min(lon1, lon2);
		double maxLon = Math.max(lon1, lon2);

		final int MAX_Z = 21, MIN_Z = 0;
		for (int z = MAX_Z; z >= MIN_Z; z--) {
			double yDiff = Math.abs(MapUtils.getTileNumberY(z, maxLat)
					- MapUtils.getTileNumberY(z, minLat));
			double xDiff = Math.abs(MapUtils.getTileNumberX(z, maxLon)
					- MapUtils.getTileNumberX(z, minLon));
			if (yDiff < 1.5 && xDiff < 1.5) {
				return z;
			}
		}
		return MIN_Z;
	}

	private double[] parseBoundingBox(String northWest, String southEast) {
		if (northWest != null && southEast != null) {
			String[] nw = northWest.split(",");
			String[] se = southEast.split(",");
			return new double[]{
					Double.parseDouble(nw[0]), // north
					Double.parseDouble(nw[1]), // west
					Double.parseDouble(se[0]), // south
					Double.parseDouble(se[1])  // east
			};
		} else {
			return new double[]{90.0, -180.0, -90.0, 180.0};
		}
	}

	private List<String> buildLangPriorityList(String preferredLangs) {
		return Algorithms.isEmpty(preferredLangs)
				? List.of("en")
				: List.of(preferredLangs.split(","));
	}

	private void fillWikiArticles(ResultSet rs, int index, Feature f, List<String> langPriority) throws SQLException {
		Array array = rs.getArray(index);
		if (array != null) {
			Object[] articles = (Object[]) array.getArray();
			for (Object article : articles) {
				if (article instanceof List<?> list) {
					String title = getFromArray(list, 0);
					String desc = getFromArray(list, 1);
					if (title != null || desc != null) {
						if (title != null) f.properties.put("wikiTitle", title);
						if (desc != null) f.properties.put("wikiDesc", desc);
						f.properties.put("wikiLang", langPriority.get(list.indexOf(article)));
						break;
					}
				}
			}
		}
	}

	private void fillWvLinks(ResultSet rs, int index, Feature f, List<String> langPriority) throws SQLException {
		Array array = rs.getArray(index);
		if (array == null) return;

		Object[] links = (Object[]) array.getArray();
		Map<Long, List<String>> result = new HashMap<>();
		Map<Long, List<String>> enLinks = new HashMap<>();
		Map<Long, List<String>> otherLinks = new HashMap<>();
		int bestIndex = 100;

		for (Object link : links) {
			if (link instanceof List<?> list) {
				long tripId = ((UnsignedLong) list.get(0)).longValue();
				String lang = getFromArray(list, 1);
				String title = getFromArray(list, 2);
				if (lang != null) {
					String url = "https://" + lang + ".wikivoyage.org/wiki/" + title;
					List<String> urlInfo = List.of(title, url);
					int idx = langPriority.indexOf(lang);
					if (idx != -1 && idx < bestIndex) {
						bestIndex = idx;
						result.put(tripId, urlInfo);
					} else if ("en".equals(lang)) {
						enLinks.put(tripId, urlInfo);
					} else {
						otherLinks.put(tripId, urlInfo);
					}
				}
			}
		}
		enLinks.forEach(result::putIfAbsent);
		otherLinks.forEach(result::putIfAbsent);
		if (!result.isEmpty()) {
			f.properties.put("wvLinks", result);
		}
	}

	private void fillPoiSubtype(String poiType, Feature f) {
		PoiType type = MapPoiTypes.getDefault().getPoiTypeByKey(poiType);
		if (type != null) {
			f.properties.put(SearchService.PoiTypeField.ICON_NAME.getFieldName(), type.getIconKeyName());
			f.properties.put(SearchService.PoiTypeField.OSM_TAG.getFieldName(), type.getOsmTag());
			f.properties.put(SearchService.PoiTypeField.OSM_VALUE.getFieldName(), type.getOsmValue());
		}
		f.properties.put("poisubtype", poiType);
	}

	private static String getFromArray(List<?> list, int index) {
		if (index < list.size()) {
			Object val = list.get(index);
			return val != null ? val.toString() : null;
		}
		return null;
	}
	
	private static String[] getHash(String s) {
		String md5 = new String(Hex.encodeHex(DigestUtils.md5(s)));
		return new String[]{md5.substring(0, 1), md5.substring(0, 2)};
	}
	
	public Set<String> processWikiImages(String articleId, String categoryName, String wiki) {
		Set<String> images = new LinkedHashSet<>();
		if (config.wikiInitialized()) {
			RowCallbackHandler h = rs -> {
				String imageTitle = rs.getString("imageTitle");
				images.add(createImageUrl(imageTitle));
			};
			if (Algorithms.isEmpty(articleId) && !Algorithms.isEmpty(wiki)) {
				articleId = retrieveArticleIdFromWikiUrl(wiki);
			}
			queryImagesByWikidataAndCategory(articleId, categoryName, h);
		}
		return images;
	}
	
	
	public Set<Map<String, Object>> processWikiImagesWithDetails(String articleId, String categoryName, String wiki) {
		Set<Map<String, Object>> imagesWithDetails = new LinkedHashSet<>();
		
		if (config.wikiInitialized()) {
			RowCallbackHandler h = rs -> {
				Map<String, Object> imageDetails = new HashMap<>();
				String imageTitle = rs.getString("imageTitle");

				imageDetails.put("mediaId", rs.getLong("mediaId"));
				imageDetails.put("image", createImageUrl(imageTitle));
				imageDetails.put("date", rs.getString("date"));
				imageDetails.put("author", rs.getString("author"));
				imageDetails.put("license", getLicense(rs.getString("license")));
				imageDetails.put("description", rs.getString("description"));

				imagesWithDetails.add(imageDetails);
			};
			if (Algorithms.isEmpty(articleId) && !Algorithms.isEmpty(wiki)) {
				articleId = retrieveArticleIdFromWikiUrl(wiki);
			}
			queryImagesByWikidataAndCategory(articleId, categoryName, h);

			if (articleId != null && !Algorithms.isEmpty(articleId)) {
				moveWikidataPhotoToFirst(imagesWithDetails, articleId);
			}
		}
		return imagesWithDetails;
	}
	
	private String createImageUrl(String imageTitle) {
		try {
			imageTitle = URLDecoder.decode(imageTitle, StandardCharsets.UTF_8);
		} catch (IllegalArgumentException e) {
			log.warn("imageTitle decode failed: " + imageTitle, e);
		}
		String[] hash = getHash(imageTitle);
		imageTitle = URLEncoder.encode(imageTitle, StandardCharsets.UTF_8);
		if (FILE_URL_TO_USE == 0) {
			return WIKIMEDIA_COMMON_SPECIAL_FILE_PATH + imageTitle;
		} else if (FILE_URL_TO_USE == 1) {
			return OSMAND_IMAGE_ROOT_URL + hash[0] + "/" + hash[1] + "/"+ imageTitle;
		} else {
			String suffix = imageTitle.endsWith(".svg") ? ".png" : "";
			return IMAGE_ROOT_URL + "thumb/" + hash[0] + "/" + hash[1] + "/" + imageTitle + "/" + THUMB_PREFIX + imageTitle + suffix;
		}
	}
	
	private void processImageQuery(String query, PreparedStatementSetter pss, RowCallbackHandler rowCallbackHandler) {
		jdbcTemplate.query(query, pss, rowCallbackHandler);
	}

	private void queryImagesByWikidataAndCategory(String wikidataId, String categoryName, RowCallbackHandler rowCallbackHandler) {
		List<Object> params = new ArrayList<>();

		boolean hasWikidataId = wikidataId != null && !Algorithms.isEmpty(wikidataId);
		boolean hasCategory = categoryName != null && !Algorithms.isEmpty(categoryName);

		if (hasWikidataId) {
			// Remove "Q" prefix from Wikidata ID if present
			wikidataId = wikidataId.startsWith("Q") ? wikidataId.substring(1) : wikidataId;
		}

		String query;
		if (hasWikidataId) {
			query = "SELECT mediaId, imageTitle, date, author, license, description, score AS views " +
					" FROM top_images_final WHERE wikidata_id = ? and dup_sim < " + SIMILARITY_CF +
					" ORDER BY score DESC, imageTitle ASC LIMIT " + LIMIT_PHOTOS_QUERY;
			params.add(wikidataId);
		} else if (hasCategory) {
			// Retrieve images based on the category name, following Python's VALID_EXTENSIONS_LOWERCASE
			query = " SELECT DISTINCT c.imgId AS mediaId, c.imgName AS imageTitle, '' AS date, '' AS author, '' AS license, '' AS description, c.views as views"
					+ " FROM wiki.categoryimages c WHERE c.catName = ? AND c.imgName != ''"
					+ " AND (c.imgName ILIKE '%.jpg' OR c.imgName ILIKE '%.png' OR c.imgName ILIKE '%.jpeg')"
					+ " ORDER BY views DESC, imgName ASC LIMIT " + LIMIT_PHOTOS_QUERY;
			params.add(categoryName.replace(' ', '_'));
		} else {
			return;
		}

		processImageQuery(
				query,
				ps -> {
					for (int i = 0; i < params.size(); i++) {
						if (params.get(i) != null) {
							ps.setString(i + 1, (String) params.get(i));
						} else {
							ps.setNull(i + 1, Types.VARCHAR);
						}
					}
				},
				rowCallbackHandler
		);
	}

	private void moveWikidataPhotoToFirst(Set<Map<String, Object>> imagesWithDetails, String wikidataId) {
		if (imagesWithDetails.isEmpty()) {
			return;
		}

		Long photoId = getPhotoIdFromWikidata(wikidataId);
		if (photoId == null) {
			return;
		}

		Map<Boolean, List<Map<String, Object>>> partitioned = imagesWithDetails.stream()
				.collect(Collectors.partitioningBy(img -> photoId.equals(img.get("mediaId"))));

		List<Map<String, Object>> matched = partitioned.get(true);
		if (matched.isEmpty()) {
			Map<String, Object> wikidataPhoto = fetchWikidataPhoto(photoId);
			if (wikidataPhoto != null && !wikidataPhoto.isEmpty()) {
				matched.add(wikidataPhoto);
			}
		}

		imagesWithDetails.clear();
		imagesWithDetails.addAll(matched);
		imagesWithDetails.addAll(partitioned.get(false));
	}

	private Long getPhotoIdFromWikidata(String wikidataId) {
		if (wikidataId.startsWith("Q")) {
			wikidataId = wikidataId.substring(1);
		}
		String query = "SELECT photoId FROM wiki.wikidata WHERE id = ? LIMIT 1";
		try {
			return jdbcTemplate.queryForObject(query, Long.class, wikidataId);
		} catch (EmptyResultDataAccessException e) {
			return null;
		} catch (Exception e) {
			log.error("Error getting photoId from wikidata for id: " + wikidataId, e);
			return null;
		}
	}

	private Map<String, Object> fetchWikidataPhoto(Long mediaId) {
		String query = "SELECT mediaId, imageTitle, date, author, license, description " +
				"FROM top_images_final WHERE mediaId = ? LIMIT 1";
		try {
			return jdbcTemplate.queryForObject(query, (rs, rowNum) -> {
				Map<String, Object> imageDetails = new HashMap<>();
				imageDetails.put("mediaId", rs.getLong("mediaId"));
				imageDetails.put("image", createImageUrl(rs.getString("imageTitle")));
				imageDetails.put("date", rs.getString("date"));
				imageDetails.put("author", rs.getString("author"));
				imageDetails.put("license", getLicense(rs.getString("license")));
				imageDetails.put("description", rs.getString("description"));
				return imageDetails;
			}, mediaId);
		} catch (EmptyResultDataAccessException e) {
			return Collections.emptyMap();
		} catch (Exception e) {
			log.error("Error fetching photo by mediaId: " + mediaId, e);
			return Collections.emptyMap();
		}
	}

	private String retrieveArticleIdFromWikiUrl(String wiki) {
		String title = wiki;
		String lang = "";
		int urlIndex = title.indexOf(".wikipedia.org/wiki/");
		if (urlIndex > 0) {
			String prefix = title.substring(0, urlIndex);
			lang = prefix.substring(prefix.lastIndexOf("/") + 1);
			title = title.substring(urlIndex + ".wikipedia.org/wiki/".length());
		} else if (title.indexOf(":") > 0) {
			String[] s = wiki.split(":");
			title = s[1];
			lang = s[0];
		}

		String id;
		try {
			if (lang.isEmpty()) {
				id = jdbcTemplate.queryForObject("SELECT id FROM wiki.wiki_mapping WHERE title = ? LIMIT 1",
						String.class, title);
			} else {
				id = jdbcTemplate.queryForObject("SELECT id FROM wiki.wiki_mapping WHERE lang = ? AND title = ? LIMIT 1",
						String.class, lang, title);
			}
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
		return id;
	}
	
	public FeatureCollection convertToFeatureCollection(List<WikiImage> images) {
		List<Feature> features = images.stream().map(imageDetails -> {
			Feature feature = new Feature(Geometry.point(new LatLon(0, 0)));
			feature.properties.put("imageTitle", imageDetails.getImageName());
			WikiMetadata.Metadata metadata = imageDetails.getMetadata();
			feature.properties.put("date", metadata.getDate());
			feature.properties.put("author", metadata.getAuthor());
			feature.properties.put("license", metadata.getLicense());
			feature.properties.put("description", metadata.getDescription());
			feature.properties.put("mediaId", imageDetails.getMediaId());
			return feature;
		}).toList();
		
		return new FeatureCollection(features.toArray(new Feature[0]));
	}

	private void parseLicenseFile() {
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream inputStream = classLoader.getResourceAsStream("wikidata/wiki_data_licenses.json");
		if (inputStream == null) {
			log.error("License file not found: wikidata/wiki_data_licenses.json");
			return;
		}
		final String VALUE = "value";

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			Gson gson = new Gson();
			JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);
			JsonArray bindings = jsonObject.getAsJsonObject("results").getAsJsonArray("bindings");
			for (JsonElement element : bindings) {
				JsonObject binding = element.getAsJsonObject();
				String itemValue = binding.getAsJsonObject("item").get(VALUE).getAsString();
				String licenseValue = binding.getAsJsonObject(VALUE).get(VALUE).getAsString();
				licenseMap.put(itemValue, licenseValue);
			}
		} catch (IOException e) {
			log.error("Failed to parse license file", e);
		}
	}

	public String getLicense(String data) {
		return licenseMap.getOrDefault("http://www.wikidata.org/entity/" + data, null);
	}
	
}
