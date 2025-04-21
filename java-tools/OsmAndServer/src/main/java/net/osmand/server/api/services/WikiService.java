package net.osmand.server.api.services;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
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
	// String url = WIKIMEDIA_COMMON_SPECIAL_FILE_PATH + fileName;
	public static final String WIKIMEDIA_COMMON_SPECIAL_FILE_PATH = "https://commons.wikimedia.org/wiki/Special:FilePath/";
	private static final String IMAGE_ROOT_URL = "https://upload.wikimedia.org/wikipedia/commons/";
	private static final String OSMAND_IMAGE_ROOT_URL = "https://data.osmand.net/wikimedia/images-1280/";
	private static final String THUMB_PREFIX = "320px-";
	protected static int FILE_URL_TO_USE = 1;

	private static final int LIMIT_OBJS_QUERY = 1000;
	private static final int LIMIT_PHOTOS_QUERY = 100;
	private static final String SIMILARITY_CF = "0.975";


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
				+ " ORDER BY views desc LIMIT " + LIMIT_OBJS_QUERY, null,"imgLat", "imgLon", null);
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
			connection.setRequestProperty("User-Agent", "OsmAnd Java Server");
			
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
			logError(urlStr, -1, e.getMessage());
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
		String shortenedContent = content != null && content.length() > 20 ? content.substring(0, 20) + "..." : content;
		log.error("Error while reading url: " + url + " code: " + code + " content: " + shortenedContent);
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

	public FeatureCollection getWikidataData(String northWest, String southEast, String lang, Set<String> filters, int zoom) {
		boolean showAll = filters.contains("0");
		String filterQuery = "";
		List<Object> filterParams = new ArrayList<>();

		if (!showAll && !filters.isEmpty()) {
			filterQuery = "AND e.topic IN (" + filters.stream().map(f -> "?").collect(Collectors.joining(", ")) + ")";
			filterParams.addAll(filters);
		}

		String lat = "COALESCE(w.lat, w.wlat)";
		String lon = "COALESCE(w.lon, w.wlon)";

		String query = "SELECT w.id, w.photoId, w.photoTitle, w.catId, w.catTitle, w.depId, w.depTitle, " +
				"w.wikiTitle, w.wikiLang, w.wikiDesc, w.wikiArticles, w.osmid, w.osmtype, w.poitype, " +
				"w.poisubtype, " +
				lat + " AS lat, " +
				lon + " AS lon, " +
				"w.wvLinks, COALESCE(e.elo, 900) AS elo, e.topic AS topic, e.categories AS categories, w.qrank " +
				"FROM wikidata w " +
				"LEFT JOIN wiki.elo_rating e ON w.id = e.id " +
				"WHERE (" + lat + " BETWEEN ? AND ? AND " + lon + " BETWEEN ? AND ?) " +
				(showAll ? "" : filterQuery) +
				"ORDER BY elo DESC, w.qrank DESC " +
				"LIMIT " + LIMIT_OBJS_QUERY;

		return getPoiData(northWest, southEast, query, filterParams, "lat", "lon", lang);
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
	
	public FeatureCollection getPoiData(String northWest, String southEast, String query, List<Object> filterParams, 
			String lat, String lon, String preferredLangs) {
		if (!config.wikiInitialized()) {
			return new FeatureCollection();
		}
		final List<String> plangs = Algorithms.isEmpty(preferredLangs) ? Arrays.asList("en")
				: Arrays.asList(preferredLangs.split(","));


		double north;
		double south;
		double east;
		double west;
		
		if (northWest != null && southEast != null) {
			north = Double.parseDouble(northWest.split(",")[0]);
			west = Double.parseDouble(northWest.split(",")[1]);
			south = Double.parseDouble(southEast.split(",")[0]);
			east = Double.parseDouble(southEast.split(",")[1]);
		} else {
			east = 180.0;
			west = -180.0;
			north = 90.0;
			south = -90.0;
		}
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
					if (col.equals(lat) || col.equals(lon)) {
						continue;
					}
					if (col.equals("wikiArticles")) {
						Array array = rs.getArray(i);
						if (array != null) {
							Object[] wikiArticles = (Object[]) array.getArray();
							StringBuilder langs = new StringBuilder();
							StringBuilder langViews = new StringBuilder();
							int pind = 100;
							for (Object article : wikiArticles) {
								if (article instanceof List<?> articleList) {
									String artLang = (String) articleList.get(0);
									String title = getFromArray(articleList, 1);
									String shortDescription = getFromArray(articleList, 2);
									String views = getFromArray(articleList, 3);
									if (!langs.isEmpty()) {
										langs.append(",");
										langViews.append(",");
									}
									langs.append(artLang);
									langViews.append(views != null ? views : "0");
									int lind = plangs.indexOf(artLang);
									if (lind < pind && lind >= 0) {
										pind = lind;
										f.properties.put("wikiLang", artLang);
										if (shortDescription != null) {
											f.properties.put("wikiDesc", shortDescription);
										}
										if (title != null) {
											f.properties.put("wikiTitle", title);
										}
									}
								}
							}
							f.properties.put("wikiLangs", langs.toString());
							f.properties.put("wikiLangViews", langViews.toString());
						}
					} else if (col.equals("wvLinks")) {
						Array array = rs.getArray(i);
						if (array != null) {
							Object[] wvLinks = (Object[]) array.getArray();
							Map<Long, List<String>> result = new HashMap<>();
							Map<Long, List<String>> enLinks = new HashMap<>();
							Map<Long, List<String>> otherLinks = new HashMap<>();
							int pind = 100;
							for (Object linkInfo : wvLinks) {
								if (linkInfo instanceof List) {
									List<?> linkInfoList = (List<?>) linkInfo;
									long tripId = ((UnsignedLong) linkInfoList.get(0)).longValue();
									String artLang = getFromArray(linkInfoList, 1);
									String title = getFromArray(linkInfoList, 2);
									if (artLang != null) {
										String url = "https://" + artLang + ".wikivoyage.org/wiki/" + title;
										List<String> urlInfo = Arrays.asList(title, url);
										int lind = plangs.indexOf(artLang);
										if (lind < pind && lind >= 0) {
											pind = lind;
											result.put(tripId, urlInfo);
										} else if (artLang.equals("en")) {
											enLinks.put(tripId, urlInfo);
										} else {
											otherLinks.put(tripId, urlInfo);
										}
									}
								}
							}
							for (Long tripId : enLinks.keySet()) {
								result.putIfAbsent(tripId, enLinks.get(tripId));
							}
							for (Long tripId : otherLinks.keySet()) {
								result.putIfAbsent(tripId, otherLinks.get(tripId));
							}
							if (!result.isEmpty()) {
								f.properties.put("wvLinks", result);
							}
						}
					} else if (col.equals("wikiTitle") && !f.properties.containsKey("wikiTitle")) {
						f.properties.put("wikiTitle", rs.getString(i));
					} else if (col.equals("wikiDesc") && !f.properties.containsKey("wikiDesc")) {
						f.properties.put("wikiDesc", rs.getString(i));
					} else if (col.equals("wikiLang") && !f.properties.containsKey("wikiLang")) {
						f.properties.put("wikiLang", rs.getString(i));
					} else if (col.equals("poisubtype")) {
						String poiType = rs.getString(i);
						PoiType type = MapPoiTypes.getDefault().getPoiTypeByKey(poiType);
						if (type != null) {
							f.properties.put(SearchService.PoiTypeField.ICON_NAME.getFieldName(),
									type.getIconKeyName());
							f.properties.put(SearchService.PoiTypeField.OSM_TAG.getFieldName(), type.getOsmTag());
							f.properties.put(SearchService.PoiTypeField.OSM_VALUE.getFieldName(), type.getOsmValue());
						}
						f.properties.put("poisubtype", rs.getString(i));
					} else {
						f.properties.put(col, rs.getString(i));
					}
				}
				return f;
			}

			
		};
		List<Feature> stream = jdbcTemplate.query(query,
				ps -> {
					if (northWest != null && southEast != null) {
						ps.setDouble(1, south);
						ps.setDouble(2, north);
						ps.setDouble(3, west);
						ps.setDouble(4, east);
					}
					for (int i = 0; i < filterParams.size(); i++) {
						ps.setObject(5 + i, filterParams.get(i));
					}
				}, rowMapper);
		return new FeatureCollection(stream.toArray(new Feature[stream.size()]));
	}
	
	private static String getFromArray(List<?> articleList, int ind) {
		if (ind < articleList.size()) {
			Object val = articleList.get(ind);
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
				
				imagesWithDetails.add(imageDetails);
			};
			if (Algorithms.isEmpty(articleId) && !Algorithms.isEmpty(wiki)) {
				articleId = retrieveArticleIdFromWikiUrl(wiki);
			}
			queryImagesByWikidataAndCategory(articleId, categoryName, h);
		}
		return imagesWithDetails;
	}
	
	private String createImageUrl(String imageTitle) {
		String[] hash = getHash(URLDecoder.decode(imageTitle, StandardCharsets.UTF_8));
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

	private void queryImagesByWikidataAndCategory(String articleId, String categoryName, RowCallbackHandler rowCallbackHandler) {
		List<Object> params = new ArrayList<>();
		boolean hasArticleId = articleId != null && !Algorithms.isEmpty(articleId);
		boolean hasCategory = categoryName != null && !Algorithms.isEmpty(categoryName);
		if (hasArticleId) {
			// Remove "Q" prefix from Wikidata ID if present
			articleId = articleId.startsWith("Q") ? articleId.substring(1) : articleId;
		}
		String query;
		if (hasArticleId) {
			query = "SELECT mediaId, imageTitle, date, author, license, score AS views " +
					" FROM top_images_final WHERE wikidata_id = ? and dup_sim < " + SIMILARITY_CF + 
					" ORDER BY score DESC LIMIT " + LIMIT_PHOTOS_QUERY;
			params.add(articleId);
		} else if (hasCategory) {
			// Retrieve images based on the category name
			query = " SELECT DISTINCT c.imgId AS mediaId, c.imgName AS imageTitle, '' AS date, '' AS author, '' AS license, c.views as views"
					+ " FROM wiki.categoryimages c WHERE c.catName = ? "
					+ " ORDER BY views DESC LIMIT "+ LIMIT_PHOTOS_QUERY;
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
