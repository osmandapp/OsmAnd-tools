package net.osmand.server.api.services;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.clickhouse.data.value.UnsignedLong;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import static net.osmand.data.MapObject.unzipContent;

import net.osmand.data.LatLon;
import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.server.controllers.pub.GeojsonClasses.FeatureCollection;
import net.osmand.server.controllers.pub.GeojsonClasses.Geometry;
import net.osmand.util.Algorithms;

@Service
public class WikiService {
	
	protected static final Log log = LogFactory.getLog(WikiService.class);
	// String url = WIKIMEDIA_COMMON_SPECIAL_FILE_PATH + fileName;
	public static final String WIKIMEDIA_COMMON_SPECIAL_FILE_PATH = "https://commons.wikimedia.org/wiki/Special:FilePath/";
	private static final String IMAGE_ROOT_URL = "https://upload.wikimedia.org/wikipedia/commons/";
	private static final String THUMB_PREFIX = "320px-";
	protected static final boolean FILENAME = true;

	private static final int LIMIT_QUERY = 1000;
	private static final int LIMITI_QUERY = 25;
	
	private static final Map<Integer, Set<String>> EXCLUDED_POI_TYPES_BY_ZOOM = Map.of(14, Set.of("shop"));
	private static final Map<Integer, Set<String>> EXCLUDED_POI_SUBTYPES_BY_ZOOM = Map.of(14, Set.of("artwork", "commercial", "memorial", "monument"));
	
	@Value("${osmand.wiki.location}")
	private String pathToWikiSqlite;
	
	@Autowired
	@Qualifier("wikiJdbcTemplate")
	JdbcTemplate jdbcTemplate;
	
	@Autowired
	DatasourceConfiguration config;
	
    
	public FeatureCollection getImages(String northWest, String southEast) {
		return getPoiData(northWest, southEast, " SELECT id, mediaId, namespace, imageTitle, imgLat, imgLon "
				+ " FROM wikigeoimages WHERE namespace = 6 AND imgLat BETWEEN ? AND ? AND imgLon BETWEEN ? AND ? "
				+ " ORDER BY views desc LIMIT " + LIMIT_QUERY, "imgLat", "imgLon", null);
	}
	
	public FeatureCollection getImagesById(long id, double lat, double lon) {
		String query = String.format("SELECT id, mediaId, imageTitle, date, author, license " +
				"FROM wikiimages WHERE id = %d " +
				"ORDER BY views DESC LIMIT " + LIMIT_QUERY, id);
		
		List<Feature> features = jdbcTemplate.query(query, (rs, rowNum) -> {
			Feature f = new Feature(Geometry.point(new LatLon(lat, lon)));
			f.properties.put("id", rs.getLong("id"));
			f.properties.put("mediaId", rs.getLong("mediaId"));
			f.properties.put("imageTitle", rs.getString("imageTitle"));
			f.properties.put("date", rs.getString("date"));
			f.properties.put("author", rs.getString("author"));
			f.properties.put("license", rs.getString("license"));
			return f;
		});
		
		return new FeatureCollection(features.toArray(new Feature[0]));
	}
	
	public FeatureCollection getWikidataData(String northWest, String southEast, String lang, Set<String> filters, int zoom) {
		String filterQuery = filters.isEmpty() ? "" : "AND poitype IN (" + filters.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", ")) + ")";
		
		Set<String> excludedPoiTypes = getExcludedTypes(EXCLUDED_POI_TYPES_BY_ZOOM, zoom);
		Set<String> excludedPoiSubtypes = getExcludedTypes(EXCLUDED_POI_SUBTYPES_BY_ZOOM, zoom);
		
		String subtypeFilter = "";
		if (!excludedPoiTypes.isEmpty()) {
			subtypeFilter += "AND poitype NOT IN (" + excludedPoiTypes.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", ")) + ") ";
		}
		if (!excludedPoiSubtypes.isEmpty()) {
			subtypeFilter += "AND poisubtype NOT IN (" + excludedPoiSubtypes.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", ")) + ") ";
		}
		
		String query = "SELECT id, photoId, photoTitle, catId, catTitle, depId, depTitle, wikiTitle, wikiLang, wikiDesc, wikiArticles, osmid, osmtype, poitype, poisubtype, lat, lon, wvLinks "
				+ "FROM wikidata WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ? "
				+ filterQuery
				+ " " + subtypeFilter
				+ " ORDER BY qrank DESC LIMIT " + LIMIT_QUERY;
		
		return getPoiData(northWest, southEast, query, "lat", "lon", lang);
	}
	
	private static Set<String> getExcludedTypes(Map<Integer, Set<String>> map, int zoom) {
		return map.entrySet().stream()
				.filter(entry -> entry.getKey() >= zoom)
				.flatMap(entry -> entry.getValue().stream())
				.collect(Collectors.toSet());
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
	
	public FeatureCollection getPoiData(String northWest, String southEast, String query, String lat, String lon, String lang) {
		if (!config.wikiInitialized()) {
			return new FeatureCollection();
		}
		
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
					if (lang != null) {
						if (col.equals("wikiArticles")) {
							Array array = rs.getArray(i);
							if (array != null) {
								Object[] wikiArticles = (Object[]) array.getArray();
								for (Object article : wikiArticles) {
									if (article instanceof List) {
										List<?> articleList = (List<?>) article;
										String langInArray = (String) articleList.get(0);
										String title = articleList.get(1) != null ? (String) articleList.get(1) : null;
										String shortDescription = articleList.get(2) != null ? (String) articleList.get(2) : null;
										if (langInArray.equals(lang)) {
											f.properties.put("wikiLang", lang);
											if (shortDescription != null) {
												f.properties.put("wikiDesc", shortDescription);
											}
											if (title != null) {
												f.properties.put("wikiTitle", title);
											}
											break;
										}
									}
								}
							}
						} else if (col.equals("wvLinks")) {
							Array array = rs.getArray(i);
							if (array != null) {
								Object[] wvLinks = (Object[]) array.getArray();
								Map<Long, List<String>> result = new HashMap<>();
								Map<Long, List<String>> enLinks = new HashMap<>();
								Map<Long, List<String>> otherLinks = new HashMap<>();
								for (Object linkInfo : wvLinks) {
									if (linkInfo instanceof List) {
										List<?> linkInfoList = (List<?>) linkInfo;
										long tripId = ((UnsignedLong) linkInfoList.get(0)).longValue();
										String langInArray = linkInfoList.get(1) != null ? (String) linkInfoList.get(1) : null;
										String title = linkInfoList.get(2) != null ? (String) linkInfoList.get(2) : null;
										String url = "https://" + langInArray + ".wikivoyage.org/wiki/" + title;
										List<String> urlInfo = Arrays.asList(title, url);
										if (langInArray != null) {
											if (langInArray.equals(lang)) {
												result.put(tripId, urlInfo);
											} else if (langInArray.equals("en")) {
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
								break;
							}
						} else if (col.equals("wikiTitle") && !f.properties.containsKey("wikiTitle")) {
							f.properties.put("wikiTitle", rs.getString(i));
						} else if (col.equals("wikiDesc") && !f.properties.containsKey("wikiDesc")) {
							f.properties.put("wikiDesc", rs.getString(i));
						} else if (col.equals("wikiLang") && !f.properties.containsKey("wikiLang")) {
							f.properties.put("wikiLang", rs.getString(i));
						} else {
							f.properties.put(col, rs.getString(i));
						}
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
				}, rowMapper);
		return new FeatureCollection(stream.toArray(new Feature[stream.size()]));
	}
	
	private static String[] getHash(String s) {
		String md5 = new String(Hex.encodeHex(DigestUtils.md5(s)));
		return new String[]{md5.substring(0, 1), md5.substring(0, 2)};
	}
	
	public Set<String> processWikiImages(String articleId, String categoryName, String wiki) {
		Set<String> images = new LinkedHashSet<>();
		if (config.wikiInitialized()) {
			RowCallbackHandler h = rs -> {
				if (FILENAME) {
					images.add(WIKIMEDIA_COMMON_SPECIAL_FILE_PATH + rs.getString(1));
				} else {
					String imageTitle = rs.getString(1);
                    imageTitle = URLDecoder.decode(imageTitle, StandardCharsets.UTF_8);
                    String[] hash = getHash(imageTitle);
                    imageTitle = URLEncoder.encode(imageTitle, StandardCharsets.UTF_8);
                    String prefix = THUMB_PREFIX;
                    String suffix = imageTitle.endsWith(".svg") ? ".png" : "";
                    images.add(IMAGE_ROOT_URL + "thumb/" + hash[0] + "/" + hash[1] + "/" + imageTitle + "/"
                            + prefix + imageTitle + suffix);
                }
			};
			if (Algorithms.isEmpty(articleId) && !Algorithms.isEmpty(wiki)) {
				String title = wiki;
				String lang = "";
				int url = title.indexOf(".wikipedia.org/wiki/");
				if (url > 0) {
					String prefix = title.substring(0, url);
					lang = prefix.substring(prefix.lastIndexOf("/") + 1, prefix.length());
					title = title.substring(url + ".wikipedia.org/wiki/".length());
				} else if (title.indexOf(":") > 0) {
					String[] s = wiki.split(":");
					title = s[1];
					lang = s[0];
				}
				String id;
				ResultSetExtractor<String> rse = rs -> {
					if (rs.next()) {
						return rs.getString(1);
					}
					return null;
				};
				if (lang.isEmpty()) {
					id = jdbcTemplate.query("SELECT id from wiki.wiki_mapping where title = ? ", rse, title);
				} else {
					id = jdbcTemplate.query("SELECT id from wiki.wiki_mapping where lang = ? and title = ? ", rse, lang,
							title);
				}
				if (id != null) {
					articleId = "Q" + id;
				}
			}
			if (!Algorithms.isEmpty(articleId) && articleId.startsWith("Q")) {
				String aid = articleId;
				jdbcTemplate.query(
						"SELECT imageTitle from wiki.wikiimages where id = ? and namespace = 6 "
								+ " order by type='P18' ? 0 : 1/(1+views) desc limit " + LIMITI_QUERY,
						ps -> ps.setString(1, aid.substring(1)), h);
			}
			if (!Algorithms.isEmpty(categoryName)) {
				jdbcTemplate.query(
						"SELECT imageTitle FROM wiki.wikiimages WHERE id = " +
								" (SELECT id FROM wiki.wikiimages WHERE imageTitle = ? AND namespace = 14 LIMIT 1) " +
								" AND type = 'P373' AND namespace = 6 ORDER BY views ASC LIMIT " + LIMITI_QUERY,
						ps -> ps.setString(1, categoryName.replace(' ', '_')), h);
			}
		}
		return images;
	}

}
