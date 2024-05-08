package net.osmand.server.api.services;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

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
	
	public FeatureCollection getWikidataData(String northWest, String southEast, String lang, Set<String> filters) {
		String filterQuery = filters.isEmpty() ? "" : "AND poitype IN (" + filters.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", ")) + ")";
		String query = "SELECT id, photoId, photoTitle, catId, catTitle, depId, depTitle, wikiTitle, wikiLang, wikiDesc, wikiArticles, osmid, osmtype, poitype, poisubtype, lat, lon "
				+ "FROM wikidata WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ? "
				+ filterQuery
				+ " ORDER BY qrank DESC LIMIT " + LIMIT_QUERY;
		
		return getPoiData(northWest, southEast, query, "lat", "lon", lang);
	}
	
	public FeatureCollection getPoiData(String northWest, String southEast, String query, String lat, String lon, String lang) {
//		String northWest = "50.5900, 30.2200";
//		String southEast = "50.2130, 30.8950";
		if (!config.wikiInitialized()) {
			return new FeatureCollection();
		}
		double north = Double.parseDouble(northWest.split(",")[0]);
		double west = Double.parseDouble(northWest.split(",")[1]);
		double south = Double.parseDouble(southEast.split(",")[0]);
		double east = Double.parseDouble(southEast.split(",")[1]);
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
						} else if (col.equals("wikiTitle") && !f.properties.containsKey("wikiTitle")) {
							f.properties.put("wikiTitle", rs.getString(i));
						} else if (col.equals("wikiDesc") && !f.properties.containsKey("wikiDesc")) {
							f.properties.put("wikiDesc", rs.getString(i));
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
					ps.setDouble(1, south);
					ps.setDouble(2, north);
					ps.setDouble(3, west);
					ps.setDouble(4, east);
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
			RowCallbackHandler h = new RowCallbackHandler() {

				@Override
				public void processRow(ResultSet rs) throws SQLException {
					if (FILENAME) {
						images.add(WIKIMEDIA_COMMON_SPECIAL_FILE_PATH + rs.getString(1));
					} else {
						String imageTitle = rs.getString(1);
						try {
							imageTitle = URLDecoder.decode(imageTitle, "UTF-8");
							String[] hash = getHash(imageTitle);
							imageTitle = URLEncoder.encode(imageTitle, "UTF-8");
							String prefix = THUMB_PREFIX;
							String suffix = imageTitle.endsWith(".svg") ? ".png" : "";
							images.add(IMAGE_ROOT_URL + "thumb/" + hash[0] + "/" + hash[1] + "/" + imageTitle + "/"
									+ prefix + imageTitle + suffix);
						} catch (UnsupportedEncodingException e) {
							System.err.println(e.getMessage());
						}
					}
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
				String id = null;
				ResultSetExtractor<String> rse = new ResultSetExtractor<String>() {

					@Override
					public String extractData(ResultSet rs) throws SQLException, DataAccessException {
						if(rs.next()) {
							return rs.getString(1);
						}
						return null;
					}
				};
				if (lang.length() == 0) {
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
						new PreparedStatementSetter() {

							@Override
							public void setValues(PreparedStatement ps) throws SQLException {
								ps.setString(1, aid.substring(1));
							}
						}, h);
			}
			if (!Algorithms.isEmpty(categoryName)) {
				jdbcTemplate.query("SELECT imageTitle from wiki.wikiimages where id = "
						+ " (select any(id) from wiki.wikiimages where imageTitle = ? and namespace = 14) "
						+ " and type='P373' and namespace = 6 order by views asc limit " + LIMITI_QUERY,
						new PreparedStatementSetter() {

							@Override
							public void setValues(PreparedStatement ps) throws SQLException {
								ps.setString(1, categoryName.replace(' ', '_'));
							}
						}, h);
			}
		}
		
		return images;
	}

}
