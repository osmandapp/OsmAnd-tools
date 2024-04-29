package net.osmand.server.api.services;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import net.osmand.data.LatLon;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.controllers.pub.GeojsonClasses;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.server.controllers.pub.GeojsonClasses.FeatureCollection;
import net.osmand.server.controllers.pub.GeojsonClasses.Geometry;
import net.osmand.util.Algorithms;
@Service
public class WikiService {
	protected static final Log log = LogFactory.getLog(WikiService.class);
	public static final String WIKIMEDIA_COMMON_SPECIAL_FILE_PATH = "https://commons.wikimedia.org/wiki/Special:FilePath/";

	private static final int LIMIT_QUERY = 100;
	@Value("${osmand.wiki.location}")
	private String pathToWikiSqlite;
	
	@Autowired
	@Qualifier("wikiJdbcTemplate")
	JdbcTemplate jdbcTemplate;
	
	@Autowired
	DatasourceConfiguration config;
	
    
	//  String northWest = "50.5900, 30.2200";
	//  String southEast = "50.2130, 30.8950";
	public FeatureCollection getPoiData(String northWest, String southEast, boolean useCommonsGeoTags) {
		if (!config.wikiInitialized()) {
			return new FeatureCollection();
		}
	    double north = Double.parseDouble(northWest.split(",")[0]);
	    double west = Double.parseDouble(northWest.split(",")[1]);
	    double south = Double.parseDouble(southEast.split(",")[0]);
	    double east = Double.parseDouble(southEast.split(",")[1]);
	    RowMapper<Feature> rowMapper = new RowMapper<GeojsonClasses.Feature>() {
	    	List<String> columnNames = null;

			@Override
			public Feature mapRow(ResultSet rs, int rowNum) throws SQLException {
				Feature f = new Feature(Geometry.point(new LatLon(rs.getDouble("lat"), rs.getDouble("lon"))));
				f.properties.put("rowNum", rowNum);
				if (columnNames == null) {
					columnNames = new ArrayList<String>();
					ResultSetMetaData rsmd = rs.getMetaData();
					for (int i = 1; i <= rsmd.getColumnCount(); i++) {
						columnNames.add(rsmd.getColumnName(i));
					}
				}
				for (int i = 1; i <= columnNames.size(); i++) {
					String col = columnNames.get(i - 1);
					if (col.equals("lat") || col.equals("lon")) {
						continue;
					}
					f.properties.put(columnNames.get(i - 1), rs.getString(i));
				}

				return f;
			}
		};
		List<Feature> stream = jdbcTemplate.query(
				" SELECT id, photoId, photoTitle, catId, catTitle, depId, depTitle, wikiTitle, wikiLang, osmid, osmtype, lat, lon  "
				+ " FROM wikidata WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ? "
				+ " ORDER BY qrank desc LIMIT " + LIMIT_QUERY,
	            new PreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps) throws SQLException {
						ps.setDouble(1, south);
						ps.setDouble(2, north);
						ps.setDouble(3, west);
						ps.setDouble(4, east);
					}
		}, rowMapper);
		return new FeatureCollection(stream.toArray(new Feature[stream.size()]));
	}
	
	static class GeoFeature {
		long id;
		long osmid;
		double lat;
		double lon;
		String poitype;
		String poisubtype;
		long qrank;
		long mediaId;
		String imageTitle;
		String wikiLang;
		String wikiTitle;
		long wikiViews;

		public String toGeoJSON() {
			Gson gson = new Gson();
			String json = gson.toJson(this);
			return String.format(
					"{\"type\": \"Feature\", \"properties\": %s, \"geometry\": {\"type\": \"Point\", \"coordinates\": [%f, %f]}}",
					json, lon, lat);
		}
	}
    
	  private static String generateQuery(boolean useCommonsGeoTags) {
	        if (useCommonsGeoTags) {
	            return "SELECT * FROM (" +
	                    "SELECT cgt.gt_page_id as id, wi.mediaId, wi.imageTitle, wi.views, " +
	                    "ROW_NUMBER() OVER(PARTITION BY cgt.gt_page_id ORDER BY wi.views DESC) as rn " +
	                    "FROM commons_geo_tags cgt " +
	                    "JOIN wikiimages wi ON cgt.gt_page_id = wi.mediaId " +
	                    "WHERE cgt.gt_lat BETWEEN ? AND ? AND cgt.gt_lon BETWEEN ? AND ? " +
	                    ") t WHERE rn = 1 " +
	                    "ORDER BY views DESC " +
	                    "LIMIT 50";
	        } else {
	            return "SELECT * FROM (" +
	                    "SELECT wd.id, wd.osmid, wd.lat, wd.lon, wd.poitype, wd.poisubtype, wd.qrank, wi.mediaId, wi.imageTitle, wi.views, wd.wikiLang, wd.wikiTitle, wd.wikiViews, " +
	                    "ROW_NUMBER() OVER(PARTITION BY wd.id ORDER BY wd.qrank DESC) as rn " +
	                    "FROM wikidata wd " +
	                    "JOIN wikiimages wi ON wd.photoId = wi.mediaId " +
	                    "WHERE wd.lat BETWEEN ? AND ? AND wd.lon BETWEEN ? AND ? " +
	                    ") t WHERE rn = 1 " +
	                    "ORDER BY qrank DESC " +
	                    "LIMIT 50";
	        }
	    }
	  
	public Set<String> processWikiImages(String articleId, String categoryName) {
		Set<String> images = new LinkedHashSet<>();
		
		return images;
	}
	
	@Deprecated
	public Set<String> processWikiImagesDelete(String articleId, String categoryName) {
		try {
			DBDialect osmDBdialect = DBDialect.SQLITE;
			Set<String> images = new LinkedHashSet<>();
			File sqliteFile = new File(pathToWikiSqlite);
			if (sqliteFile.exists()) {
				Connection conn = osmDBdialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
				if (articleId != null) {
					articleId = articleId.startsWith("Q") ? articleId.substring(1) : articleId;
					addImage(conn, articleId, images);
					addImagesFromCategory(conn, articleId, images);
					addImagesFromDepict(conn, articleId, images);
				}
				if (categoryName != null) {
					addImagesFromCategoryByName(conn, categoryName, images);
				}
			} else {
				log.error("commonswiki.sqlite file doesn't exist");
			}
			return images;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Deprecated
	private void addImage(Connection conn, String articleId, Set<String> images) throws SQLException {
		String selectQuery = "SELECT value FROM wikidata_properties where id=? and type='P18'";
		addImagesFromQuery(conn, articleId, images, selectQuery);
	}
	
	@Deprecated
	private void addImagesFromDepict(Connection conn, String articleId, Set<String> images) throws SQLException {
		String selectQuery = "SELECT name FROM common_content " +
				"INNER JOIN common_depict ON common_depict.id = common_content.id " +
				"WHERE common_depict.depict_qid = ?";
		addImagesFromQuery(conn, articleId, images, selectQuery);
	}
	@Deprecated
	private void addImagesFromCategory(Connection conn, String articleId, Set<String> images) throws SQLException {
		String selectQuery = "SELECT common_content_1.name FROM common_content " +
				"INNER JOIN wikidata_properties ON common_content.name = value " +
				"INNER JOIN common_category_links ON category_id = common_content.id " +
				"INNER JOIN common_content common_content_1 ON common_category_links.id = common_content_1.id " +
				"WHERE wikidata_properties.id = ? AND type = 'P373'";
		addImagesFromQuery(conn, articleId, images, selectQuery);
	}
	@Deprecated
	private void addImagesFromCategoryByName(Connection conn, String categoryName, Set<String> images) throws SQLException {
		String selectQuery = "SELECT common_content_1.name FROM common_content " +
				"JOIN common_category_links ON common_category_links.category_id = common_content.id AND common_content.name = ? " +
				"JOIN common_content common_content_1 ON common_category_links.id = common_content_1.id";
		addImagesFromQuery(conn, categoryName, images, selectQuery);
	}
	
	@Deprecated
	private void addImagesFromQuery(Connection conn, String param, Set<String> images, String selectQuery) throws SQLException {
		PreparedStatement selectImageFileNames = conn.prepareStatement(selectQuery);
		selectImageFileNames.setString(1, param);
		ResultSet rs = selectImageFileNames.executeQuery();
		while (rs.next()) {
			String fileName = rs.getString(1);
			if (!Algorithms.isEmpty(fileName)) {
				String url = WIKIMEDIA_COMMON_SPECIAL_FILE_PATH + fileName;
				images.add(url);
			}
		}
	}

}
