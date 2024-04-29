package net.osmand.server.utils;

import com.google.gson.Gson;
import net.osmand.server.api.services.SearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Component
public class WikiConnection {
    
    @Autowired
    SearchService searchService;
    
    private static final String BASE_NAME = "wiki";
    private static final String URL = "jdbc:clickhouse://213.239.196.116:8123/" + BASE_NAME;
    private static final String USER = "default";
    private static final String PASSWORD = System.getenv("PASSWORD");
    
    public static void main(String[] args) {
        getBaseInfo();
        String northWest = "50.5900, 30.2200";
        String southEast = "50.2130, 30.8950";
        List<String> geoJsonFeatures = getPoiData(northWest, southEast, true);
        geoJsonFeatures.forEach(System.out::println);
    }
    
    public static void getBaseInfo() {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            // Getting list of tables
            String queryTables = "SELECT name FROM system.tables WHERE database = '" + BASE_NAME + "'";
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(queryTables)) {
                while (rs.next()) {
                    String tableName = rs.getString("name");
                    System.out.println("Table: " + tableName);
                    
                    // Getting columns for each table
                    String queryColumns = "SELECT name, type FROM system.columns WHERE table = '" + tableName + "' AND database = '" + BASE_NAME + "'";
                    try (ResultSet rsColumns = statement.executeQuery(queryColumns)) {
                        System.out.println("Columns:");
                        while (rsColumns.next()) {
                            String columnName = rsColumns.getString("name");
                            String columnType = rsColumns.getString("type");
                            System.out.println(columnName + " - " + columnType);
                        }
                    }
                    
                    // Getting a sample record from each table
                    String querySample = "SELECT * FROM " + tableName + " LIMIT 1";
                    try (ResultSet rsSample = statement.executeQuery(querySample)) {
                        ResultSetMetaData metaData = rsSample.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        
                        System.out.println("Sample data:");
                        if (rsSample.next()) {
                            for (int i = 1; i <= columnCount; i++) {
                                String columnValue = rsSample.getString(i);
                                System.out.print(metaData.getColumnName(i) + ": " + columnValue + " | ");
                            }
                            System.out.println("\n");
                        } else {
                            System.out.println("No data found.\n");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Connection or SQL execution error: " + e.getMessage());
            e.printStackTrace();
        }
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
        
        public GeoFeature(long id, long osmid, double lat, double lon, String poitype, String poisubtype, long qrank, long mediaId, String imageTitle, String wikiLang, String wikiTitle, long wikiViews) {
            this.id = id;
            this.osmid = osmid;
            this.lat = lat;
            this.lon = lon;
            this.poitype = poitype;
            this.poisubtype = poisubtype;
            this.qrank = qrank;
            this.mediaId = mediaId;
            this.imageTitle = imageTitle;
            this.wikiLang = wikiLang;
            this.wikiTitle = wikiTitle;
            this.wikiViews = wikiViews;
        }
        
        public String toGeoJSON() {
            Gson gson = new Gson();
            String json = gson.toJson(this);
            return String.format("{\"type\": \"Feature\", \"properties\": %s, \"geometry\": {\"type\": \"Point\", \"coordinates\": [%f, %f]}}", json, lon, lat);
        }
    }
    
    public static List<String> getPoiData(String northWest, String southEast, boolean useCommonsGeoTags) {
        
        double north = Double.parseDouble(northWest.split(",")[0]);
        double west = Double.parseDouble(northWest.split(",")[1]);
        double south = Double.parseDouble(southEast.split(",")[0]);
        double east = Double.parseDouble(southEast.split(",")[1]);
        
        String query = generateQuery(useCommonsGeoTags);
        
        List<String> features = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pst = connection.prepareStatement(query)) {
            
            pst.setDouble(1, Math.min(south, north));
            pst.setDouble(2, Math.max(north, south));
            pst.setDouble(3, Math.min(west, east));
            pst.setDouble(4, Math.max(east, west));
            
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    GeoFeature feature = new GeoFeature(
                            (Long) getColumnValue(rs, "id", 0L),
                            (Long) getColumnValue(rs, "osmid", 0L),
                            (Double) getColumnValue(rs, "lat", 0.0),
                            (Double) getColumnValue(rs, "lon", 0.0),
                            (String) getColumnValue(rs, "poitype", ""),
                            (String) getColumnValue(rs, "poisubtype", ""),
                            (Long) getColumnValue(rs, "qrank", 0L),
                            (Long) getColumnValue(rs, "mediaId", 0L),
                            (String) getColumnValue(rs, "imageTitle", ""),
                            (String) getColumnValue(rs, "wikiLang", ""),
                            (String) getColumnValue(rs, "wikiTitle", ""),
                            (Long) getColumnValue(rs, "wikiViews", 0L)
                    );
                    features.add(feature.toGeoJSON());
                }
            }
        } catch (SQLException e) {
            System.out.println("Error during database query execution: " + e.getMessage());
            e.printStackTrace();
        }
        return features;
    }
    
    public static Object getColumnValue(ResultSet rs, String columnName, Object defaultValue) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columns = rsmd.getColumnCount();
        for (int x = 1; x <= columns; x++) {
            if (columnName.equals(rsmd.getColumnName(x))) {
                Object value = rs.getObject(columnName);
                return value != null ? value : defaultValue;
            }
        }
        return defaultValue;
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
}
