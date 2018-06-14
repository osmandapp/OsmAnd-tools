package net.osmand.travelguidecreator;

import net.osmand.PlatformUtil;
import net.osmand.travelguidecreator.connection.SQLiteJDBCDriverConnection;
import org.apache.commons.logging.Log;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class TravelGuideCreatorMain {

    private static final int BATCH_SIZE = 30;
    private static final Log log = PlatformUtil.getLog(TravelGuideCreatorMain.class);

    public static void main(String[] args) throws SQLException, IOException {
        String dir = "";
        if (args.length < 1) {
            while (dir.isEmpty()) {
                System.out.print("Please provide the path to the data directory: ");
                Scanner scanner = new Scanner( System.in );
                dir = scanner.nextLine();
            }
        } else {
            dir = args[0];
        }
        generateTravelGuide(dir);
    }

    private static void generateTravelGuide(String dir) throws SQLException, IOException {
        File directory = new File(dir);
        if (!directory.isDirectory()) {
            throw new RuntimeException("Supplied path is not a directory");
        }
        File[] files = directory.listFiles();
        Connection conn = SQLiteJDBCDriverConnection.SQLITE.getDatabaseConnection((dir.endsWith("/") ? dir : dir + "/")
                + "travel_guide.sqlite", log);
        if (conn == null) {
            log.error("Couldn't establish the database connection");
            System.exit(1);
        }
        Map<String, List<File>> mapping = getFileMapping(files);
        generateTravelSqlite(mapping, conn);
        generateSearchTable(conn);
        conn.close();
    }

    private static void generateSearchTable(Connection conn) throws SQLException {
        log.debug("Finishing database creation");
        conn.createStatement().execute("DROP TABLE IF EXISTS travel_search;");
        conn.createStatement()
                .execute("CREATE TABLE travel_search(search_term text, trip_id long, article_title text, lang text)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_search_term ON travel_search(search_term);");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_search_city ON travel_search(trip_id)");

        PreparedStatement insertSearch = conn.prepareStatement("INSERT INTO travel_search VALUES (?, ?, ?, ?)");
        PreparedStatement data = conn.prepareStatement("SELECT trip_id, title, lang, is_part_of FROM travel_articles");

        ResultSet rs = data.executeQuery();
        int batch = 0;
        while (rs.next()) {
            String title = rs.getString("title");
            String titleToSplit = title.replaceAll("[/\\)\\(-]", " ").replaceAll(" +", " ");
            String lang = rs.getString("lang");
            long id = rs.getLong("trip_id");
            for (String s : titleToSplit.split(" ")) {
                insertSearch.setString(1, s.toLowerCase());
                insertSearch.setLong(2, id);
                insertSearch.setString(3, title);
                insertSearch.setString(4, lang);
                insertSearch.addBatch();
                if (batch++ > 500) {
                    insertSearch.executeBatch();
                    batch = 0;
                }
            }
        }
        finishPrep(insertSearch);
        data.close();
        rs.close();
    }

    private static void generateTravelSqlite(Map<String,List<File>> mapping, Connection conn) throws SQLException, IOException {
        conn.createStatement()
                .execute("CREATE TABLE IF NOT EXISTS travel_articles(title text, content_gz blob, is_part_of text, lat double," +
                        " lon double, image_title text not null, gpx_gz blob, trip_id long, " +
                        "original_id long, lang text, contents_json text, aggregated_part_of)");
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS popular_articles(title text, trip_id long,"
                + " population long, order_index long, popularity_index long, lat double, lon double, lang text)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_title ON travel_articles(title);");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_id ON travel_articles(trip_id);");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO travel_articles(title, content_gz, is_part_of, lat, lon, " +
                "image_title, gpx_gz, trip_id , original_id , " +
                "lang, contents_json, aggregated_part_of) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )");
        int count = 0;
        int batch = 0;
        for (String title : mapping.keySet()) {
            List<File> files = mapping.get(title);
            File gpx = null;
            File html = null;
            for (File f : files) {
                if (f.getName().endsWith(".gpx")) {
                    gpx = f;
                } else if (f.getName().endsWith(".html")){
                    html = f;
                }
            }
            if (gpx == null || html == null) {
                continue;
            }
            byte[] gpxBytes = compress(Files.readAllBytes(gpx.toPath()));
            byte[] htmlBytes = compress(Files.readAllBytes(html.toPath()));
            int column = 1;
            prep.setString(column++, title);
            prep.setBytes(column++, htmlBytes);
            prep.setString(column++, "");
            prep.setNull(column++, Types.DOUBLE);
            prep.setNull(column++, Types.DOUBLE);
            prep.setString(column++, "");
            // gpx_gz
            prep.setBytes(column++, gpxBytes);
            // skip trip_id column
            prep.setLong(column++, ++count);
            prep.setLong(column++, count);
            prep.setString(column++, "en");
            prep.setString(column++, "");
            prep.setString(column, "");
            prep.addBatch();
            if(batch++ > BATCH_SIZE) {
                prep.executeBatch();
                batch = 0;
            }
        }
        finishPrep(prep);
        log.debug("Successfully created a travel book. Size: " + count);
    }

    private static void finishPrep(PreparedStatement ps) throws SQLException {
        ps.addBatch();
        ps.executeBatch();
        ps.close();
    }

    private static byte[] compress(byte[] content){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try{
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
            gzipOutputStream.write(content);
            gzipOutputStream.close();
        } catch(IOException e){
            throw new RuntimeException(e);
        }
        return byteArrayOutputStream.toByteArray();
    }

    private static Map<String, List<File>> getFileMapping(File[] files) {
        Map<String, List<File>> result = new HashMap<>();
        for (File f : files) {
            String filename = f.getName();
            filename = filename.substring(0, filename.lastIndexOf("."));
            List<File> currList = result.get(filename);
            if (currList == null) {
                currList = new ArrayList<>();
                currList.add(f);
                result.put(filename, currList);
            } else {
                currList.add(f);
                result.put(filename, currList);
            }
        }
        return result;
    }
}
