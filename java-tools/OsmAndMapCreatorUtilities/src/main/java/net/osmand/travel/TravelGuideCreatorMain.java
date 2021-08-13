package net.osmand.travel;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import net.osmand.PlatformUtil;
import net.osmand.binary.MapZooms;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;

import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

import static net.osmand.IndexConstants.*;

public class TravelGuideCreatorMain {

    private static final int BATCH_SIZE = 30;
    private static final Log LOG = PlatformUtil.getLog(TravelGuideCreatorMain.class);
    public static final String TRAVEL_GUIDE_NAME = "travel_guide";

    public static void main(String[] args) throws SQLException, IOException, XmlPullParserException, InterruptedException {
        String dir = "";
        if (args.length < 1) {
			while (dir.isEmpty()) {
				System.out.print("Please provide the path to the data directory: ");
				Scanner scanner = new Scanner(System.in);
				dir = scanner.nextLine().trim();
				scanner.close();
			}
        } else {
            dir = args[0];
        }
        if (dir.equals("--help") || dir.equals("-h")) {
            printHelp();
            return;
        }
        new TravelGuideCreatorMain().generateTravelGuide(dir);
    }

    private static void printHelp() {
        System.out.println("Usage: <path to directory with html and gpx files " +
                "(should contain gpx and html files with identical names)>: " +
                "The utility creates an .travel.obf file that contains articles and points from the specified directory.");
    }

    private void generateTravelGuide(String dir) throws SQLException, IOException, XmlPullParserException, InterruptedException {
        File directory = new File(dir);
        if (!directory.isDirectory()) {
            throw new RuntimeException("Supplied path is not a directory");
        }
        File[] files = directory.listFiles();
        File sqliteFile = new File(directory, TRAVEL_GUIDE_NAME + BINARY_WIKIVOYAGE_MAP_INDEX_EXT);
        Connection conn = DBDialect.SQLITE.getDatabaseConnection(sqliteFile.getCanonicalPath(), LOG);
        if (conn == null) {
            LOG.error("Couldn't establish the database connection");
            System.exit(1);
        }
        Map<String, List<File>> mapping = getFileMapping(files);
        WikivoyageDataGenerator dataGenerator = new WikivoyageDataGenerator();
        generateTravelSqlite(mapping, conn);
        dataGenerator.generateSearchTable(conn);
        createPopularArticlesTable(conn);
        conn.close();
        File osmFile = new File(directory, TRAVEL_GUIDE_NAME + OSM_GZ_EXT);
        WikivoyageGenOSM.genWikivoyageOsm(sqliteFile, osmFile, -1);
        IndexCreatorSettings settings = new IndexCreatorSettings();
        settings.indexPOI = true;
        IndexCreator ic = new IndexCreator(directory, settings);
        ic.setMapFileName(directory.getName() + BINARY_TRAVEL_GUIDE_MAP_INDEX_EXT);
        MapRenderingTypesEncoder types = new MapRenderingTypesEncoder(settings.renderingTypesFile, osmFile.getName());
        ic.generateIndexes(osmFile, new ConsoleProgressImplementation(), null, MapZooms.getDefault(), types, LOG);
        osmFile.delete();
        sqliteFile.delete();
        new File("regions.ocbf").delete();
    }

    private void createPopularArticlesTable(Connection conn) throws SQLException {
        conn.createStatement().execute("CREATE TABLE popular_articles(title text, trip_id long,"
                + " population long, order_index long, popularity_index long, lat double, lon double, lang text)");
    }

    private void generateTravelSqlite(Map<String,List<File>> mapping, Connection conn) throws SQLException, IOException {
    	WikivoyageLangPreparation.createInitialDbStructure(conn, false);
        try {
            conn.createStatement().execute("ALTER TABLE travel_articles ADD COLUMN aggregated_part_of");
            conn.createStatement().execute("ALTER TABLE travel_articles ADD COLUMN is_parent_of");
        } catch (Exception e) {
            System.err.println("Column aggregated_part_of already exists");
        }
        PreparedStatement prep = WikivoyageLangPreparation.generateInsertPrep(conn, false);
        int count = 0;
        int batch = 0;
        for (String title : mapping.keySet()) {
            List<File> files = mapping.get(title);
            File gpx = null;
            File html = null;
            for (File f : files) {
                if (f.getName().endsWith(GPX_FILE_EXT)) {
                    gpx = f;
                } else if (f.getName().endsWith(HTML_EXT)) {
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
            prep.setString(column++, "");
            // gpx_gz
            prep.setBytes(column++, gpxBytes);
            // skip trip_id column
            prep.setLong(column++, ++count);
            prep.setLong(column++, count);
            prep.setString(column++, "en");
            prep.setString(column, "");
            prep.addBatch();
            if(batch++ > BATCH_SIZE) {
                prep.executeBatch();
                batch = 0;
            }
        }
        if (batch > 0) {
            prep.addBatch();
            prep.executeBatch();
        }
        prep.close();
        LOG.debug("Successfully created a travel book. Size: " + count);
    }

	private byte[] compress(byte[] content) {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		try {
			GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
			gzipOutputStream.write(content);
			gzipOutputStream.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return byteArrayOutputStream.toByteArray();
	}

    private Map<String, List<File>> getFileMapping(File[] files) {
        Map<String, List<File>> result = new HashMap<>();
        for (File f : files) {
            String filename = f.getName();
            if (!filename.endsWith(HTML_EXT) && !filename.endsWith(GPX_FILE_EXT)) {
                continue;
            }
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
