package net.osmand.osm.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.CollatorStringMatcher.StringMatcherMode;
import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.data.Amenity;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.data.preparation.DBDialect;
import net.osmand.util.MapUtils;


public class WikivoyageOSMDataGenerator {
	
	private static final Log log = PlatformUtil.getLog(WikivoyageOSMDataGenerator.class);
	private static final String[] columns = new String[] {"osm_id long", "city_type text", "population text", "country text", "region text"};
	// threshold = 70 km
	private static final double DISTANCE_THRESHOLD = 70000;

	public static void main(String[] args) throws SQLException, IOException {

		String folder = "";
		if(args.length == 0) {	
			folder = "/home/user/osmand/wikivoyage/";
		}
		if(args.length > 0) {
			folder = args[1];
		} else if (folder.isEmpty()) {
			System.err.println("You should provide the path to the wikivoyage.sqlite file.");
			System.exit(-1);
		}
		final String sqliteFilePath = folder + "wikivoyage.sqlite";
		if (!new File(sqliteFilePath).exists()) {
			System.err.println("The path to the wikivoyage.sqlite is incorrect. Exiting...");
			System.exit(-1);
		}
		processDb(new File(sqliteFilePath));
	}

	private static void processDb(File sqliteFile) throws SQLException, IOException {
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = (Connection) dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
		for (String s : columns) {
			try {
				conn.createStatement().execute("ALTER TABLE wikivoyage_articles ADD COLUMN " + s);
			} catch (SQLException ex) {
				System.out.println("Column alredy exsists");
			}
		}
		File basemap = new File("/home/user/osmand/wikivoyage/World_basemap_2.obf");
		RandomAccessFile raf = new RandomAccessFile(basemap, "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, basemap);
		PreparedStatement stat = conn.prepareStatement("SELECT title, lat, lon FROM wikivoyage_articles");
		ResultSet rs = stat.executeQuery();
		while(rs.next()) {
			SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(0, 0, rs.getString(1),
					0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
			reader.searchPoiByName(req);
			long lat = rs.getLong(2);
			long lon = rs.getLong(3);
			LatLon ll = new LatLon(lat, lon);
			Amenity neededResult;
			for (Amenity a : req.getSearchResults()) {
				LatLon thisLoc = a.getLocation();
				System.out.println(/**a.getType().getTranslation() +
						" " + a.getSubType() + " " + a.getName() + " " + a.getLocation()**/ "This latlon: " + a.getLocation().toString() + " location from db: " + ll + " Distance: " + MapUtils.getDistance(ll, a.getLocation()));
				if (MapUtils.getDistance(ll, thisLoc) < DISTANCE_THRESHOLD || ((int) lat == (int) thisLoc.getLatitude()) && (int) lon == (int) thisLoc.getLongitude()) {
					neededResult = a;
					System.out.println("Accepted a result");
				}
			}
		}
	}

}
