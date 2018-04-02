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
import net.osmand.data.City;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.data.preparation.DBDialect;


public class WikivoyageOSMDataGenerator {
	
	private static final Log log = PlatformUtil.getLog(WikivoyageOSMDataGenerator.class);
	private static final String[] columns = new String[] {"osm_id long", "city_type text", "population text", "country text", "region text"};

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
//		for (String s : columns) {
//			conn.createStatement().execute("ALTER TABLE wikivoyage_articles ADD COLUMN " + s);
//		}
		File basemap = new File("/home/user/osmand/wikivoyage/World_basemap_2.obf");
		RandomAccessFile raf = new RandomAccessFile(basemap, "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, basemap);
		PreparedStatement stat = conn.prepareStatement("SELECT title FROM wikivoyage_articles");
		ResultSet rs = stat.executeQuery();
//		while (rs.next()) {
//			SearchRequest<MapObject> req = BinaryMapIndexReader.buildAddressByNameRequest(new ResultMatcher<MapObject>() {
//				@Override
//				public boolean publish(MapObject object) {
////					if (object instanceof Street) {
////						System.out.println(object + " " + ((Street) object).getCity());
////					} else {
//						System.out.println(object + " " + object.getId());
////					}
//					return true;
//				}
//
//				@Override
//				public boolean isCancelled() {
//					return false;
//				}
//			}, rs.getString(1), StringMatcherMode.CHECK_ONLY_STARTS_WITH);
//			reader.searchAddressDataByName(req);
//		}
		List<City> cs = reader.getCities(null, BinaryMapAddressReaderAdapter.CITY_TOWN_TYPE);
		for (City c : cs) {
			c.toString();
		}
	}

}
