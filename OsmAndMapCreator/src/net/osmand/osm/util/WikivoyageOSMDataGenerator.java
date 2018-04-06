package net.osmand.osm.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.CollatorStringMatcher.StringMatcherMode;
import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.data.Amenity;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.data.preparation.DBDialect;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.util.MapUtils;


public class WikivoyageOSMDataGenerator {
	
	private static final Log log = PlatformUtil.getLog(WikivoyageOSMDataGenerator.class);
	private static final String[] columns = new String[] {"osm_id long", "city_type text", "population text", "country text", "region text"};
	private static OsmandRegions regions;
	
	public static void main(String[] args) throws SQLException, IOException {
		String file = "";
		String regionsFile = "";
		String citiesObf = "";
		if(args.length == 0) {	
			file = "/home/user/osmand/wikivoyage/wikivoyage.sqlite";
			regionsFile = "/home/user/repo/resources/countries-info/regions.ocbf";
			citiesObf = "/home/user/Cities.obf";
		}
		if(args.length > 2) {
			file = args[0];
			regionsFile = args[1];
			citiesObf = args[2];
		} else if (file.isEmpty()) {
			System.err.println("You should provide the path to the wikivoyage.sqlite file.");
			System.exit(-1);
		}
		File sqliteFile = new File(file);
		File countriesObfFile = new File(citiesObf);
		if (!sqliteFile.exists() || !countriesObfFile.exists()) {
			System.err.println("The path to the wikivoyage.sqlite is incorrect. Exiting...");
			System.exit(-1);
		}
		regions = new OsmandRegions();
		regions.prepareFile(regionsFile);
		regions.cacheAllCountries();
		processDb(sqliteFile, countriesObfFile);
	}

	private static void processDb(File sqliteFile, File citiesObf) throws SQLException, IOException {
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = (Connection) dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
		for (String s : columns) {
			try {
				conn.createStatement().execute("ALTER TABLE wikivoyage_articles ADD COLUMN " + s);
			} catch (SQLException ex) {
				System.out.println("Column alredy exsists");
			}
		}
		RandomAccessFile raf = new RandomAccessFile(citiesObf, "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, citiesObf);
		PreparedStatement stat = conn.prepareStatement("SELECT title, lat, lon FROM wikivoyage_articles");
		PreparedStatement updateStatement = conn.prepareStatement("UPDATE wikivoyage_articles SET lat = ?, lon = ?, osm_id = ?, city_type = ?, population = ?, "
				+ "country = ?, region = ? WHERE title = ?");
		PreparedStatement ps = conn.prepareStatement("SELECT Count(*) FROM wikivoyage_articles");
		int columns = 0;
		ResultSet res = ps.executeQuery();
		while (res.next()) {
			columns = res.getInt(1);
		}
		int count = 0;
		ResultSet rs = stat.executeQuery();
		while(rs.next()) {
			String title = rs.getString(1);
			SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(0, 0, title,
					0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
			reader.searchPoiByName(req);
			long lat = rs.getLong(2);
			long lon = rs.getLong(3);
			LatLon ll = new LatLon(lat, lon);
			List<Amenity> results = req.getSearchResults();
			Amenity acceptedResult = results.size() == 1 ? results.get(0) : getClosestMatch(results, ll);
			if (acceptedResult != null) {
				int column = 1;
				LatLon coords = acceptedResult.getLocation();
				updateStatement.setDouble(column++, coords.getLatitude());
				updateStatement.setDouble(column++, coords.getLongitude());
				updateStatement.setLong(column++, acceptedResult.getId() >> 1);
				updateStatement.setString(column++, acceptedResult.getSubType());
				updateStatement.setString(column++, acceptedResult.getAdditionalInfo("population"));
				List<String> regionsList = getRegions(coords.getLatitude(), coords.getLongitude());
				WorldRegion country = null;
				WorldRegion region = null;
				for (String reg : regionsList) {
					WorldRegion regionData = regions.getRegionDataByDownloadName(reg);
					if (regionData == null) {
						continue;
					}
					if (regionData.getLevel() == 2) {
						country = regionData;
					} else if (regionData.getLevel() > 2) {
						region = regionData;
					}
				}
				updateStatement.setString(column++, country == null ? "" : country.getLocaleName());
				updateStatement.setString(column++, region == null ? "" : region.getRegionDownloadName());				
				updateStatement.setString(column++, title);
				updateStatement.executeUpdate();
				updateStatement.clearParameters();
				if (count++ % 100 == 0) {
					System.out.println((((double)count / (double)columns) * 100d) + "%");
				}
			}
		}
		updateStatement.close();
		stat.close();
    	conn.close();
    	raf.close();
	}

	private static Amenity getClosestMatch(List<Amenity> results, LatLon fromDB) {
		if (results.isEmpty()) {
			return null;
		}
		Amenity result = results.get(0);
		double distance = MapUtils.getDistance(fromDB, result.getLocation());;
		for (int i = 1; i < results.size(); i++) {
			Amenity a = results.get(i);
			LatLon thisLoc = a.getLocation();
			double calculatedDistance = MapUtils.getDistance(fromDB, thisLoc);
			if (calculatedDistance < distance) {
				result = a;
			}
		}
		return result;
	}
	
	private static List<String> getRegions(double lat, double lon) throws IOException {
		List<String> keyNames = new ArrayList<>();
		int x31 = MapUtils.get31TileNumberX(lon);
		int y31 = MapUtils.get31TileNumberY(lat);
		List<BinaryMapDataObject> cs = regions.query(x31, y31);
		for (BinaryMapDataObject b : cs) {
			if(regions.contain(b, x31, y31)) {
				keyNames.add(regions.getDownloadName(b));
			}
		}
		return keyNames;
	}

}
