package net.osmand.data.changeset;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.util.MapUtils;

public class CalculateCountryForChangesets {
	
	public static void main(String[] args) throws Exception {
		if(args[0].equals("calculate_countries")) {
			calculateCountries();
		}
	}

	private static void calculateCountries() throws Exception {
		// jdbc:postgresql://user:secret@localhost
		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/changeset",
				System.getenv("DB_USER"), System.getenv("DB_PWD"));
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM countries");
			boolean empty = !rs.next() || rs.getInt(1) == 0;
			rs.close();
			Map<WorldRegion, Integer> map = new LinkedHashMap<WorldRegion, Integer>();
			OsmandRegions or = initCountriesTable(conn, empty, map);
			
			rs = stat.executeQuery("select id, minlat, minlon, maxlat, maxlon from changesets where id not in (select changesetid from changeset_country) limit 1000;");
			while(rs.next()) {
				double minlat = rs.getDouble(2);
				double minlon = rs.getDouble(3);
				double maxlat = rs.getDouble(4);
				double maxlon = rs.getDouble(5);
				String changesetId = rs.getString(1);
				List<BinaryMapDataObject> objs = or.queryBbox(MapUtils.get31TileNumberX(minlon), MapUtils.get31TileNumberX(maxlon), 
						MapUtils.get31TileNumberY(maxlat), MapUtils.get31TileNumberY(minlat));
				for(BinaryMapDataObject o : objs) {
					String full = or.getFullName(o);
					WorldRegion reg = or.getRegionData(full);
					System.out.println(changesetId  + " " + full + " " + reg + " " + map.get(reg));
				}
			}
			
			
			
		} finally {
			conn.close();
		}
		
	}

	private static OsmandRegions initCountriesTable(Connection conn, boolean empty, Map<WorldRegion, Integer> map) throws IOException, SQLException {
		
		OsmandRegions or = new OsmandRegions();
		File regions = new File("OsmAndMapCreator/regions.ocbf");
		if(!regions.exists()) {
			 regions = new File("regions.ocbf");
		}
		or.prepareFile(regions.getAbsolutePath());
		or.cacheAllCountries();
		WorldRegion worldRegion = or.getWorldRegion();
		if (empty) {
			int id = 0;
			PreparedStatement ps = conn
					.prepareStatement("INSERT INTO countries(id, parentid, name, fullname, downloadname, clat, clon)"
							+ " VALUES(?, ?, ?, ?, ?, ?, ?)");
			LinkedList<WorldRegion> queue = new LinkedList<WorldRegion>();
			queue.add(worldRegion);
			while (!queue.isEmpty()) {
				WorldRegion wr = queue.pollFirst();
				id++;
				map.put(wr, id);
				ps.setInt(1, id);
				WorldRegion parent = wr.getSuperregion();
				if(parent != null) {
					ps.setInt(2, map.get(parent));
				} else {
					ps.setInt(2, 0);
				}
				
				ps.setString(3, wr.getLocaleName());
				ps.setString(4, wr.getRegionId());
				ps.setString(5, wr.getRegionDownloadName());
				if(wr.getRegionCenter() != null) {
					ps.setDouble(6, wr.getRegionCenter().getLatitude());
					ps.setDouble(7, wr.getRegionCenter().getLongitude());
				} else {
					ps.setDouble(6, 0);
					ps.setDouble(7, 0);
				}
				ps.addBatch();
				List<WorldRegion> lst = wr.getSubregions();
				if(lst != null) {
					queue.addAll(lst);
				}
			}
			ps.executeBatch();
			ps.close();
		}
		map.clear();
		ResultSet rs = conn.createStatement().executeQuery("select id, fullname from countries");
		while(rs.next()) {
			int id = rs.getInt(1);
			WorldRegion rd = or.getRegionData(rs.getString(2));
			if(rd == null) {
				throw new UnsupportedOperationException(rs.getString(2) + " not found");
			}
			map.put(rd, id);
		}
		
		Iterator<Entry<WorldRegion, Integer>> it = map.entrySet().iterator();
		while(it.hasNext()){
			Entry<WorldRegion, Integer> e = it.next();
			System.out.println(e.getKey().getLocaleName() + " " + e.getValue());
		}
			
		return or;
	}

}
