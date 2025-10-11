package net.osmand.obf.preparation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;

public class DBStreetDAO extends AbstractIndexPartCreator {

	public static class SimpleStreet {
		private final long id;
		private final long cityId;
		private final String name;
		private final String cityPart;
		private final String langs;
		private LatLon location;
		private String nameEn;

		public SimpleStreet(long id, String name, long cityId, String cityPart, double latitude, double longitude,
				String langs, String nameEn) {
			this(id, name, cityId, cityPart, new LatLon(latitude, longitude), langs, nameEn);
		}

		public SimpleStreet(long id, String name, long cityId, String cityPart, LatLon location, String langs, String nameEn) {
			this.id = id;
			this.name = name;
			this.cityId = cityId;
			this.cityPart = cityPart;
			this.location = location;
			this.langs = langs;
			this.nameEn = nameEn;
		}

		public String getCityPart() {
			return cityPart;
		}

		public long getCityId() {
			return cityId;
		}

		public String getLangs() {
			return langs;
		}

		public long getId() {
			return id;
		}

		public String getName() {
			return name;
		}
		
		public String getNameEn() {
			return nameEn;
		}

		public LatLon getLocation() {
			return location;
		}
	}

	protected PreparedStatement addressStreetStat;
	private PreparedStatement addressStreetNodeStat;
	private PreparedStatement addressBuildingStat;
	private PreparedStatement addressSearchStreetStat;
	private PreparedStatement addressSearchBuildingStat;
	private PreparedStatement addressRemoveBuildingStat;
	private PreparedStatement addressSearchStreetNodeStat;
	private PreparedStatement addressSearchStreetStatWithoutCityPart;

	private Connection mapConnection;
	private PreparedStatement addressStreetUpdateCityPart;
	private PreparedStatement addressStreetLangsUpdate;

	public void createDatabaseStructure(Connection mapConnection, DBDialect dialect) throws SQLException {
		this.mapConnection = mapConnection;
		Statement stat = mapConnection.createStatement();
        stat.executeUpdate("create table street (id bigint primary key, latitude double, longitude double, " +
					"name varchar(1024), name_en varchar(1024), city bigint, citypart varchar(1024), langs varchar(1024))");
	    
        // create index on name ?
        stat.executeUpdate("create table building (id bigint, latitude double, longitude double, " +
                         "name2 varchar(1024), name_en2 varchar(1024), lat2 double, lon2 double, interval int, interpolateType varchar(50), " +
						"name varchar(1024), name_en varchar(1024), street bigint, postcode varchar(1024))");
        

        stat.executeUpdate("create table street_node (id bigint, latitude double, longitude double, " +
						"street bigint, way bigint)");
        stat.close();
        createPrimaryIndexes(mapConnection);

		addressStreetStat = createPrepareStatement(mapConnection,"insert into street (id, latitude, longitude, name, name_en, city, citypart, langs) values (?, ?, ?, ?, ?, ?, ?, ?)");
		addressStreetNodeStat = createPrepareStatement(mapConnection,"insert into street_node (id, latitude, longitude, street, way) values (?, ?, ?, ?, ?)");
		addressBuildingStat = createPrepareStatement(mapConnection,"insert into building (id, latitude, longitude, name, name_en, street, postcode, name2, name_en2, lat2, lon2, interval, interpolateType) values (?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,? ,?)");
		addressSearchStreetStat = createPrepareStatement(mapConnection,"SELECT id,latitude,longitude,langs,name_en FROM street WHERE ? = city AND ? = citypart AND ? = name");
		addressSearchStreetStatWithoutCityPart = createPrepareStatement(mapConnection,"SELECT id,name,citypart,latitude,longitude,langs,name_en FROM street WHERE ? = city AND ? = name");
		addressStreetUpdateCityPart = createPrepareStatement(mapConnection,"UPDATE street SET citypart = ? WHERE id = ?");
		addressStreetLangsUpdate = createPrepareStatement(mapConnection,"UPDATE street SET name_en = ? || name_en, langs = ? WHERE id = ?");
		addressSearchBuildingStat = createPrepareStatement(mapConnection,"SELECT id FROM building where ? = id");
		addressRemoveBuildingStat = createPrepareStatement(mapConnection,"DELETE FROM building where ? = id");
		addressSearchStreetNodeStat = createPrepareStatement(mapConnection,"SELECT way FROM street_node WHERE ? = way");
	}

	public void createIndexes(Connection mapConnection) throws SQLException {
		Statement stat = mapConnection.createStatement();
        stat.executeUpdate("create index building_loc on building (latitude, longitude)");
        stat.executeUpdate("create index street_node_street on street_node (street)");
        stat.close();
	}
	
	public void createPrimaryIndexes(Connection mapConnection) throws SQLException {
		Statement stat = mapConnection.createStatement();
        stat.executeUpdate("create index street_cnp on street (city,citypart,name,id)");
        stat.executeUpdate("create index street_city on street (city)");
        stat.executeUpdate("create index street_id on street (id)");
        stat.executeUpdate("create index building_postcode on building (postcode)");
        stat.executeUpdate("create index building_street on building (street)");
        stat.executeUpdate("create index building_id on building (id)");
        
        stat.executeUpdate("create index street_node_way on street_node (way)");
        stat.close();
	}

	protected void writeStreetWayNodes(Set<Long> streetIds, Way way) throws SQLException {
		for (Long streetId : streetIds) {
			for (Node n : way.getNodes()) {
				if (n == null) {
					continue;
				}
				addressStreetNodeStat.setLong(1, n.getId());
				addressStreetNodeStat.setDouble(2, n.getLatitude());
				addressStreetNodeStat.setDouble(3, n.getLongitude());
				addressStreetNodeStat.setLong(5, way.getId());
				addressStreetNodeStat.setLong(4, streetId);
				addBatch(addressStreetNodeStat);
			}
		}
	}

	protected void cleanCityPart() throws SQLException {
		Statement stat = mapConnection.createStatement();
		stat.executeUpdate("UPDATE street SET citypart = null WHERE id IN (SELECT id FROM street WHERE name IN (SELECT name FROM street GROUP BY name HAVING count(DISTINCT citypart) <= 1))");
		stat.close();
	}

	protected void writeBuilding(Set<Long> streetIds, Building building) throws SQLException {
		for (Long streetId : streetIds) {
			addBuildtingToBatch(building, streetId, null, building.getLocation());
			Iterator<Entry<String, LatLon>> it = building.getEntrances().entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, LatLon> next = it.next();
				addBuildtingToBatch(building, streetId, next.getKey(), next.getValue());
			}
		}
	}

	private void addBuildtingToBatch(Building building, Long streetId, String ref, LatLon loc) throws SQLException {
		addressBuildingStat.setLong(1, building.getId());
		addressBuildingStat.setDouble(2, loc.getLatitude());
		addressBuildingStat.setDouble(3, loc.getLongitude());
		addressBuildingStat.setLong(6, streetId);
		addressBuildingStat.setString(7, building.getPostcode() == null ? null : building.getPostcode().toUpperCase());
		if(ref != null) {
			addressBuildingStat.setString(4, building.getName() + IndexAddressCreator.ENTRANCE_BUILDING_DELIMITER + ref);
			addressBuildingStat.setString(5, "");
		} else {
			addressBuildingStat.setString(4, building.getName());
			addressBuildingStat.setString(5, Algorithms.encodeMap(building.getNamesMap(false)));
			addressBuildingStat.setString(8, building.getName2());
			addressBuildingStat.setString(9, building.getName2());
			LatLon l = building.getLatLon2();
			addressBuildingStat.setDouble(10, l == null ? 0 : l.getLatitude());
			addressBuildingStat.setDouble(11, l == null ? 0 : l.getLongitude());
			addressBuildingStat.setInt(12, building.getInterpolationInterval());
			if (building.getInterpolationType() == null) {
				addressBuildingStat.setString(13, null);
			} else {
				addressBuildingStat.setString(13, building.getInterpolationType().toString());
			}
		}
		addBatch(addressBuildingStat);
	}

	public DBStreetDAO.SimpleStreet findStreet(String name, City city) throws SQLException {
		addressSearchStreetStatWithoutCityPart.setLong(1, city.getId());
		addressSearchStreetStatWithoutCityPart.setString(2, name);
		ResultSet rs = addressSearchStreetStatWithoutCityPart.executeQuery();
		DBStreetDAO.SimpleStreet foundId = null;
		if (rs.next()) {
			foundId = new SimpleStreet(rs.getLong(1), rs.getString(2), city.getId(), rs.getString(3), rs.getDouble(4),
					rs.getDouble(5), rs.getString(6), rs.getString(7));
		}
		rs.close();
		return foundId;
	}

	public DBStreetDAO.SimpleStreet findStreet(String name, City city, String cityPart) throws SQLException {
		if (cityPart == null) {
			return findStreet(name, city);
		}
		addressSearchStreetStat.setLong(1, city.getId());
		addressSearchStreetStat.setString(2, cityPart);
		addressSearchStreetStat.setString(3, name);
		ResultSet rs = addressSearchStreetStat.executeQuery();
		DBStreetDAO.SimpleStreet foundId = null;
		if (rs.next()) {
			foundId = new SimpleStreet(rs.getLong(1), name, city.getId(), cityPart, rs.getDouble(2), rs.getDouble(3),
					rs.getString(4), rs.getString(5));
		}
		rs.close();
		return foundId;
	}

	private long streetIdSequence = 0;

	protected String constructLangs(Map<String, String> names) {
		String langs = "";
		if (names != null) {
			for (String l : names.keySet()) {
				langs += l + ";";
			}
		}
		return langs;
	}

	public long insertStreet(String name, Map<String, String> names, LatLon location,
			City city, String cityPart) throws SQLException {
		long streetId = fillInsertStreetStatement(name, names, location, city, cityPart,
				constructLangs(names));
		// execute the insert statement
		addressStreetStat.execute();
		// commit immediately to search after
		mapConnection.commit();

		return streetId;
	}


	protected long fillInsertStreetStatement(String name, Map<String, String> names,
			LatLon location, City city, String cityPart, String langs)
			throws SQLException {
		long streetId = streetIdSequence++;
		addressStreetStat.setLong(1, streetId);
		addressStreetStat.setString(4, name);
		addressStreetStat.setString(5, Algorithms.encodeMap(names));
		addressStreetStat.setDouble(2, location.getLatitude());
		addressStreetStat.setDouble(3, location.getLongitude());
		addressStreetStat.setLong(6, city.getId());
		addressStreetStat.setString(7, cityPart);
		addressStreetStat.setString(8, langs);
		return streetId;
	}

	public boolean findBuilding(Entity e) throws SQLException {
		commit(); //we are doing batch adds, to search, we must commit
		addressSearchBuildingStat.setLong(1, e.getId());
		ResultSet rs = addressSearchBuildingStat.executeQuery();
		boolean exist = rs.next();
		rs.close();
		return exist;
	}

	public boolean removeBuilding(Entity e) throws SQLException {
		executePendingPreparedStatements(); //ala flush
		addressRemoveBuildingStat.setLong(1, e.getId());
		boolean res = addressRemoveBuildingStat.execute();
		commit();
		return res;
	}

	public boolean findStreetNode(Entity e) throws SQLException {
		commit(); //we are doing batch adds, to search, we must commit
		addressSearchStreetNodeStat.setLong(1, e.getId());
		ResultSet rs = addressSearchStreetNodeStat.executeQuery();
		boolean exist = rs.next();
		rs.close();
		return exist;
	}

	public void commit() throws SQLException {
		if (executePendingPreparedStatements()) {
			mapConnection.commit();
		}
	}

	public void close() throws SQLException {
		closePreparedStatements(addressStreetStat, addressStreetNodeStat, addressBuildingStat);
	}

	public DBStreetDAO.SimpleStreet updateStreetCityPart(DBStreetDAO.SimpleStreet street, String cityPart) throws SQLException {
		addressStreetUpdateCityPart.setString(1, cityPart);
		addressStreetUpdateCityPart.setLong(2, street.getId());
		addressStreetUpdateCityPart.executeUpdate();
		mapConnection.commit();
		return new SimpleStreet(street.getId(), street.getName(), street.getCityId(), cityPart, street.getLocation(),
				street.getLangs(), street.getNameEn());
	}

	public DBStreetDAO.SimpleStreet updateStreetLangs(DBStreetDAO.SimpleStreet street, Map<String, String> newNames) throws SQLException {
		String langs = street.getLangs() + constructLangs(newNames);
		newNames.putAll(Algorithms.decodeMap(street.getNameEn()));
		String nameEn = Algorithms.encodeMap(newNames);
		addressStreetLangsUpdate.setString(1, nameEn);
		addressStreetLangsUpdate.setString(2, langs);
		addressStreetLangsUpdate.setLong(3, street.getId());
		addressStreetLangsUpdate.executeUpdate();
		mapConnection.commit();
		return new SimpleStreet(street.getId(), street.getName(), street.getCityId(), street.getCityPart(),
				street.getLocation(), langs, nameEn);
	}
}