package net.osmand.obf.preparation;

import gnu.trove.set.hash.TLongHashSet;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.obf.preparation.DBStreetDAO.SimpleStreet;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;

public class CachedDBStreetDAO extends DBStreetDAO
{
	private Map<String, SimpleStreet> addressStreetLocalMap = new HashMap<String, SimpleStreet>();
	private TLongHashSet addressBuildingLocalSet = new TLongHashSet();
	private TLongHashSet addressStreetNodeLocalSet = new TLongHashSet();

	@Override
	public SimpleStreet findStreet(String name, City city, String cityPart) {
		return addressStreetLocalMap.get(createStreetUniqueName(name, city.getId(), cityPart)); //$NON-NLS-1$
	}

	@Override
	public SimpleStreet findStreet(String name, City city) {
		return addressStreetLocalMap.get(createStreetUniqueName(name, city.getId())); //$NON-NLS-1$
	}

	private String createStreetUniqueName(String name, Long cityId, String cityPart) {
		return name + '_' + cityId + '_' + cityPart;
	}

	private String createStreetUniqueName(String name, Long cityId) {
		return name + '_' + cityId;
	}

	@Override
	protected void writeStreetWayNodes(Set<Long> streetId, Way way)
			throws SQLException {
		super.writeStreetWayNodes(streetId, way);
		addressStreetNodeLocalSet.add(way.getId());
	}

	@Override
	protected void writeBuilding(Set<Long> streetId, Building building)
			throws SQLException {
		super.writeBuilding(streetId, building);
		addressBuildingLocalSet.add(building.getId());
	}

	@Override
	public long insertStreet(String name, Map<String, String> names, LatLon location, City city, String cityPart) throws SQLException {
		String langs = constructLangs(names);
		//batch the insert
		long streetId = fillInsertStreetStatement(name, names, location, city, cityPart, langs);
		addBatch(addressStreetStat);
		SimpleStreet ss = new SimpleStreet(streetId, name, city.getId(), cityPart,location, langs, Algorithms.encodeMap(names));
		addressStreetLocalMap.put(createStreetUniqueName(name, city.getId(), cityPart), ss);
		addressStreetLocalMap.put(createStreetUniqueName(name, city.getId()), ss);
		return streetId;
	}



	@Override
	public SimpleStreet updateStreetCityPart(SimpleStreet street, String cityPart) throws SQLException {
		commit(); //we are doing batch updates, so we must commit before this update
		SimpleStreet updatedSS = super.updateStreetCityPart(street, cityPart);
		addressStreetLocalMap.put(createStreetUniqueName(street.getName(), street.getCityId()), updatedSS);
		addressStreetLocalMap.put(createStreetUniqueName(street.getName(), street.getCityId(), cityPart), updatedSS);
		return updatedSS;
	}

	@Override
	public DBStreetDAO.SimpleStreet updateStreetLangs(DBStreetDAO.SimpleStreet street, Map<String, String> newNames) throws SQLException {
		commit(); //we are doing batch updates, so we must commit before this update
		SimpleStreet updatedSS = super.updateStreetLangs(street, newNames);
		addressStreetLocalMap.put(createStreetUniqueName(street.getName(), street.getCityId()), updatedSS);
		addressStreetLocalMap.put(createStreetUniqueName(street.getName(), street.getCityId(), street.getCityPart()), updatedSS);
		return updatedSS;
	}

	@Override
	public boolean findBuilding(Entity e) {
		return addressBuildingLocalSet.contains(e.getId());
	}

	@Override
	public boolean removeBuilding(Entity e) throws SQLException {
		addressBuildingLocalSet.remove(e.getId());
		return super.removeBuilding(e);
	}
	@Override
	public boolean findStreetNode(Entity e) {
		return addressStreetNodeLocalSet.contains(e.getId());
	}
}