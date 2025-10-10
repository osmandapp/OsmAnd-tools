package net.osmand.obf.preparation;

import net.osmand.data.Boundary;
import net.osmand.data.City;
import net.osmand.data.DataTileManager;
import net.osmand.data.QuadRect;
import net.osmand.osm.edit.Entity;
import net.osmand.util.MapUtils;

import java.util.*;

/**
 * CityDataStorage stores all type of CityType except CityType.POSTCODE.
 * CityType.BOUNDARY - are stored as notAssignedBoundariesю
 * 
 * Take into account that not all objects will have streets, for example
 * DISTRICT, BOROUGH are not storedAsSeparateAdminEntity so here they are used
 * to have proper boundaries
 */
public class CityDataStorage {

	public Map<City, Boundary> cityBoundaries = new HashMap<City, Boundary>();
	public Map<Boundary, List<City>> boundaryToContainingCities = new HashMap<Boundary, List<City>>();
	private static final double CITY_VILLAGE_DIST = 30000;
	private DataTileManager<City> cityVillageManager = new DataTileManager<City>(13);
	private static final double CITY_DIST = 70000;
	private DataTileManager<City> cityManager = new DataTileManager<City>(10);
	private Map<Entity.EntityId, City> cities = new LinkedHashMap<Entity.EntityId, City>();
	private List<Boundary> notAssignedBoundaries = new ArrayList<Boundary>();

	public List<City> getClosestObjects(double latitude, double longitude, boolean includeOnlyStored) {
		List<City> result = new ArrayList<>();
		result = cityManager.getClosestObjects(latitude, longitude, CITY_DIST);
		for (City c : cityVillageManager.getClosestObjects(latitude, longitude, CITY_VILLAGE_DIST)) {
			if (!includeOnlyStored || c.getType().storedAsSeparateAdminEntity()) {
				result.add(c);
			}
		}
		return result;
	}

	public void registerObject(double latitude, double longitude, City city, Entity e) {
		if (city.getType() == City.CityType.CITY || city.getType() == City.CityType.TOWN) {
			cityManager.registerObject(latitude, longitude, city);
		} else {
			cityVillageManager.registerObject(latitude, longitude, city);
		}
		cities.put(Entity.EntityId.valueOf(e), city);
	}
	
	public City getRegisteredCity(Entity e) {
		return cities.get(Entity.EntityId.valueOf(e));
	}

	public Boundary getBoundaryByCity(City c) {
		return cityBoundaries.get(c);
	}

	public void setCityBoundary(City c, Boundary b) {
		cityBoundaries.put(c, b);
	}

	public boolean isCityHasBoundary(City c) {
		return cityBoundaries.containsKey(c);
	}

	public List<City> getCityListByBoundary(Boundary b) {
		if (boundaryToContainingCities.containsKey(b)) {
			return boundaryToContainingCities.get(b);
		}
		return null;
	}

	public void attachAllCitiesToBoundary(Boundary boundary) {
		List<City> list = new ArrayList<City>(1);
		for (City c : cities.values()) {
			if (boundary.containsPoint(c.getLocation())) {
				list.add(c);
			}
		}
		if (list.size() > 0) {
			boundaryToContainingCities.put(boundary, list);
		}
	}

	public List<Boundary> getNotAssignedBoundaries() {
		return notAssignedBoundaries;
	}

	public void addNotAssignedBoundary(Boundary boundary) {
		notAssignedBoundaries.add(boundary);
	}

	public void removeNotAssignedBoundary(Boundary boundary) {
		notAssignedBoundaries.remove(boundary);
	}

	public Collection<City> getAllCities() {
		return cities.values();
	}

	public int citiesSize() {
		return cities.size();
	}

	public void assignBbox(City c) {
		Boundary b = cityBoundaries.get(c);
		if (c.getBbox31() == null && b != null) {
			assignBbox(c, b);
		}
	}

	public void assignBbox(City c, Boundary b) {
		QuadRect bbox = b.getMultipolygon().getLatLonBbox();
		c.setBbox31(new int[] { MapUtils.get31TileNumberX(bbox.left), MapUtils.get31TileNumberY(bbox.top),
				MapUtils.get31TileNumberX(bbox.right), MapUtils.get31TileNumberY(bbox.bottom) });
	}
}
