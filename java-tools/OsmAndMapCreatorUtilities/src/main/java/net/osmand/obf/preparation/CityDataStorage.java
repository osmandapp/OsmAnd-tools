package net.osmand.obf.preparation;

import net.osmand.data.Boundary;
import net.osmand.data.City;
import net.osmand.data.DataTileManager;
import net.osmand.osm.edit.Entity;

import java.util.*;

public class CityDataStorage {

    public Map<City, Boundary> cityBoundaries = new HashMap<City, Boundary>();
    public Map<Boundary, List<City>> boundaryToContainingCities = new HashMap<Boundary, List<City>>();
    private static final double CITY_VILLAGE_DIST = 30000;
    private DataTileManager<City> cityVillageManager = new DataTileManager<City>(13);
    private static final double CITY_DIST = 70000;
    private DataTileManager<City> cityManager = new DataTileManager<City>(10);
    private Map<Entity.EntityId, City> cities = new LinkedHashMap<Entity.EntityId, City>();
    private List<Boundary> notAssignedBoundaries = new ArrayList<Boundary>();
    private static final int SHIFT_BOUNDARY_CENTER = 2;

    public List<City> getClosestObjects(double latitude, double longitude) {
        List<City> result = new ArrayList<>();
        result = cityManager.getClosestObjects(latitude, longitude, CITY_DIST);
        result.addAll(cityVillageManager.getClosestObjects(latitude, longitude, CITY_VILLAGE_DIST));
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
}
