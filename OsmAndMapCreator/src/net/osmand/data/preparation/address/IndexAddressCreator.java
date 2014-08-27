package net.osmand.data.preparation.address;

import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.osmand.IProgress;
import net.osmand.data.Boundary;
import net.osmand.data.Building;
import net.osmand.data.Building.BuildingInterpolation;
import net.osmand.data.City;
import net.osmand.data.City.CityType;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.MultipolygonBuilder;
import net.osmand.data.Street;
import net.osmand.data.preparation.AbstractIndexPartCreator;
import net.osmand.data.preparation.BinaryFileReference;
import net.osmand.data.preparation.BinaryMapIndexWriter;
import net.osmand.data.preparation.DBDialect;
import net.osmand.data.preparation.OsmDbAccessorContext;
import net.osmand.data.preparation.address.DBStreetDAO.SimpleStreet;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.EntityParser;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.swing.Messages;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.sf.junidecode.Junidecode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IndexAddressCreator extends AbstractIndexPartCreator{
	
	private static final Log log = LogFactory.getLog(IndexAddressCreator.class);
	private final Log logMapDataWarn;

	private PreparedStatement addressCityStat;

	// MEMORY address : choose what to use ?
	private boolean loadInMemory = true;

	// MEMORY address : address structure
	// load it in memory
	private Map<EntityId, City> cities = new LinkedHashMap<EntityId, City>();
	private DataTileManager<City> cityVillageManager = new DataTileManager<City>(13);
	private DataTileManager<City> cityManager = new DataTileManager<City>(10);
	private List<Relation> postalCodeRelations = new ArrayList<Relation>();
	private Map<City, Boundary> cityBoundaries = new HashMap<City, Boundary>();
	private Map<Boundary,List<City>> boundaryToContainingCities = new HashMap<Boundary,List<City>>();
	private List<Boundary> notAssignedBoundaries = new ArrayList<Boundary>();
	private TLongHashSet visitedBoundaryWays = new TLongHashSet();
	
	private boolean normalizeStreets; 
	private String[] normalizeDefaultSuffixes;
	private String[] normalizeSuffixes;
	
	//TODO make it an option
	private boolean DEBUG_FULL_NAMES = false; //true to see attached cityPart and boundaries to the street names
	
	private static final int ADDRESS_NAME_CHARACTERS_TO_INDEX = 4;
	
	Connection mapConnection;
	DBStreetDAO streetDAO;


	
	public IndexAddressCreator(Log logMapDataWarn){
		this.logMapDataWarn = logMapDataWarn;
		streetDAO = loadInMemory ? new CachedDBStreetDAO() : new DBStreetDAO();
	}
	
	
	public void initSettings(boolean normalizeStreets, String[] normalizeDefaultSuffixes, String[] normalizeSuffixes,
			String cityAdminLevel) {
		cities.clear();
		cityManager.clear();
		postalCodeRelations.clear();
		cityBoundaries.clear();
		notAssignedBoundaries.clear();
		this.normalizeStreets = normalizeStreets;
		this.normalizeDefaultSuffixes = normalizeDefaultSuffixes;
		this.normalizeSuffixes = normalizeSuffixes;
	}

	public void registerCityIfNeeded(Entity e) {
		if (e instanceof Node && e.getTag(OSMTagKey.PLACE) != null) {
			City city = EntityParser.parseCity((Node) e);
			if(city != null) {
				regCity(city, e);
			}
		}
	}


	private void regCity(City city, Entity e) {
		LatLon l = city.getLocation();
		if (city.getType() != null && !Algorithms.isEmpty(city.getName()) && l != null) {
			if (city.getType() == CityType.CITY || city.getType() == CityType.TOWN) {
				cityManager.registerObject(l.getLatitude(), l.getLongitude(), city);
			} else {
				cityVillageManager.registerObject(l.getLatitude(), l.getLongitude(), city);
			}
			cities.put(EntityId.valueOf(e), city);
		}
	}
	
	public void indexBoundariesRelation(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		Boundary boundary = extractBoundary(e, ctx);
		// Bucurest has admin level 4
		boolean boundaryValid = boundary != null && (!boundary.hasAdminLevel() || boundary.getAdminLevel() >= 4) &&
				boundary.getCenterPoint() != null && !Algorithms.isEmpty(boundary.getName());
		if (boundaryValid) {
			LatLon boundaryCenter = boundary.getCenterPoint();
			List<City> citiesToSearch = new ArrayList<City>();
			citiesToSearch.addAll(cityManager.getClosestObjects(boundaryCenter.getLatitude(), boundaryCenter.getLongitude(), 3));
			citiesToSearch.addAll(cityVillageManager.getClosestObjects(boundaryCenter.getLatitude(), boundaryCenter.getLongitude(), 3));

			City cityFound = null;
			String boundaryName = boundary.getName().toLowerCase();
			String altBoundaryName = Algorithms.isEmpty(boundary.getAltName()) ? "" : boundary.getAltName().toLowerCase();
			if(boundary.hasAdminCenterId()) {
				for (City c : citiesToSearch) {
					if (c.getId() == boundary.getAdminCenterId()) {
						cityFound = c;
						boundary.setCity(c);
						break;
					}
				}
			}
			if(cityFound == null) {
				for (City c : citiesToSearch) {
					if ((boundaryName.equalsIgnoreCase(c.getName()) || altBoundaryName.equalsIgnoreCase(c.getName())) 
							&& boundary.containsPoint(c.getLocation())) {
						cityFound = c;
						boundary.setCity(c);
						break;
					}
				}
			}
			// We should not look for similarities, this can be 'very' wrong (we can find much bigger region)....
			// but here we just find what is the center of the boundary
			// False case : London Borough of Richmond upon Thames (bigger) -> Richmond!
			if (cityFound == null) {
				for (City c : citiesToSearch) {
					String lower = c.getName().toLowerCase();
					if (nameContains(boundaryName, lower) || nameContains(altBoundaryName, lower)) {
						if (boundary.containsPoint(c.getLocation())) {
							cityFound = c;
							break;
						}
					}
				}
			}
			// Monaco doesn't have place=town point, but has boundary with tags & admin_level
			// It could be wrong if the boundary doesn't match center point
			if(cityFound == null /*&& !boundary.hasAdminLevel() */&& 
					(boundary.getCityType() == CityType.TOWN ||
					boundary.getCityType() == CityType.HAMLET || 
					boundary.getCityType() == CityType.SUBURB || 
					boundary.getCityType() == CityType.VILLAGE)) {
				if(e instanceof Relation) {
					ctx.loadEntityRelation((Relation) e);
				}
				cityFound = createMissingCity(e, boundary.getCityType());
				boundary.setAdminCenterId(cityFound.getId());
			}
			if (cityFound != null) {
				putCityBoundary(boundary, cityFound);
			} else {
				
				logBoundaryChanged(boundary, null);
				notAssignedBoundaries.add(boundary);
			}
			attachAllCitiesToBoundary(boundary);
		} else if (boundary != null){
			if(logMapDataWarn != null) {
				logMapDataWarn.warn("Not using boundary: " + boundary + " " + boundary.getBoundaryId());
			} else {
				log.info("Not using boundary: " + boundary + " " + boundary.getBoundaryId());
			}
		}
	}


	private boolean nameContains(String boundaryName, String lower) {
		if(Algorithms.isEmpty(boundaryName)) {
			return false;
		}
		return boundaryName.startsWith(lower + " ") || boundaryName.endsWith(" " + lower)
				|| boundaryName.contains(" " + lower + " ");
	}


	private void attachAllCitiesToBoundary(Boundary boundary) {
		String boundaryName = boundary.getName();
		List<City> list = new ArrayList<City>(1);
		for (City c : cities.values()) {
			if (boundary.containsPoint(c.getLocation())) {
				if (!c.getName().equals(boundaryName)) {
					String isin = c.getIsInValue();
					if(isin == null || isin.isEmpty()) {
						c.setIsin(boundaryName);
					}
					if(c.getClosestCity() == null) {
						c.setClosestCity(boundary.getCity());
					}
				}
				list.add(c);
			}
		}
		if(list.size() > 0) {
			boundaryToContainingCities.put(boundary, list);
		}
	}

	public void tryToAssignBoundaryToFreeCities(IProgress progress) {
		progress.startWork(cities.size());
		// Why to do this? This is completely incorrect to do with suburbs because it assigns boundary that is much bigger 
		// than suburb and after that findCityPart works incorrectly
		//for cities without boundaries, try to find the right one
		int smallestAdminLevel = 7; //start at level 8 for now...
		for (City c : cities.values()) {
			progress.progress(1);
			Boundary cityB = cityBoundaries.get(c);
			if (cityB == null && (c.getType() == CityType.CITY || c.getType() == CityType.TOWN)) {
				LatLon location = c.getLocation();
				Boundary smallestBoundary = null;
				// try to found boundary
				for (Boundary b : notAssignedBoundaries) {
					if (b.getAdminLevel() >= smallestAdminLevel) {
						if (b.containsPoint(location.getLatitude(), location.getLongitude())) {
							// the bigger the admin level, the smaller the boundary :-)
							smallestAdminLevel = b.getAdminLevel();
							smallestBoundary = b;
						}
					}
				}
				if (smallestBoundary != null) {
					putCityBoundary(smallestBoundary, c);
					notAssignedBoundaries.remove(smallestBoundary);
				}
			}
		}
	}

	private int extractBoundaryAdminLevel(Entity e) {
		int adminLevel = -1;
		try {
			String tag = e.getTag(OSMTagKey.ADMIN_LEVEL);
			if (tag == null) {
				return adminLevel;
			}
			return Integer.parseInt(tag);
		} catch (NumberFormatException ex) {
			return adminLevel;
		}
	}
	

	// lower is better
//  1. place = city (Moscow, Nizhniy Novgorod)
//	2. Cuxhoven admin_level = 7 win admin_level = 6
//	3. Catania admin_level = 8 win admin_level = 6
//	4. Zurich admin_level = 6 win admin_level = 4
//  5. Bucurest admin_level = 4 win nothing
	private int getCityBoundaryImportance(Boundary b, City c) {
		boolean nameEq = b.getName().equalsIgnoreCase(c.getName());
		if(!Algorithms.isEmpty(b.getAltName()) && !nameEq) {
			nameEq = b.getAltName().equalsIgnoreCase(c.getName());
		}
		boolean cityBoundary = b.getCityType() != null;
		// max 10
		int adminLevelImportance = getAdminLevelImportance(b);
		if(nameEq) {
			if(cityBoundary) {
				return 0;
			} else if(c.getId() == b.getAdminCenterId() || 
					!b.hasAdminCenterId()){
				return adminLevelImportance;
			}
			return 10 + adminLevelImportance;
		} else {
			if(c.getId() == b.getAdminCenterId()) {
				return 20 + adminLevelImportance;
			} else {
				return 30  + adminLevelImportance;
			}
		}
	}


	private int getAdminLevelImportance(Boundary b) {
		int adminLevelImportance = 5;
		if(b.hasAdminLevel()) {
			int adminLevel = b.getAdminLevel();
			if(adminLevel == 8) {
				adminLevelImportance = 1;
			} else if(adminLevel == 7) {
				adminLevelImportance = 2;
			} else if(adminLevel == 6) {
				adminLevelImportance = 3;
			} else if(adminLevel == 9) {
				adminLevelImportance = 4;
			} else if(adminLevel == 10) {
				adminLevelImportance = 5;
			} else {
				adminLevelImportance = 6;
			}
		}
		return adminLevelImportance;
	}
	
	private Boundary putCityBoundary(Boundary boundary, City cityFound) {
		final Boundary oldBoundary = cityBoundaries.get(cityFound);
		if(oldBoundary == null) {
			cityBoundaries.put(cityFound, boundary);
			logBoundaryChanged(boundary, cityFound);
			return oldBoundary;
		} else if (oldBoundary.getAdminLevel() == boundary.getAdminLevel()
				&& oldBoundary != boundary
				&& boundary.getName().equalsIgnoreCase(
						oldBoundary.getName())) {
			oldBoundary.mergeWith(boundary);
			return oldBoundary;
		} else {
			int old = getCityBoundaryImportance(oldBoundary, cityFound);
			int n = getCityBoundaryImportance(boundary, cityFound);
			if (n < old) {
				cityBoundaries.put(cityFound, boundary);
				logBoundaryChanged(boundary, cityFound);
			}
			return oldBoundary;
		}
	}


	private void logBoundaryChanged(Boundary boundary, City cityFound) {
		String s = "City " + (cityFound == null ? " not found " : " : " +cityFound.getName());
		s += " boundary: " + boundary.toString() + " " + boundary.getBoundaryId();
		if (logMapDataWarn != null) {
			logMapDataWarn.info(s);
		} else {
			log.info(s);
		}
	}

	private Boundary extractBoundary(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		if(e instanceof Node) {
			return null;
		}
		long centerId = 0;
		CityType ct = CityType.valueFromString(e.getTag(OSMTagKey.PLACE));
		if(ct == null && "townland".equals(e.getTag(OSMTagKey.LOCALITY))) {
			if(e instanceof Relation) {
				ctx.loadEntityRelation((Relation) e);
			}
			centerId = createMissingCity(e, CityType.SUBURB).getId();
			ct = CityType.SUBURB;
		}
		boolean administrative = "administrative".equals(e.getTag(OSMTagKey.BOUNDARY));
		if (administrative || ct != null) {
			if (e instanceof Way && visitedBoundaryWays.contains(e.getId())) {
				return null;
			}
			
			String bname = e.getTag(OSMTagKey.NAME);
			MultipolygonBuilder m = new MultipolygonBuilder();
			if (e instanceof Relation) {
				Relation aRelation = (Relation) e;
				ctx.loadEntityRelation(aRelation);
				Map<Entity, String> entities = aRelation.getMemberEntities();
				for (Entity es : entities.keySet()) {
					if (es instanceof Way) {
						boolean inner = "inner".equals(entities.get(es)); //$NON-NLS-1$
						if (inner) {
							m.addInnerWay((Way) es);
						} else {
							String wName = es.getTag(OSMTagKey.NAME);
							// if name are not equal keep the way for further check (it could be different suburb)
							if (Algorithms.objectEquals(wName, bname) || wName == null) {
								visitedBoundaryWays.add(es.getId());
							}
							m.addOuterWay((Way) es);
						}
					} else if (es instanceof Node && ("admin_centre".equals(entities.get(es)) || "admin_center".equals(entities.get(es)))) {
						centerId =  es.getId();
					} else if (es instanceof Node && ("label".equals(entities.get(es)) && centerId == 0)) {
						centerId =  es.getId();
					}
				}
			} else if (e instanceof Way) {
				m.addOuterWay((Way) e);
			}
			Boundary boundary = new Boundary(m); 
			boundary.setName(bname);
			boundary.setAltName(e.getTag("short_name")); // Goteborg
			boundary.setAdminLevel(extractBoundaryAdminLevel(e));
			boundary.setBoundaryId(e.getId());
			boundary.setCityType(ct);
			if(centerId != 0) {
				boundary.setAdminCenterId(centerId);
			}
			return boundary;
		} else {
			return null;
		}
	}


	private City createMissingCity(Entity e, CityType t) throws SQLException {
		City c = EntityParser.parseCity(e, t);
//		long centerId = e.getId();
		regCity(c, e);
		writeCity(c);
		commitWriteCity();
		return c;
	}
	
	public void indexAddressRelation(Relation i, OsmDbAccessorContext ctx) throws SQLException {
		if ("street".equals(i.getTag(OSMTagKey.TYPE)) || "associatedStreet".equals(i.getTag(OSMTagKey.TYPE))) { //$NON-NLS-1$
			
			LatLon l = null;
			String streetName = null;
			Set<String> isInNames = null;
			ctx.loadEntityRelation(i);
			
			Collection<Entity> members = i.getMembers("street");
			for(Entity street : members) { // find the first street member with name and use it as a street name
				String name = street.getTag(OSMTagKey.NAME);
				if (name != null) {
					streetName = name;
					l = street.getLatLon();
					isInNames = street.getIsInNames();
					break;
				}
			}
			
			if (streetName == null) { // use relation name as a street name
				streetName = i.getTag(OSMTagKey.NAME);
				l = i.getMemberEntities().keySet().iterator().next().getLatLon(); // get coordinates from any relation member
				isInNames = i.getIsInNames();
			}
			
			if (streetName != null) {
				Set<Long> idsOfStreet = getStreetInCity(isInNames, streetName, null, null, l);
				if (!idsOfStreet.isEmpty()) {
					Collection<Entity> houses = i.getMembers("house"); // both house and address roles can have address
					houses.addAll(i.getMembers("address"));
					for (Entity house : houses) {
						String hname = house.getTag(OSMTagKey.ADDR_HOUSE_NAME);
						if(hname == null) {
							hname = house.getTag(OSMTagKey.ADDR_HOUSE_NUMBER);
						}
						if (hname == null)
							continue;
						
						if (!streetDAO.findBuilding(house)) {
							// process multipolygon (relation) houses - preload members to create building with correct latlon
							if (house instanceof Relation)
								ctx.loadEntityRelation((Relation) house);
							Building building = EntityParser.parseBuilding(house);
							if (building.getLocation() == null) {
								log.warn("building with empty location! id: " + house.getId());
							}
							building.setName(hname);
							
							streetDAO.writeBuilding(idsOfStreet, building);
						}
					}
				}
			}
		}
	}
	

	public String normalizeStreetName(String name) {
		if(name == null) {
			return null;
		}
		name = name.trim();
		if (normalizeStreets) {
			String newName = name;
			boolean processed = newName.length() != name.length();
			for (String ch : normalizeDefaultSuffixes) {
				int ind = checkSuffix(newName, ch);
				if (ind != -1) {
					newName = cutSuffix(newName, ind, ch.length());
					processed = true;
					break;
				}
			}

			if (!processed) {
				for (String ch : normalizeSuffixes) {
					int ind = checkSuffix(newName, ch);
					if (ind != -1) {
						newName = putSuffixToEnd(newName, ind, ch.length());
						processed = true;
						break;
					}
				}
			}
			if (processed) {
				return newName;
			}
		}
		return name;
	}

	private int checkSuffix(String name, String suffix) {
		int i = -1;
		boolean searchAgain = false;
		do {
			i = name.indexOf(suffix, i);
			searchAgain = false;
			if (i > 0) {
				if (Character.isLetterOrDigit(name.charAt(i - 1))) {
					i++;
					searchAgain = true;
				}
			}
		} while (searchAgain);
		return i;
	}

	private String cutSuffix(String name, int ind, int suffixLength) {
		String newName = name.substring(0, ind);
		if (name.length() > ind + suffixLength + 1) {
			newName += name.substring(ind + suffixLength + 1);
		}
		return newName.trim();
	}

	private String putSuffixToEnd(String name, int ind, int suffixLength) {
		if (name.length() <= ind + suffixLength) {
			return name;

		}
		String newName;
		if (ind > 0) {
			newName = name.substring(0, ind);
			newName += name.substring(ind + suffixLength);
			newName += name.substring(ind - 1, ind + suffixLength);
		} else {
			newName = name.substring(suffixLength + 1) + name.charAt(suffixLength) + name.substring(0, suffixLength);
		}

		return newName.trim();
	}

	public Set<Long> getStreetInCity(Set<String> isInNames, String name, String nameEn, String ref, final LatLon location) throws SQLException {
		if (location == null) {
			return Collections.emptySet();
		
		}
		if (name != null) {
			name = normalizeStreetName(name);
		}
		else {
			name = normalizeStreetName(ref);
		}
		Set<City> result = new LinkedHashSet<City>();
		List<City> nearestObjects = new ArrayList<City>();
		nearestObjects.addAll(cityManager.getClosestObjects(location.getLatitude(),location.getLongitude()));
		nearestObjects.addAll(cityVillageManager.getClosestObjects(location.getLatitude(),location.getLongitude()));
		//either we found a city boundary the street is in
		for (City c : nearestObjects) {
			Boundary boundary = cityBoundaries.get(c);
			if (isInNames.contains(c.getName()) || (boundary != null && boundary.containsPoint(location))) {
				result.add(c);
			}
		}
		// or we need to find closest city
		Collections.sort(nearestObjects, new Comparator<City>() {
			@Override
			public int compare(City c1, City c2) {
				double r1 = relativeDistance(location, c1);
				double r2 = relativeDistance(location, c2);
				return Double.compare(r1, r2);
			}
		});
		for(City c : nearestObjects) {
			if(relativeDistance(location, c) > 0.2) {
				if(result.isEmpty()) {
					result.add(c);
				}
				break;
			} else if(!result.contains(c)) {
				// city doesn't have boundary or there is a mistake in boundaries and we found nothing before
				if(!cityBoundaries.containsKey(c) || result.isEmpty()) {
					result.add(c);
				}
			}
		}
		return registerStreetInCities(name, nameEn, location, result);
	}


	private Set<Long> registerStreetInCities(String name, String nameEn, LatLon location, Collection<City> result) throws SQLException {
		if (result.isEmpty()) {
			return Collections.emptySet();
		}
		if (Algorithms.isEmpty(nameEn) && name != null) {
			nameEn = Junidecode.unidecode(name);
		}

		Set<Long> values = new TreeSet<Long>();
		for (City city : result) {
			String nameInCity = name;
			String nameEnInCity = nameEn;
			if(nameInCity == null ||  nameEnInCity == null) {
				nameInCity = "<" +city.getName()+">";
				nameEnInCity = "<" +city.getEnName()+">";
			}
			long streetId = getOrRegisterStreetIdForCity(nameInCity, nameEnInCity, location, city);
			values.add(streetId);
		}
		return values;
	}


	private long getOrRegisterStreetIdForCity(String name, String nameEn, LatLon location, City city) throws SQLException {
		String cityPart = findCityPart(location, city);
		SimpleStreet foundStreet = streetDAO.findStreet(name, city, cityPart);
		if (foundStreet == null) {
			// by default write city with cityPart of the city
			if(cityPart == null ) {
				cityPart = city.getName();
			}
			return streetDAO.insertStreet(name, nameEn, location, city, cityPart);
		} else {
			return foundStreet.getId();
		}
	}

	private String findCityPart(LatLon location, City city) {
		String cityPart = city.getName();
		boolean found = false;
		Boundary cityBoundary = cityBoundaries.get(city);
		if (cityBoundary != null) {
			List<City> subcities = boundaryToContainingCities.get(cityBoundary);
			if (subcities != null) {
				for (City subpart : subcities) {
					if (subpart != city) {
						Boundary subBoundary = cityBoundaries.get(subpart);
						if (cityBoundary != null && subBoundary != null && subBoundary.getAdminLevel() > cityBoundary.getAdminLevel()) {
							// old code
							cityPart = findNearestCityOrSuburb(subBoundary, location); // subpart.getName();
							// ?FIXME
							if(subBoundary.containsPoint(location)) {
								cityPart = subpart.getName();
								found = true;
								break;	
							}
						}
						else if(cityBoundary != null && cityBoundary.containsPoint(subpart.getLocation())
								&& (subpart.getType() == CityType.HAMLET
										|| subpart.getType() == CityType.NEIGHBOURHOOD
										|| subpart.getType() == CityType.ISOLATED_DWELLING
										|| subpart.getType() == CityType.LOCALITY)) {
							subpart.setIsin(cityBoundary.getName());
						}
					}
				}
			}
		}
		if (!found) {
			Boundary b = cityBoundaries.get(city);
			cityPart = findNearestCityOrSuburb(b, location);
		}
		return cityPart;
	}

	private String findNearestCityOrSuburb(Boundary greatestBoundary, LatLon location) {
		String result = null;
		double dist = Double.MAX_VALUE;
		List<City> list = new ArrayList<City>();
		if(greatestBoundary != null) {
			result = greatestBoundary.getName();
			list = boundaryToContainingCities.get(greatestBoundary);
		} else {
			list.addAll(cityManager.getClosestObjects(location.getLatitude(),location.getLongitude()));
			list.addAll(cityVillageManager.getClosestObjects(location.getLatitude(),location.getLongitude()));
		}
		if(list != null) {
			for (City c : list) {
				double actualDistance = MapUtils.getDistance(location, c.getLocation());
				if (actualDistance < 1.5 * c.getType().getRadius() && actualDistance < dist) {
					result = c.getName();
					dist = actualDistance;
				}
			}
		}
		return result;
	}



	private double relativeDistance(LatLon point, City c) {
		return MapUtils.getDistance(c.getLocation(), point) / c.getType().getRadius();
	}
	
	public void iterateMainEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		// index not only buildings but also nodes that belongs to addr:interpolation ways
		// currently not supported because nodes are indexed first with buildings 
		String interpolation = e.getTag(OSMTagKey.ADDR_INTERPOLATION);
		if (e instanceof Way && interpolation != null ){
			BuildingInterpolation type = null;
			int interpolationInterval = 0;
			if(interpolation != null) {
				try {
					type = BuildingInterpolation.valueOf(interpolation.toUpperCase());
				} catch (RuntimeException ex) {
					try {
						interpolationInterval = Integer.parseInt(interpolation);
					} catch(NumberFormatException ex2) {
					}
				}
			}
			if (type != null || interpolationInterval > 0) {
				List<Node> nodesWithHno = new ArrayList<Node>();
				for (Node n : ((Way) e).getNodes()) {
					if (n.getTag(OSMTagKey.ADDR_HOUSE_NUMBER) != null) {
						String strt = n.getTag(OSMTagKey.ADDR_STREET);
						if (strt == null) {
							strt = n.getTag(OSMTagKey.ADDR_PLACE);
						}
						if (strt != null) {
							nodesWithHno.add(n);
						}
					}
				}
				if (nodesWithHno.size() > 1) {
					for (int i = 1; i < nodesWithHno.size(); i++) {
						Node first = nodesWithHno.get(i - 1);
						Node second = nodesWithHno.get(i);
						boolean exist = streetDAO.findBuilding(first);
						if (exist) {
							streetDAO.removeBuilding(first);
						}
						LatLon l = e.getLatLon();
						String strt = first.getTag(OSMTagKey.ADDR_STREET);
						if (strt == null) {
							strt = first.getTag(OSMTagKey.ADDR_PLACE);
						}
						Set<Long> idsOfStreet = getStreetInCity(first.getIsInNames(), strt, null, null, l);
						if (!idsOfStreet.isEmpty()) {
							Building building = EntityParser.parseBuilding(first);
							building.setInterpolationInterval(interpolationInterval);
							building.setInterpolationType(type);
							building.setName(first.getTag(OSMTagKey.ADDR_HOUSE_NUMBER));
							building.setName2(second.getTag(OSMTagKey.ADDR_HOUSE_NUMBER));
							building.setLatLon2(second.getLatLon());
							streetDAO.writeBuilding(idsOfStreet, building);
						}
					}
				}
			}
		}
		String houseName = e.getTag(OSMTagKey.ADDR_HOUSE_NAME);
		String houseNumber = e.getTag(OSMTagKey.ADDR_HOUSE_NUMBER);
		String street = null;
		if (houseNumber != null) {
			street = e.getTag(OSMTagKey.ADDR_STREET);
			if (street == null) {
				street = e.getTag(OSMTagKey.ADDR_PLACE);
			}
		}
		String street2 = e.getTag(OSMTagKey.ADDR_STREET2);
		if ((houseName != null || houseNumber != null) ) {
			if(e instanceof Relation) {
				ctx.loadEntityRelation((Relation) e);
				Collection<Entity> outs = ((Relation) e).getMembers("outer");
				if(!outs.isEmpty()) {
					e = outs.iterator().next();
				}
			}
			// skip relations
			boolean exist = e instanceof Relation || streetDAO.findBuilding(e);
			if (!exist) {
				LatLon l = e.getLatLon();
				Set<Long> idsOfStreet = getStreetInCity(e.getIsInNames(), street, null, null, l);
				if (!idsOfStreet.isEmpty()) {
					Building building = EntityParser.parseBuilding(e);
					String hname = houseName;
					if(hname == null) {
						hname = houseNumber;
					}
					int i = hname.indexOf('-');
					if (i != -1 && interpolation != null) {
						building.setInterpolationInterval(1);
						try {
							building.setInterpolationType(BuildingInterpolation.valueOf(interpolation.toUpperCase()));
						} catch (RuntimeException ex) {
							try {
								building.setInterpolationInterval(Integer.parseInt(interpolation));
							} catch (NumberFormatException ex2) {
							}
						}
						building.setName(hname.substring(0, i));
						building.setName2(hname.substring(i + 1));
					} else if ((street2 != null) && !street2.isEmpty()) {
						int secondNumber = hname.indexOf('/');
						if(secondNumber == -1 || !(secondNumber < hname.length() - 1)) {
							building.setName(hname);
						} else {
							building.setName(hname.substring(0, secondNumber));
							Building building2 = EntityParser.parseBuilding(e);
							building2.setName(hname.substring(secondNumber + 1));
							Set<Long> ids2OfStreet = getStreetInCity(e.getIsInNames(), street2, null, null, l);
							ids2OfStreet.removeAll(idsOfStreet); //remove duplicated entries!
							if(!ids2OfStreet.isEmpty()) {
								streetDAO.writeBuilding(ids2OfStreet, building2);
							} else {
								building.setName2(building2.getName());
							}
						}
					}
					else {
						building.setName(hname);
					}
					
					streetDAO.writeBuilding(idsOfStreet, building);
				}
			}
		} else if (e instanceof Way /* && OSMSettings.wayForCar(e.getTag(OSMTagKey.HIGHWAY)) */
				&& e.getTag(OSMTagKey.HIGHWAY) != null
				&& (e.getTag(OSMTagKey.NAME) != null || e.getTag(OSMTagKey.REF) != null)
				&& isStreetTag(e.getTag(OSMTagKey.HIGHWAY))) {
			// suppose that streets with names are ways for car
			// Ignore all ways that have house numbers and highway type
			
			// if we saved address ways we could checked that we registered before
			boolean exist = streetDAO.findStreetNode(e);

			Set<String> isInNames = e.getIsInNames();
			if(isInNames.isEmpty()) {
				isInNames = new HashSet<String>();
				for(Boundary b: boundaryToContainingCities.keySet()) {
					if(b.containsPoint(e.getLatLon())) {
						isInNames.add(b.getName());
					}
				}
			}

			// check that street way is not registered already
			if (!exist) {
				LatLon l = e.getLatLon();
				Set<Long> idsOfStreet = getStreetInCity(isInNames,
						e.getTag(OSMTagKey.NAME), e.getTag(OSMTagKey.NAME_EN),
						e.getTag(OSMTagKey.REF), l);
				if (!idsOfStreet.isEmpty()) {
					streetDAO.writeStreetWayNodes(idsOfStreet, (Way) e);
				}
			}
		} else if (e instanceof Relation) {
			if (e.getTag(OSMTagKey.POSTAL_CODE) != null) {
				ctx.loadEntityRelation((Relation) e);
				postalCodeRelations.add((Relation) e);
			}
		}
	}
	
	private boolean isStreetTag(String highwayValue) {
		return !"platform".equals(highwayValue);
	}


	private void writeCity(City city) throws SQLException {
		addressCityStat.setLong(1, city.getId());
		addressCityStat.setDouble(2, city.getLocation().getLatitude());
		addressCityStat.setDouble(3, city.getLocation().getLongitude());
		addressCityStat.setString(4, city.getName());
		addressCityStat.setString(5, city.getEnName());
		addressCityStat.setString(6, CityType.valueToString(city.getType()));
		addBatch(addressCityStat);
	}
	
	
	public void writeCitiesIntoDb() throws SQLException {
		for (City c : cities.values()) {
			if(c.getType() != CityType.DISTRICT &&
					//c.getType() != CityType.SUBURB &&
					c.getType() != CityType.NEIGHBOURHOOD &&
					c.getType() != CityType.ISOLATED_DWELLING &&
					c.getType() != CityType.LOCALITY){
				writeCity(c);
			}
		}
		// commit to put all cities
		commitWriteCity();
	}


	private void commitWriteCity() throws SQLException {
		if (pStatements.get(addressCityStat) > 0) {
			addressCityStat.executeBatch();
			pStatements.put(addressCityStat, 0);
			mapConnection.commit();
		}
	}
	
	public void processingPostcodes() throws SQLException {
		streetDAO.commit();
		PreparedStatement pstat = mapConnection.prepareStatement("UPDATE building SET postcode = ? WHERE id = ?");
		pStatements.put(pstat, 0);
		for (Relation r : postalCodeRelations) {
			String tag = r.getTag(OSMTagKey.POSTAL_CODE);
			for (EntityId l : r.getMemberIds()) {
				pstat.setString(1, tag);
				pstat.setLong(2, l.getId());
				addBatch(pstat);
			}
		}
		if (pStatements.get(pstat) > 0) {
			pstat.executeBatch();
		}
		pStatements.remove(pstat);
	}
	
	

	private static final int CITIES_TYPE = 1;
	private static final int POSTCODES_TYPE = 2;
	private static final int VILLAGES_TYPE = 3;

	public void writeBinaryAddressIndex(BinaryMapIndexWriter writer, String regionName, IProgress progress) throws IOException, SQLException {
		streetDAO.close();
		closePreparedStatements(addressCityStat);
		mapConnection.commit();

		writer.startWriteAddressIndex(regionName);
		Map<CityType, List<City>> cities = readCities(mapConnection);
		PreparedStatement streetstat = mapConnection.prepareStatement(//
				"SELECT A.id, A.name, A.name_en, A.latitude, A.longitude, "+ //$NON-NLS-1$
				"B.id, B.name, B.name_en, B.latitude, B.longitude, B.postcode, A.cityPart, "+ //$NON-NLS-1$
				" B.name2, B.name_en2, B.lat2, B.lon2, B.interval, B.interpolateType, A.cityPart == C.name as MainTown " +
				"FROM street A LEFT JOIN building B ON B.street = A.id JOIN city C ON A.city = C.id " + //$NON-NLS-1$
				"WHERE A.city = ? ORDER BY MainTown DESC, A.name ASC"); //$NON-NLS-1$
		PreparedStatement waynodesStat =
			 mapConnection.prepareStatement("SELECT A.id, A.latitude, A.longitude FROM street_node A WHERE A.street = ? "); //$NON-NLS-1$

		// collect suburbs with is in value
		List<City> suburbs = new ArrayList<City>();
		List<City> cityTowns = new ArrayList<City>();
		List<City> villages = new ArrayList<City>();
		List<City> hamlet_iso_dwelling_loc = new ArrayList<City>();
		for(CityType t : cities.keySet()) {
			if(t == CityType.CITY || t == CityType.TOWN){
				cityTowns.addAll(cities.get(t));
			} else if(t == CityType.VILLAGE || t == CityType.HAMLET) {
				villages.addAll(cities.get(t));
			} else if(t == CityType.SUBURB) {
				for(City c : cities.get(t)){
					if(c.getIsInValue() != null) {
						suburbs.add(c);
					}
				}
			}
			if(t == CityType.HAMLET
			|| t == CityType.ISOLATED_DWELLING
			|| t == CityType.LOCALITY) {
				for(City small_city: cities.get(t)) {
					Boundary smallestBoundary = null;
					for(Boundary b: boundaryToContainingCities.keySet()) {
						if(b.containsPoint(small_city.getLocation())
							&& b.getAdminLevel() <= 9) {
							if(smallestBoundary == null
							|| smallestBoundary.getAdminLevel() < b.getAdminLevel())
								smallestBoundary = b;
						}
					}
					if (smallestBoundary != null) {
						small_city.setIsin(smallestBoundary.getName());
						hamlet_iso_dwelling_loc.add(small_city);
					}
				}
			}
		}
		
		progress.startTask(Messages.getString("IndexCreator.SERIALIZING_ADRESS"), cityTowns.size() + villages.size() / 100 + 1); //$NON-NLS-1$
		
		Map<String, List<MapObject>> namesIndex = new TreeMap<String, List<MapObject>>(Collator.getInstance());
		Map<String, City> postcodes = new TreeMap<String, City>();
		writeCityBlockIndex(writer, CITIES_TYPE,  streetstat, waynodesStat, suburbs, cityTowns, postcodes, namesIndex, progress);
		writeCityBlockIndex(writer, VILLAGES_TYPE,  streetstat, waynodesStat, hamlet_iso_dwelling_loc, villages, postcodes, namesIndex, progress);
		
		// write postcodes		
		List<BinaryFileReference> refs = new ArrayList<BinaryFileReference>();		
		writer.startCityBlockIndex(POSTCODES_TYPE);
		ArrayList<City> posts = new ArrayList<City>(postcodes.values());
		for (City s : posts) {
			refs.add(writer.writeCityHeader(s, -1));
		}
		for (int i = 0; i < posts.size(); i++) {
			City postCode = posts.get(i);
			BinaryFileReference ref = refs.get(i);
			putNamedMapObject(namesIndex, postCode, ref.getStartPointer());
			writer.writeCityIndex(postCode, new ArrayList<Street>(postCode.getStreets()), null, ref);
		}
		writer.endCityBlockIndex();


		progress.finishTask();

		writer.writeAddressNameIndex(namesIndex);
		writer.endWriteAddressIndex();
		writer.flush();
		streetstat.close();
		if (waynodesStat != null) {
			waynodesStat.close();
		}

	}
	
	
	private void putNamedMapObject(Map<String, List<MapObject>> namesIndex, MapObject o, long fileOffset){
		String name = o.getName();
		parsePrefix(name, o, namesIndex);
		if (fileOffset > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("File offset > 2 GB.");
		}
		o.setFileOffset((int) fileOffset);
	}
	
	private void parsePrefix(String name, MapObject data, Map<String, List<MapObject>> namesIndex) {
		int prev = -1;
		for (int i = 0; i <= name.length(); i++) {
			if (i == name.length() || (!Character.isLetter(name.charAt(i)) && !Character.isDigit(name.charAt(i)) && name.charAt(i) != '\'')) {
				if (prev != -1) {
					String substr = name.substring(prev, i);
					if (substr.length() > ADDRESS_NAME_CHARACTERS_TO_INDEX) {
						substr = substr.substring(0, ADDRESS_NAME_CHARACTERS_TO_INDEX);
					}
					String val = substr.toLowerCase();
					if(!namesIndex.containsKey(val)){
						namesIndex.put(val, new ArrayList<MapObject>());
					}
					namesIndex.get(val).add(data);
					prev = -1;
				}
			} else {
				if(prev == -1){
					prev = i;
				}
			}
		}
		
	}


	private void writeCityBlockIndex(BinaryMapIndexWriter writer, int type, PreparedStatement streetstat, PreparedStatement waynodesStat,
			List<City> suburbs, List<City> cities, Map<String, City> postcodes, Map<String, List<MapObject>> namesIndex, IProgress progress)			
			throws IOException, SQLException {
		List<BinaryFileReference> refs = new ArrayList<BinaryFileReference>();
		// 1. write cities
		writer.startCityBlockIndex(type);
		for (City c : cities) {
			refs.add(writer.writeCityHeader(c, c.getType().ordinal()));
		}
		for (int i = 0; i < cities.size(); i++) {
			City city = cities.get(i);
			BinaryFileReference ref = refs.get(i);
			putNamedMapObject(namesIndex, city, ref.getStartPointer());
			if (type == CITIES_TYPE) {
				progress.progress(1);
			} else {
				if ((cities.size() - i) % 100 == 0) {
					progress.progress(1);
				}
			}
			Map<Street, List<Node>> streetNodes = new LinkedHashMap<Street, List<Node>>();
			List<City> listSuburbs = null;
			if (suburbs != null) {
				for (City suburb : suburbs) {
					if (suburb.getIsInValue().toLowerCase().contains(city.getName().toLowerCase())) {
						if (listSuburbs == null) {
							listSuburbs = new ArrayList<City>();
						}
						listSuburbs.add(suburb);
					}
				}
			}
			long time = System.currentTimeMillis();
			List<Street> streets = readStreetsBuildings(streetstat, city, waynodesStat, streetNodes, listSuburbs);
			long f = System.currentTimeMillis() - time;
			writer.writeCityIndex(city, streets, streetNodes, ref);
			int bCount = 0;
			// register postcodes and name index
			for (Street s : streets) {
				putNamedMapObject(namesIndex, s, s.getFileOffset());
				
				for (Building b : s.getBuildings()) {
					bCount++;
					if (city.getPostcode() != null && b.getPostcode() == null) {
						b.setPostcode(city.getPostcode());
					}
					if (b.getPostcode() != null) {
						if (!postcodes.containsKey(b.getPostcode())) {
							City p = City.createPostcode(b.getPostcode());
							p.setLocation(b.getLocation().getLatitude(), b.getLocation().getLongitude());
							postcodes.put(b.getPostcode(), p);
						}
						City post = postcodes.get(b.getPostcode());
						Street newS = post.getStreet(s.getName());
						if(newS == null) {
							newS = new Street(post);
							newS.setName(s.getName());
							newS.setEnName(s.getEnName());
							newS.setLocation(s.getLocation().getLatitude(), s.getLocation().getLongitude());
							//newS.getWayNodes().addAll(s.getWayNodes());
							newS.setId(s.getId());
							post.registerStreet(newS);
						}
						newS.addBuildingCheckById(b);
					}
				}
			}
			if (f > 500) {
				if (logMapDataWarn != null) {
					logMapDataWarn.info("! " + city.getName() + " ! " + f + " ms " + streets.size() + " streets " + bCount + " buildings");
				} else {
					log.info("! " + city.getName() + " ! " + f + " ms " + streets.size() + " streets " + bCount + " buildings");
				}
			}
		}
		writer.endCityBlockIndex();
	}

	public void commitToPutAllCities() throws SQLException {
		// commit to put all cities
		streetDAO.commit();
	}

	public void createDatabaseStructure(Connection mapConnection, DBDialect dialect) throws SQLException {
		this.mapConnection = mapConnection;
		streetDAO.createDatabaseStructure(mapConnection, dialect);
		createAddressIndexStructure(mapConnection, dialect);
		addressCityStat = mapConnection.prepareStatement("insert into city (id, latitude, longitude, name, name_en, city_type) values (?, ?, ?, ?, ?, ?)");

		pStatements.put(addressCityStat, 0);
	}
	
	private void createAddressIndexStructure(Connection conn, DBDialect dialect) throws SQLException{
		Statement stat = conn.createStatement();
		
        stat.executeUpdate("create table city (id bigint primary key, latitude double, longitude double, " +
        			"name varchar(1024), name_en varchar(1024), city_type varchar(32))");
        stat.executeUpdate("create index city_ind on city (id, city_type)");
        
//        if(dialect == DBDialect.SQLITE){
//        	stat.execute("PRAGMA user_version = " + IndexConstants.ADDRESS_TABLE_VERSION); //$NON-NLS-1$
//        }
        stat.close();
	}
	
	private List<Street> readStreetsBuildings(PreparedStatement streetBuildingsStat, City city, PreparedStatement waynodesStat,
			Map<Street, List<Node>> streetNodes, List<City> citySuburbs) throws SQLException {
		TLongObjectHashMap<Street> visitedStreets = new TLongObjectHashMap<Street>();
		Map<String, List<Street>> uniqueNames = new LinkedHashMap<String, List<Street>>();

		// read streets for city
		readStreetsAndBuildingsForCity(streetBuildingsStat, city, waynodesStat, streetNodes, visitedStreets, uniqueNames);
		// read streets for suburbs of the city
		if (citySuburbs != null) {
			for (City suburb : citySuburbs) {
				readStreetsAndBuildingsForCity(streetBuildingsStat, suburb, waynodesStat, streetNodes, visitedStreets, uniqueNames);
			}
		}
		mergeStreetsWithSameNames(streetNodes, uniqueNames);
		return new ArrayList<Street>(streetNodes.keySet());
	}
	

	private void mergeStreetsWithSameNames(Map<Street, List<Node>> streetNodes, Map<String, List<Street>> uniqueNames) {
		for(String streetName : uniqueNames.keySet()) {
			List<Street> streets = uniqueNames.get(streetName);
			if(streets.size() > 1) {
				mergeStreets(streets, streetNodes);
			}
		}
	}


	private void mergeStreets(List<Street> streets, Map<Street, List<Node>> streetNodes) {
		for(int i = 0; i < streets.size() - 1 ; ) {
			Street s = streets.get(i);
			boolean merged = false;
			for (int j = i + 1; j < streets.size(); ) {
				Street candidate = streets.get(j);
				if(getDistance(s, candidate, streetNodes) <= 900) { 
					merged = true;
					//logMapDataWarn.info("City : " + s.getCity() + 
					//	" combine 2 district streets '" + s.getName() + "' with '" + candidate.getName() + "'");
					s.mergeWith(candidate);
					if(!candidate.getName().equals(s.getName())) {
						candidate.getCity().unregisterStreet(candidate.getName());
					}
					List<Node> old = streetNodes.remove(candidate);
					streetNodes.get(s).addAll(old);
					streets.remove(j);
				} else {
					j++;
				}
			}
			if(!merged) {
				i++;
			}
		}
	}

	private double getDistance(Street s, Street c, Map<Street, List<Node>> streetNodes) {
		List<Node> thisWayNodes = streetNodes.get(s);
		List<Node> oppositeStreetNodes = streetNodes.get(c);
		if(thisWayNodes.size() == 0) {
			thisWayNodes = Collections.singletonList(new Node(s.getLocation().getLatitude(), s.getLocation().getLongitude(), -1));
		}
		if(oppositeStreetNodes.size() == 0) {
			oppositeStreetNodes = Collections.singletonList(new Node(c.getLocation().getLatitude(), c.getLocation().getLongitude(), -1));
		}
		double md = Double.POSITIVE_INFINITY;
		for(Node n : thisWayNodes) {
			for(Node d : oppositeStreetNodes) {
				if(n != null && d != null) {
					md = Math.min(md, OsmMapUtils.getDistance(n, d));
				}
			}
		}
		return md;
	}


	private void readStreetsAndBuildingsForCity(PreparedStatement streetBuildingsStat, City city,
			PreparedStatement waynodesStat, Map<Street, List<Node>> streetNodes, TLongObjectHashMap<Street> visitedStreets,
			Map<String, List<Street>> uniqueNames) throws SQLException {
		streetBuildingsStat.setLong(1, city.getId());
		ResultSet set = streetBuildingsStat.executeQuery();
		while (set.next()) {
			long streetId = set.getLong(1);
			if (!visitedStreets.containsKey(streetId)) {
				String streetName = set.getString(2);
				String streetEnName = set.getString(3);
				double lat = set.getDouble(4);
				double lon = set.getDouble(5);
				// load the street nodes
				List<Node> thisWayNodes = loadStreetNodes(streetId, waynodesStat);
				if (!uniqueNames.containsKey(streetName)) {
					uniqueNames.put(streetName, new ArrayList<Street>());
				}
				Street street = new Street(city);
				uniqueNames.get(streetName).add(street);
				street.setLocation(lat, lon);
				street.setId(streetId);
				// If there are more streets with same name in different districts.
				// Add district name to all other names. If sorting is right, the first street was the one in the city
				String district = set.getString(12);
				String cityPart = district == null || district.equals(city.getName()) ? "" : " (" + district + ")";
				street.setName(streetName + cityPart);
				street.setEnName(streetEnName + cityPart);
				streetNodes.put(street, thisWayNodes);

				visitedStreets.put(streetId, street); // mark the street as visited
			}
			if (set.getObject(6) != null) {
				Street s = visitedStreets.get(streetId);
				Building b = new Building();
				b.setId(set.getLong(6));
				b.setName(set.getString(7));
				b.setEnName(set.getString(8));
				b.setLocation(set.getDouble(9), set.getDouble(10));
				b.setPostcode(set.getString(11));
				b.setName2(set.getString(13));
				// no en name2 for now
				b.setName2(set.getString(14));
				double lat2 = set.getDouble(15);
				double lon2 = set.getDouble(16);
				if (lat2 != 0 || lon2 != 0) {
					b.setLatLon2(new LatLon(lat2, lon2));
				}
				b.setInterpolationInterval(set.getInt(17));
				String type = set.getString(18);
				if (type != null) {
					b.setInterpolationType(BuildingInterpolation.valueOf(type));
				}

				s.addBuildingCheckById(b);
			}
		}

		set.close();
	}


	private List<Node> loadStreetNodes(long streetId, PreparedStatement waynodesStat) throws SQLException {
		List<Node> list = new ArrayList<Node>();
		waynodesStat.setLong(1, streetId);
		ResultSet rs = waynodesStat.executeQuery();
		while (rs.next()) {
			list.add(new Node(rs.getDouble(2), rs.getDouble(3), rs.getLong(1)));
		}
		rs.close();
		return list;
	}


	public Map<CityType, List<City>> readCities(Connection c) throws SQLException{
		Map<CityType, List<City>> cities = new LinkedHashMap<City.CityType, List<City>>();
		for(CityType t : CityType.values()) {
			cities.put(t, new ArrayList<City>());
		}
		
		Statement stat = c.createStatement();
		ResultSet set = stat.executeQuery("select id, latitude, longitude , name , name_en , city_type from city"); //$NON-NLS-1$
		while(set.next()){
			CityType type = CityType.valueFromString(set.getString(6));
			City city = new City(type);
			city.setName(set.getString(4));
			city.setEnName(set.getString(5));
			city.setLocation(set.getDouble(2), 
					set.getDouble(3));
			city.setId(set.getLong(1));
			cities.get(type).add(city);
			
			if (DEBUG_FULL_NAMES) { 
				Boundary cityB = cityBoundaries.get(city);
				if (cityB != null) {
					city.setName(city.getName() + " " + cityB.getAdminLevel() + ":" + cityB.getName());
				}
			}
		}
		set.close();
		stat.close();
		
		Comparator<City> comparator = new Comparator<City>() {
			@Override
			public int compare(City o1, City o2) {
				return Collator.getInstance().compare(o1.getName(), o2.getName());
			}
		};
		for(List<City> t : cities.values()) {
			Collections.sort(t, comparator);
		}
		return cities;
	}
}
