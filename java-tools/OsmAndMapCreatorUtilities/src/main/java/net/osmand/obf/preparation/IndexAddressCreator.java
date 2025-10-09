package net.osmand.obf.preparation;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Collator;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import net.osmand.IProgress;
import net.osmand.OsmAndCollator;
import net.osmand.binary.BinaryMapAddressReaderAdapter.CityBlocks;
import net.osmand.binary.CommonWords;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Boundary;
import net.osmand.data.Building;
import net.osmand.data.Building.BuildingInterpolation;
import net.osmand.data.City;
import net.osmand.data.City.CityType;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Multipolygon;
import net.osmand.data.MultipolygonBuilder;
import net.osmand.data.QuadRect;
import net.osmand.data.Street;
import net.osmand.obf.preparation.DBStreetDAO.SimpleStreet;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.EntityParser;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.ArabicNormalizer;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static net.osmand.obf.preparation.OsmDbCreator.SHIFT_ID;

public class IndexAddressCreator extends AbstractIndexPartCreator {

	private static final Log log = LogFactory.getLog(IndexAddressCreator.class);
	private final Log logMapDataWarn;

	private PreparedStatement addressCityStat;

	// MEMORY address : choose what to use ?
	private boolean loadInMemory = true;

	// MEMORY address : address structure
	// load it in memory
	private List<Long> debugCityIds = new ArrayList<>();
	private List<Relation> postalCodeRelations = new ArrayList<Relation>();
	private Map<Entity, Boundary> postcodeBoundaries = new HashMap<>();
	private TLongHashSet visitedBoundaryWays = new TLongHashSet();

	private boolean normalizeStreets;
	private String[] normalizeDefaultSuffixes;
	private String[] normalizeSuffixes;

	private boolean DEBUG_FULL_NAMES = false; //true to see attached cityPart and boundaries to the street names

	private TreeSet<String> langAttributes = new TreeSet<String>();
	public static final String ENTRANCE_BUILDING_DELIMITER = ", ";
    private static final int SHIFT_BOUNDARY_CENTER = 2;

	Connection mapConnection;
	DBStreetDAO streetDAO;
	private PreparedStatement postcodeSetStat;
	private IndexCreatorSettings settings;
	private CityDataStorage cityDataStorage;


	public IndexAddressCreator(Log logMapDataWarn, IndexCreatorSettings settings) {
		this.logMapDataWarn = logMapDataWarn;
		this.settings = settings;
		streetDAO = loadInMemory ? new CachedDBStreetDAO() : new DBStreetDAO();
		cityDataStorage = new CityDataStorage();
	}

	public CityDataStorage getCityDataStorage() {
		return cityDataStorage;
	}


	public void registerCityIfNeeded(Entity e) {
		if (e instanceof Node && e.getTag(OSMTagKey.PLACE) != null) {
			City city = EntityParser.parseCity((Node) e);
			if (city != null) {
				city.setNames(getOtherNames(e));
				regCity(city, e);
			}
		}
	}


	private void regCity(City city, Entity e) {
		LatLon l = city.getLocation();
		if (!Algorithms.isEmpty(city.getName()) && l != null) {
			cityDataStorage.registerObject(l.getLatitude(), l.getLongitude(), city, e);
			debugCityIds.add(city.getId());
		}
	}

	public void indexBoundariesRelation(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		Boundary boundary = extractBoundary(e, ctx);
		// Bucharest has admin level 4
		boolean boundaryValid = boundary != null && (!boundary.hasAdminLevel() || boundary.getAdminLevel() >= 4) &&
				boundary.getCenterPoint() != null && !Algorithms.isEmpty(boundary.getName());
		if (boundaryValid) {
			LatLon boundaryCenter = boundary.getCenterPoint();
			List<City> citiesToSearch = cityDataStorage.getClosestObjects(boundaryCenter.getLatitude(), boundaryCenter.getLongitude());

			City cityFound = null;
			String boundaryName = boundary.getName().toLowerCase();
			String altBoundaryName = Algorithms.isEmpty(boundary.getAltName()) ? "" : boundary.getAltName().toLowerCase();
			if (boundary.hasAdminCenterId()) {
				for (City c : citiesToSearch) {
					if (c.getId() == boundary.getAdminCenterId() >> SHIFT_BOUNDARY_CENTER) {
						boundary.setCityType(c.getType());
						cityFound = c;
						break;
					}
				}
			}
			if (cityFound == null) {
				for (City c : citiesToSearch) {
					if ((boundaryName.equalsIgnoreCase(c.getName()) || altBoundaryName.equalsIgnoreCase(c.getName()))
							&& boundary.containsPoint(c.getLocation())) {
						cityFound = c;
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
			if (cityFound == null /*&& !boundary.hasAdminLevel() */ &&
					(boundary.getCityType() == CityType.TOWN ||
							// boundary.getCityType() == CityType.CITY ||
							boundary.getCityType() == CityType.HAMLET ||
							boundary.getCityType() == CityType.SUBURB ||
							boundary.getCityType() == CityType.VILLAGE)) {
				if (e instanceof Relation) {
					ctx.loadEntityRelation((Relation) e);
				}
				cityFound = createMissingCity(e, boundary.getCityType());
				boundary.setAdminCenterId(getBoundaryCenter(cityFound.getId(), e));
			}
			System.out.println("Boundary " + boundaryName);
			if (cityFound != null) {
				putCityBoundary(boundary, cityFound);
			} else {
				logBoundaryChanged(boundary, null, 0, 0);
				cityDataStorage.addNotAssignedBoundary(boundary);
			}
			attachAllCitiesToBoundary(boundary);
		} else if (boundary != null) {
			if (logMapDataWarn != null) {
				logMapDataWarn.warn("Not using boundary: " + boundary + " " + boundary.getBoundaryId());
			} else {
				log.info("Not using boundary: " + boundary + " " + boundary.getBoundaryId());
			}
		}
	}


	private boolean nameContains(String boundaryName, String lower) {
		if (Algorithms.isEmpty(boundaryName)) {
			return false;
		}
		return boundaryName.startsWith(lower + " ") || boundaryName.endsWith(" " + lower)
				|| boundaryName.contains(" " + lower + " ");
	}


	private void attachAllCitiesToBoundary(Boundary boundary) {
		cityDataStorage.attachAllCitiesToBoundary(boundary);
	}

	public void tryToAssignBoundaryToFreeCities(IProgress progress) {
		progress.startWork(cityDataStorage.citiesSize());
		// Why to do this? This is completely incorrect to do with suburbs because it assigns boundary that is much bigger
		// than suburb and after that findCityPart works incorrectly
		//for cities without boundaries, try to find the right one
		int smallestAdminLevel = 7; //start at level 8 for now...
		for (City c : cityDataStorage.getAllCities()) {
			progress.progress(1);
			Boundary cityB = cityDataStorage.getBoundaryByCity(c);
			if (cityB == null && (c.getType() == CityType.CITY || c.getType() == CityType.TOWN)) {
				LatLon location = c.getLocation();
				Boundary smallestBoundary = null;
				// try to found boundary
				for (Boundary b : cityDataStorage.getNotAssignedBoundaries()) {
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
					cityDataStorage.removeNotAssignedBoundary(smallestBoundary);
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
		if (!Algorithms.isEmpty(b.getAltName()) && !nameEq) {
			nameEq = b.getAltName().equalsIgnoreCase(c.getName());
		}
		boolean cityBoundary = b.getCityType() != null;
		// max 10
		int adminLevelImportance = getAdminLevelImportance(b);
		// boundary centerId can be from NODE or WAY with shift, from RELATION - without
		if (nameEq) {
			if (cityBoundary) {
				return 0;
			} else if (c.getId() == b.getAdminCenterId() >> SHIFT_BOUNDARY_CENTER ||
					!b.hasAdminCenterId()) {
				return adminLevelImportance;
			}
			return 10 + adminLevelImportance;
		} else {
			if (c.getId() == b.getAdminCenterId() >> SHIFT_BOUNDARY_CENTER) {
				return 20 + adminLevelImportance;
			} else {
				return 30 + adminLevelImportance;
			}
		}
	}


	private int getAdminLevelImportance(Boundary b) {
		int adminLevelImportance = 5;
		if (b.hasAdminLevel()) {
			int adminLevel = b.getAdminLevel();
			if (adminLevel == 8) {
				adminLevelImportance = 1;
			} else if (adminLevel == 7) {
				adminLevelImportance = 2;
			} else if (adminLevel == 6) {
				adminLevelImportance = 3;
			} else if (adminLevel == 9) {
				adminLevelImportance = 4;
			} else if (adminLevel == 10) {
				adminLevelImportance = 5;
			} else {
				adminLevelImportance = 6;
			}
		}
		return adminLevelImportance;
	}

	private Boundary putCityBoundary(Boundary boundary, City cityFound) {
		final Boundary oldBoundary = cityDataStorage.getBoundaryByCity(cityFound);
		if (oldBoundary == null) {
			cityDataStorage.setCityBoundary(cityFound, boundary);
			logBoundaryChanged(boundary, cityFound,
					getCityBoundaryImportance(boundary, cityFound), 100);
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
				cityDataStorage.setCityBoundary(cityFound, boundary);
				logBoundaryChanged(boundary, cityFound, n, old);
			}
			return oldBoundary;
		}
	}


	private void logBoundaryChanged(Boundary boundary, City cityFound, int priority, int oldpriority) {
		String s = "City " + (cityFound == null ? " not found " : " : " + cityFound.getName());
		s += " boundary: " + boundary.toString() + " " + boundary.getBoundaryId() +
				" priority:" + priority + " oldpriority:" + oldpriority;
		if (logMapDataWarn != null) {
			logMapDataWarn.info(s);
		} else {
			log.info(s);
		}
	}

	private Boundary extractBoundary(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		if (e instanceof Node) {
			return null;
		}
		long centerId = 0;
		CityType ct = CityType.valueFromString(e.getTag(OSMTagKey.PLACE));
		// if a place that has addr_place is a neighbourhood mark it as a suburb (made for the suburbs of Venice)
		boolean isNeighbourhood = e.getTag(OSMTagKey.ADDR_PLACE) != null && "neighbourhood".equals(e.getTag(OSMTagKey.PLACE));
		if ((ct == null && "townland".equals(e.getTag(OSMTagKey.LOCALITY))) || isNeighbourhood) {
			if (e instanceof Relation) {
				ctx.loadEntityRelation((Relation) e);
			}
			final City city = createMissingCity(e, CityType.SUBURB);
			if (city != null) {
				centerId = getBoundaryCenter(city.getId(), e);
				ct = CityType.SUBURB;
			}
		}
		boolean administrative = "administrative".equals(e.getTag(OSMTagKey.BOUNDARY));
		boolean census = "census".equals(e.getTag(OSMTagKey.BOUNDARY));
		boolean postalCode = "postal_code".equals(e.getTag(OSMTagKey.BOUNDARY));
		if (administrative || census || postalCode || ct != null) {
			if (e instanceof Way && visitedBoundaryWays.contains(e.getId())) {
				return null;
			}

			String bname = e.getTag(OSMTagKey.NAME);
			MultipolygonBuilder m = new MultipolygonBuilder();
			if (e instanceof Relation) {
				Relation aRelation = (Relation) e;
				ctx.loadEntityRelation(aRelation);
				for (RelationMember es : aRelation.getMembers()) {
					if (es.getEntity() instanceof Way) {
						boolean inner = "inner".equals(es.getRole()); //$NON-NLS-1$
						if (inner) {
							m.addInnerWay((Way) es.getEntity());
						} else {
							String wName = es.getEntity().getTag(OSMTagKey.NAME);
							// if name are not equal keep the way for further check (it could be different suburb)
							if (Algorithms.objectEquals(wName, bname) || wName == null) {
								visitedBoundaryWays.add(es.getEntity().getId());
							}
							m.addOuterWay((Way) es.getEntity());
						}
					} else if (es.getEntity() instanceof Node &&
							("admin_centre".equals(es.getRole()) || "admin_center".equals(es.getRole()))) {
						centerId = getBoundaryCenter(es.getEntity().getId(), es.getEntity());
					} else if (es.getEntity() instanceof Node && ("label".equals(es.getRole()) && centerId == 0)) {
						centerId = getBoundaryCenter(es.getEntity().getId(), es.getEntity());
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
			if (centerId != 0) {
				boundary.setAdminCenterId(centerId);
			}
			return boundary;
		} else {
			return null;
		}
	}

	private long getBoundaryCenter(long centerId, Entity e) {
	    if (e instanceof Node) {
	        long id = centerId << SHIFT_ID;
	        return (id << SHIFT_BOUNDARY_CENTER) + 0;
        }
	    if (e instanceof Way) {
            long id = centerId << SHIFT_ID;
            return (id << SHIFT_BOUNDARY_CENTER) + 1;
        }
	    //relation
	    return (centerId << SHIFT_BOUNDARY_CENTER) + 2;
    }


	private City createMissingCity(Entity e, CityType t) throws SQLException {
		City c = EntityParser.parseCity(e, t);
		c.setNames(getOtherNames(e));
		if (c.getLocation() == null) {
			return null;
		}
		if (debugCityIds.contains(c.getId())) {
			log.error("SQL ERROR!!! City ID already exists in \"city\" table \n" +
					"insert into city (id, latitude, longitude, name, name_en, city_type) values (" +
					+c.getId() +
					", " + c.getLocation().getLatitude() +
					"," + c.getLocation().getLongitude() +
					", " + c.getName() +
					", " + Algorithms.encodeMap(c.getNamesMap(true)) +
					", " + CityType.valueToString(c.getType()) + ")");
		}
		regCity(c, e);
		writeCity(c);
		commitWriteCity();
		return c;
	}

	public void indexAddressRelation(Relation i, OsmDbAccessorContext ctx, IndexCreationContext icc) throws SQLException {
		if ("street".equals(i.getTag(OSMTagKey.TYPE)) || "associatedStreet".equals(i.getTag(OSMTagKey.TYPE))) { //$NON-NLS-1$

			LatLon l = null;
			String streetName = null;
			Set<String> isInNames = null;
			ctx.loadEntityRelation(i);

			streetName = i.getTag(OSMTagKey.NAME);
			Iterator<Entity> it = i.getMemberEntities(null).iterator();
			while(l == null && it.hasNext()) {
				l = it.next().getLatLon(); // get coordinates from any relation member
			}
			isInNames = i.getIsInNames();
			String postcode = i.getTag(OSMTagKey.ADDR_POSTCODE);
			if (streetName == null) { // use relation name as a street name
				Collection<Entity> members = i.getMemberEntities("street");
				for (Entity street : members) { // find the first street member with name and use it as a street name
					String name = street.getTag(OSMTagKey.NAME);
					if (name != null) {
						streetName = name;
						l = street.getLatLon();
						isInNames = street.getIsInNames();
						break;
					}
				}
			}


			if (streetName != null) {
				Set<Long> idsOfStreet = getStreetInCity(isInNames, streetName, null, l, icc);
				if (!idsOfStreet.isEmpty()) {
					Collection<Entity> houses = i.getMemberEntities("house"); // both house and address roles can have address
					houses.addAll(i.getMemberEntities("address"));
					for (Entity house : houses) {
						String hname = null;
						String second = null;

						if (settings.houseNumberPreferredOverName) {
							hname = house.getTag(OSMTagKey.ADDR_HOUSE_NUMBER);
							second = house.getTag(OSMTagKey.ADDR_HOUSE_NAME);
						} else {
							hname = house.getTag(OSMTagKey.ADDR_HOUSE_NAME);
							second = house.getTag(OSMTagKey.ADDR_HOUSE_NUMBER);
						}
						if (hname == null) {
							hname = second;
							second = null;
						}
						if (hname == null)
							continue;
						if (settings.houseNameAddAdditionalInfo && second != null) {
							hname += " - [" + second + "]";
						}

						if (!streetDAO.findBuilding(house)) {
							// process multipolygon (relation) houses - preload members to create building with correct latlon
							if (house instanceof Relation)
								ctx.loadEntityRelation((Relation) house);
							Building building = EntityParser.parseBuilding(house);
							if (building.getLocation() == null) {
								log.warn("building with empty location! id: " + house.getId());
							} else {
								building.setName(hname);
								if(Algorithms.isEmpty(building.getPostcode())) {
									building.setPostcode(postcode);
								}
								streetDAO.writeBuilding(idsOfStreet, building);
							}
						}
					}
				}
			}
		}
	}


	public String normalizeStreetName(String name, LatLon location, IndexCreationContext icc) {
		if (name == null) {
			return null;
		}
		name = name.trim();
		name = name.replace("’", "'");
		name = icc.decryptAbbreviations(name, location, settings.addRegionTag);

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
		if (name.length() > ind + suffixLength) {
			newName += name.substring(ind + suffixLength);
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

	public Set<Long> getStreetInCity(Set<String> isInNames, String name, Map<String, String> names, final LatLon location, IndexCreationContext icc) throws SQLException {
		if (location == null) {
			return Collections.emptySet();

		}
		name = normalizeStreetName(name, location, icc);
		Set<City> result = new LinkedHashSet<City>();
		List<City> nearestObjects = cityDataStorage.getClosestObjects(location.getLatitude(), location.getLongitude());
		//either we found a city boundary the street is in
		for (City c : nearestObjects) {
			if (!c.getType().storedAsSeparateAdminEntity()) {
				// ignore districts, boroughs, neighbourhood, so we don't get streets missing in
				// the final obf
				continue;
			}

			Boundary boundary = cityDataStorage.getBoundaryByCity(c);
			if (isInNames.contains(c.getName()) || (boundary != null && boundary.containsPoint(location))) {
				// revert due to massive problems with suburb, village and other inside city objects
				//if (boundary != null && boundary.getCityType() != null && boundary.getCityType().storedAsSeparateAdminEntity()) {
				result.add(c);
				// }
			}
		}
        if (settings.indexByProximity) {
    		// or we need to find closest city
	    	Collections.sort(nearestObjects, new Comparator<City>() {
		    	@Override
		    	public int compare(City c1, City c2) {
			    	double r1 = relativeDistance(location, c1);
				    double r2 = relativeDistance(location, c2);
			    	return Double.compare(r1, r2);
			    }
		    });
    		for (City c : nearestObjects) {
	    		if (!c.getType().storedAsSeparateAdminEntity()) {
		    		// ignore districts, boroughs, neighbourhood, so we don't get streets missing in
			    	// the final obf
				    continue;
    			}

    			if (relativeDistance(location, c) > 0.2) {
	    			if (result.isEmpty()) {
	    				result.add(c);
		    		}
			    	break;
    			} else if (!result.contains(c)) {
	    			// city doesn't have boundary or there is a mistake in boundaries and we found nothing before
	    			if (!cityDataStorage.isCityHasBoundary(c) || result.isEmpty()) {
	    				result.add(c);
	    			}
		    	}
	    	}
        }
		return registerStreetInCities(name, names, location, result);
	}


	private Set<Long> registerStreetInCities(String name, Map<String, String> names, LatLon location, Collection<City> result) throws SQLException {
		if (result.isEmpty()) {
			return Collections.emptySet();
		}
		Set<Long> values = new TreeSet<Long>();
		for (City city : result) {
			String nameInCity = name;
			if (nameInCity == null) {
				nameInCity = "<" + city.getName() + ">";
				names = new HashMap<String, String>();
				Iterator<Entry<String, String>> it = city.getNamesMap(true).entrySet().iterator();
				while (it.hasNext()) {
					Entry<String, String> e = it.next();
					names.put(e.getKey(), "<" + e.getValue() + ">");

				}
			}
			long streetId = getOrRegisterStreetIdForCity(nameInCity, names, location, city);
			values.add(streetId);
		}
		return values;
	}


	private long getOrRegisterStreetIdForCity(String name, Map<String, String> names, LatLon location, City city) throws SQLException {
		String cityPart;
        if (settings.indexByProximity) {
            cityPart = findCityPart(location, city);
        } else {
            cityPart = city.getName();
        }
		SimpleStreet foundStreet = streetDAO.findStreet(name, city, cityPart);
		if (foundStreet == null) {
			// by default write city with cityPart of the city
			if (cityPart == null) {
				cityPart = city.getName();
			}
			if (names != null) {
				langAttributes.addAll(names.keySet());
			}
			return streetDAO.insertStreet(name, names, location, city, cityPart);
		} else {
			if (names != null) {
				Map<String, String> addNames = null;
				for (String s : names.keySet()) {
					if (!foundStreet.getLangs().contains(s + ";")) {
						if (addNames == null) {
							addNames = new HashMap<String, String>();
						}
						addNames.put(s, names.get(s));
					}
				}
				if (addNames != null) {
					foundStreet = streetDAO.updateStreetLangs(foundStreet, addNames);
				}
			}
			return foundStreet.getId();
		}
	}

	private String findCityPart(LatLon location, City city) {
		String cityPart = city.getName();
		boolean found = false;
		Boundary cityBoundary = cityDataStorage.getBoundaryByCity(city);
		if (cityBoundary != null) {
			List<City> subcities = cityDataStorage.getCityListByBoundary(cityBoundary);
			if (subcities != null) {
				for (City subpart : subcities) {
					if (subpart != city) {
						Boundary subBoundary = cityDataStorage.getBoundaryByCity(subpart);
						if (cityBoundary != null && subBoundary != null && subBoundary.getAdminLevel() > cityBoundary.getAdminLevel()) {
							// old code
							cityPart = findNearestCityOrSuburb(subBoundary, location); // subpart.getName();
							// ?FIXME
							if (subBoundary.containsPoint(location)) {
								cityPart = subpart.getName();
								found = true;
								break;
							}
						}
					}
				}
			}
		}
		if (!found) {
			Boundary b = cityDataStorage.getBoundaryByCity(city);
			cityPart = findNearestCityOrSuburb(b, location);
		}
		return cityPart;
	}

	private String findNearestCityOrSuburb(Boundary greatestBoundary, LatLon location) {
		String result = null;
		double dist = Double.MAX_VALUE;
		List<City> list = new ArrayList<City>();
		if (greatestBoundary != null) {
			result = greatestBoundary.getName();
			list = cityDataStorage.getCityListByBoundary(greatestBoundary);
		} else {
			list = cityDataStorage.getClosestObjects(location.getLatitude(), location.getLongitude());
		}
		if (list != null) {
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

	public void iterateMainEntity(Entity e, OsmDbAccessorContext ctx, IndexCreationContext icc) throws SQLException {
		// index not only buildings but also nodes that belongs to addr:interpolation ways
		// currently not supported because nodes are indexed first with buildings
		String interpolation = e.getTag(OSMTagKey.ADDR_INTERPOLATION);
		String interpolationByHouseNumber = null;
		if (interpolation == null) {
			interpolationByHouseNumber = getInterpolationByHouseNumber(e);
		}
		if (e instanceof Way && (interpolation != null || interpolationByHouseNumber != null)) {
			BuildingInterpolation type = null;
			int interpolationInterval = 0;
			try {
				type = BuildingInterpolation.valueOf(interpolation.toUpperCase());
			} catch (RuntimeException ex) {
				try {
					interpolationInterval = Integer.parseInt(interpolation);
				} catch (NumberFormatException ex2) {
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
						Set<Long> idsOfStreet = getStreetInCity(first.getIsInNames(), strt, null, l, icc);
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
		String houseNumber = normalizeHousenumber(e.getTag(OSMTagKey.ADDR_HOUSE_NUMBER));

		String street = null;
		if (houseNumber != null || houseName != null) {
			street = e.getTag(OSMTagKey.ADDR_STREET);
			if (street == null) {
				street = e.getTag(OSMTagKey.ADDR_PLACE);
			}
		}
		String street2 = e.getTag(OSMTagKey.ADDR_STREET2);
		if (Algorithms.isEmpty(street2)) {
			street2 = e.getTag(OSMTagKey.ADDR2_STREET);
		}
		if ((houseName != null || houseNumber != null)) {
			if (e instanceof Relation) {
				ctx.loadEntityRelation((Relation) e);
				Collection<Entity> outs = ((Relation) e).getMemberEntities("outer");
				if (!outs.isEmpty()) {
					e = outs.iterator().next();
				}
			}
			// skip relations
			boolean exist = e instanceof Relation || streetDAO.findBuilding(e);
			if (!exist) {
				LatLon l = e.getLatLon();
				Set<Long> idsOfStreet = getStreetInCity(e.getIsInNames(), street, null, l, icc);
				if (!idsOfStreet.isEmpty()) {
					Building building = EntityParser.parseBuilding(e);
					String hname = null;
					String second = null;

					if (settings.houseNumberPreferredOverName) {
						hname = houseNumber;
						second = houseName;
					} else {
						hname = houseName;
						second = houseNumber;
					}
					if (hname == null) {
						hname = second;
						second = null;
					}
					String additionalHname = "";
					if (settings.houseNameAddAdditionalInfo && second != null) {
						additionalHname = " - [" + second + "]";
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
					} else {
						building.setName(hname + additionalHname);
					}
					if (!Algorithms.isEmpty(street2)) {
						String secondHno= e.getTag(OSMTagKey.ADDR2_HOUSE_NUMBER);
						String firstNo = building.getName();
						int secondNumberInd = hname.indexOf('/');
						if (secondNumberInd != -1 && secondNumberInd < hname.length() - 1 &&
								Algorithms.isEmpty(secondHno)) {
							firstNo = hname.substring(0, secondNumberInd) + additionalHname;
							secondHno = hname.substring(secondNumberInd + 1);
						}
						if (secondHno != null) {
							Building building2 = EntityParser.parseBuilding(e);
							building2.setName(hname.substring(secondNumberInd + 1) + additionalHname);
							Set<Long> ids2OfStreet = getStreetInCity(e.getIsInNames(), street2, null, l, icc);
							ids2OfStreet.removeAll(idsOfStreet); // remove duplicated entries!
							if (!ids2OfStreet.isEmpty()) {
								streetDAO.writeBuilding(ids2OfStreet, building2);
								building.setName(firstNo);
							}
						}
					}
					streetDAO.writeBuilding(idsOfStreet, building);
				}
			}
		} else if (e instanceof Way /* && OSMSettings.wayForCar(e.getTag(OSMTagKey.HIGHWAY)) */
				&& e.getTag(OSMTagKey.HIGHWAY) != null && e.getTag(OSMTagKey.NAME) != null &&
				isStreetTag(e.getTag(OSMTagKey.HIGHWAY)) && !"yes".equals(e.getTag(OSMTagKey.AREA))) {
			// suppose that streets with names are ways for car
			// Ignore all ways that have house numbers and highway type

			// if we saved address ways we could checked that we registered before
			boolean exist = streetDAO.findStreetNode(e);


			// check that street way is not registered already
			if (!exist) {
				LatLon l = e.getLatLon();
				Set<Long> idsOfStreet = getStreetInCity(e.getIsInNames(), e.getTag(OSMTagKey.NAME), getOtherNames(e), l, icc);
				if (!idsOfStreet.isEmpty()) {
					streetDAO.writeStreetWayNodes(idsOfStreet, (Way) e);
				}
			}
		}
		if (e.getTag(OSMTagKey.POSTAL_CODE) != null) {
			if ("postal_code".equals(e.getTag(OSMTagKey.BOUNDARY))) {
				Boundary boundary = extractBoundary(e, ctx);
				if (boundary != null) {
					postcodeBoundaries.put(e, boundary);
				}
			} else if (e instanceof Relation) {
				ctx.loadEntityRelation((Relation) e);
				postalCodeRelations.add((Relation) e);
			}
		}
	}

	private String getInterpolationByHouseNumber(Entity e) {
		String number = e.getTag(OSMTagKey.ADDR_HOUSE_NUMBER);
		if (number != null) {
			Matcher houseNumberWithDash = Pattern.compile("(\\d+)-(\\d+)").matcher(number);
			if (houseNumberWithDash.matches()) {
				long firstNumber = Long.parseLong(houseNumberWithDash.group(1));
				long secondNumber = Long.parseLong(houseNumberWithDash.group(2));
				if (firstNumber % 2 == 0 && secondNumber % 2 == 0) {
					return "even";
				}
				if (firstNumber % 2 != 0 && secondNumber % 2 != 0) {
					return "odd";
				}
				return "all";
			}
		}
		return null;
	}

	private String normalizeHousenumber(String hno) {
		if(hno != null) {
			if(hno.toLowerCase().endsWith("bis")) {
				hno = hno.substring(0, hno.length() - "bis".length()).trim() + " bis";
			} else if(hno.toLowerCase().endsWith("quater")) {
				hno = hno.substring(0, hno.length() - "quater".length()).trim() + " quater";
			} else if(hno.toLowerCase().endsWith("ter")) {
				hno = hno.substring(0, hno.length() - "ter".length()).trim() + " ter";
			}
		}
		return hno;
	}


	public void cleanCityPart() throws SQLException {
		streetDAO.cleanCityPart();
	}

	private Map<String, String> getOtherNames(Entity e) {
		Map<String, String> m = null;
        List<String> languages = Arrays.asList(MapRenderingTypes.langs);
		for (String t : e.getTagKeySet()) {
			String prefix =  null;
			if(t.startsWith("name:")) {
                String lang = t.substring(5);
                if (languages.contains(lang)) {
                    prefix = "name:";
                }
			} else if(t.startsWith("old_name")){
				prefix = "";
			} else if(t.startsWith("alt_name")){
				prefix = "";
			} else if(t.startsWith("loc_name")){
				prefix = "";
			}
			if (prefix != null) {
				if (m == null) {
					m = new HashMap<String, String>();
				}
				m.put(t.substring(prefix.length()), e.getTag(t));
			}
		}
		return m;
	}

	private boolean isStreetTag(String highwayValue) {
		return !"platform".equals(highwayValue);
	}


	private void writeCity(City city) throws SQLException {
		addressCityStat.setLong(1, city.getId());
		addressCityStat.setDouble(2, city.getLocation().getLatitude());
		addressCityStat.setDouble(3, city.getLocation().getLongitude());
		addressCityStat.setString(4, city.getName());
		addressCityStat.setString(5, Algorithms.encodeMap(city.getNamesMap(true)));
		addressCityStat.setString(6, CityType.valueToString(city.getType()));
		addBatch(addressCityStat);
		langAttributes.addAll(city.getNamesMap(false).keySet());
	}


	public void writeCitiesIntoDb() throws SQLException {
		for (City c : cityDataStorage.getAllCities()) {
			if (c.getType().storedAsSeparateAdminEntity()) {
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

	private void setPostcodeForBuilding(String postcode, long buildingId) throws SQLException {
		postcodeSetStat.setString(1, postcode);
		postcodeSetStat.setLong(2, buildingId);
		addBatch(postcodeSetStat);
	}


	private void processPostcodeRelations() throws SQLException {
		for (Relation r : postalCodeRelations) {
			for (RelationMember l : r.getMembers()) {
				if(l.getEntityId() != null) {
					setPostcodeForBuilding(r.getTag(OSMTagKey.POSTAL_CODE), l.getEntityId().getId());
				}
			}
		}
	}

	public void processPostcodes() throws SQLException {
		streetDAO.commit();
		pStatements.put(postcodeSetStat, 0);
		processPostcodeRelations();
		if (pStatements.get(postcodeSetStat) > 0) {
			postcodeSetStat.executeBatch();
		}
		pStatements.remove(postcodeSetStat);
	}


//	private static final int CITIES_TYPE = 1;
//	private static final int POSTCODES_TYPE = 2;
//	private static final int VILLAGES_TYPE = 3;

	public void writeBinaryAddressIndex(BinaryMapIndexWriter writer, String regionName, IProgress progress) throws IOException, SQLException {
		processPostcodes();
		cleanCityPart();
		streetDAO.close();
		closePreparedStatements(addressCityStat);
		mapConnection.commit();
		createDatabaseIndexes(mapConnection);
		mapConnection.commit();

		Map<String, City> postcodes = new TreeMap<String, City>();
		updatePostcodeBoundaries(progress, postcodes);
		mapConnection.commit();

		List<String> additionalTags = new ArrayList<String>();
		Map<String, Integer> tagRules = new HashMap<String, Integer>();
		int ind = 0;
		for (String lng : langAttributes) {
			additionalTags.add("name:" + lng);
			tagRules.put("name:" + lng, ind);
			ind++;
		}
		writer.startWriteAddressIndex(regionName, additionalTags);
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
		for (CityType t : cities.keySet()) {
			if (t == CityType.CITY || t == CityType.TOWN) {
				cityTowns.addAll(cities.get(t));
			} else {
				villages.addAll(cities.get(t));
			}
			if (t == CityType.SUBURB) {
				for (City c : cities.get(t)) {
					if (c.getIsInValue() != null) {
						suburbs.add(c);
					}
				}
			}
		}



		Map<String, List<MapObject>> namesIndex = new TreeMap<String, List<MapObject>>(Collator.getInstance());

		progress.startTask(settings.getString("IndexCreator.SERIALIZING_ADDRESS"), cityTowns.size() + villages.size() / 100 + 1); //$NON-NLS-1$

		writeCityBlockIndex(writer, CityBlocks.CITY_TOWN_TYPE.index, streetstat, waynodesStat, suburbs, cityTowns,
				postcodes, namesIndex, tagRules, progress);
		writeCityBlockIndex(writer, CityBlocks.VILLAGES_TYPE.index, streetstat, waynodesStat, null, villages, postcodes,
				namesIndex, tagRules, progress);

		// write postcodes
		List<BinaryFileReference> refs = new ArrayList<BinaryFileReference>();
		writer.startCityBlockIndex(CityBlocks.POSTCODES_TYPE.index);
		ArrayList<City> posts = new ArrayList<City>(postcodes.values());
		for (City s : posts) {
			// TODO Enable in 5.3 (5.2 version will support postcode ordinal)
//			refs.add(writer.writeCityHeader(s, CityType.POSTCODE.ordinal(), tagRules));
			refs.add(writer.writeCityHeader(s, -1, tagRules));
		}
		for (int i = 0; i < posts.size(); i++) {
			City postCode = posts.get(i);
			BinaryFileReference ref = refs.get(i);
			putNamedMapObject(namesIndex, postCode, ref.getStartPointer(), settings);
			ArrayList<Street> streets = new ArrayList<Street>(postCode.getStreets());
			Collections.sort(streets, new Comparator<Street>() {
				final net.osmand.Collator clt = OsmAndCollator.primaryCollator();

				@Override
				public int compare(Street o1, Street o2) {
					return clt.compare(o1.getName(), o2.getName());
				}

			});
			writer.writeCityIndex(postCode, streets, null, ref, tagRules);
		}
		writer.endCityBlockIndex();
		
		// write unassigned boundaries
		writer.startCityBlockIndex(CityBlocks.BOUNDARY_TYPE.index);
		refs = new ArrayList<BinaryFileReference>();
		List<Boundary> notAssignedBoundaries = this.cityDataStorage.getNotAssignedBoundaries();
		List<City> boundariesAsCities = new ArrayList<>();
		for (Boundary b : notAssignedBoundaries) {
			// TODO add bbox
			City c = new City(CityType.BOUNDARY);
			c.setId(ObfConstants.createMapObjectIdFromOsmId(b.getBoundaryId(), EntityType.RELATION));
			c.setLocation(b.getCenterPoint());
			c.setName(b.getName());
			if (!Algorithms.isEmpty(b.getAltName())) {
				c.setEnName(b.getAltName());
			}
			boundariesAsCities.add(c);
		}
		for (City c : boundariesAsCities) {
			refs.add(writer.writeCityHeader(c, c.getType().ordinal(), tagRules));
		}
		for (int i = 0; i < notAssignedBoundaries.size(); i++) {
			City b = boundariesAsCities.get(i);
			BinaryFileReference ref = refs.get(i);
			putNamedMapObject(namesIndex, b, ref.getStartPointer(), settings);
			writer.writeCityIndex(b, Collections.emptyList(), null, ref, tagRules);
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

	private void updatePostcodeBoundaries(IProgress progress, Map<String, City> postcodes) throws SQLException {
		progress.startTask("Process postcode boundaries", postcodeBoundaries.size());
		Iterator<Entry<Entity, Boundary>> it = postcodeBoundaries.entrySet().iterator();
		PreparedStatement ps =
				mapConnection.prepareStatement("SELECT postcode, latitude, longitude, id"
						+ " FROM building where latitude <= ? and latitude >= ? and longitude >= ? and longitude <= ? ");
		TLongObjectHashMap<String> assignPostcodes = new TLongObjectHashMap<>();
		while(it.hasNext()) {
			Entry<Entity, Boundary> e = it.next();
			String postcode = e.getKey().getTag(OSMTagKey.POSTAL_CODE);
			Multipolygon mp = e.getValue().getMultipolygon();
			QuadRect bbox = mp.getLatLonBbox();
			if (bbox.width() > 0) {
				ps.setDouble(1, bbox.top);
				ps.setDouble(2, bbox.bottom);
				ps.setDouble(3, bbox.left);
				ps.setDouble(4, bbox.right);
				ResultSet rs = ps.executeQuery();
				while (rs.next()) {
					String pst = rs.getString(1);
					if (Algorithms.isEmpty(pst)) {
						if (mp.containsPoint(rs.getDouble(2), rs.getDouble(3))) {
							assignPostcodes.put(rs.getLong(4), postcode);
						}
					}
				}
			}
			progress.progress(1);
		}
		ps.close();
		ps = mapConnection.prepareStatement("UPDATE "
				+ " building set postcode = ? where id = ? ");
		TLongObjectIterator<String> its = assignPostcodes.iterator();
		int cnt = 0;
		while(its.hasNext()) {
			its.advance();
			ps.setString(1, its.value());
			ps.setLong(2, its.key());
			ps.addBatch();
			if(cnt > BATCH_SIZE) {
				ps.executeBatch();
				cnt = 0;
			}
		}
		ps.executeBatch();
		ps.close();
	}


	public static void putNamedMapObject(Map<String, List<MapObject>> namesIndex, MapObject o, long fileOffset,
			IndexCreatorSettings settings) {
		String name = o.getName();
		parsePrefix(name, o, namesIndex, settings);
		for (String nm : o.getOtherNames()) {
			if (!nm.equals(name)) {
				parsePrefix(nm, o, namesIndex, settings);
			}
		}
		if (fileOffset > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("File offset > 2 GB.");
		}
		o.setFileOffset((int) fileOffset);
	}

	private static String stripBraces(String localeName) {
		int i = localeName.indexOf('(');
		String retName = localeName;
		if (i > -1) {
			retName = localeName.substring(0, i);
			int j = localeName.indexOf(')', i);
			if (j > -1) {
				retName = retName.trim() + ' ' + localeName.substring(j);
			}
		}
		return retName;
	}

    private static void parsePrefix(String name, MapObject data, Map<String, List<MapObject>> namesIndex,
                                              IndexCreatorSettings settings) {
        name = Algorithms.normalizeSearchText(name);
        name = stripBraces(name);
		Set<String> splitNames = splitNames(name);
        if (ArabicNormalizer.isSpecialArabic(name)) {
            String arabic = ArabicNormalizer.normalize(name);
            if (arabic != null && !arabic.equals(name)) {
                splitNames.addAll(Algorithms.splitByWordsLowercase(arabic));
            }
        }
        List<String> namesToAdd = new ArrayList<>(splitNames);
		// remove common words
		int pos = 0;
		while(namesToAdd.size() > 1 && pos != -1) {
			int prioP = Integer.MAX_VALUE;
			pos = -1;
			for(int k = 0; k < namesToAdd.size(); k++) {
				int prio = CommonWords.getCommon(namesToAdd.get(k));
				if(prio != -1 && prio < prioP) {
					pos = k;
					prioP = prio;
				}
			}
			if(pos != -1) {
				namesToAdd.remove(pos);
			}
		}

		// add to the map
		for(String substr : namesToAdd) {
			if (substr.length() > settings.charsToBuildAddressNameIndex) {
				substr = substr.substring(0, settings.charsToBuildAddressNameIndex);
			}
			String val = substr.toLowerCase();
			List<MapObject> list = namesIndex.get(val);
			if (list == null) {
				list = new ArrayList<MapObject>();
				namesIndex.put(val, list);
			}
			if (!list.contains(data)) {
				list.add(data);
			}
		}

	}

    private static Set<String> splitNames(String name) {
        int prev = -1;
        Set<String> namesToAdd = new HashSet<>();

        for (int i = 0; i <= name.length(); i++) {
            boolean isHyphenNearNumber = i != name.length() && name.charAt(i) == '-'
                    && ((i + 1 < name.length() && Character.isDigit(name.charAt(i + 1)))
                    || (i - 1 >= 0 && Character.isDigit(name.charAt(i - 1))));
            if (i == name.length() || (!Character.isLetter(name.charAt(i)) && !Character.isDigit(name.charAt(i)) &&
                    name.charAt(i) != '\'' && !isHyphenNearNumber)) {
                if (prev != -1) {
                    String substr = name.substring(prev, i);
                    namesToAdd.add(substr.toLowerCase());
                    prev = -1;
                }
            } else {
                if (prev == -1) {
                    prev = i;
                }
            }
        }
        return namesToAdd;
    }


	private void writeCityBlockIndex(BinaryMapIndexWriter writer, int type, PreparedStatement streetstat, PreparedStatement waynodesStat,
			List<City> suburbs, List<City> cities, Map<String, City> postcodes, Map<String, List<MapObject>> namesIndex,
			Map<String, Integer> tagRules, IProgress progress)
			throws IOException, SQLException {
		List<BinaryFileReference> refs = new ArrayList<BinaryFileReference>();
		// 1. write cities
		writer.startCityBlockIndex(type);
		for (City c : cities) {
			refs.add(writer.writeCityHeader(c, c.getType().ordinal(), tagRules));
		}
		for (int i = 0; i < cities.size(); i++) {
			City city = cities.get(i);
			BinaryFileReference ref = refs.get(i);
			putNamedMapObject(namesIndex, city, ref.getStartPointer(), settings);
			if (type == CityBlocks.CITY_TOWN_TYPE.index) {
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
			writer.writeCityIndex(city, streets, streetNodes, ref, tagRules);

			int bCount = 0;
			// register postcodes and name index
			for (Street s : streets) {
				putNamedMapObject(namesIndex, s, s.getFileOffset(), settings);

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
						Street newS = post.getStreetByName(s.getName());
						if (newS == null) {
							newS = new Street(post);
							newS.copyNames(s);
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

	public void createDatabaseIndexes(Connection mapConnection) throws SQLException {
		Statement stat = mapConnection.createStatement();
		stat.executeUpdate("create index city_ind on city (id, city_type)");
		stat.close();
		streetDAO.createIndexes(mapConnection);
	}

	public void createDatabaseStructure(Connection mapConnection, DBDialect dialect) throws SQLException {
		this.mapConnection = mapConnection;
		streetDAO.createDatabaseStructure(mapConnection, dialect);
		Statement stat = mapConnection.createStatement();
        stat.executeUpdate("create table city (id bigint primary key, latitude double, longitude double, " +
        			"name varchar(1024), name_en varchar(1024), city_type varchar(32))");
        stat.close();
		addressCityStat = mapConnection.prepareStatement("insert into city (id, latitude, longitude, name, name_en, city_type) values (?, ?, ?, ?, ?, ?)");
		postcodeSetStat = mapConnection.prepareStatement("UPDATE building SET postcode = ? WHERE id = ?");

		pStatements.put(addressCityStat, 0);
	}

	private List<Street> readStreetsBuildings(PreparedStatement streetBuildingsStat, City city, PreparedStatement waynodesStat,
			Map<Street, List<Node>> streetNodes, List<City> citySuburbs) throws SQLException {
		TLongObjectHashMap<Street> visitedStreets = new TLongObjectHashMap<>();
		Map<String, List<Street>> uniqueNames = new TreeMap<>(OsmAndCollator.primaryCollator());

		// read streets for city
		readStreetsAndBuildingsForCity(streetBuildingsStat, city, waynodesStat, streetNodes, visitedStreets, uniqueNames);
		// read streets for suburbs of the city
		if (citySuburbs != null) {
			for (City suburb : citySuburbs) {
				readStreetsAndBuildingsForCity(streetBuildingsStat, suburb, waynodesStat, streetNodes, visitedStreets, uniqueNames);
			}
		}
		mergeStreetsWithSameNames(streetNodes, uniqueNames);
		return new ArrayList<>(streetNodes.keySet());
	}

	private void mergeStreetsWithSameNames(Map<Street, List<Node>> streetNodes, Map<String, List<Street>> uniqueNames) {
		uniqueNames.keySet()
				.stream()
				.map(uniqueNames::get)
				.filter(streets -> streets.size() > 1)
				.forEach(streets -> mergeStreets(streets, streetNodes));
	}

	private void mergeStreets(List<Street> streets, Map<Street, List<Node>> streetNodes) {
		// Merge streets to streets with biggest amount of intersections.
		// Streets, that were extracted from addr:street tag has no intersections at all.
		streets.sort(Collections.reverseOrder((s0, s1) -> Algorithms.compare(s0.getIntersectedStreets().size(), s1.getIntersectedStreets().size())));
		streets.sort(Collections.reverseOrder((s0, s1) -> Algorithms.compare(s0.getBuildings().size(), s1.getBuildings().size())));
		for (int i = 0; i < streets.size() - 1; ) {
			Street s = streets.get(i);
			boolean merged = false;
			for (int j = i + 1; j < streets.size(); ) {
				Street candidate = streets.get(j);
				if (getDistance(s, candidate, streetNodes) <= 900) {
					merged = true;
					//logMapDataWarn.info("City : " + s.getCity() +
					//	" combine 2 district streets '" + s.getName() + "' with '" + candidate.getName() + "'");
					s.mergeWith(candidate);
					candidate.getCity().unregisterStreet(candidate);
					List<Node> old = streetNodes.remove(candidate);
					streetNodes.get(s).addAll(old);
					streets.remove(j);
				} else {
					j++;
				}
			}
			if (!merged) {
				i++;
			}
		}
	}

	private double getDistance(Street s, Street c, Map<Street, List<Node>> streetNodes) {
		List<Node> thisWayNodes = streetNodes.get(s);
		List<Node> oppositeStreetNodes = streetNodes.get(c);
		if (thisWayNodes.size() == 0) {
			thisWayNodes = Collections.singletonList(new Node(s.getLocation().getLatitude(), s.getLocation().getLongitude(), -1));
		}
		if (oppositeStreetNodes.size() == 0) {
			oppositeStreetNodes = Collections.singletonList(new Node(c.getLocation().getLatitude(), c.getLocation().getLongitude(), -1));
		}
		double md = Double.POSITIVE_INFINITY;
		for (Node n : thisWayNodes) {
			for (Node d : oppositeStreetNodes) {
				if (n != null && d != null) {
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
				Map<String, String> names = Algorithms.decodeMap(set.getString(3));
				double lat = set.getDouble(4);
				double lon = set.getDouble(5);
				// load the street nodes
				List<Node> thisWayNodes = loadStreetNodes(streetId, waynodesStat);
				Street street = addStreetToUniqueNamesMap(uniqueNames, streetName, names, city);
				street.setLocation(lat, lon);
				street.setId(streetId);
				// If there are more streets with same name in different districts.
				// Add district name to all other names. If sorting is right, the first street was the one in the city
				String district = set.getString(12);
				String cityPart = district == null || district.equals(city.getName()) ? "" : " (" + district + ")";
				street.setName(streetName + cityPart);
				for (String lang : names.keySet()) {
					street.setName(lang, names.get(lang) + cityPart);
				}
				streetNodes.put(street, thisWayNodes);
				city.registerStreet(street);

				visitedStreets.put(streetId, street); // mark the street as visited
			}
			if (set.getObject(6) != null) {
				Street s = visitedStreets.get(streetId);
				Building b = new Building();
				b.setId(set.getLong(6));
				b.copyNames(set.getString(7), null, Algorithms.decodeMap(set.getString(8)));
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

	private Street addStreetToUniqueNamesMap(Map<String, List<Street>> uniqueNames, String currStrName, Map<String, String> names, City city) {
		String resStrName = currStrName;
		String foundStrName;
		if (!uniqueNames.containsKey(currStrName)) {
			foundStrName = findSameStrNameByUniqueNames(uniqueNames, names);
			if (foundStrName == null) {
				foundStrName = findSameStrNameByAllStrNames(uniqueNames, currStrName);
			}
			if (foundStrName != null) {
				resStrName = foundStrName;
			} else {
				uniqueNames.put(currStrName, new ArrayList<>());
			}
		}
		Street street = new Street(city);
		uniqueNames.get(resStrName).add(street);
		return street;
	}

	private String findSameStrNameByAllStrNames(Map<String, List<Street>> uniqueNames, String currentStreetName) {
		for (Map.Entry<String, List<Street>> uniqueName : uniqueNames.entrySet()) {
			for (Street uniqueStreet : uniqueName.getValue()) {
				if (findSameStreetName(uniqueStreet, currentStreetName) != null) {
					return uniqueName.getKey();
				}
			}
		}
		return null;
	}

	private String findSameStreetName(Street uniqueStreet, String currentStreetName) {
		return getOtherNames(uniqueStreet)
				.stream()
				.filter(name -> name.equals(currentStreetName))
				.findFirst()
				.orElse(null);
	}

	private String findSameStrNameByUniqueNames(Map<String, List<Street>> uniqueNames, Map<String, String> names) {
	    List<String> namesList = names.keySet().stream().filter(name -> !name.startsWith("old")).collect(Collectors.toList());
		return uniqueNames.keySet()
				.stream()
				.filter(namesList::contains)
				.findFirst()
				.orElse(null);
	}

	private List<String> getOtherNames(Street str) {
		return str.getNamesMap(true).entrySet()
				.stream()
				.filter(name -> !name.getKey().contains("old"))
				.map(Entry::getValue)
				.collect(Collectors.toList());
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


	public Map<CityType, List<City>> readCities(Connection c) throws SQLException {
		Map<CityType, List<City>> cities = new LinkedHashMap<City.CityType, List<City>>();
		for (CityType t : CityType.values()) {
			cities.put(t, new ArrayList<City>());
		}

		Statement stat = c.createStatement();
		ResultSet set = stat.executeQuery("select id, latitude, longitude , name , name_en , city_type from city"); //$NON-NLS-1$
		while (set.next()) {
			CityType type = CityType.valueFromString(set.getString(6));
			City city = new City(type);
			city.copyNames(set.getString(4), null, Algorithms.decodeMap(set.getString(5)));
			city.setLocation(set.getDouble(2), set.getDouble(3));
			city.setId(set.getLong(1));
			cities.get(type).add(city);

			if (DEBUG_FULL_NAMES) {
				Boundary cityB = cityDataStorage.getBoundaryByCity(city);
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
		for (List<City> t : cities.values()) {
			Collections.sort(t, comparator);
		}
		return cities;
	}


	static {

	}
}
