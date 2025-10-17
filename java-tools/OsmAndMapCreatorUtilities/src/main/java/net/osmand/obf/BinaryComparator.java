package net.osmand.obf;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.osmand.Collator;
import net.osmand.OsmAndCollator;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapAddressReaderAdapter.CityBlocks;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.Amenity;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.json.JSONObject;

public class BinaryComparator {

	public static final int BUFFER_SIZE = 1 << 20;
	private static double CITY_SIMILARITY_DISTANCE = 5500;
	private static double CITY_SIMILARITY_DISTANCE_POSSIBLE = 25500;
	private final static Log log = PlatformUtil.getLog(BinaryComparator.class);
	private static Set<Integer> COMPARE_SET = new HashSet<Integer>();
	private static final int CITY_COMPARE = 11;
	private static final int CITY_NAME_COMPARE = 12;
	private static final int STREET_COMPARE = 21;
	private static final int STREET_NAME_COMPARE = 22;
	private static final int BUILDINGS_COMPARE = 31;
	private static final int INTERSECTIONS_COMPARE = 41;
	private static final int POI_COMPARE = 51;
	private static final int POI_DETAILS = 55;
	private static final int COMPARE_UNIQUE_1 = 91;
	private static final int COMPARE_UNIQUE_2 = 92;
	private static final Integer[] ADDRESS_COMPARE = { CITY_COMPARE, CITY_NAME_COMPARE, STREET_COMPARE,
			STREET_NAME_COMPARE, BUILDINGS_COMPARE, INTERSECTIONS_COMPARE };
	private static final Map<String, Integer> COMPARE_ARGS = new HashMap<String, Integer>();
	static {
		COMPARE_ARGS.put("--cities", CITY_COMPARE);
		COMPARE_ARGS.put("--city-names", CITY_NAME_COMPARE);
		COMPARE_ARGS.put("--streets", STREET_COMPARE);
		COMPARE_ARGS.put("--street-names", STREET_NAME_COMPARE);
		COMPARE_ARGS.put("--buildings", BUILDINGS_COMPARE);
		COMPARE_ARGS.put("--intersections", INTERSECTIONS_COMPARE);
		COMPARE_ARGS.put("--poi", POI_COMPARE);
		COMPARE_ARGS.put("--poi-details", POI_DETAILS);
		COMPARE_ARGS.put("--unique-1", COMPARE_UNIQUE_1);
		COMPARE_ARGS.put("--unique-2", COMPARE_UNIQUE_2);
	}

	private static final String[] fileNameByNumber = {"first file", "second file"};
	private int ELEM_ID = -1;
	private FileOutputStream fosm = null;
	public static final String helpMessage = "[--cities] [--city-names] [--streets] [--street-names] [--buildings] [--intersections] [--poi] [--poi-details]" +
			" [--osm=file_path] [--add] [--rm] <first> <second>: compare <first> and <second> (map & routing data is not supported)";

	public static void main(String[] args) throws IOException {
		BinaryComparator in = new BinaryComparator();
		// test cases show info
		if (args.length == 1 && "test".equals(args[0])) {
			in.compare(new String[]{
					System.getProperty("maps.dir") + "Andorra_europe_2.obf",
					System.getProperty("maps.dir") + "Andorra_europe.obf",
//					"--cities", "--city-names",
//					"--streets", "--street-names",
//					"--buildings", "--intersections",
					"--poi",
					"--poi-details",
					"--unique-1", "--unique-2",
//					"--osm=" + System.getProperty("maps.dir") + "compare.osm"
			});
		} else {
			in.compare(args);
		}
	}

	private void compare(String[] argArr) throws IOException {
		if (argArr == null || argArr.length < 2) {
			System.out.println(helpMessage);
			System.exit(1);
		}
		List<BinaryMapIndexReader> indexes = new ArrayList<BinaryMapIndexReader>();
		List<RandomAccessFile> rafs = new ArrayList<RandomAccessFile>();
		for (int i = 0; i < argArr.length; i++) {
			String arg = argArr[i];
			if (arg.startsWith("--osm=")) {
				fosm = new FileOutputStream(arg.substring("--osm=".length()));
			} else if (arg.startsWith("--")) {
				if (COMPARE_ARGS.containsKey(arg)) {
					COMPARE_SET.add(COMPARE_ARGS.get(arg));
				} else {
					System.out.print("Error: unknown argument");
					System.out.println(helpMessage);
					System.exit(1);
				}
			} else {
				RandomAccessFile raf = new RandomAccessFile(arg, "r");
				BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, new File(arg));
				indexes.add(reader);
				rafs.add(raf);
			}
		}
		if (COMPARE_SET.isEmpty()) {
			COMPARE_SET.addAll(COMPARE_ARGS.values());
		}
		if (isOsmOutput()) {
			fosm.write("<?xml version='1.0' encoding='utf-8'?>\n".getBytes());
			fosm.write("<osm version='0.6'>\n".getBytes());
		}
		Set<Integer> addressCompareSet = new HashSet<Integer>(COMPARE_SET);
		addressCompareSet.retainAll(Arrays.asList(ADDRESS_COMPARE));
		if (!addressCompareSet.isEmpty()) {
			compareAddress(indexes.get(0), indexes.get(1));
		}
		if (COMPARE_SET.contains(POI_COMPARE) || COMPARE_SET.contains(POI_DETAILS)) {
			comparePoi(indexes.get(0), indexes.get(1));
		}
		if (isOsmOutput()) {
			fosm.write("</osm>".getBytes());
			fosm.close();
		}
	}

	private List<Amenity> loadAmenities(BinaryMapIndexReader index) throws IOException {
		List<Amenity> amenities = new ArrayList<Amenity>(index.searchPoi(BinaryMapIndexReader.buildSearchPoiRequest(
				0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, -1,
				BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER,
				null)));
		Collections.sort(amenities, getNaturalOrder());
		log.info("Read " + amenities.size() + " amenities from " + index.getFile());
		return amenities;
	}

	private Comparator<Amenity> getNaturalOrder() {
		return new Comparator<Amenity>() {

			@Override
			public int compare(Amenity o1, Amenity o2) {
				int c;
				if (o1 == null || o2 == null) {
					return o1 == o2 ? 0 : (o1 == null ? 1 : -1);
				}
				if (o1.getId() < 0 || o2.getId() < 0) {
					if (o1.getId() > 0) {
						return 1;
					} else if (o2.getId() > 0) {
						return -1;
					}
					long h1 = latlon(o1);
					long h2 = latlon(o2);
					c = Algorithms.compare(h1, h2);
				} else {
					c = Algorithms.compare(o1.getId(), o2.getId());
				}
				if (c == 0) {
					int l = Algorithms.compare(o1.getType().ordinal(), o2.getType().ordinal());
					if (l == 0) {
						return o1.getSubType().compareTo(o2.getSubType());
					}
					return l;
				}
				return c;
			}
		};
	}

	public static long latlon(Amenity amenity) {
		LatLon loc = amenity.getLocation();
		return ((long) MapUtils.getTileNumberX(21, loc.getLongitude()) << 31 | (long) MapUtils.getTileNumberY(21, loc.getLatitude()));
	}

	private void comparePoiDetails(Amenity a0, Amenity a1) throws IOException {
		if (!Algorithms.objectEquals(a0.getSubType(), a1.getSubType())) {
			printMapObject(POI_DETAILS, a0,
					"Amenity subtypes are not equal " + a0.getSubType() + " <> " + a1.getSubType());
		}
		if (!Algorithms.objectEquals(a0.getAdditionalInfoKeys(), a1.getAdditionalInfoKeys())) {
			printMapObject(POI_DETAILS, a0,
					"Amenity info key is not equal " + a0.getAdditionalInfoKeys() + " <> " + a1.getAdditionalInfoKeys());
		}
		if (!Algorithms.objectEquals(new TreeSet<String>(a0.getAdditionalInfoValues(false)), new TreeSet<String>(a1.getAdditionalInfoValues(false)))) {
			printMapObject(POI_DETAILS, a0,
					"Amenity info is not equal " + a0.getAdditionalInfoValues(false) + " <> " + a1.getAdditionalInfoValues(false));
		}
		if (!Algorithms.objectEquals(a0.getNamesMap(true), a1.getNamesMap(true))) {
			printMapObject(POI_DETAILS, a0,
					"Amenity name is not equal " + a0.getNamesMap(true) + " <> " + a1.getNamesMap(true));
		}
		if (MapUtils.getDistance(a0.getLocation(), a1.getLocation()) > 50) {
			printMapObject(POI_DETAILS, a0,
					"Amenitis are too far" + a0.getLocation() + " <> " + a1.getLocation() + " " + MapUtils.getDistance(a0.getLocation(), a1.getLocation()));
		}
	}

	private void comparePoi(BinaryMapIndexReader i0, BinaryMapIndexReader i1) throws IOException {
		List<Amenity> amenities0 = loadAmenities(i0);
		List<Amenity> amenities1 = loadAmenities(i1);
		int i = 0;
		int j = 0;
		int[] uniqueCount = {0, 0};
		Comparator<Amenity> c = getNaturalOrder();
		Amenity a0 = get(amenities0, 0);
		Amenity a1 = get(amenities1, 0);
		while (i < amenities0.size() || j < amenities1.size()) {
			int cmp = c.compare(a0, a1);
			if (cmp < 0) {
				if (COMPARE_SET.contains(COMPARE_UNIQUE_1) && COMPARE_SET.contains(POI_COMPARE)) {
					uniqueCount[0]++;
					printAmenity(a0, 0);
				}
				i++;
				a0 = get(amenities0, i);
			} else if (cmp > 0) {
				if (COMPARE_SET.contains(COMPARE_UNIQUE_2) && COMPARE_SET.contains(POI_COMPARE)) {
					uniqueCount[1]++;
					printAmenity(a1, 1);
				}
				j++;
				a1 = get(amenities1, j);
			} else {
				if (COMPARE_SET.contains(POI_DETAILS) && a0 != null && a1 != null) {
					comparePoiDetails(a0, a1);
				}
				i++;
				j++;
				a0 = get(amenities0, i);
				a1 = get(amenities1, j);
			}
		}
		for (int compareUnique : Arrays.asList(COMPARE_UNIQUE_1, COMPARE_UNIQUE_2)) {
			if (COMPARE_SET.contains(compareUnique)) {
				int uniqueToFile = compareUnique - COMPARE_UNIQUE_1;
				log.info("Amenities present only in " + fileNameByNumber[uniqueToFile] + ": " + uniqueCount[uniqueToFile]);
			}
		}
	}

	private void compareAddress(BinaryMapIndexReader i0, BinaryMapIndexReader i1) throws IOException {
		for (CityBlocks cityType : CityBlocks.values()) {
			if (!cityType.cityGroupType) {
				continue;
			}
			List<City> ct0 = i0.getCities(null, cityType);
			List<City> ct1 = i1.getCities(null, cityType);
			Comparator<City> c = comparator();
			Collections.sort(ct0, c);
			Collections.sort(ct1, c);
			int i = 0;
			int j = 0;
			printComment("CITY TYPE: " + cityType);
			while (i < ct0.size() || j < ct1.size()) {
				City c0 = get(ct0, i);
				City c1 = get(ct1, j);
				int cmp = c.compare(c0, c1);
				if (cmp < 0) {
					while (c.compare(c0, c1) < 0) {
						if (COMPARE_SET.contains(CITY_COMPARE) && COMPARE_SET.contains(COMPARE_UNIQUE_1)) {
							City ps = searchSimilarCities(c0, ct1, j);
							if (ps != null) {
								int distance = (int) MapUtils.getDistance(c0.getLocation(), ps.getLocation());
								printMapObject(CITY_COMPARE, c0, "(1). Extra city in 1st file: " + c0
										+ "( " + distance + " m ) possible duplicate " + ps);
							} else {
								printMapObject(CITY_COMPARE, c0, "(1)! Extra city in 1st file: " + c0);
							}
						}
						i++;
						c0 = get(ct0, i);
					}
				} else if (cmp > 0) {
					while (c.compare(c0, c1) > 0) {
						if (COMPARE_SET.contains(CITY_COMPARE) && COMPARE_SET.contains(COMPARE_UNIQUE_2)) {
							City ps = searchSimilarCities(c1, ct0, i);
							if (ps != null) {
								int distance = (int) MapUtils.getDistance(c1.getLocation(), ps.getLocation());
								printMapObject(CITY_COMPARE, c1, "(1). Extra city in 2nd file: " + c1
										+ "( " + distance + " m ) possible duplicate " + ps);
							} else {
								printMapObject(CITY_COMPARE, c1, "(1)! Extra city in 2nd file: " + c0);
							}
						}
						j++;
						c1 = get(ct1, j);
					}
				} else {
//					if(cityType == BinaryMapAddressReaderAdapter.CITY_TOWN_TYPE) {
//						System.out.println("Same city " + c1.getName()  + " == " + c0.getName());
//					}
					i++;
					j++;
					i0.preloadStreets(c0, null, null);
					i1.preloadStreets(c1, null, null);
					if (COMPARE_SET.contains(CITY_NAME_COMPARE) && !c0.getNamesMap(true).equals(c1.getNamesMap(true))) {
						printComment("(1). City all names are not same : " + c1 + " "
								+ (new JSONObject(c0.getNamesMap(true)) + " != "
								+ (new JSONObject(c1.getNamesMap(true)))));
					}
					if (c0.getStreets().size() != c1.getStreets().size()) {
						if (COMPARE_SET.contains(STREET_COMPARE)) {
							if (!isOsmOutput()) {
								printComment("(2). City streets " + c1 + ":  " + c0.getStreets().size() + " <> " + c1.getStreets().size());
							}
							List<String> s0 = new ArrayList<String>();
							List<String> s1 = new ArrayList<String>();
							for (Street s : c0.getStreets()) {
								if (c1.getStreetByName(s.getName()) == null) {
									s0.add(s.getName());
									if (isOsmOutput()) {
										printMapObject(STREET_COMPARE, s, "(2) Street " + s
												+ "is not present in 2nd file");
									}
								}
							}
							for (Street s : c1.getStreets()) {
								if (c0.getStreetByName(s.getName()) == null) {
									if (isOsmOutput()) {
										printMapObject(STREET_COMPARE, s, "(2) Street " + s
												+ " is not present in 1st file");
									}
									s1.add(s.getName());
								}
							}
							if (s0.isEmpty() && s1.isEmpty()) {
								// locations of streets are not equal
								printMapObject(STREET_COMPARE, c0, "(2) Number of streets with same name is not equal" + c0.getStreets());
							} else {
								printComment("(2).. " + s0 + "<>" + s1);
							}
						}
					} else {
						// compare streets
						for (int ij = 0; ij < c1.getStreets().size(); ij++) {
							Street s0 = c0.getStreets().get(ij);
							Street s1 = c1.getStreets().get(ij);
							if (!s0.getNamesMap(true).equals(s1.getNamesMap(true)) && COMPARE_SET.contains(STREET_NAME_COMPARE)) {
								printMapObject(STREET_NAME_COMPARE, s0,
										"(2)- Street all names are not same : " + c1 + " " + s0.getNamesMap(true) + " <> " + s1.getNamesMap(true));
							}
							if (s0.getName().equals(s1.getName())) {
								i0.preloadBuildings(s0, null, null);
								i1.preloadBuildings(s1, null, null);
								if (COMPARE_SET.contains(BUILDINGS_COMPARE)) {
									if (s0.getBuildings().size() != s1.getBuildings().size()) {
										printMapObject(BUILDINGS_COMPARE, s0,
												"(3). Buildings size: " + s0.getBuildings().size() + "<>"
														+ s1.getBuildings().size() + " " + c0 + ", " + s0);
									} else {
										for (int it = 0; it < s0.getBuildings().size(); it++) {
											Building b0 = s0.getBuildings().get(it);
											Building b1 = s1.getBuildings().get(it);
											if (!b0.getName().equals(b1.getName())) {
												printMapObject(BUILDINGS_COMPARE, b0,
														"(4). Buildings name: " + b0.getName() + "<>"
																+ b1.getName() + " " + c0 + ", " + s0);
											}
											if (!Algorithms.objectEquals(b0.getPostcode(), b1.getPostcode())) {
												printMapObject(BUILDINGS_COMPARE, b0,
														"(4). Buildings postcode: " + b0.getPostcode()
																+ "<>" + b1.getPostcode() + " " + c0 + ", " + s0);
											}
										}
									}
								}
								if (COMPARE_SET.contains(INTERSECTIONS_COMPARE)) {
									if (s0.getIntersectedStreets().size() != s1.getIntersectedStreets().size()) {
										printMapObject(INTERSECTIONS_COMPARE, s0,
												"(5). Intersections size: " + s0.getIntersectedStreets().size() + "<>"
														+ s1.getIntersectedStreets().size() + " " + c0 + ", " + s0);
									} else {
										Collections.sort(s0.getIntersectedStreets(), MapObject.BY_NAME_COMPARATOR);
										Collections.sort(s1.getIntersectedStreets(), MapObject.BY_NAME_COMPARATOR);
										for (int it = 0; it < s0.getIntersectedStreets().size(); it++) {
											Street st0 = s0.getIntersectedStreets().get(it);
											Street st1 = s1.getIntersectedStreets().get(it);
											if (!st0.getName().equals(st1.getName())
												// || !st0.getNamesMap(true).equals(st1.getNamesMap(true))
													) {
												printMapObject(INTERSECTIONS_COMPARE, st0,
														"(5). Intersections names <> : " + st0
																+ "<>" + st1 + " " + c0 + ", " + s0 + " ");
											}
											if (MapUtils.getDistance(st0.getLocation(), st1.getLocation()) > 1500) {
												printMapObject(INTERSECTIONS_COMPARE, st0,
														"(5). Intersections location <> : " + st0
																+ "<>" + st1 + " " + c0 + ", " + s0 + " ");
											}
										}
									}
								}
							} else {
								if (COMPARE_SET.contains(STREET_NAME_COMPARE)) {
									printMapObject(STREET_NAME_COMPARE, s0, "(3)? Street name order: " + s0 + "!=" + s1 + " " + c0);
								}
							}
						}
					}
				}
			}
		}
	}

	private void printMapObject(int type, MapObject obj, String msg) throws IOException {
		if (!isOsmOutput()) {
			System.out.println(msg);
		} else {

			fosm.write(("  <node lat='" + obj.getLocation().getLatitude() + "' lon='" + obj.getLocation().getLongitude() + "' "
					+ " id='" + (ELEM_ID--) + "'>\n").getBytes());
			fosm.write(("  <tag k='comment' v='" +
					msg.replace('\'', '_').replace("<", "&lt;").replace(">", "&gt;")
					.replace("&", "&amp;")
					+ "'/>\n").getBytes());
			;
			fosm.write(("  <tag k='name' v='" + type + "'/>\n").getBytes());
			;
			fosm.write(("  </node>\n").getBytes());
			;
		}
	}

	private void printAmenity(Amenity amenity, int uniqueToFile) throws IOException {
		printMapObject(POI_COMPARE, amenity,
				"Amenity exist only in " + fileNameByNumber[uniqueToFile] + ": " + amenity.toString());
	}


	private boolean isOsmOutput() {
		return fosm != null;
	}

	private void printComment(String string) throws IOException {
		if (!isOsmOutput()) {
			System.out.println(string);
		} else {
			fosm.write(("<!-- " + string + "-->\n").getBytes());
		}
	}

	private City searchSimilarCities(City city, List<City> search, int j) {
		// scan similar cities
		Collator collator = OsmAndCollator.primaryCollator();
		boolean offByOneError = false;
		for (int t = Math.min(j, search.size() -1) ; t >= 0; t--) {
			City ps = search.get(t);
			if (collator.compare(strip(city.getName()), strip(ps.getName())) != 0) {
				if (offByOneError) {
					break;
				} else {
					offByOneError = true;
					continue;
				}
			}
			if (MapUtils.getDistance(city.getLocation(), ps.getLocation()) < CITY_SIMILARITY_DISTANCE_POSSIBLE) {
				return ps;
			}
		}

		offByOneError = false;
		for (int t = j; t < search.size(); t++) {
			City ps = search.get(t);
			if (collator.compare(strip(city.getName()), strip(ps.getName())) != 0) {
				if (offByOneError) {
					break;
				} else {
					offByOneError = true;
					continue;
				}
			}
			if (MapUtils.getDistance(city.getLocation(), ps.getLocation()) < CITY_SIMILARITY_DISTANCE_POSSIBLE) {
				return ps;
			}
		}
		return null;
	}

	private <T> T get(List<T> list, int i) {
		return i >= list.size() ? null : list.get(i);
	}

	private String strip(String name) {
		return name.indexOf('(') != -1 ? name.substring(0, name.indexOf('(')).trim() : name;
	}

	private Comparator<City> comparator() {
		return new Comparator<City>() {
			Collator collator = OsmAndCollator.primaryCollator();

			@Override
			public int compare(City o1, City o2) {
				if (o1 == null || o2 == null) {
					return o1 == o2 ? 0 : (o1 == null ? 1 : -1);
				}
				int c = collator.compare(strip(o1.getName()), strip(o2.getName()));
				if (c == 0) {
					if (MapUtils.getDistance(o1.getLocation(), o2.getLocation()) < CITY_SIMILARITY_DISTANCE) {
						return 0;
					}
					c = Double.compare(MapUtils.getDistance(o1.getLocation(), 0, 0),
							MapUtils.getDistance(o2.getLocation(), 0, 0));
				}
				return c;
			}

		};
	}


}
