package net.osmand;


import java.io.File;
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

import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;

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
	private static final Map<String, Integer> COMPARE_ARGS = new HashMap<String, Integer>() {{
		put("--cities", CITY_COMPARE);
		put("--city-names", CITY_NAME_COMPARE);
		put("--streets", STREET_COMPARE);
		put("--street-names", STREET_NAME_COMPARE);
		put("--buildings", BUILDINGS_COMPARE);
		put("--intersections", INTERSECTIONS_COMPARE);
	}};
	public static final String helpMessage = "[--cities] [--city-names] [--streets] [--street-names] [--buildings] [--intersections] <first> <second>: compare <first> and <second>";

	public static void main(String[] args) throws IOException {
		BinaryComparator in = new BinaryComparator();
		// test cases show info
		if (args.length == 1 && "test".equals(args[0])) {
			in.compare(new String[]{
					System.getProperty("maps.dir") +"Netherlands_europe_2.road.obf",
					System.getProperty("maps.dir") + "Netherlands_europe.road.obf",
					"--cities", "--city-names"
					,"--streets", "--street-names"
					,"--buildings", "--intersections"
					});
		} else {
			in.compare(args);
		}
	}

	private void compare(List<String> args) throws IOException {
		if (args == null || args.size() < 2) {
			System.out.println(helpMessage);
			System.exit(1);
		}
		args = new ArrayList<String>(args);
		int i = 0;
		do {
			String arg = args.get(i);
			if (arg.startsWith("--")) {
				if (COMPARE_ARGS.containsKey(arg)) {
					COMPARE_SET.add(COMPARE_ARGS.get(arg));
					args.remove(i);
				} else {
					System.out.print("Error: unknown argument");
					System.out.println(helpMessage);
					System.exit(1);
				}
			} else {
				i++;
			}
		} while (i < args.size());
		if (COMPARE_SET.isEmpty()) {
			COMPARE_SET.addAll(COMPARE_ARGS.values());
		}
		BinaryMapIndexReader[] indexes = new BinaryMapIndexReader[args.size()];
		RandomAccessFile[] rafs = new RandomAccessFile[args.size()];
		for (i = 0; i < args.size(); i++) {
			rafs[i] = new RandomAccessFile(args.get(i), "r");
			indexes[i] = new BinaryMapIndexReader(rafs[i], new File(args.get(i)));
		}
		AddressRegion r0 = getAddressRegion(indexes[0]);
		AddressRegion r1 = getAddressRegion(indexes[1]);
		compare(r0, indexes[0], r1, indexes[1]);

	}

	private void compare(AddressRegion r0, BinaryMapIndexReader i0, AddressRegion r1, BinaryMapIndexReader i1) throws IOException {
		for (int cityType : BinaryMapAddressReaderAdapter.CITY_TYPES) {
			List<City> ct0 = i0.getCities(null, cityType);
			List<City> ct1 = i1.getCities(null, cityType);
			Comparator<City> c = comparator();
			Collections.sort(ct0, c);
			Collections.sort(ct1, c);
			int i = 0;
			int j = 0;
			System.out.println("CITY TYPE: " + cityType);
			while (i < ct0.size() || j < ct1.size()) {
				City c0 = get(ct0, i);
				City c1 = get(ct1, j);
				int cmp = c.compare(c0, c1);
				if (cmp < 0) {
					while (c.compare(c0, c1) < 0) {
						if (COMPARE_SET.contains(CITY_COMPARE)) {
							City ps = searchSimilarCities(c0, ct1, j);
							if (ps != null) {
								int distance = (int) MapUtils.getDistance(c0.getLocation(), ps.getLocation());
								System.out.println("(1). Extra city in 1st file: " + c0
										+ "( " + distance + " m ) possible duplicate " + ps);
							} else {
								System.out.println("(1)! Extra city in 1st file: " + c0);
							}
						}
						i++;
						c0 = get(ct0, i);
					}
				} else if (cmp > 0) {
					while (c.compare(c0, c1) > 0) {
						if (COMPARE_SET.contains(CITY_COMPARE)) {
							City ps = searchSimilarCities(c1, ct0, i);
							if (ps != null) {
								int distance = (int) MapUtils.getDistance(c1.getLocation(), ps.getLocation());
								System.out.println("(1). Extra city in 2nd file: " + c1
										+ "( " + distance + " m ) possible duplicate " + ps);
							} else {
								System.out.println("(1)! Extra city in 2nd file: " + c1);
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
					i0.preloadStreets(c0, null);
					i1.preloadStreets(c1, null);
					if (!c0.getNamesMap(true).equals(c1.getNamesMap(true)) && COMPARE_SET.contains(CITY_NAME_COMPARE)) {
						System.out.println("(1). City all names are not same : " + c1 + " " + c0.getNamesMap(true) + " != " + c1.getNamesMap(true));
					}
					if (c0.getStreets().size() != c1.getStreets().size()) {
						if (COMPARE_SET.contains(STREET_COMPARE)) {
							System.out.println("(2). City streets " + c1 + ":  " + c0.getStreets().size() + " != " + c1.getStreets().size());
							List<String> s0 = new ArrayList<String>();
							List<String> s1 = new ArrayList<String>();
							for (Street s : c0.getStreets()) {
								if (c1.getStreetByName(s.getName()) == null) {
									s0.add(s.getName());
								}
							}
							for (Street s : c1.getStreets()) {
								if (c0.getStreetByName(s.getName()) == null) {
									s1.add(s.getName());
								}
							}
							if (s0.isEmpty() && s1.isEmpty()) {
								// locations of streets are not equal
								System.out.println("(2)? " + c0.getStreets());
							} else {
								System.out.println("(2).. " + s0 + "!=" + s1);
							}
						}
					} else {
						// compare streets
						for (int ij = 0; ij < c1.getStreets().size(); ij++) {
							Street s0 = c0.getStreets().get(ij);
							Street s1 = c1.getStreets().get(ij);
							if (!s0.getNamesMap(true).equals(s1.getNamesMap(true)) && COMPARE_SET.contains(STREET_NAME_COMPARE)) {
								System.out.println("(2)- Street all names are not same : " + c1 + " " + s0.getNamesMap(true) + " != " + s1.getNamesMap(true));
							}
							if (s0.getName().equals(s1.getName())) {
								i0.preloadBuildings(s0, null);
								i1.preloadBuildings(s1, null);
								if (COMPARE_SET.contains(BUILDINGS_COMPARE)) {
									if (s0.getBuildings().size() != s1.getBuildings().size()) {
										System.out.println("(3). Buildings size: " + s0.getBuildings().size() + "!="
												+ s1.getBuildings().size() + " " + c0 + ", " + s0);
									} else {
										for (int it = 0; it < s0.getBuildings().size(); it++) {
											Building b0 = s0.getBuildings().get(it);
											Building b1 = s1.getBuildings().get(it);
											if (!b0.getName().equals(b1.getName())) {
												System.out.println("(4). Buildings name: " + b0.getName() + "!="
														+ b1.getName() + " " + c0 + ", " + s0);
											}
											if (!Algorithms.objectEquals(b0.getPostcode(), b1.getPostcode())) {
												System.out.println("(4). Buildings postcode: " + b0.getPostcode()
														+ "!=" + b1.getPostcode() + " " + c0 + ", " + s0);
											}
										}
									}
								}
								if (COMPARE_SET.contains(INTERSECTIONS_COMPARE)) {
									if (s0.getIntersectedStreets().size() != s1.getIntersectedStreets().size()) {
										System.out.println("(5). Intersections size: "
												+ s0.getIntersectedStreets().size() + "!="
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
												System.out.println("(5). Intersections names <> : " + st0
														+ "!=" + st1 + " " + c0 + ", " + s0 + " ");
											}
											if (MapUtils.getDistance(st0.getLocation(), st1.getLocation()) > 1500) {
												System.out.println("(5). Intersections location <> : " + st0
														+ "!=" + st1 + " " + c0 + ", " + s0);
											}
										}
									}
								}
							} else {
								if (COMPARE_SET.contains(STREET_NAME_COMPARE)) {
									System.out.println("(3)? Street name order: " + s0 + "!=" + s1 + " " + c0);
								}
							}
						}

					}
				}
			}
		}
		// TODO (6) Compare by name index
	}

	private City searchSimilarCities(City city, List<City> search, int j) {
		// scan similar cities
		Collator collator = OsmAndCollator.primaryCollator();
		for (int t = j; t >= 0; t--) {
			City ps = search.get(t);
			if (collator.compare(strip(city.getName()), strip(ps.getName())) != 0) {
				break;
			}
			if (MapUtils.getDistance(city.getLocation(), ps.getLocation()) < CITY_SIMILARITY_DISTANCE_POSSIBLE) {
				return ps;
			}
		}

		for (int t = j; t < search.size(); t++) {
			City ps = search.get(t);
			if (collator.compare(strip(city.getName()), strip(ps.getName())) != 0) {
				break;
			}
			if (MapUtils.getDistance(city.getLocation(), ps.getLocation()) < CITY_SIMILARITY_DISTANCE_POSSIBLE) {
				return ps;
			}
		}
		return null;
	}

	private City get(List<City> ct0, int i) {
		return i >= ct0.size() ? null : ct0.get(i);
	}

	private String strip(String name) {
		name = name.indexOf('(') != -1 ? name.substring(0, name.indexOf('(')).trim() : name;
		// Remove spaces in Netherlands' postcodes
		name = (name.length() == 7 && 
				Character.isDigit(name.charAt(0)) && Character.isDigit(name.charAt(1)) && 
				Character.isDigit(name.charAt(2)) && Character.isDigit(name.charAt(3)) && 
				name.charAt(4) == ' ') ? name.replaceAll(" ", "") : name;
		return name;
	}

	private Comparator<City> comparator() {
		return new Comparator<City>() {
			Collator collator = OsmAndCollator.primaryCollator();

			@Override
			public int compare(City o1, City o2) {
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


	private AddressRegion getAddressRegion(BinaryMapIndexReader i) {
		List<BinaryIndexPart> list = i.getIndexes();
		for (BinaryIndexPart p : list) {
			if (p instanceof AddressRegion) {
				return (AddressRegion) p;
			}
		}
		return null;
	}


}
