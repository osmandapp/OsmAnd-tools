package net.osmand.obf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.binary.BinaryHHRouteReaderAdapter.HHRouteRegion;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapAddressReaderAdapter.CityBlocks;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.Street;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;
import net.osmand.router.tester.RandomRouteTester;
import net.osmand.util.MapUtils;

public class ObfChecker {

	private static final int LIMIT_HH_POINTS_NEEDED = 100_000;
	private static final int MAX_BUILDING_DISTANCE = 100;

	public static void main(String[] args) {
		// TODO
//		OsmAndMapCreator/utilities.sh random-route-tester \
//        --maps-dir="$OBF_FOLDER" --iterations=3 --obf-prefix="$FILE" \
//        --profile="$PROFILE" --min-dist=1 --no-conditionals \
//        --max-dist=50 2>&1
		if (args.length == 1 && args[0].equals("--test")) {
			args = new String[] { System.getProperty("maps.dir") + "Us_california_northamerica_2.road.obf" };
		}
		Map<String, String> argMap = new LinkedHashMap<>();
		List<String> files = new ArrayList<>();
		for (String a : args) {
			if (a.startsWith("--")) {
				String[] k = a.substring(2).split("=");
				argMap.put(k[0], k.length == 1 ? "" : k[1]);
			} else {
				files.add(a);
			}
		}
		int failed = 0;
		try {
			for (String file : files) {
				boolean ok = check(file, argMap);
				if (!ok) {
					failed++;
				}
			}
			if (failed == 0) {
				System.out.println("OK");
			}
			System.exit(failed);
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static boolean check(String file, Map<String, String> argMap) throws Exception {
		File oFile = new File(file);
		RandomAccessFile r = new RandomAccessFile(oFile.getAbsolutePath(), "r");
		boolean ok = true;
		if (oFile.length() > Integer.MAX_VALUE) {
			ok = false;
			System.err.println("File exceeds max integer value that causes issue with C++ protobuf");
		}
		BinaryMapIndexReader index = new BinaryMapIndexReader(r, oFile);
		MapIndex mi = null;
		HHRouteRegion car = null;
		HHRouteRegion bicycle = null;
		PoiRegion poi = null;
		RouteRegion routeRegion = null;
		long routeSectionSize = 0;
		AddressRegion address = null;
		boolean world = oFile.getName().toLowerCase().startsWith("world");
		for (BinaryIndexPart p : index.getIndexes()) {
			if (p instanceof MapIndex) {
				mi = (MapIndex) p;
			} else if (p instanceof HHRouteRegion hr) {
				if (hr.profile.equals("car")) {
					car = hr;
				} else if (hr.profile.equals("bicycle")) {
					bicycle = hr;
				}
				ok &= checkHHRegion(index, hr);
			} else if (p instanceof PoiRegion) {
				poi = (PoiRegion) p;
			} else if (p instanceof AddressRegion) {
				address = (AddressRegion) p;
			} else if (p instanceof RouteRegion) {
				routeRegion = (RouteRegion) p;
				routeSectionSize = p.getLength();
			}
		}
		
		if (routeSectionSize > LIMIT_HH_POINTS_NEEDED * 2 && /*(car == null || bicycle == null) && */!world) {
			int cnt = 0;
			SearchRequest<RouteDataObject> sr = BinaryMapIndexReader.buildSearchRouteRequest(0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
			List<RouteSubregion> regions = index.searchRouteIndexTree(sr,  routeRegion.getSubregions());
			for (RouteSubregion rs : regions) {
				if (cnt > LIMIT_HH_POINTS_NEEDED) {
					break;
				}
				List<RouteDataObject> ls = index.loadRouteIndexData(rs);
				for (RouteDataObject rdo : ls) {
					if (rdo != null && rdo.getHighway() != null) {
						cnt += rdo.getPointsLength();
					}
				}
			}
			if (cnt > LIMIT_HH_POINTS_NEEDED) {
				ok &= runRandomRouteTester(oFile);
				ok &= checkNull(oFile, car, "Missing HH route section for car - route section bytes: " + routeSectionSize);
				ok &= checkNull(oFile, bicycle,
						"Missing HH route section for bicycle - route section bytes: " + routeSectionSize);
			}
		}
//		if (routeSectionSize > LIMIT_HH_POINTS_NEEDED) {
//			ok &= runRandomRouteTester(oFile);
//		}
		ok &= checkNull(oFile, mi, "Missing Map section");
		if (!world) {
			ok &= checkNull(oFile, poi, "Missing Poi section");
			ok &= checkNull(oFile, address, "Missing address section");
			ok &= checkNull(oFile, routeRegion, "Missing routing section");
			if (!checkSimpleAddress(index, address, true)) {
				ok = false;
			}
		}

		index.close();
		return ok;
	}

	private static boolean checkSimpleAddress(BinaryMapIndexReader index, AddressRegion address, boolean checkDuplicateBuildings) throws IOException {
		StringBuilder errors = new StringBuilder();
		int cityInd = 0, streetInd = 0;
		int errInd = 0;
		// CityBlocks.VILLAGES_TYPE duplicate street
		long time = System.currentTimeMillis();
//		for (CityBlocks cityType : EnumSet.of(CityBlocks.CITY_TOWN_TYPE, CityBlocks.POSTCODES_TYPE)) {
		for (CityBlocks cityType : EnumSet.of(CityBlocks.CITY_TOWN_TYPE, CityBlocks.POSTCODES_TYPE, CityBlocks.VILLAGES_TYPE )) {
			List<City> cities = index.getCities(null, cityType, address, null);
			for (City c : cities) {
				cityInd++;
				index.preloadStreets(c, null, true, null);
				TreeSet<String> set = new TreeSet<>();
				for (Street s : c.getStreets()) {
					// to do ignore for now
					if (s.getName().startsWith("<")) {
						continue;
					}
					streetInd++;
					if (set.contains(s.getName())) {
						String err = String.format(" %d. duplicate street '%s' in '%s'", errInd++, s.getName(),
								c.getName());
						addErr(errors, errInd, err);
					}
					set.add(s.getName());
					if (!checkDuplicateBuildings) {
						continue;
					}
					Map<String, Building> map = new TreeMap<>();
					for (Building b : s.getBuildings()) {
						Building bld = map.get(b.getName());
						if (bld == null) {
							map.put(b.getName(), b);
						} else {
							double dist = MapUtils.getDistance(b.getLocation(), bld.getLocation());
							if (dist > MAX_BUILDING_DISTANCE) {
								String err = String.format(
										" %d. Buildings '%s' ('%s' in '%s') too far %.2f km (%.5f, %.5f - %.5f, %.5f) ",
										errInd++, b.getName(), s.getName(), c.getName(), dist / 1000,
										b.getLocation().getLatitude(), b.getLocation().getLongitude(),
										bld.getLocation().getLatitude(), bld.getLocation().getLongitude());
								addErr(errors, errInd, err);
							}
						}
					}
				}
				c.getStreets().clear(); // free memory
			}
		}
		if (!errors.isEmpty()) {
			System.err.printf("Checked %d cities, %d streets (%.1f s) - found %d errors in address section\n", cityInd, streetInd,
					(System.currentTimeMillis() - time) / 1000.0f, errInd);
//			System.err.println("Errors in address section: " + errors);
		}
//		return errors.isEmpty();
		return true;
	}

	private static void addErr(StringBuilder errors, int errInd, String err) {
		if (errInd % 5 == 0) {
			errors.append("\n");
		} else if (errors.length() < 1_000_000) {
			errors.append(err);
		}
	}

	private static boolean checkHHRegion(BinaryMapIndexReader index, HHRouteRegion hr) throws IOException {
		boolean ok = true;
		TLongObjectHashMap<NetworkDBPoint> pnts = index.initHHPoints(hr, (short) 0, NetworkDBPoint.class);
		for (NetworkDBPoint pnt : pnts.valueCollection()) {
			if (pnt.dualPoint == null) {
				System.err.printf("Error in map %s - %s missing dual point \n", index.getFile().getName(), pnt);
				ok = false;
			}
		}
		return ok;
	}

	private static boolean checkNull(File f, Object o, String string) {
		if (o == null) {
			System.err.println("[" + f.getName() + "] " + string);
			return false;
		}
		return true;
	}

	private static boolean runRandomRouteTester(File oFile) throws Exception {
		String directory = oFile.getAbsoluteFile().getParent();
		String filename = oFile.getName();
		String [] args = {
				"--maps-dir=" + directory,
				"--obf-prefix=" + filename,
				"--no-native-library",
				"--no-html-report",

				"--avoid-brp-java",
				"--avoid-brp-cpp",
				"--avoid-hh-cpp",

				"--use-hh-points", // load random points from HH-sections only
				"--max-shift=1000", // random shift to activate A* calculations (m)

				"--iterations=10",
				"--stop-at-first-route",

				"--profile=car",
				"--min-dist=5", // km
				"--max-dist=10", // km
		};
		return RandomRouteTester.run(args) == RandomRouteTester.EXIT_SUCCESS;
	}

}
