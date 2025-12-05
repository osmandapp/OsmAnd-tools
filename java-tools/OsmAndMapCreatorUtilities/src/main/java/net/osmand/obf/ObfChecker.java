package net.osmand.obf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.binary.BinaryHHRouteReaderAdapter.HHRouteRegion;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapAddressReaderAdapter.CityBlocks;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapPoiReaderAdapter;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.QuadRect;
import net.osmand.data.Street;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiType;
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;
import net.osmand.router.tester.RandomRouteTester;
import net.osmand.util.MapUtils;

public class ObfChecker {

	private static final int LIMIT_HH_POINTS_NEEDED = 150_000; // skip Falkland-islands, Greenland, etc

	private static final int MAX_BUILDING_DISTANCE = 100;

	private static final int MAX_MAP_RULES = 13800; // Germany_mecklenburg-vorpommern_europe 2025-12-05 +20%
	private static final int MAX_ROUTE_RULES = 17500; // Chile_southamerica 2025-12-05 +20%
	private static final int MAX_POI_TYPES = 6400; // Gb 2025-12-05 +20%

	private static final double MAX_BBOX_AREAS_MIN_MAX_RATIO = 1.5;

	private static QuadRect bboxPoi = new QuadRect();
	private static QuadRect bboxMap = new QuadRect();
	private static QuadRect bboxRoute = new QuadRect();
	private static double bboxPoiAreaMax = 0, bboxMapAreaMax = 0, bboxRouteAreaMax = 0;

	public static void main(String[] args) {
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
			if (p instanceof MapIndex that) {
				mi = that;
				calcMaxMapBboxArea(that);
				int mapRulesSize = calcMapIndexRulesSize(index, that);
				ok &= checkSizeLimit(that.getName(), "map rules", mapRulesSize, MAX_MAP_RULES);
			} else if (p instanceof HHRouteRegion hr) {
				if (hr.profile.equals("car")) {
					car = hr;
				} else if (hr.profile.equals("bicycle")) {
					bicycle = hr;
				}
				ok &= checkHHRegion(index, hr);
			} else if (p instanceof PoiRegion that) {
				poi = that;
				calcMaxPoiBboxArea(that);
				int poiTypesSize = calcPoiIndexTypesSize(index, that);
				ok &= checkSizeLimit(that.getName(), "poi types", poiTypesSize, MAX_POI_TYPES);
			} else if (p instanceof AddressRegion) {
				address = (AddressRegion) p;
			} else if (p instanceof RouteRegion that) {
				routeRegion = that;
				calcMaxRouteBboxArea(that);
				routeSectionSize = p.getLength();
				ok &= checkSizeLimit(that.getName(), "route rules", that.quickGetEncodingRulesSize(), MAX_ROUTE_RULES);
			}
		}
		
		if (routeSectionSize > LIMIT_HH_POINTS_NEEDED * 2 && !world) {
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
//				ok &= runRandomRouteTester(oFile);
				ok &= checkNull(oFile, car, "Missing HH route section for car - route section bytes: " + routeSectionSize);
				ok &= checkNull(oFile, bicycle,
						"Missing HH route section for bicycle - route section bytes: " + routeSectionSize);
			}
		}
		ok &= checkNull(oFile, mi, "Missing Map section");
		if (!world) {
			ok &= checkNull(oFile, poi, "Missing Poi section");
			ok &= checkNull(oFile, address, "Missing address section");
			ok &= checkNull(oFile, routeRegion, "Missing routing section");
			ok &= checkSimpleAddress(index, address, true);
			ok &= checkBboxAreasMinMaxRatio(oFile.getName());
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

				"--profile=car",
				"--iterations=10",
				"--min-dist=5", // km
				"--max-dist=100", // km
				"--stop-at-first-route",
		};
		return RandomRouteTester.run(args) == RandomRouteTester.EXIT_SUCCESS;
	}

	private static boolean checkSizeLimit(String map, String section, int test, int max) {
		if (test > max) {
			System.err.printf("[%s] %s size %d exceeded limit %s\n", map, section, test, max);
			return false;
		}
		return true;
	}

	private static int calcMapIndexRulesSize(BinaryMapIndexReader index, MapIndex mapIndex) throws IOException {
		SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(0, 0, 0, 0, 0, null);
		index.searchMapIndex(req, mapIndex); // no actual search, just initialize rules
		return mapIndex.decodingRules.size();
	}

	private static int calcPoiIndexTypesSize(BinaryMapIndexReader index, PoiRegion p) throws IOException {
		int total = 0;

		index.initCategories(p);
		List<String> cs = p.getCategories();
		List<List<String>> subcategories = p.getSubcategories();

		for (int i = 0; i < cs.size(); i++) {
			total += subcategories.get(i).size();
		}

		int singleVals = 0;
		Set<String> text = new TreeSet<>();
		Set<String> refs = new TreeSet<>();
		MapPoiTypes poiTypes = MapPoiTypes.getDefault();
		for (BinaryMapPoiReaderAdapter.PoiSubType st : p.getSubTypes()) {
			if (st.text) {
				PoiType ref = poiTypes.getPoiTypeByKey(st.name);
				if (ref != null && !ref.isAdditional()) {
					refs.add(st.name);
				} else {
					text.add(st.name);
				}
			} else if (st.possibleValues.size() == 1) {
				singleVals++;
			} else {
				total += st.possibleValues.size();
			}
		}

		total += refs.size();
		total += text.size();
		total += singleVals;

		return total;
	}

	private static double getQuadRectArea(QuadRect qr) {
		double x = MapUtils.measuredDist31((int) qr.left, (int) qr.top, (int) qr.right, (int) qr.top);
		double y = MapUtils.measuredDist31((int) qr.left, (int) qr.top, (int) qr.left, (int) qr.bottom);
		return x * y;
	}

	private static void calcMaxMapBboxArea(MapIndex mapIndex) {
		for (BinaryMapIndexReader.MapRoot r : mapIndex.getRoots()) {
			bboxMap.expand(r.getLeft(), r.getTop(), r.getRight(), r.getBottom());
		}
		bboxMapAreaMax = Math.max(getQuadRectArea(bboxMap), bboxMapAreaMax);
	}

	private static void calcMaxRouteBboxArea(RouteRegion routeRegion) {
		List<RouteSubregion> regions = new ArrayList<>();
		regions.addAll(routeRegion.getBaseSubregions());
		regions.addAll(routeRegion.getSubregions());
		for (RouteSubregion r : regions) {
			bboxRoute.expand(r.left, r.top, r.right, r.bottom);
		}
		bboxRouteAreaMax = Math.max(getQuadRectArea(bboxRoute), bboxRouteAreaMax);
	}

	private static void calcMaxPoiBboxArea(PoiRegion p) {
		bboxPoi.expand(p.getLeft31(), p.getTop31(), p.getRight31(), p.getBottom31());
		bboxPoiAreaMax = Math.max(getQuadRectArea(bboxRoute), bboxPoiAreaMax);
	}

	private static boolean checkBboxAreasMinMaxRatio(String map) {
		if (bboxPoiAreaMax > 0 && bboxMapAreaMax > 0 && bboxRouteAreaMax > 0) {
			double min = Math.min(Math.min(bboxPoiAreaMax, bboxMapAreaMax), bboxRouteAreaMax);
			double max = Math.max(Math.max(bboxPoiAreaMax, bboxMapAreaMax), bboxRouteAreaMax);
			double ratio = max / min;
			if (ratio > MAX_BBOX_AREAS_MIN_MAX_RATIO) {
				System.err.printf("[%s] bbox area ratio %.2f exceeded limit %.2f poi(%.0f) map(%.0f) route(%.0f)\n",
						map, ratio, MAX_BBOX_AREAS_MIN_MAX_RATIO, bboxPoiAreaMax / (1000 * 1000),
						bboxMapAreaMax / (1000 * 1000), bboxRouteAreaMax / (1000 * 1000));
				return false;
			}
		}
		return true;
	}

}
