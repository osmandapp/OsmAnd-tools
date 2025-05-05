package net.osmand.obf;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.osmand.binary.BinaryHHRouteReaderAdapter.HHRouteRegion;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.RouteDataObject;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;

public class ObfChecker {

	private static int LIMIT_HH_POINTS_NEEDED = 100_000; 
	public static void main(String[] args) {
		// TODO
//		OsmAndMapCreator/utilities.sh random-route-tester \
//        --maps-dir="$OBF_FOLDER" --iterations=3 --obf-prefix="$FILE" \
//        --profile="$PROFILE" --min-dist=1 --no-conditionals \
//        --max-dist=50 2>&1
		if (args.length == 1 && args[0].equals("--test")) {
			args = new String[] { System.getProperty("maps.dir") + "Canada_nunavut_northamerica_2.obf" };
		}
		Map<String, String> argMap = new LinkedHashMap<String, String>();
		List<String> files = new ArrayList<String>();
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
		for (BinaryIndexPart p : index.getIndexes()) {
			if (p instanceof MapIndex) {
				mi = (MapIndex) p;
			} else if (p instanceof HHRouteRegion) {
				HHRouteRegion hr = (HHRouteRegion) p;
				if (hr.profile.equals("car")) {
					car = hr;
				} else if (hr.profile.equals("bicycle")) {
					bicycle = hr;
				}
			} else if (p instanceof PoiRegion) {
				poi = (PoiRegion) p;
			} else if (p instanceof AddressRegion) {
				address = (AddressRegion) p;
			} else if (p instanceof RouteRegion) {
				routeRegion = (RouteRegion) p;
				routeSectionSize = p.getLength();
			}
		}
		
		if (routeSectionSize > LIMIT_HH_POINTS_NEEDED * 2 && (car == null || bicycle == null)) {
			int cnt = 0;
			SearchRequest<RouteDataObject> sr = BinaryMapIndexReader.buildSearchRouteRequest(0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
			List<RouteSubregion> regions = index.searchRouteIndexTree(sr,  routeRegion.getSubregions());
			for (RouteSubregion rs : regions) {
				if (cnt > LIMIT_HH_POINTS_NEEDED) {
					break;
				}
				List<RouteDataObject> ls = index.loadRouteIndexData(rs);
				for (RouteDataObject rdo : ls) {
					if (rdo.getHighway() != null) {
						cnt += rdo.getPointsLength();
					}
				}
			}
			if (cnt > LIMIT_HH_POINTS_NEEDED) {
				ok &= checkNull(car, "Missing HH route section for car - route section bytes: " + routeSectionSize);
				ok &= checkNull(bicycle,
						"Missing HH route section for bicycle - route section bytes: " + routeSectionSize);
			}
		}
		ok &= checkNull(mi, "Missing Map section");
		ok &= checkNull(poi, "Missing Poi section");
		ok &= checkNull(address, "Missing address section");
		ok &= checkNull(routeRegion, "Missing routing section");

		index.close();
		return ok;
	}

	private static boolean checkNull(Object o, String string) {
		if (o == null) {
			System.err.println(string);
			return false;
		}
		return true;
	}
}
