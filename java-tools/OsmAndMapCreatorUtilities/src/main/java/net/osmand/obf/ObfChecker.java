package net.osmand.obf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import gnu.trove.map.hash.TLongObjectHashMap;
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
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;

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
					if (rdo != null && rdo.getHighway() != null) {
						cnt += rdo.getPointsLength();
					}
				}
			}
			if (cnt > LIMIT_HH_POINTS_NEEDED) {
				ok &= checkNull(oFile, car, "Missing HH route section for car - route section bytes: " + routeSectionSize);
				ok &= checkNull(oFile, bicycle,
						"Missing HH route section for bicycle - route section bytes: " + routeSectionSize);
			}
		}
		ok &= checkNull(oFile, mi, "Missing Map section");
		if (!oFile.getName().toLowerCase().startsWith("world")) {
			ok &= checkNull(oFile, poi, "Missing Poi section");
			ok &= checkNull(oFile, address, "Missing address section");
			ok &= checkNull(oFile, routeRegion, "Missing routing section");
		}
		
		index.close();
		return ok;
	}

	private static boolean checkHHRegion(BinaryMapIndexReader index, HHRouteRegion hr) throws IOException {
		boolean ok = true;
		TLongObjectHashMap<NetworkDBPoint> pnts = index.initHHPoints(hr, (short) 0, NetworkDBPoint.class);
		for (NetworkDBPoint pnt : pnts.valueCollection()) {
			if (pnt.dualPoint == null) {
				System.err.printf("Error in map %s - %s missing dual point \n", index.getFile().getName(), pnt.toString());
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
}
