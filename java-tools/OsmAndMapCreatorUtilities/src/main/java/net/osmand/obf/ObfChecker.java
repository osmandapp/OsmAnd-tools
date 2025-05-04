package net.osmand.obf;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.osmand.binary.BinaryHHRouteReaderAdapter.HHRouteRegion;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;

public class ObfChecker {

	public static void main(String[] args) {
		// TODO
//		OsmAndMapCreator/utilities.sh random-route-tester \
//        --maps-dir="$OBF_FOLDER" --iterations=3 --obf-prefix="$FILE" \
//        --profile="$PROFILE" --min-dist=1 --no-conditionals \
//        --max-dist=50 2>&1
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
		index.close();
		ok &= checkNull(car, "Missing HH route section for car - route section bytes: " + routeSectionSize);
		ok &= checkNull(bicycle, "Missing HH route section for bicycle - route section bytes: " + routeSectionSize);
		ok &= checkNull(mi, "Missing Map section");
		ok &= checkNull(poi, "Missing Poi section");
		ok &= checkNull(address, "Missing address section");
		ok &= checkNull(routeRegion, "Missing routing section");

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
