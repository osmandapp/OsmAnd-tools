package net.osmand.data.diff;

import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.MapZooms.MapZoomPair;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.Amenity;
import net.osmand.map.OsmandRegions;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class ObfRegionSplitter {
	
	
	public static void main(String[] args) throws IOException {
		if(args.length == 1 && args[0].equals("test")) {
			args = new String[5];
			args[0] = "/Users/victorshcherb/osmand/maps/diff/2017_08_28_01_00_diff.obf.gz";
			args[1] = "/Users/victorshcherb/osmand/maps/diff/regions";
			args[2] = "/Users/victorshcherb/osmand/repos/android/OsmAnd/src/net/osmand/map/regions.ocbf";
			args[3] = "";
			args[4] = "_01_00";
		}
		if (args.length <= 3) {
			System.err.println("Usage: <path_to_world_obf_diff> <path_to_result_folder> <path_to_regions.ocbf> <subfolder_name> <file_suffix>");
			return;
		}
		
		ObfRegionSplitter thisGenerator = new ObfRegionSplitter();
		thisGenerator.split(args);
	}

	private void split(String[] args) throws IOException {
		File worldObf = new File(args[0]);
		File ocbfFile = new File(args[2]);
		File dir = new File(args[1]);
		String subFolder = args.length > 3 ? args[3] : "";
		String fileSuffix = args.length > 4 ? args[4] : "";
		if (!worldObf.exists() || !ocbfFile.exists()) {
			System.out.println("Incorrect file!");
			System.exit(1);
		}
		if (!dir.exists()) {
			dir.mkdir();
		}
		try {
			ObfFileInMemory fl = new ObfFileInMemory();
			fl.readObfFiles(Collections.singletonList(worldObf));
			OsmandRegions osmandRegions = new OsmandRegions();
			osmandRegions.prepareFile(ocbfFile.getAbsolutePath());
			osmandRegions.cacheAllCountries();

			Map<String, Map<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>>> regionsMapData = splitRegionMapData(fl,
					osmandRegions);
			Map<String, TLongObjectHashMap<RouteDataObject>> regionsRouteData = splitRegionRouteData(fl,
					osmandRegions);
			Map<String, TLongObjectHashMap<Amenity>> regionsPoiData = splitRegionPoiData(fl, osmandRegions);
			
			TreeSet<String> regionNames = new TreeSet<>();
			regionNames.addAll(regionsMapData.keySet());
			regionNames.addAll(regionsRouteData.keySet());

			for (String regionName : regionNames) {
				File folder = new File(dir, regionName);
				if (!Algorithms.isEmpty(subFolder)) {
					folder = new File(folder, subFolder);
				}
				folder.mkdirs();
				File result = new File(folder, Algorithms.capitalizeFirstLetter(regionName) + fileSuffix + ".obf.gz");
				ObfFileInMemory obf = new ObfFileInMemory();

				Map<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>> mp = regionsMapData.get(regionName);
				if (mp != null) {
					for (MapZoomPair mzPair : mp.keySet()) {
						obf.putMapObjects(mzPair, mp.get(mzPair).valueCollection(), true);
					}
				}
				
				TLongObjectHashMap<RouteDataObject> ro = regionsRouteData.get(regionName);
				if(ro != null) {
					obf.putRoutingData(ro, true);
				}
				// TODO split POI
				// TODO split Transport
				
				obf.updateTimestamp(fl.getTimestamp());
				obf.writeFile(result);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
			

	private Map<String, TLongObjectHashMap<Amenity>> splitRegionPoiData(ObfFileInMemory fl,
			OsmandRegions osmandRegions) throws IOException {
		Map<String, TLongObjectHashMap<Amenity>> result = new HashMap<>();
		TLongObjectHashMap<Amenity> poiData = fl.getPoiData();
		for (Amenity obj : poiData.valueCollection()) {
			int x = MapUtils.get31TileNumberX(obj.getLocation().getLatitude());
			int y = MapUtils.get31TileNumberY(obj.getLocation().getLongitude());
			List<BinaryMapDataObject> l = osmandRegions.query(x, y);
			for (BinaryMapDataObject b : l) {
				if (osmandRegions.contain(b, x, y)) {
					String dw = osmandRegions.getDownloadName(b);
					if (!Algorithms.isEmpty(dw) && osmandRegions.isDownloadOfType(b, OsmandRegions.MAP_TYPE)) {
						TLongObjectHashMap<Amenity> mp = result.get(dw);
						if (mp == null) {
							mp = new TLongObjectHashMap<>();
							result.put(dw, mp);
						}
						mp.put(obj.getId(), obj);
					}
				}
			}
		}
		return result;
	}

	private Map<String, TLongObjectHashMap<RouteDataObject>> splitRegionRouteData(ObfFileInMemory fl,
			OsmandRegions osmandRegions) throws IOException {
		Map<String, TLongObjectHashMap<RouteDataObject>> result = new HashMap<>();
		TLongObjectHashMap<RouteDataObject> routingData = fl.getRoutingData();
		for (RouteDataObject obj : routingData.valueCollection()) {
			int x = obj.getPoint31XTile(0);
			int y = obj.getPoint31YTile(0);
			List<BinaryMapDataObject> l = osmandRegions.query(x, y);
			for (BinaryMapDataObject b : l) {
				if (osmandRegions.contain(b, x, y)) {
					String dw = osmandRegions.getDownloadName(b);
					if (!Algorithms.isEmpty(dw) && osmandRegions.isDownloadOfType(b, OsmandRegions.MAP_TYPE)) {
						TLongObjectHashMap<RouteDataObject> mp = result.get(dw);
						if (mp == null) {
							mp = new TLongObjectHashMap<>();
							result.put(dw, mp);
						}
						mp.put(obj.getId(), obj);
					}
				}
			}
		}
		return result;
	}

	private Map<String, Map<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>>> splitRegionMapData(ObfFileInMemory allMapObjects,
			OsmandRegions osmandRegions) throws IOException {
		Map<String, Map<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>>> result = new HashMap<>();
		for (MapZoomPair p : allMapObjects.getZooms()) {
			TLongObjectHashMap<BinaryMapDataObject> objects = allMapObjects.get(p);
			for (BinaryMapDataObject obj : objects.valueCollection()) {
				int x = obj.getPoint31XTile(0);
				int y = obj.getPoint31YTile(0);
				List<BinaryMapDataObject> l = osmandRegions.query(x, y);
				for (BinaryMapDataObject b : l) {
					if (osmandRegions.contain(b, x, y)) {
						String dw = osmandRegions.getDownloadName(b);
						if (!Algorithms.isEmpty(dw) && osmandRegions.isDownloadOfType(b, OsmandRegions.MAP_TYPE)) {
							Map<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>> mp = result.get(dw);
							if(mp == null) {
								mp = new LinkedHashMap<>();
								result.put(dw, mp);
							}
							TLongObjectHashMap<BinaryMapDataObject> list = mp.get(p);
							if (list == null) {
								list = new TLongObjectHashMap<>();
								mp.put(p, list);
							}
							list.put(obj.getId(), obj);
						}
					}
				}
			}
		}
		return result;
	}
}
