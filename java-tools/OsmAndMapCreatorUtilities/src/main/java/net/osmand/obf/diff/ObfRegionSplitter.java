package net.osmand.obf.diff;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.MapZooms.MapZoomPair;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.Amenity;
import net.osmand.data.TransportStop;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import gnu.trove.map.hash.TLongObjectHashMap;

public class ObfRegionSplitter {
	
	
	public static void main(String[] args) throws IOException {
		if(args.length == 1 && args[0].equals("test")) {
			args = new String[5];
			args[0] = "/Users/victorshcherb/osmand/maps/olive/19_07_29_20_30_diff.obf";
			args[1] = "/Users/victorshcherb/osmand/maps/olive/regions";
			args[2] = "";
			args[3] = "_20_30";
		}
		if (args.length <= 3) {
			System.err.println("Usage: <path_to_world_obf_diff> <path_to_result_folder> <subfolder_name> <file_suffix>");
			return;
		}
		
		ObfRegionSplitter thisGenerator = new ObfRegionSplitter();
		thisGenerator.split(args);
	}

	private void split(String[] args) throws IOException {
		File worldObf = new File(args[0]);
		File dir = new File(args[1]);
		String subFolder = args.length > 2 ? args[2] : "";
		String fileSuffix = args.length > 3 ? args[3] : "";
		if (!worldObf.exists()) {
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
			osmandRegions.prepareFile();
			osmandRegions.cacheAllCountries();

			Map<String, Map<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>>> regionsMapData = splitRegionMapData(fl,osmandRegions);
			Map<String, TLongObjectHashMap<RouteDataObject>> regionsRouteData = splitRegionRouteData(fl, osmandRegions);
			Map<String, TLongObjectHashMap<Map<String, Amenity>>> regionsPoiData = splitRegionPoiData(fl, osmandRegions);
			Map<String, TLongObjectHashMap<TransportStop>> regionsTransportData = splitRegionTransportData(fl, osmandRegions);
			TreeSet<String> regionNames = new TreeSet<>();
			regionNames.addAll(regionsMapData.keySet());
			regionNames.addAll(regionsRouteData.keySet());
			regionNames.addAll(regionsPoiData.keySet());
			regionNames.addAll(regionsTransportData.keySet());

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
				if (ro != null) {
					obf.putRoutingData(ro, true);
				}
				TLongObjectHashMap<Map<String, Amenity>> poi = regionsPoiData.get(regionName);
				if (poi != null) {
					obf.putPoiData(poi, true);
				}
				TLongObjectHashMap<TransportStop> stops = regionsTransportData.get(regionName);
				if (stops != null) {
					Collection<TransportStop> stopsCollection = stops.valueCollection();
					obf.setTransportRoutes(fl.getTransportRoutes());
					obf.putTransportStops(stopsCollection, true);
				}
				obf.updateTimestamp(fl.getTimestamp());
				obf.writeFile(result, true);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
			

	private Map<String, TLongObjectHashMap<Map<String, Amenity>>> splitRegionPoiData(ObfFileInMemory fl,
 			OsmandRegions osmandRegions) throws IOException {
 		Map<String, TLongObjectHashMap<Map<String, Amenity>>> result = new HashMap<>();
 		TLongObjectHashMap<Map<String, Amenity>> poiData = fl.getPoiObjects();
 		for (Map<String, Amenity> objMap : poiData.valueCollection()) {
 			Amenity obj = objMap.values().iterator().next();
 			int x = MapUtils.get31TileNumberX(obj.getLocation().getLongitude());
 			int y = MapUtils.get31TileNumberY(obj.getLocation().getLatitude());
 			List<BinaryMapDataObject> l = osmandRegions.query(x, y);
 			for (BinaryMapDataObject b : l) {
 				if (osmandRegions.contain(b, x, y)) {
 					String dw = osmandRegions.getDownloadName(b);
 					WorldRegion wr = osmandRegions.getRegionDataByDownloadName(dw);
 					if (dw == null || wr == null) {
						continue;
					}
					if (!Algorithms.isEmpty(dw) && wr.isRegionMapDownload()) {
 						TLongObjectHashMap<Map<String, Amenity>> mp = result.get(dw);
 						if (mp == null) {
 							mp = new TLongObjectHashMap<>();
 							result.put(dw, mp);
 						}
 						mp.put(obj.getId(), objMap);
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
//			if(obj.getPointsLength() == 0) {
//				continue;
//			}
			int x = obj.getPoint31XTile(0);
			int y = obj.getPoint31YTile(0);
			List<BinaryMapDataObject> l = osmandRegions.query(x, y);
			for (BinaryMapDataObject b : l) {
				if (osmandRegions.contain(b, x, y)) {
					String dw = osmandRegions.getDownloadName(b);
					WorldRegion wr = osmandRegions.getRegionDataByDownloadName(dw);
					if (dw == null || wr == null) {
						continue;
					}
					if (!Algorithms.isEmpty(dw) && wr.isRegionMapDownload()) {
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

	private Map<String, TLongObjectHashMap<TransportStop>> splitRegionTransportData(ObfFileInMemory fl,
			OsmandRegions osmandRegions) throws IOException {
		Map<String, TLongObjectHashMap<TransportStop>> result = new HashMap<>();
		TLongObjectHashMap<TransportStop> transportStops = fl.getTransportStops();
		for (TransportStop stop : transportStops.valueCollection()) {
			int x = stop.x31;
			int y = stop.y31;
			List<BinaryMapDataObject> l = osmandRegions.query(x, y);
			for (BinaryMapDataObject b : l) {
				if (osmandRegions.contain(b, x, y)) {
					String dw = osmandRegions.getDownloadName(b);
					WorldRegion wr = osmandRegions.getRegionDataByDownloadName(dw);
					if (dw == null || wr == null) {
						continue;
					}
					if (!Algorithms.isEmpty(dw) && wr.isRegionMapDownload()) {
						TLongObjectHashMap<TransportStop> mp = result.get(dw);
						if (mp == null) {
							mp = new TLongObjectHashMap<>();
							result.put(dw, mp);
						}
						mp.put(stop.getId(), stop);
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
						WorldRegion wr = osmandRegions.getRegionDataByDownloadName(dw);
						if (dw == null || wr == null) {
							continue;
						}
						if (!Algorithms.isEmpty(dw) && wr.isRegionMapDownload()) {
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
