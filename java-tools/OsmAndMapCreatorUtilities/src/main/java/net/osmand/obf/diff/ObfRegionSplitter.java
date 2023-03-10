package net.osmand.obf.diff;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapRouteReaderAdapter;
import net.osmand.binary.MapZooms.MapZoomPair;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.Amenity;
import net.osmand.data.TransportStop;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

import gnu.trove.map.hash.TLongObjectHashMap;

public class ObfRegionSplitter {
	
	
	public static void main(String[] args) throws IOException {
		if(args.length == 1 && args[0].equals("test")) {
			args = new String[6];
			args[0] = "/Users/macmini/OsmAnd/overpass/23_03_10_06_00.obf.gz";
			args[1] = "/Users/macmini/OsmAnd/overpass/split_obf/";
			args[2] = "";
			args[3] = "_11_05";
			args[4] = "--srtm=/Users/macmini/OsmAnd/overpass/srtm/";
		}
		if (args.length <= 3) {
			System.err.println("Usage: <path_to_world_obf_diff> <path_to_result_folder> <subfolder_name> <file_suffix> --srtm=<folder with srtm>");
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

		IndexHeightData heightData = null;
		if (args.length > 4 && args[4].startsWith("--srtm=")) {
			String srtmDataFolderUrl = args[4].replace("--srtm=", "");
			File heightDir = new File(srtmDataFolderUrl);
			if (heightDir.exists()) {
				heightData = new IndexHeightData();
				heightData.setSrtmData(srtmDataFolderUrl, dir);
			}
		}

		try {
			ObfFileInMemory fl = new ObfFileInMemory();
			fl.readObfFiles(Collections.singletonList(worldObf));
			OsmandRegions osmandRegions = new OsmandRegions();
			osmandRegions.prepareFile();
			osmandRegions.cacheAllCountries();

			Map<String, Map<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>>> regionsMapData = splitRegionMapData(fl,osmandRegions);
			Map<String, TLongObjectHashMap<RouteDataObject>> regionsRouteData = splitRegionRouteData(fl, osmandRegions, heightData);
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
					if (!Algorithms.isEmpty(dw) && (wr.isRegionMapDownload() || wr.isRegionRoadsDownload())) {
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
			OsmandRegions osmandRegions, IndexHeightData heightData) throws IOException {
		Map<String, TLongObjectHashMap<RouteDataObject>> result = new HashMap<>();
		TLongObjectHashMap<RouteDataObject> routingData = fl.getRoutingData();
		long time = System.currentTimeMillis();
		int count = 0;
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
					if (!Algorithms.isEmpty(dw) && (wr.isRegionMapDownload() || wr.isRegionRoadsDownload())) {
						TLongObjectHashMap<RouteDataObject> mp = result.get(dw);
						if (mp == null) {
							mp = new TLongObjectHashMap<>();
							result.put(dw, mp);
						}
						if (heightData != null) {
							attachElevationData(obj, heightData);
							count++;
						}
						mp.put(obj.getId(), obj);
					}
				}
			}
		}
		long t = (System.currentTimeMillis() - time) / 1000L;
		long p = count / t;
		System.out.println("Attach elevation data to ROUTE DATA section.");
		System.out.println("Time total:" + t + " count:" + count + " per/sec:" + p);
		return result;
	}

	private void attachElevationData(RouteDataObject obj, IndexHeightData heightData) {
		List<Node> nodes = new ArrayList<>();
		int id = 1;
		for(int i = 0; i < obj.getPointsLength(); i++, id++) {
			double lon = MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
			double lat = MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
			Node n = new Node(lat, lon, id);
			nodes.add(n);
		}

		// Prepare Way with Nodes
		Map<String, String> cachedNodeTags = new HashMap<>();
		for (int i = 0; i < obj.getPointsLength(); i++) {
			if (obj.getPointNames(i) != null) {
				//String[] vs = obj.getPointNames(i);
				int[] keys = obj.getPointNameTypes(i);
				for (int j = 0; j < keys.length; j++) {
					BinaryMapRouteReaderAdapter.RouteTypeRule rt = obj.region.quickGetEncodingRule(keys[j]);
					nodes.get(i).putTag(rt.getTag(), rt.getValue());
					cachedNodeTags.put(rt.getTag(), rt.getValue());
				}
			}
			if (obj.getPointTypes(i) != null) {
				int[] keys = obj.getPointTypes(i);
				for (int j = 0; j < keys.length; j++) {
					BinaryMapRouteReaderAdapter.RouteTypeRule rt = obj.region.quickGetEncodingRule(keys[j]);
					nodes.get(i).putTag(rt.getTag(), rt.getValue());
					cachedNodeTags.put(rt.getTag(), rt.getValue());
				}
			}
		}

		Way way = new Way(obj.getId(), nodes);
		Map<String, String> cachedTags = new HashMap<>();
		for (int t : obj.getTypes()) {
			String tag = obj.region.routeEncodingRules.get(t).getTag();
			String val = obj.region.routeEncodingRules.get(t).getValue();
			way.putTag(tag, val);
			cachedTags.put(tag, val);
		}

		heightData.proccess(way);

		// Write result to RouteDataObject
		if (way.getTags().size() > obj.types.length) {
			int[] types = Arrays.copyOf(obj.types, way.getTags().size());
			int index = obj.types.length;
			for (Map.Entry<String, String> entry : way.getTags().entrySet()) {
				String tag = entry.getKey();
				if (!cachedTags.containsKey(tag)) {
					String val = entry.getValue();
					int ruleId = obj.region.searchRouteEncodingRule(tag, val);
					if (ruleId == -1) {
						ruleId = obj.region.routeEncodingRules.size();
						obj.region.initRouteEncodingRule(ruleId, tag, val);
					}
					if (index < types.length) {
						types[index] = ruleId;
					} else {
						System.out.println("Error. Write elevation tags of way. Array index out of bounds exception");
						break;
					}
					index++;
				}
			}
			obj.types = types;
		}

		nodes = way.getNodes();
		Map <Integer, List<Integer>> pointTypesMap = new HashMap<>();
		for (int i = 0; i < obj.getPointsLength(); i++) {
			Node n = nodes.get(i);
			Map<String, String> tags = n.getTags();
			if (tags.size() == 0) {
				continue;
			}
			List<Integer> rulesList = new ArrayList<>();
			for (Map.Entry<String, String> entry : tags.entrySet()) {
				String tag = entry.getKey();
				if (!cachedNodeTags.containsKey(tag)) {
					String val = entry.getValue();
					int ruleId = obj.region.searchRouteEncodingRule(tag, val);
					if (ruleId == -1) {
						ruleId = obj.region.routeEncodingRules.size();
						obj.region.initRouteEncodingRule(ruleId, tag, val);
					}
					rulesList.add(ruleId);
				}
			}
			pointTypesMap.put(i, rulesList);
		}

		if (pointTypesMap.size() > 0) {
			if (obj.pointTypes == null) {
				obj.pointTypes = new int[pointTypesMap.size()][];
			}
			for (Map.Entry<Integer, List<Integer>> entry : pointTypesMap.entrySet()) {
				int pointIndex = entry.getKey();
				List<Integer> pointTypes = entry.getValue();
				if (pointTypes.size() == 0) {
					continue;
				}
				if (pointIndex >= obj.pointTypes.length) {
					obj.pointTypes = Arrays.copyOf(obj.pointTypes, pointIndex + 1);
				}
				int index = 0;
				if (obj.pointTypes[pointIndex] == null) {
					obj.pointTypes[pointIndex] = new int[pointTypes.size()];
				} else if (obj.pointTypes[pointIndex].length > 0) {
					index = obj.pointTypes[pointIndex].length;
					obj.pointTypes[pointIndex] = Arrays.copyOf(obj.pointTypes[pointIndex], obj.pointTypes[pointIndex].length + pointTypes.size());
				}
				for (int i = 0; i < pointTypes.size(); i++) {
					int type = pointTypes.get(i);
					obj.pointTypes[pointIndex][index] = type;
					index++;
				}
			}
		}
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
					if (!Algorithms.isEmpty(dw) && (wr.isRegionMapDownload() || wr.isRegionRoadsDownload())) {
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
						if (!Algorithms.isEmpty(dw) && (wr.isRegionMapDownload() || wr.isRegionRoadsDownload())) {
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
