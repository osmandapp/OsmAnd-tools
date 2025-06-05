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
			args = new String[4];
			args[0] = "~/24_08_04_21_40.obf";
			args[1] = "~/Desktop/split/";
			args[2] = "";
			args[3] = "_24_08_04_21_40";
//			args[4] = "--srtm=/Users/macmini/OsmAnd/overpass/srtm/";
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
 				if (OsmandRegions.contain(b, x, y)) {
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
		Map<Integer, List<Long>> sortedMap = createSortedMap(routingData);
		for (List<Long> keys : sortedMap.values()) {
			for (long key : keys) {
				RouteDataObject obj = routingData.get(key);
				if (isPossibleVandalism(obj)) {
					continue;
				}
				if (heightData != null) {
					attachElevationData(obj, heightData);
					count++;
				}
				int x = obj.getPoint31XTile(0);
				int y = obj.getPoint31YTile(0);
				List<BinaryMapDataObject> l = osmandRegions.query(x, y);
				for (BinaryMapDataObject b : l) {
					if (OsmandRegions.contain(b, x, y)) {
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
							
							mp.put(obj.getId(), obj);
						}
					}
				}
			}
		}
		long t = (System.currentTimeMillis() - time) / 1000L;
		long p = t > 0 ? count / t : count;
		System.out.println("Attach elevation data to ROUTE DATA section.");
		System.out.println("Time total:" + t + " count:" + count + " per/sec:" + p);
		return result;
	}

	private Map<Integer, List<Long>> createSortedMap(TLongObjectHashMap<RouteDataObject> routingData) {
		Map<Integer, List<Long>> sortedMap = new HashMap<>();
		for (long key : routingData.keys()) {
			RouteDataObject rdo = routingData.get(key);
			if (rdo.getPointsLength() > 0) {
				double lon = MapUtils.get31LongitudeX(rdo.getPoint31XTile(0));
				double lat = MapUtils.get31LatitudeY(rdo.getPoint31YTile(0));
				int lt = (int) lat;
				int ln = (int) lon;
				double lonDelta = lon - ln;
				double latDelta = lat - lt;
				if (lonDelta < 0) {
					ln -= 1;
				}
				if (latDelta < 0) {
					lt -= 1;
				}
				int id = IndexHeightData.getTileId(lt, ln);
				if (sortedMap.containsKey(id)) {
					List<Long> list = sortedMap.get(id);
					list.add(key);
				} else {
					List<Long> list = new ArrayList<>();
					list.add(key);
					sortedMap.put(id, list);
				}
			}
		}
		return sortedMap;
	}

	private void attachElevationData(RouteDataObject obj, IndexHeightData heightData) {

		// Prepare Way with Nodes
		List<Node> nodes = new ArrayList<>();
		int id = 1;
		for (int i = 0; i < obj.getPointsLength(); i++, id++) {
			double lon = MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
			double lat = MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
			Node n = new Node(lat, lon, id);
			nodes.add(n);
		}

		Way simpleWay = new Way(obj.getId(), nodes);
		List<String> removeAfterSrtm = new ArrayList<>();
		for (int t : obj.getTypes()) {
			BinaryMapRouteReaderAdapter.RouteTypeRule type = obj.region.routeEncodingRules.get(t);
			String tag = type.getTag();
			if (IndexHeightData.ELEVATION_TAGS.contains(tag)) {
				// already has SRTM tags
				return;
			}
			// simpleWay requires original tags for SRTM processing
			simpleWay.putTag(tag, type.getValue());
			removeAfterSrtm.add(tag);
		}

		heightData.proccess(simpleWay);
		simpleWay.removeTags(removeAfterSrtm.toArray(new String[0]));

		// Write result to RouteDataObject
		if (simpleWay.getTags().size() > 0) {
			int[] types = Arrays.copyOf(obj.types, obj.types.length + simpleWay.getTags().size());
			int index = obj.types.length;
			for (Map.Entry<String, String> entry : simpleWay.getTags().entrySet()) {
				String tag = entry.getKey();
				String val = entry.getValue();
				int ruleId = obj.region.searchRouteEncodingRule(tag, val);
				if (ruleId == -1) {
					ruleId = obj.region.routeEncodingRules.size();
					obj.region.initRouteEncodingRule(ruleId, tag, val);
				}
				types[index] = ruleId;
				index++;
			}
			obj.types = types;
		}

		nodes = simpleWay.getNodes();
		for (int i = 0; i < obj.getPointsLength(); i++) {
			Node n = nodes.get(i);
			Map<String, String> tags = n.getTags();
			if (tags.size() == 0) {
				continue;
			}
			int ind = 0;
			int size = tags.size();
			if (obj.pointTypes != null && obj.pointTypes.length > i && obj.pointTypes[i] != null && obj.pointTypes[i].length > 0) {
				// save previous pointTypes
				ind = obj.pointTypes[i].length;
				obj.pointTypes[i] = Arrays.copyOf(obj.pointTypes[i], obj.pointTypes[i].length + size);
			} else {
				obj.setPointTypes(i, new int[size]);
			}
			for (Map.Entry<String, String> entry : tags.entrySet()) {
				//no other tags in Node except elevation
				String tag = entry.getKey();
				String val = entry.getValue();
				int ruleId = obj.region.searchRouteEncodingRule(tag, val);
				if (ruleId == -1) {
					ruleId = obj.region.routeEncodingRules.size();
					obj.region.initRouteEncodingRule(ruleId, tag, val);
				}
				obj.pointTypes[i][ind] = ruleId;
				ind++;
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
				if (OsmandRegions.contain(b, x, y)) {
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
	
	private boolean isPossibleVandalism(BinaryMapDataObject b) {
		if ("ferry".equals(b.getTagValue("route"))) {
			return false;
		}
		for (int j = 1; j < b.getPointsLength(); j++) {
			double dist = MapUtils.squareRootDist31(b.getPoint31XTile(j - 1), b.getPoint31YTile(j - 1),
					b.getPoint31XTile(j), b.getPoint31YTile(j));
			if (dist > IndexHeightData.MAX_LAT_LON_DIST) {
				return true;
			}
		}
		return false;
	}
	
	private boolean isPossibleVandalism(RouteDataObject b) {
		if ("ferry".equals(b.getRoute())) {
			return false;
		}
		for (int j = 1; j < b.getPointsLength(); j++) {
			double dist = MapUtils.squareRootDist31(b.getPoint31XTile(j - 1), b.getPoint31YTile(j - 1),
					b.getPoint31XTile(j), b.getPoint31YTile(j));
			if (dist > IndexHeightData.MAX_LAT_LON_DIST) {
				return true;
			}
		}
		return false;
	}

	private Map<String, Map<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>>> splitRegionMapData(ObfFileInMemory allMapObjects,
			OsmandRegions osmandRegions) throws IOException {
		Map<String, Map<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>>> result = new HashMap<>();
		for (MapZoomPair p : allMapObjects.getZooms()) {
			TLongObjectHashMap<BinaryMapDataObject> objects = allMapObjects.get(p);
			for (BinaryMapDataObject obj : objects.valueCollection()) {
				if (isPossibleVandalism(obj)) {
					continue;
				}
				int x = obj.getPoint31XTile(0);
				int y = obj.getPoint31YTile(0);
				List<BinaryMapDataObject> l = osmandRegions.query(x, y);
				for (BinaryMapDataObject b : l) {
					if (OsmandRegions.contain(b, x, y)) {
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
