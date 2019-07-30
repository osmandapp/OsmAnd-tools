package net.osmand.obf.diff;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.MapZooms.MapZoomPair;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.Amenity;
import net.osmand.data.MapObject;
import net.osmand.data.TransportRoute;
import net.osmand.data.TransportStop;
import net.osmand.obf.BinaryInspector;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import rtree.RTreeException;

public class ObfDiffGenerator {
	private static final long ID_MULTIPOLYGON_LIMIT = 1l << 41l;

	private static final int COORDINATES_PRECISION_COMPARE = 0;
	
	private static final String OSMAND_CHANGE_VALUE = "delete";
	private static final String OSMAND_CHANGE_TAG = "osmand_change";
	
	public static void main(String[] args) throws IOException, RTreeException {
		if(args.length == 1 && args[0].equals("test")) {
			args = new String[4];
			args[0] = "/Users/victorshcherb/osmand/maps/olive/19_07_29_20_30_before.obf.gz";
			args[1] = "/Users/victorshcherb/osmand/maps/olive/19_07_29_20_30_after.obf.gz";
//			args[2] = "stdout";
			args[2] = "/Users/victorshcherb/osmand/maps/olive/19_07_29_20_30_diff.obf";
			args[3] = "/Users/victorshcherb/osmand/maps/olive/19_07_29_20_30_diff.osm.gz";
		}
		if (args.length < 3) {
			System.out.println("Usage: <path to old obf> <path to new obf> <[result file name] or [stdout]> <path to diff file (optional)>");
			System.exit(1);
			return;
		}
		try {
			ObfDiffGenerator generator = new ObfDiffGenerator();
			generator.run(args);
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void run(String[] args) throws IOException, RTreeException, SQLException {
		File start = new File(args[0]);
		File end = new File(args[1]);
		File diff = args.length < 4 ? null : new File(args[3]);
		File result = args[2].equals("stdout") ? null : new File(args[2]);
		if (!start.exists()) {
			System.err.println("Input Obf file doesn't exist: " + start.getAbsolutePath());
			System.exit(1);
			return;
		}
		if (!end.exists()) {
			System.err.println("Input Obf file doesn't exist: " + end.getAbsolutePath());
			System.exit(1);
			return;
		}
		generateDiff(start, end, result, diff);
	}

	private void generateDiff(File start, File end, File result, File diff) throws IOException, RTreeException, SQLException {
		ObfFileInMemory fStart = new ObfFileInMemory();
		fStart.readObfFiles(Collections.singletonList(start));
		ObfFileInMemory fEnd = new ObfFileInMemory();
		fEnd.readObfFiles(Collections.singletonList(end));
		
		Set<EntityId> modifiedObjIds = null;
		if (diff != null) {
			try {
				modifiedObjIds = DiffParser.fetchModifiedIds(diff);
			} catch (IOException | XmlPullParserException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Comparing the files...");
		compareMapData(fStart, fEnd, result == null, modifiedObjIds);
		compareRouteData(fStart, fEnd, result == null, modifiedObjIds);
		comparePOI(fStart, fEnd, result == null, modifiedObjIds);
		compareTransport(fStart, fEnd, result == null, modifiedObjIds);
		
		System.out.println("Finished comparing.");
		if (result != null) {
			if (result.exists()) {
				result.delete();
			}
			fEnd.writeFile(result, false);
		}
	}

	private void compareTransport(ObfFileInMemory fStart, ObfFileInMemory fEnd, boolean print, Set<EntityId> modifiedObjIds) {
		TLongObjectHashMap<TransportStop> startStopData = cleanStopsAndAdjustId(fStart.getTransportStops());
		TLongObjectHashMap<TransportStop> endStopData = cleanStopsAndAdjustId(fEnd.getTransportStops());
		if (endStopData == null) {
			return;
		}
		TLongObjectHashMap<TransportStop> endStopDataDeleted = new TLongObjectHashMap<>();
		// Walk through all stops and drop non changed. If stop was deleted - mark it and add to the result.
		TLongHashSet existingStops = new TLongHashSet();
		for (Long stopId : startStopData.keys()) {
			TransportStop stopS = startStopData.get(stopId);
			TransportStop stopE = endStopData.get(stopId);
			existingStops.add(stopId);
			if (stopE == null) {
				if (print) {
					System.out.println("Transport stop " + stopId + " is missing in (2): " + stopS);
				} else {
					EntityId aid = getTransportEntityId(stopS, EntityType.NODE);
					EntityId aidWay = getTransportEntityId(stopS, EntityType.WAY);
					if (modifiedObjIds == null || modifiedObjIds.contains(aid) || 
							modifiedObjIds.contains(aidWay)) {
						stopS.setDeleted();
						endStopData.put(stopId, stopS);
					} else {
						// we need to put stop anyway (but don't mark as deleted)
						endStopDataDeleted.put(stopId, stopS);
					}
				}
			} else {
				boolean cmp = stopE.compareStop(stopS);
				if (!cmp) {
					if (print) {
						System.out.println("Transport stop " + stopId + " is not equal: " + stopS + " != " + stopE);
					}
				}
				if(cmp || print){
					TransportStop stop = endStopData.remove(stopId);
					endStopDataDeleted.put(stopId, stop);
				}
			}
		}
		
		TLongObjectIterator<TransportStop> stopIterator = endStopData.iterator();
		while(stopIterator.hasNext()) {
			stopIterator.advance();
			TransportStop s = stopIterator.value();
			if (!s.isDeleted()) {
				long stopId = stopIterator.key();
				if (print) {
					System.out.println("Transport stop " + s.getId() + " is missing in (1): " + s);
				} else if(!existingStops.contains(stopId)){
					EntityId aid = getTransportEntityId(s, EntityType.NODE);
					EntityId aidWay = getTransportEntityId(s, EntityType.WAY);
					if (modifiedObjIds != null && !modifiedObjIds.contains(aid) && !modifiedObjIds.contains(aidWay)) {
						// stop was not actually modified (so delete it) it will be possible added back if route was creates
						stopIterator.remove();
						endStopDataDeleted.put(stopId, s);
					}
				}
			}
		}
		// Walk through all routes and drop non changed. If route was deleted - mark it and add to the result.
		// Offsets will be regenerated at write procedure
		TLongObjectHashMap<TransportRoute> startRouteData = fStart.getTransportRoutes();
		TLongObjectHashMap<TransportRoute> endRouteData = fEnd.getTransportRoutes();
		TLongObjectHashMap<TransportStop> routeDeletedStops = new TLongObjectHashMap<>();
		TLongHashSet existingRoutes = new TLongHashSet();
		for (Long routeId : startRouteData.keys()) {
			existingRoutes.add(routeId);
			TransportRoute routeS = startRouteData.get(routeId);
			TransportRoute routeE = endRouteData.get(routeId);
			routeDeletedStops.clear();
			if (routeE == null) {
				EntityId aid = getTransportRouteId(routeS);
				if (modifiedObjIds != null && !modifiedObjIds.contains(aid)) {
					throw new IllegalStateException("Transport route " + routeId + " is missing in (2): " + routeS);
				}
				// mark route as deleted on all stops!
				for(TransportStop s : routeS.getForwardStops()) {
					routeDeletedStops.put(adjustTransportRouteStopIdToId(s.getId()), s);
				}
				if (print) {
					System.out.println("Transport route " + routeId + " is missing in (2): " + routeS);
				}
			} else {
				boolean cmp = routeE.compareRoute(routeS);
				if (!cmp) {
					if (print) {
						System.out.println("Transport route " + routeId + " is not equal: " + routeS + " != " + routeE);
					}
					// mark on all previous stops to be deleted
					if (routeS != null) {
						for (TransportStop s : routeS.getForwardStops()) {
							routeDeletedStops.put(adjustTransportRouteStopIdToId(s.getId()), s);
						}
					}
					// don't mark to delete if it will be added
					for(TransportStop s : routeE.getForwardStops()) {
						long stopId = adjustTransportRouteStopIdToId(s.getId());
						routeDeletedStops.remove(stopId);
						TransportStop originalStop = checkEndStopData(endStopData, endStopDataDeleted, 
								s, routeId, stopId);
						originalStop.addRouteId(routeId);
					}
					
				}
				if(cmp || print){
					endRouteData.remove(routeId);
				}
			}
			for(long stopId : routeDeletedStops.keys()) {
				TransportStop originalStop = checkEndStopData(endStopData, endStopDataDeleted, 
						routeDeletedStops.get(stopId), routeId, stopId);
				originalStop.addDeletedRouteId(routeId);
			}
			
			
		}
		
		for (TransportRoute route : endRouteData.valueCollection()) {
			if (print) {
				System.out.println("Transport route " + (route.getId() / 2) + " is missing in (1): " + route);
			} else if(!existingRoutes.contains(route.getId())){
				for(TransportStop s : route.getForwardStops()) {
					long routeId = route.getId().longValue();
					TransportStop originalStop = checkEndStopData(endStopData, endStopDataDeleted, 
							s, routeId, s.getId());
					originalStop.addRouteId(routeId);
				}
			}
		}
	}

	private TransportStop checkEndStopData(TLongObjectHashMap<TransportStop> endStopData,
			TLongObjectHashMap<TransportStop> endStopDataDeleted, TransportStop errorStop,
			long routeId, long stopId) {
		if(!endStopData.contains(stopId)) {
			if(!endStopDataDeleted.contains(stopId)) {
				throw new IllegalArgumentException(
						String.format("Missing stop %d for route %d: %s", stopId, routeId / 2, errorStop));
			}
			endStopData.put(stopId, endStopDataDeleted.get(stopId));
		}
		return endStopData.get(stopId);
	}
	
	private TLongObjectHashMap<TransportStop> cleanStopsAndAdjustId(TLongObjectHashMap<TransportStop> transportStops) {
		if (transportStops == null) {
			return null;
		}
		// don't create copy needed for diff!
		for (TransportStop s : transportStops.valueCollection()) {
			s.setRoutesIds(null);
			s.setDeletedRoutesIds(null);
		}
		return transportStops;
	}

	public static long adjustTransportStopIdToId(long id) {
//		long r = id & 0xffffffffL;
//		if(r  > Integer.MAX_VALUE) {
//			r = r - 0xffffffffL - 1;
//		}
		// should be just id after fixes
//		return r;
		return id;
	}

	public static long adjustTransportRouteStopIdToId(long id) {
		// should be just id after fixes
		return id;
	}

	private void comparePOI(ObfFileInMemory fStart, ObfFileInMemory fEnd, boolean print, Set<EntityId> modifiedObjIds) {
		TLongObjectHashMap<Map<String, Amenity>> startPoiSource = fStart.getPoiObjects();
		TLongObjectHashMap<Map<String, Amenity>> endPoiSource = fEnd.getPoiObjects();
		if (endPoiSource == null) {
			return;
		}
		Map<String, Amenity> startPoi = buildPoiMap(startPoiSource);
		Map<String, Amenity> endPoi = buildPoiMap(endPoiSource);
		if (print) {
			System.out.println("Compare POI");
		}
		for (String idx : startPoi.keySet()) {
			Amenity objE = endPoi.get(idx);
			Amenity objS = startPoi.get(idx);
			EntityId aid = getMapObjectId(objS);
			if (print) {
				if (objE == null) {
					System.out.println("POI " + idx + " is missing in (2): " + objS);
				} else {
					if (!objS.comparePoi(objE)) {
						System.out.println("POI " + idx + " is not equal: " + objS + " != " + objE);
					}
					endPoi.remove(idx);
				}
			} else {
				if (objE == null) {
					if (modifiedObjIds == null || modifiedObjIds.contains(aid) || aid == null) {
						objS.setAdditionalInfo(OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
						endPoi.put(idx, objS);
						if (endPoiSource.get(objS.getId()) == null) {
							endPoiSource.put(objS.getId(), new TreeMap<String, Amenity>());
						}
						endPoiSource.get(objS.getId()).put(objS.getType().getKeyName(), objS);
					}

				} else {
					if (objS.comparePoi(objE)) {
						endPoi.remove(idx);
						endPoiSource.get(objS.getId()).remove(objS.getType().getKeyName());
					}
				}
			}
		}

		if (print) {
			Iterator<Entry<String, Amenity>> it = endPoi.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Amenity> e = it.next();
				System.out.println("POI " + e.getKey() + " is missing in (1): " + e.getValue());
			}
		}
	}

	private Map<String, Amenity> buildPoiMap(TLongObjectHashMap<Map<String, Amenity>> startPoiSource) {
		HashMap<String, Amenity> map = new HashMap<>();
		for (Map<String, Amenity> am : startPoiSource.valueCollection()) {
			Iterator<Entry<String, Amenity>> it = am.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Amenity> next = it.next();
				String key = next.getValue().getId() + ":" + next.getKey();
				map.put(key, next.getValue());
			}
		}
		return map;
	}


	private void compareMapData(ObfFileInMemory fStart, ObfFileInMemory fEnd, boolean print, Set<EntityId> modifiedObjIds) {
		fStart.filterAllZoomsBelow(13);
		fEnd.filterAllZoomsBelow(13);		
		MapIndex mi = fEnd.getMapIndex();
		int deleteId;
		Integer rl = mi.getRule(OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
		if(rl != null) {
			deleteId = rl; 
		} else {
			deleteId = mi.decodingRules.size() + 1;
			mi.initMapEncodingRule(0, deleteId, OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
		}
		for (MapZoomPair mz : fStart.getZooms()) {
			TLongObjectHashMap<BinaryMapDataObject> startData = fStart.get(mz);
			TLongObjectHashMap<BinaryMapDataObject> endData = fEnd.get(mz);
			if(print) {
				System.out.println("Compare map " + mz);
			}
			if (endData == null) {
				continue;
			}
			for (Long idx : startData.keys()) {
				BinaryMapDataObject objE = endData.get(idx);
				BinaryMapDataObject objS = startData.get(idx);
				EntityId thisEntityId = getMapEntityId(objS.getId());
				if (print) {
					if (objE == null) {
						System.out.println("Map " + idx + " is missing in (2): " + toString(objS));
					} else {
						if (//!objS.getMapIndex().decodeType(objS.getTypes()[0]).tag.equals(OSMAND_CHANGE_TAG) &&
								!objE.compareBinary(objS, COORDINATES_PRECISION_COMPARE )) {
							System.out.println("Map " + idx + " is not equal: " + toString(objS) + " != " + toString(objE));
						}
						endData.remove(idx);
					}
				} else {
					if (objE == null) {
						if (modifiedObjIds == null || modifiedObjIds.contains(thisEntityId) || thisEntityId == null) {
							BinaryMapDataObject obj = new BinaryMapDataObject(idx, objS.getCoordinates(), null,
									objS.getObjectType(), objS.isArea(), new int[] { deleteId }, null);
							endData.put(idx, obj);
						} 
					} else if (objE.compareBinary(objS, COORDINATES_PRECISION_COMPARE)) {
						endData.remove(idx);
					}
				}
			}
			if(print) {
				for (BinaryMapDataObject e : endData.valueCollection()) {
					System.out.println("Map " + e.getId() + " is missing in (1): " + toString(e));
				}
			}
		}

	}

	private EntityId getMapObjectId(MapObject objS) {
		Long id = objS.getId();
		if (id < ID_MULTIPOLYGON_LIMIT && id > 0) {
			if ((id % 2) == 0) {
				return new EntityId(EntityType.NODE, id >> 1);
			} else {
				return new EntityId(EntityType.WAY, id >> 1);
			}
		}
		return null;
	}
	
	private EntityId getTransportEntityId(MapObject objS, EntityType tp) {
		Long id = objS.getId();
		// we didn't store way or node (so it should be checked twice probably) 
		return new EntityId(tp, id >> (BinaryInspector.SHIFT_ID));
	}

	private EntityId getTransportRouteId(TransportRoute route) {
		return new EntityId(EntityType.RELATION, route.getId() >> 1);
	}

	private EntityId getMapEntityId(long id) {
		if (id < ID_MULTIPOLYGON_LIMIT && id > 0) {
			if ((id % 2) == 0) {
				return new EntityId(EntityType.NODE, id >> (BinaryInspector.SHIFT_ID + 1));
			} else {
				return new EntityId(EntityType.WAY, id >> (BinaryInspector.SHIFT_ID + 1));
			}
		}
		return null;
	}

	private String toString(BinaryMapDataObject objS) {
		StringBuilder s = new StringBuilder();
		BinaryInspector.printMapDetails(objS, s, false);
		return s.toString();
	}

	private void compareRouteData(ObfFileInMemory fStart, ObfFileInMemory fEnd, boolean print, Set<EntityId> modifiedObjIds) {
		RouteRegion ri = fEnd.getRouteIndex();
		int deleteId = ri.searchRouteEncodingRule(OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
		if (deleteId == -1) {
			deleteId = ri.routeEncodingRules.size();
			if(deleteId == 0) {
				deleteId = 1;
			}
			ri.initRouteEncodingRule(deleteId, OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
		}

		TLongObjectHashMap<RouteDataObject> startData = fStart.getRoutingData();
		TLongObjectHashMap<RouteDataObject> endData = fEnd.getRoutingData();
		if (endData == null) {
			return;
		}
		for (Long idx : startData.keys()) {
			RouteDataObject objE = endData.get(idx);
			RouteDataObject objS = startData.get(idx);
			if (print) {
				if (objE == null) {
					System.out.println("Route " + idx + " is missing in (2): " + objS);
				} else {
					if (!objE.compareRoute(objS)) {
						System.out.println("Route " + idx + " is not equal: " + objS + " != " + objE);
					}
					endData.remove(idx);
				}
			} else {
				if (objE == null) {
					EntityId wayId = new EntityId(EntityType.WAY, idx >> (BinaryInspector.SHIFT_ID));
					if (modifiedObjIds == null || modifiedObjIds.contains(wayId)) {
						RouteDataObject rdo = generateDeletedRouteObject(ri, deleteId, objS);
						endData.put(idx, rdo);
					}
				} else if (objE.compareRoute(objS)) {
					endData.remove(idx);
				}
			}
		}
		if(print) {
			for(RouteDataObject e: endData.valueCollection()) {
				System.out.println("Route " + e.getId() + " is missing in (1): " + e);
			}
		}
	}

	private RouteDataObject generateDeletedRouteObject(RouteRegion ri, int deleteId, RouteDataObject objS) {
		RouteDataObject rdo = new RouteDataObject(ri);
		rdo.id = objS.id;
		rdo.pointsX = objS.pointsX;
		rdo.pointsY = objS.pointsY;
		rdo.types = new int[] { deleteId };
		return rdo;
	}
	
	private static class DiffParser {
		
		private static final String ATTR_ID = "id";
		private static final String TYPE_RELATION = "relation";
		private static final String TYPE_WAY = "way";
		private static final String TYPE_NODE = "node";

		public static Set<EntityId> fetchModifiedIds(File diff) throws IOException, XmlPullParserException {
			Set<EntityId> result = new HashSet<>();
			InputStream fis ;
			if(diff.getName().endsWith(".gz")) {
				fis = new GZIPInputStream(new FileInputStream(diff));
			} else {
				fis = new FileInputStream(diff);
			}
			XmlPullParser parser = PlatformUtil.newXMLPullParser();
			parser.setInput(fis, "UTF-8");
			int tok;
			while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
				if (tok == XmlPullParser.START_TAG ) {
					String name = parser.getName();
					if (TYPE_NODE.equals(name)) {
						result.add(new EntityId(EntityType.NODE, parseLong(parser, ATTR_ID, -1)));
					} else if (TYPE_WAY.equals(name) ) {
						result.add(new EntityId(EntityType.WAY, parseLong(parser, ATTR_ID, -1)));
					} else if (TYPE_RELATION.equals(name)) {
						result.add(new EntityId(EntityType.RELATION, parseLong(parser, ATTR_ID, -1)));
					}	
				}
			}
			return result;
		}
		
		protected static long parseLong(XmlPullParser parser, String name, long defVal){
			long ret = defVal; 
			String value = parser.getAttributeValue("", name);
			if(value == null) {
				return defVal;
			}
			try {
				ret = Long.parseLong(value);
			} catch (NumberFormatException e) {
			}
			return ret;
		}
	}	
}
