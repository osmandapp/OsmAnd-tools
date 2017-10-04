package net.osmand.data.diff;

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

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import java.util.TreeMap;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryInspector;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.MapZooms.MapZoomPair;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.Amenity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import rtree.RTreeException;

public class ObfDiffGenerator {
	private static final long ID_MULTIPOLYGON_LIMIT = 1l << 41l;

	private static final int COORDINATES_PRECISION_COMPARE = 0;
	
	private static final String OSMAND_CHANGE_VALUE = "delete";
	private static final String OSMAND_CHANGE_TAG = "osmand_change";
	
	public static void main(String[] args) throws IOException, RTreeException {
		if(args.length == 1 && args[0].equals("test")) {
			args = new String[3];
			args[0] = "/Users/victorshcherb/osmand/maps/diff/17_09_03_22_15_before.obf.gz";
			args[1] = "/Users/victorshcherb/osmand/maps/diff/17_09_03_22_15_after.obf.gz";
			args[2] = "/Users/victorshcherb/osmand/maps/diff/Diff.obf";
//			args[2] = "stdout";
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
		File diff = args.length < 4 ? null :new File(args[3]);
		File result  = args.length < 4 || args[2].equals("stdout") ? null :new File(args[2]);
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
		
		Set<EntityId> deletedObjIds = null;
		if (diff != null) {
			try {
				deletedObjIds = DiffParser.fetchDeletedIds(diff);
			} catch (IOException | XmlPullParserException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Comparing the files...");
		compareMapData(fStart, fEnd, result == null, deletedObjIds);
		compareRouteData(fStart, fEnd, result == null, deletedObjIds);
		comparePOI(fStart, fEnd, result == null);
		// TODO Compare Transport
		//compareTransport(fStart, fEnd);
		
		System.out.println("Finished comparing.");
		if (result != null) {
			if (result.exists()) {
				result.delete();
			}
			fEnd.writeFile(result, false);
		}
	}
	
	private void comparePOI(ObfFileInMemory fStart, ObfFileInMemory fEnd, boolean print) {
	 	TLongObjectHashMap<Map<String, Amenity>> startPoiSource = fStart.getPoiObjects();
	 	TLongObjectHashMap<Map<String, Amenity>> endPoiSource = fEnd.getPoiObjects();
	 	if (endPoiSource == null) {
	 		return;
	 	}
	 	Map<String, Amenity> startPoi = buildPoiMap(startPoiSource);
	 	Map<String, Amenity> endPoi = buildPoiMap(endPoiSource);
	 	if(print) {
			System.out.println("Compare POI");
		}
		for (String idx : startPoi.keySet()) {
			Amenity objE = endPoi.get(idx);
			Amenity objS = startPoi.get(idx);
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
					objS.setAdditionalInfo(OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
					endPoi.put(idx, objS);
					if (endPoiSource.get(objS.getId()) == null) {
						endPoiSource.put(objS.getId(), new TreeMap<String, Amenity>());
					}
					endPoiSource.get(objS.getId()).put(objS.getType().getKeyName(), objS);
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


	private void compareMapData(ObfFileInMemory fStart, ObfFileInMemory fEnd, boolean print, Set<EntityId> deletedObjIds) {
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
						boolean osmchangeIsMissing = deletedObjIds == null;
						if (!osmchangeIsMissing && deletedObjIds.contains(thisEntityId)) {
							// Object with this id is not present in the second obf & was deleted according to diff
							BinaryMapDataObject obj = new BinaryMapDataObject(idx, objS.getCoordinates(), null,
									objS.getObjectType(), objS.isArea(), new int[] { deleteId }, null);
							endData.put(idx, obj);	
							
												
						} else if (osmchangeIsMissing) {
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
	
	private EntityId getMapEntityId(long id) {
		if (id < ID_MULTIPOLYGON_LIMIT) {
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

	private void compareRouteData(ObfFileInMemory fStart, ObfFileInMemory fEnd, boolean print, Set<EntityId> deletedObjIds) {
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
					boolean osmchangeIsMissing = deletedObjIds == null;
					if (!osmchangeIsMissing && deletedObjIds.contains(wayId)) {
						// Object with this id is not present in the second obf
						RouteDataObject rdo = new RouteDataObject(ri);
						rdo.id = objS.id;
						rdo.pointsX = objS.pointsX;
						rdo.pointsY = objS.pointsY;
						rdo.types = new int[] { deleteId };
						endData.put(idx, rdo);
					} else if (osmchangeIsMissing) {
						RouteDataObject rdo = new RouteDataObject(ri);
						rdo.id = objS.id;
						rdo.pointsX = objS.pointsX;
						rdo.pointsY = objS.pointsY;
						rdo.types = new int[] { deleteId };
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
	
	private static class DiffParser {
		
		private static final String ATTR_ID = "id";
		private static final String TYPE_RELATION = "relation";
		private static final String TYPE_WAY = "way";
		private static final String TYPE_NODE = "node";
		private static final String TYPE_ATTR_VALUE = "delete";
		private static final String TYPE_ATTR = "type";
		private static EntityId currentParsedEntity;

		public static Set<EntityId> fetchDeletedIds(File diff) throws IOException, XmlPullParserException {
			Set<EntityId> result = new HashSet<>();
			InputStream fis = new FileInputStream(diff);
			XmlPullParser parser = PlatformUtil.newXMLPullParser();
			parser.setInput(fis, "UTF-8");
			int tok;
			boolean parsing = false;
			
			while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
				if (tok == XmlPullParser.START_TAG ) {
					if (parser.getAttributeValue("", TYPE_ATTR) != null) {
						if (parser.getAttributeValue("", TYPE_ATTR).equals(TYPE_ATTR_VALUE)) {
							parsing = true;
						}
					}
					String name = parser.getName();
					if (TYPE_NODE.equals(name) && parsing) {
						currentParsedEntity = new EntityId(EntityType.NODE, parseLong(parser, ATTR_ID, -1));
						
					} else if (TYPE_WAY.equals(name) && parsing) {
						currentParsedEntity = new EntityId(EntityType.WAY, parseLong(parser, ATTR_ID, -1));
					} else if (TYPE_RELATION.equals(name) && parsing) {
						currentParsedEntity = new EntityId(EntityType.RELATION, parseLong(parser, ATTR_ID, -1));
					}	
				} else if (tok == XmlPullParser.END_TAG) {
					parsing = false;
					if (currentParsedEntity != null) {
						result.add(currentParsedEntity);
						currentParsedEntity = null;
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
