package net.osmand.data.diff;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.binary.MapZooms.MapZoomPair;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.Amenity;
import rtree.RTreeException;

public class ObfDiffGenerator {
	
	
	private static final String OSMAND_CHANGE_VALUE = "delete";
	private static final String OSMAND_CHANGE_TAG = "osmand_change";
	public static final int BUFFER_SIZE = 1 << 20;
	
	public static void main(String[] args) throws IOException, RTreeException {
		if(args.length == 1 && args[0].equals("test")) {
			args = new String[3];
			args[0] = "/Users/victorshcherb/osmand/maps/diff/Diff-start.obf";
			args[1] = "/Users/victorshcherb/osmand/maps/diff/Diff-end.obf";
			args[2] = "/Users/victorshcherb/osmand/maps/diff/2017_08_28_01_00_diff.obf";
		}
		if (args.length != 3) {
			System.out.println("Usage: <path to old obf> <path to new obf> <result file name>");
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
	
	private void run(String[] args) throws IOException, RTreeException {
		File start = new File(args[0]);
		File end = new File(args[1]);
		File result  = new File(args[2]);
		if (!start.exists() || !end.exists()) {
			System.err.println("Input Obf file doesn't exist");
			System.exit(1);
			return;
		}
		generateDiff(start, end, result);
	}

	private void generateDiff(File start, File end, File result) throws IOException, RTreeException {
		ObfFileInMemory fStart = new ObfFileInMemory();
		fStart.readObfFiles(Collections.singletonList(start));
		ObfFileInMemory fEnd = new ObfFileInMemory();
		fEnd.readObfFiles(Collections.singletonList(end));

		System.out.println("Comparing the files...");
		compareMapData(fStart, fEnd);
		// Compare Routing
		compareRouteData(fStart, fEnd);
		// TODO Compare POI
		comparePOI(fStart, fEnd);
		// TODO Compare Transport
		//compareTransport(fStart, fEnd);
		
		System.out.println("Finished comparing.");
		if (result.exists()) {
			result.delete();
		}
		fEnd.writeFile(result);
	}

	private void comparePOI(ObfFileInMemory fStart, ObfFileInMemory fEnd) {
		TLongObjectHashMap<Amenity> startPoi = fStart.getPoiData();
		TLongObjectHashMap<Amenity> endPoi = fEnd.getPoiData();
		
		if (endPoi == null) {
			return;
		}
		
		for (long idx : startPoi.keys()) {
			Amenity objE = endPoi.get(idx);
			Amenity objS = startPoi.get(idx);
			if (objE == null) {
				// TODO add delete tags and put into the result set
				Amenity am = new Amenity();
				am.setAdditionalInfo(OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
				am.setX(objS.getX());
				am.setY(objS.getY());
				endPoi.put(idx, am);
			}
			else if (objE.comparePoi(objS)) {
				endPoi.remove(idx);
			}
		}
		
	}

	private void compareMapData(ObfFileInMemory fStart, ObfFileInMemory fEnd) {
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
			if (endData == null) {
				continue;
			}
			for (Long idx : startData.keys()) {
				BinaryMapDataObject objE = endData.get(idx);
				BinaryMapDataObject objS = startData.get(idx);
				if (objE == null) {
					// Object with this id is not present in the second obf
					BinaryMapDataObject obj = new BinaryMapDataObject(idx, objS.getCoordinates(), null,
							objS.getObjectType(), objS.isArea(), new int[] { deleteId }, null);
					endData.put(idx, obj);
				} else if (objE.compareBinary(objS)) {
					endData.remove(idx);
				}
			}
		}
	}
	
	private void compareRouteData(ObfFileInMemory fStart, ObfFileInMemory fEnd) {
		RouteRegion ri = fEnd.getRouteIndex();
		int deleteId = ri.searchRouteEncodingRule(OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
		if (deleteId == -1) {
			deleteId = ri.routeEncodingRules.size();
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
			if (objE == null) {
				// Object with this id is not present in the second obf
				RouteDataObject rdo = new RouteDataObject(ri);
				rdo.pointsX = objS.pointsX; 
				rdo.pointsY = objS.pointsY;
				rdo.types = new int[] {deleteId };
				endData.put(idx, rdo);
			} else if (objE.compareRoute(objS)) {
				endData.remove(idx);
			}
		}
	}

	
}
