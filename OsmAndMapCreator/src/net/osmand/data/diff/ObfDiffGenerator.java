package net.osmand.data.diff;

import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.MapZooms.MapZoomPair;
import rtree.RTreeException;

public class ObfDiffGenerator {
	
	
	private static final String OSMAND_CHANGE_VALUE = "delete";
	private static final String OSMAND_CHANGE_TAG = "osmand_change";
	public static final int BUFFER_SIZE = 1 << 20;
	
	public static void main(String[] args) throws IOException, RTreeException {
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
			System.exit(1);
			System.err.println("Input Obf file doesn't exist");
			return;
		}
		generateDiff(start, end, result);
	}

	private void generateDiff(File start, File end, File result) throws IOException, RTreeException {
		ObfFileInMemory fStart = new ObfFileInMemory();
		fStart.readObfFiles(Collections.singletonList(start));
		ObfFileInMemory fEnd = new ObfFileInMemory();
		fEnd.readObfFiles(Collections.singletonList(end));
		// TODO Compare POI, Transport, Routing
		// TODO compare zoom level 13-14 and pick up only area, point objects (not line objects!)
		fStart.filterAllZoomsBelow(15);
		fEnd.filterAllZoomsBelow(15);
		MapIndex mi = fEnd.getMapIndex();
		int deleteId = mi.decodingRules.size() + 1;
		mi.initMapEncodingRule(0, deleteId, OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
		System.out.println("Comparing the files...");
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
		System.out.println("Finished comparing.");
		if (result.exists()) {
			result.delete();
		}
		fEnd.writeFile(result);
	}

	
}
