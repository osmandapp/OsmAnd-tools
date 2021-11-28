package net.osmand.swing;

import gnu.trove.list.array.TIntArrayList;
import net.osmand.NativeLibrary;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import static net.osmand.binary.BinaryMapIndexReader.buildSearchRequest;

public class RouteSelector {
	private final NativeSwingRendering nativeLibRendering;

	public RouteSelector(NativeSwingRendering nativeLibRendering) {
		this.nativeLibRendering = nativeLibRendering;
	}

	public List<Way> getRoute(NativeLibrary.RenderedObject renderedObject) {
		List<Way> wL = new ArrayList<>();
		String type = "hiking";

		if (renderedObject.isRoute(type)) {
			List<Way> wL1 = getRoute(renderedObject, wL, type);
			if (wL1 != null) return wL1;
		} else {
			Way w = new Way(-1);
			TIntArrayList x1 = renderedObject.getX();
			TIntArrayList y1 = renderedObject.getY();
			for (int i = 0; i < x1.size(); i++) {
				Node n = new Node(MapUtils.get31LatitudeY(y1.get(i)),
						MapUtils.get31LongitudeX(x1.get(i)), -1);
				w.addNode(n);
			}
			wL.add(w);
		}

		return wL;
	}

	private List<Way> getRoute(NativeLibrary.RenderedObject renderedObject, List<Way> wL, String type) {
		TIntArrayList x1 = renderedObject.getX();
		TIntArrayList y1 = renderedObject.getY();
		NativeSwingRendering.MapDiff mapDiffs = nativeLibRendering.getMapDiffs(MapUtils.get31LatitudeY(y1.get(0)),
				MapUtils.get31LongitudeX(x1.get(0)));
		if (mapDiffs == null) {
			return wL;
		}
		File mapFile = mapDiffs.baseFile;
		List<BinaryMapDataObject> segmentList = new ArrayList<>();
		try {
			List<String> tagKeyList = renderedObject.getRouteStringKeys(type);
			RandomAccessFile raf = new RandomAccessFile(mapFile, "r");
			BinaryMapIndexReader indexReader = new BinaryMapIndexReader(raf, mapFile);
			BinaryMapIndexReader.MapIndex mapIndex = indexReader.getMapIndexes().get(0);
			final SearchRequest<BinaryMapDataObject> req = buildSearchRequest(0, Integer.MAX_VALUE,
					0, Integer.MAX_VALUE, 15, (types, index) -> true,
					new ResultMatcher<BinaryMapDataObject>() {
						@Override
						public boolean publish(BinaryMapDataObject object) {
							Map<Integer, List<String>> objectTagMap = new HashMap<>();
							for (int routeIdx = 1; routeIdx <= tagKeyList.size(); routeIdx++) {
								for (int i = 0; i < object.getObjectNames().keys().length; i++) {
									TagValuePair tp = object.getMapIndex().decodeType(object.getObjectNames().keys()[i]);
									if (tp != null && tp.tag != null && (tp.tag).startsWith("route_" + type + "_" + routeIdx)) {
										String value = object.getObjectNames().get(object.getObjectNames().keys()[i]);
										putTag(objectTagMap, routeIdx, tp.tag + value);
									}
								}
								for (int i = 0; i < object.getTypes().length; i++) {
									TagValuePair tp = object.getMapIndex().decodeType(object.getTypes()[i]);
									if (tp != null && tp.tag != null && (tp.tag).startsWith("route_" + type + "_" + routeIdx)) {
										String value = (tp.value == null) ? "" : tp.value;
										putTag(objectTagMap, routeIdx, tp.tag + value);
									}
								}
								for (int i = 0; i < object.getAdditionalTypes().length; i++) {
									TagValuePair tp = object.getMapIndex().decodeType(object.getAdditionalTypes()[i]);
									if (tp != null && tp.tag != null && (tp.tag).startsWith("route_" + type + "_" + routeIdx)) {
										String value = (tp.value == null) ? "" : tp.value;
										putTag(objectTagMap, routeIdx, tp.tag + value);
									}
								}
							}
							if (!objectTagMap.isEmpty()) {
								for (int routeIdx = 1; routeIdx <= tagKeyList.size(); routeIdx++) {
									String tagKey = tagKeyList.get(routeIdx - 1);
									List<String> objectTagList = objectTagMap.get(routeIdx);
									if (objectTagList != null) {
										Collections.sort(objectTagList);
										String objectTagKey = String.join("", objectTagList);
										System.out.println(objectTagKey);
										if (Algorithms.stringsEqual(tagKey, objectTagKey)) {
											segmentList.add(object);
										}
									}
								}
							}
							return false;
						}

						private void putTag(Map<Integer, List<String>> objectTagMap, int routeIdx, String value) {
							List<String> currList = objectTagMap.get(routeIdx);
							if (currList == null) {
								currList = new ArrayList<>();
							}
							currList.add(value);
							objectTagMap.put(routeIdx, currList);
						}

						@Override
						public boolean isCancelled() {
							return false;
						}
					});
			indexReader.searchMapIndex(req, mapIndex);
			System.out.println("tagKeyList == " + tagKeyList);
		} catch (IOException e) {
			e.printStackTrace();
		}
		printSegmentList(segmentList);
		for (BinaryMapDataObject obj : segmentList) {
			Way w = new Way(-1);
			if (obj.getPointsLength() > 2) {
				for (int i = 1; i < obj.getPointsLength(); i++) {
					int x = obj.getPoint31XTile(i);
					int y = obj.getPoint31YTile(i);
					Node n = new Node(MapUtils.get31LatitudeY(y), MapUtils.get31LongitudeX(x), -1);
					w.addNode(n);
				}
				wL.add(w);
			}
		}
		return null;
	}

	void printSegmentList(List<BinaryMapDataObject> segmentList) {
		for (BinaryMapDataObject obj : segmentList) {
			int[] coords = obj.getCoordinates();
			if (coords.length > 2) {
				System.out.printf("%d %d %d %d \n", coords[0], coords[1], coords[coords.length - 2],
						coords[coords.length - 1]);
			}
		}
	}
}
