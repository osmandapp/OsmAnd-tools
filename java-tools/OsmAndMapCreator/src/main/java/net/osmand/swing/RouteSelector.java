package net.osmand.swing;

import gnu.trove.list.array.TIntArrayList;
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

import static net.osmand.NativeLibrary.*;
import static net.osmand.binary.BinaryMapIndexReader.buildSearchRequest;

public class RouteSelector {
	private final NativeSwingRendering nativeLibRendering;

	public RouteSelector(NativeSwingRendering nativeLibRendering) {
		this.nativeLibRendering = nativeLibRendering;
	}

	public List<Way> getRoute(RenderedObject renderedObject) {
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

	private List<Way> getRoute(RenderedObject renderedObject, List<Way> wL, String type) {
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
							for (int routeIdx = 1; routeIdx <= getRouteQuantity(object); routeIdx++) {
								String prefix = "route_" + type + "_" + routeIdx;
								for (int i = 0; i < object.getObjectNames().keys().length; i++) {
									TagValuePair tp = object.getMapIndex().decodeType(object.getObjectNames().keys()[i]);
									if (tp != null && tp.tag != null && (tp.tag).startsWith(prefix)) {
										String value = object.getObjectNames().get(object.getObjectNames().keys()[i]);
										putTag(objectTagMap, routeIdx, value);
									}
								}
								for (int i = 0; i < object.getTypes().length; i++) {
									TagValuePair tp = object.getMapIndex().decodeType(object.getTypes()[i]);
									if (tp != null && tp.tag != null && (tp.tag).startsWith(prefix)) {
										String value = (tp.value == null) ? "" : tp.value;
										putTag(objectTagMap, routeIdx, value);
									}
								}
								for (int i = 0; i < object.getAdditionalTypes().length; i++) {
									TagValuePair tp = object.getMapIndex().decodeType(object.getAdditionalTypes()[i]);
									if (tp != null && tp.tag != null && (tp.tag).startsWith(prefix)) {
										String value = (tp.value == null) ? "" : tp.value;
										putTag(objectTagMap, routeIdx, value);
									}
								}
							}
							if (!objectTagMap.isEmpty()) {
								for (String tagKey : tagKeyList) {
									for (Map.Entry<Integer, List<String>> entry : objectTagMap.entrySet()) {
										List<String> objectTagList = entry.getValue();
										Collections.sort(objectTagList);
										String objectTagKey = String.join("", objectTagList);
										if (Algorithms.stringsEqual(tagKey, objectTagKey)) {
											segmentList.add(object);
										}
									}
								}
							}
							return false;
						}

						private void putTag(Map<Integer, List<String>> objectTagMap, int routeIdx, String value) {
							List<String> tagList = objectTagMap.get(routeIdx);
							if (tagList == null) {
								tagList = new ArrayList<>();
							}
							tagList.add(value);
							objectTagMap.put(routeIdx, tagList);
						}

						private int getRouteQuantity(BinaryMapDataObject object) {
							List<String> tagsList = new ArrayList<>();
							for (int i = 0; i < object.getAdditionalTypes().length; i++) {
								TagValuePair tp = object.getMapIndex().decodeType(object.getAdditionalTypes()[i]);
								tagsList.add(tp.tag);
							}
							Collections.sort(tagsList);
							int routeQuantity = 0;
							for (int i = tagsList.size() - 1; i > 0; i--) {
								String tag = tagsList.get(i);
								if (tag.startsWith("route_" + type)) {
									routeQuantity = Algorithms.extractIntegerNumber(tag);
									break;
								}
							}
							return routeQuantity;
						}

						@Override
						public boolean isCancelled() {
							return false;
						}
					});
			indexReader.searchMapIndex(req, mapIndex);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (BinaryMapDataObject obj : segmentList) {
			Way w = new Way(-1);
			if (obj.getPointsLength() > 1) {
				for (int i = 0; i < obj.getPointsLength(); i++) {
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
}
