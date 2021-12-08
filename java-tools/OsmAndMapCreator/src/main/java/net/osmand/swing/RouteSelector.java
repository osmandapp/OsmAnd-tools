package net.osmand.swing;

import gnu.trove.list.array.TIntArrayList;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import static net.osmand.NativeLibrary.*;

public class RouteSelector {
	public static final String ROUTE_PREFIX = "route_";
	public static final int DEVIATE = 200;
	private final NativeSwingRendering nativeLibRendering;
	String type;
	String tagKey;

	public RouteSelector(NativeSwingRendering nativeLibRendering) {
		this.nativeLibRendering = nativeLibRendering;
	}

	public void getRoute(MapPanel map, RenderedObject renderedObject, String tagKey, String type) {
		this.type = type;
		this.tagKey = tagKey;
		new Thread(() -> {
			List<Way> ways = getAllRoutes(renderedObject);
			DataTileManager<Way> points = new DataTileManager<>(11);
			for (Way w : ways) {
				LatLon n = w.getLatLon();
				points.registerObject(n.getLatitude(), n.getLongitude(), w);
			}
			map.setPoints(points);
		}).start();
	}

	private List<Way> getAllRoutes(RenderedObject renderedObject) {
		List<Way> wayList = new ArrayList<>();
		TIntArrayList x1 = renderedObject.getX();
		TIntArrayList y1 = renderedObject.getY();
		NativeSwingRendering.MapDiff mapDiffs = nativeLibRendering.getMapDiffs(MapUtils.get31LatitudeY(y1.get(0)),
				MapUtils.get31LongitudeX(x1.get(0)));
		if (mapDiffs == null) {
			return wayList;
		}
		File mapFile = mapDiffs.baseFile;
		List<BinaryMapDataObject> foundSegmentList = new ArrayList<>();
		List<BinaryMapDataObject> finalSegmentList = new ArrayList<>();
		BinaryMapDataObject startSegment = null;

		int x = x1.get(0);
		int y = y1.get(0);
		int xStart = 0;
		int yStart = 0;
		long id = renderedObject.getId();

		try {
			RandomAccessFile raf = new RandomAccessFile(mapFile, "r");
			BinaryMapIndexReader indexReader = new BinaryMapIndexReader(raf, mapFile);
			BinaryMapIndexReader.MapIndex mapIndex = indexReader.getMapIndexes().get(0);

			final SearchRequest<BinaryMapDataObject> req = buildSearchRequest(foundSegmentList, x, y);
			foundSegmentList.clear();
			indexReader.searchMapIndex(req, mapIndex);
			if (!foundSegmentList.isEmpty()) {
				for (BinaryMapDataObject foundSegment : foundSegmentList) {
					if (id == foundSegment.getId()) {
						startSegment = foundSegment;
						break;
					}
				}
				BinaryMapDataObject segment = startSegment;
				finalSegmentList.add(segment);
				xStart = segment.getPoint31XTile(0);
				yStart = segment.getPoint31YTile(0);
				x = segment.getPoint31XTile(segment.getPointsLength() - 1);
				y = segment.getPoint31YTile(segment.getPointsLength() - 1);
			}
			getRoutePart(finalSegmentList, x, y, indexReader, mapIndex);
			getRoutePart(finalSegmentList, xStart, yStart, indexReader, mapIndex);

		} catch (IOException e) {
			e.printStackTrace();
		}
		for (BinaryMapDataObject segment : finalSegmentList) {
			Way w = new Way(-1);
			if (segment.getPointsLength() > 1) {
				for (int i = 0; i < segment.getPointsLength(); i++) {
					x = segment.getPoint31XTile(i);
					y = segment.getPoint31YTile(i);
					Node n = new Node(MapUtils.get31LatitudeY(y), MapUtils.get31LongitudeX(x), -1);
					w.addNode(n);
				}
				wayList.add(w);
			}
		}
		return wayList;
	}

	private void getRoutePart(List<BinaryMapDataObject> finalSegmentList, int x, int y,
	                          BinaryMapIndexReader indexReader, BinaryMapIndexReader.MapIndex mapIndex) throws IOException {
		List<BinaryMapDataObject> foundSegmentList = new ArrayList<>();
		boolean exit = false;
		while (!exit) {
			final SearchRequest<BinaryMapDataObject> req = buildSearchRequest(foundSegmentList, x, y);
			foundSegmentList.clear();
			indexReader.searchMapIndex(req, mapIndex);
			exit = true;
			int yTmp = y;
			int xTmp = x;
			foundSegmentList.removeIf(s -> finalSegmentList.stream()
					.anyMatch(fs -> fs.compareBinary(s, 0))
					|| !isConnected(s, xTmp, yTmp)
					&& !isRoundabout(s));

			for (BinaryMapDataObject foundSegment : foundSegmentList) {
				if (isRoundabout(foundSegment)) {
					finalSegmentList.add(foundSegment);
					foundSegment = processRoundabout(foundSegment, finalSegmentList, indexReader, mapIndex);
					int xb = foundSegment.getPoint31XTile(0);
					int yb = foundSegment.getPoint31YTile(0);
					int xe = foundSegment.getPoint31XTile(foundSegment.getPointsLength() - 1);
					int ye = foundSegment.getPoint31YTile(foundSegment.getPointsLength() - 1);
					double distBegin = MapUtils.getSqrtDistance(x, y, xb, yb);
					double distEnd = MapUtils.getSqrtDistance(x, y, xe, ye);
					if (distBegin < distEnd) {
						x = xb;
						y = yb;
					} else {
						x = xe;
						y = ye;
					}
				}
				finalSegmentList.add(foundSegment);
				int xNext;
				int yNext;
				if (foundSegmentList.size() > 1) {
					xNext = foundSegment.getPoint31XTile(foundSegment.getPointsLength() - 1);
					yNext = foundSegment.getPoint31YTile(foundSegment.getPointsLength() - 1);
					if (xNext == x && yNext == y) {
						xNext = foundSegment.getPoint31XTile(0);
						yNext = foundSegment.getPoint31YTile(0);
					}
					getRoutePart(finalSegmentList, xNext, yNext, indexReader, mapIndex);
				} else {
					exit = false;
					xNext = foundSegment.getPoint31XTile(foundSegment.getPointsLength() - 1);
					yNext = foundSegment.getPoint31YTile(foundSegment.getPointsLength() - 1);
					if (xNext == x && yNext == y) {
						x = foundSegment.getPoint31XTile(0);
						y = foundSegment.getPoint31YTile(0);
					} else {
						x = xNext;
						y = yNext;
					}
				}
			}
		}
	}

	private BinaryMapDataObject processRoundabout(BinaryMapDataObject foundSegment, List<BinaryMapDataObject> finalSegmentList,
	                                              BinaryMapIndexReader indexReader,
	                                              BinaryMapIndexReader.MapIndex mapIndex) throws IOException {
		List<BinaryMapDataObject> foundSegmentList = new ArrayList<>();
		for (int i = 0; i < foundSegment.getPointsLength(); i++) {
			final SearchRequest<BinaryMapDataObject> req = buildSearchRequest(foundSegmentList,
					foundSegment.getPoint31XTile(i), foundSegment.getPoint31YTile(i));
			foundSegmentList.clear();
			indexReader.searchMapIndex(req, mapIndex);
			if (!foundSegmentList.isEmpty()) {
				foundSegmentList.removeIf(s -> finalSegmentList.stream()
						.anyMatch(fs -> fs.compareBinary(s, 0)));
				if (!foundSegmentList.isEmpty()) {
					break;
				}
			}
		}
		if (!foundSegmentList.isEmpty()) {
			return foundSegmentList.get(0);
		}
		return foundSegment;
	}

	private SearchRequest<BinaryMapDataObject> buildSearchRequest(List<BinaryMapDataObject> foundSegmentList,
	                                                              int xc, int yc) {
		return BinaryMapIndexReader.buildSearchRequest(xc - DEVIATE, xc + DEVIATE,
				yc - DEVIATE, yc + DEVIATE,
				15, (types, index) -> true,
				new ResultMatcher<BinaryMapDataObject>() {
					@Override
					public boolean publish(BinaryMapDataObject object) {
						Map<Integer, List<String>> objectTagMap = new HashMap<>();
						for (int routeIdx = 1; routeIdx <= getRouteQuantity(object); routeIdx++) {
							String prefix = ROUTE_PREFIX + type + "_" + routeIdx;
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
							for (Map.Entry<Integer, List<String>> entry : objectTagMap.entrySet()) {
								List<String> objectTagList = entry.getValue();
								Collections.sort(objectTagList);
								String objectTagKey = String.join("", objectTagList);
								if (Algorithms.stringsEqual(tagKey, objectTagKey)) {
									foundSegmentList.add(object);
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
							if (tag.startsWith(ROUTE_PREFIX + type)) {
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
	}

	private boolean isRoundabout(BinaryMapDataObject segment) {
		int last = segment.getPointsLength() - 1;
		return last != 0 && segment.getPoint31XTile(last) == segment.getPoint31XTile(0)
				&& segment.getPoint31YTile(last) == segment.getPoint31YTile(0);
	}

	private boolean isConnected(BinaryMapDataObject segment, int xc, int yc) {
		int last = segment.getPointsLength() - 1;
		return xc == segment.getPoint31XTile(last) && yc == segment.getPoint31YTile(last)
				|| xc == segment.getPoint31XTile(0) && yc == segment.getPoint31YTile(0);
	}
}
