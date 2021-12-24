package net.osmand.obf;

import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.render.RenderingRuleProperty;
import net.osmand.render.RenderingRuleSearchRequest;
import net.osmand.render.RenderingRulesStorage;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import java.io.*;
import java.util.*;

import static net.osmand.binary.BinaryMapIndexReader.*;

public class IconVisibilityComparator {

	public static final int ICON = 0;
	public static final int TEXT = 1;
	public static final int ALL = 2;
	public static final int TEST_FILE = 0;
	public static final int RENDER_FILE = 1;
	Map<Integer, List<VisibleObject>> mapObjectMap = new LinkedHashMap<>();
	Map<Integer, Integer> maxIconOrderInZoom = new LinkedHashMap<>();
	Map<Integer, Integer> maxTextOrderInZoom = new LinkedHashMap<>();

	public static void main(String[] args) throws IOException {
		String[] defArgs = {"Synthetic_test_rendering.obf","default.render.xml"};
		for (String arg : args) {
			if (arg.startsWith("--test-obf=")) {
				defArgs[TEST_FILE] = arg.substring("--test-obf=".length());
			} else if (arg.startsWith("--render=")) {
				defArgs[RENDER_FILE] = arg.substring("--render=".length());
			}
		}
		IconVisibilityComparator iconComparator = new IconVisibilityComparator();
		iconComparator.compare(defArgs[TEST_FILE], defArgs[RENDER_FILE]);
	}

	void compare(String filePath, String renderFilePath) throws IOException {
		File file = new File(filePath);
		RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(r, file);
		RenderingRulesStorage storage = getRenderingStorage(renderFilePath);
		RenderingRuleSearchRequest request = new RenderingRuleSearchRequest(storage);
		initCustomRules(storage, request);
		for (MapIndex mapIndex : reader.getMapIndexes()) {
			for (MapRoot root : mapIndex.getRoots()) {

				for (int zoom = root.getMaxZoom(); zoom >= root.getMinZoom(); zoom--) {
					final int[] maxOrder = {0, 0};
					final int[] statCounts = {0, 0, 0};
					int finalZoom = zoom;
					final SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(
							0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, zoom,
							(types, index) -> true,
							new ResultMatcher<BinaryMapDataObject>() {
								@Override
								public boolean publish(BinaryMapDataObject obj) {
									statCounts[ALL]++;
									for (int j = 0; j < obj.getTypes().length; j++) {
										int wholeType = obj.getTypes()[j];
										TagValuePair pair = mapIndex.decodeType(wholeType);
										request.setInitialTagValueZoom(pair.tag, pair.value, finalZoom, obj);
										request.search(RenderingRulesStorage.TEXT_RULES);
										int textOrder = -1;//request.getIntPropertyValue(request.ALL.R_TEXT_ORDER);
										if (textOrder > 0) {
											if (textOrder > maxOrder[TEXT]) {
												maxOrder[TEXT] = textOrder;
											}
											statCounts[TEXT]++;
										}
										request.setInitialTagValueZoom(pair.tag, pair.value, finalZoom, obj);
										request.search(RenderingRulesStorage.POINT_RULES);
										int iconOrder = request.getIntPropertyValue(request.ALL.R_ICON_ORDER);
										if (iconOrder > 0) {
											if (iconOrder > maxOrder[ICON]) {
												maxOrder[ICON] = iconOrder;
											}
											statCounts[ICON]++;
										}
										if (textOrder > 0 || iconOrder > 0) {
											VisibleObject vo = new VisibleObject();
											vo.mapDataObject = obj;
											vo.icon = request.getStringPropertyValue(request.ALL.R_ICON);
											vo.iconOrder = iconOrder;
											vo.textOrder = textOrder;
											List<VisibleObject> voList = mapObjectMap
													.computeIfAbsent(finalZoom, k -> new ArrayList<>());
											voList.add(vo);
										}
									}

									return false;
								}

								@Override
								public boolean isCancelled() {
									return false;
								}
							});

					reader.searchMapIndex(req, mapIndex);
					System.out.printf("zoom %d total objects: %d ", zoom, statCounts[ALL]);
					if (maxOrder[ICON] != 0) {
						maxIconOrderInZoom.put(zoom, maxOrder[ICON]);
						System.out.printf("icon: %d , maxOrder %d",
								statCounts[ICON], maxOrder[ICON]);
					}
					if (maxOrder[TEXT] != 0) {
						maxIconOrderInZoom.put(zoom, maxOrder[TEXT]);
						System.out.printf("text: %d , maxOrder %d ",
								statCounts[TEXT], maxOrder[TEXT]);
					}
					System.out.println();
				}
			}
		}
		printDiffs();
	}

	private void printDiffs() {
		List<VisibleObject> groupA = new ArrayList<>();
		List<VisibleObject> groupB = new ArrayList<>();
		List<Integer> selectedZooms = new ArrayList<>(maxIconOrderInZoom.keySet());
		Collections.sort(selectedZooms);
		for (int zoom = selectedZooms.get(0); zoom < selectedZooms.get(selectedZooms.size() - 1); zoom++) {
			int maxIconOrderZoom = 0;
			int maxTextOrderZoom = 0;
			if (maxIconOrderInZoom.containsKey(zoom)) {
				maxIconOrderZoom = maxIconOrderInZoom.get(zoom);
			}
			if (maxTextOrderInZoom.containsKey(zoom)) {
				maxTextOrderZoom = maxTextOrderInZoom.get(zoom);
			}
			List<VisibleObject> zoomList = mapObjectMap.get(zoom);
			List<VisibleObject> zoomNextList = mapObjectMap.get(zoom + 1);
			groupA.clear();
			groupB.clear();
			System.out.printf("zoom: %d (min order %d) -> %d%n", zoom, maxIconOrderZoom, (zoom + 1));
			for (VisibleObject oz1 : zoomNextList) {
				for (VisibleObject oz : zoomList) {
					if (oz.mapDataObject.getId() == oz1.mapDataObject.getId()) {
						groupB.add(oz);
						break;
					}
				}
				if (!groupB.contains(oz1)) {
					groupA.add(oz1);
				}
			}
			for (VisibleObject b : groupB) {
				printMapObj(zoom, b, "B");
			}
			for (VisibleObject a : groupA) {
//				printMapObj(zoom, a, "A");
			}
		}
	}

	private void printMapObj(int zoom, VisibleObject obj, String group) {
		System.out.printf("ZOOM %d: %s id %-10d icon %s tags[", zoom, group, obj.mapDataObject.getId() >> 7, obj.icon);
		for (int at = 0; at < obj.mapDataObject.getTypes().length; at++) {
			TagValuePair tagValuePair = obj.mapDataObject.getMapIndex().decodeType(obj.mapDataObject.getTypes()[at]);
			System.out.printf("\"%s\" ", tagValuePair.tag + "=" + tagValuePair.value);
		}
		System.out.println("] " + obj.iconOrder);
	}

	RenderingRulesStorage getRenderingStorage(String renderFilePath) throws IOException {
		final Map<String, String> renderingConstants = new LinkedHashMap<>();
		InputStream is = new FileInputStream(renderFilePath);
		try {
			XmlPullParser parser = PlatformUtil.newXMLPullParser();
			parser.setInput(is, "UTF-8");
			int tok;
			while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
				if (tok == XmlPullParser.START_TAG) {
					String tagName = parser.getName();
					if (tagName.equals("renderingConstant")) {
						if (!renderingConstants.containsKey(parser.getAttributeValue("", "name"))) {
							renderingConstants.put(parser.getAttributeValue("", "name"),
									parser.getAttributeValue("", "value"));
						}
					}
				}
			}
		} catch (XmlPullParserException | IOException e) {
			e.printStackTrace();
		} finally {
			is.close();
		}
		RenderingRulesStorage storage = new RenderingRulesStorage("default", renderingConstants);
		is = new FileInputStream(renderFilePath);
		try {
			storage.parseRulesFromXmlInputStream(is, (nm, ref) -> null);
		} catch (XmlPullParserException e) {
			e.printStackTrace();
		} finally {
			is.close();
		}
		return storage;
	}

	private void initCustomRules(RenderingRulesStorage storage, RenderingRuleSearchRequest request) {
		for (RenderingRuleProperty customProp : storage.PROPS.getCustomRules()) {
			if (customProp.isString()) {
				request.setStringFilter(customProp, "");
			} else if (customProp.isBoolean()) {
				request.setBooleanFilter(customProp, false);
			} else {
				request.setIntFilter(customProp, -1);
			}
		}
		request.saveState();
	}

	static class VisibleObject {
		BinaryMapDataObject mapDataObject;
		int textOrder;
		int iconOrder;
		String icon;
	}
}
