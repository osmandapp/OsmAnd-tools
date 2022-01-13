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

public class IconVisibility {

	public static final int ICON = 0;
	public static final int TEXT = 1;
	public static final int ALL = 2;
	public static final int TEST_FILE = 0;
	public static final int RENDER_FILE = 1;
	public static final String[] defArgs = {"src/test/resources/Synthetic_test_rendering.obf", "default.render.xml"};
	public static final String helpMessage = "use --test-obf=file.obf [--render=file.render.xml] for compare icon visibility in the file.obf map on the file.render.xml style\n" +
			"\t if --render is omitted used default.render.xml";
	Map<Integer, List<VisibleObject>> mapObjectMap = new LinkedHashMap<>();
	Map<Integer, Integer> maxIconOrderInZoom = new LinkedHashMap<>();
	Map<Integer, Integer> maxTextOrderInZoom = new LinkedHashMap<>();
	private final StringBuilder outMessage = new StringBuilder();

	public static void main(String[] args) throws IOException {
		if (args == null || args.length < 1) {
			System.out.println(helpMessage);
			return;
		}
		boolean validArgs = false;
		for (String arg : args) {
			if (arg.startsWith("--test-obf=")) {
				defArgs[TEST_FILE] = arg.substring("--test-obf=".length());
				validArgs = true;
			} else if (arg.startsWith("--render=")) {
				defArgs[RENDER_FILE] = arg.substring("--render=".length());
			}
		}
		if (!validArgs) {
			System.out.println(helpMessage);
			return;
		}
		IconVisibility iconComparator = new IconVisibility();
		iconComparator.compare(defArgs[TEST_FILE], defArgs[RENDER_FILE]);
	}

	public String compare(String filePath, String renderFilePath) throws IOException {
		File file = new File(filePath);
		RandomAccessFile raf = new RandomAccessFile(file.getAbsolutePath(), "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, file);
		RenderingRulesStorage storage = getRenderingStorage(renderFilePath);
		RenderingRuleSearchRequest request = new RenderingRuleSearchRequest(storage);
		initCustomRules(storage, request);
		loadMapObject(reader, request);
		return compareOrder();
	}

	private void loadMapObject(BinaryMapIndexReader reader, RenderingRuleSearchRequest request) throws IOException {
		for (MapIndex mapIndex : reader.getMapIndexes()) {
			for (MapRoot root : mapIndex.getRoots()) {

				for (int zoom = root.getMaxZoom(); zoom >= root.getMinZoom(); zoom--) {
					final int[] maxOrder = {0, 0};
					final int[] statCounts = {0, 0, 0};
					int finalZoom = zoom;
					final BinaryMapIndexReader.SearchRequest<BinaryMapDataObject> req = buildSearchRequest(
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
										int textOrder = -1;
										// uncomment for textOrder check
										// int textOrder = request.getIntPropertyValue(request.ALL.R_TEXT_ORDER);
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
					outMessage.append(String.format("zoom %d total objects: %d ", zoom, statCounts[ALL]));
					if (maxOrder[ICON] != 0) {
						maxIconOrderInZoom.put(zoom, maxOrder[ICON]);
						outMessage.append(String.format("icon: %d , maxIconOrder %d",
								statCounts[ICON], maxOrder[ICON]));
					}
					if (maxOrder[TEXT] != 0) {
						maxIconOrderInZoom.put(zoom, maxOrder[TEXT]);
						outMessage.append(String.format("text: %d , maxTextOrder %d ",
								statCounts[TEXT], maxOrder[TEXT]));
					}
					outMessage.append("\n");
				}
			}
		}
	}

	private String compareOrder() {
		List<VisibleObject> visibleObjects = new ArrayList<>();
		List<Integer> selectedZooms = new ArrayList<>(maxIconOrderInZoom.keySet());
		String testMessage = "";
		if (selectedZooms.isEmpty()) {
			System.out.println("Icons not found");
			return "Icons not found";
		}
		Collections.sort(selectedZooms);
		int maxIconOrderZoom = 0;
		int maxTextOrderZoom = 0;
		boolean test = true;
		List<Long> checkedObject = new ArrayList<>();
		for (int zoom = selectedZooms.get(0); zoom < selectedZooms.get(selectedZooms.size() - 1); zoom++) {
			outMessage.append(String.format("zoom: %d (min icon order %d) -> %d%n", zoom, maxIconOrderZoom, (zoom + 1)));
			List<VisibleObject> zoomList = mapObjectMap.get(zoom);
			List<VisibleObject> zoomNextList = mapObjectMap.get(zoom + 1);
			visibleObjects.clear();
			for (VisibleObject objectNextZoom : zoomNextList) {
				for (VisibleObject object : zoomList) {
					if (object.mapDataObject.getId() == objectNextZoom.mapDataObject.getId() //  ){
							&& !checkedObject.contains(objectNextZoom.mapDataObject.getId())) {
						visibleObjects.add(objectNextZoom);
						break;
					}
				}
			}
			for (VisibleObject object : visibleObjects) {
				if (object.iconOrder <= maxIconOrderZoom) {
					checkedObject.add(object.mapDataObject.getId());
					outMessage.append(object.toStringWithZoom(zoom))
							.append(String.format(" <= max order (%d) for zoom %d%n", maxIconOrderZoom, zoom - 1));
					if (test) {
						testMessage = object.toStringWithZoom(zoom) + String.format(" <= max order (%d) for zoom %d%n",
								maxIconOrderZoom, zoom - 1);
						test = false;
					}
				}
			}

			if (maxIconOrderInZoom.containsKey(zoom)) {
				maxIconOrderZoom = maxIconOrderInZoom.get(zoom);
			}
			if (maxTextOrderInZoom.containsKey(zoom)) {
				maxTextOrderZoom = maxTextOrderInZoom.get(zoom);
			}
		}
		if (!test) {
			System.out.println(outMessage);
		}
		return testMessage;
	}

	RenderingRulesStorage getRenderingStorage(String renderFilePath) throws IOException {
		final Map<String, String> renderingConstants = new LinkedHashMap<>();
		InputStream is = getInputStream(renderFilePath);
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
		is = getInputStream(renderFilePath);
		final RenderingRulesStorage.RenderingRulesStorageResolver resolver = (name, ref) -> {
			RenderingRulesStorage depends = new RenderingRulesStorage(name, renderingConstants);
			depends.parseRulesFromXmlInputStream(RenderingRulesStorage.class.getResourceAsStream(name + ".render.xml"), ref);
			return depends;
		};
		try {
			storage.parseRulesFromXmlInputStream(is, resolver);
		} catch (XmlPullParserException e) {
			e.printStackTrace();
		} finally {
			is.close();
		}
		return storage;
	}

	private InputStream getInputStream(String renderFilePath) throws FileNotFoundException {
		InputStream is;
		if (renderFilePath.equals("default.render.xml")) {
			is = RenderingRulesStorage.class.getResourceAsStream(renderFilePath);
		} else {
			is = new FileInputStream(renderFilePath);
		}
		return is;
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

		public String toStringWithZoom(int zoom) {
			StringBuilder result = new StringBuilder(String.format("ZOOM %d: id %-10d icon %s tags[", zoom,
					mapDataObject.getId() >> 7, icon));
			for (int at = 0; at < mapDataObject.getTypes().length; at++) {
				TagValuePair tagValuePair = mapDataObject.getMapIndex().decodeType(mapDataObject.getTypes()[at]);
				result.append(String.format("\"%s\" ", tagValuePair.tag + "=" + tagValuePair.value));
			}
			result.append(String.format("] order=%d", iconOrder));
			return result.toString();
		}
	}
}
