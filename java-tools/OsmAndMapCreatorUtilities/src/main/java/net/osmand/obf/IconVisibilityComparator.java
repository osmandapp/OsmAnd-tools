package net.osmand.obf;

import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.render.RenderingRuleSearchRequest;
import net.osmand.render.RenderingRulesStorage;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import java.io.*;
import java.util.*;

import static net.osmand.binary.BinaryMapIndexReader.*;

public class IconVisibilityComparator {
	String RENDER_FILE = "/home/user/osmand/issues/i985/default.render.xml";
	String TEST_FILE = "/home/user/osmand/issues/i985/Synthetic_test_rendering.obf";
	Map<Integer, List<VisibleObject>> mapObjectMap = new LinkedHashMap<>();
	private int maxZoom = 0;
	private int minZoom = 22;

	public static void main(String[] args) throws IOException {
		IconVisibilityComparator iconComparator = new IconVisibilityComparator();
		iconComparator.compare();
	}

	void compare() throws IOException {
		File file = new File(TEST_FILE);
		RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(r, file);
		RenderingRuleSearchRequest request = new RenderingRuleSearchRequest(getRenderingStorage());
		for (MapIndex mapIndex : reader.getMapIndexes()) {
			for (MapRoot root : mapIndex.getRoots()) {
				int rootMaxZoom = root.getMaxZoom();
				int rootMinZoom = root.getMinZoom();
				System.out.printf("min %d max %d %n", rootMinZoom, rootMaxZoom);
				if (rootMaxZoom > maxZoom) {
					maxZoom = rootMaxZoom;
				}
				if (rootMinZoom < minZoom) {
					minZoom = rootMinZoom;
				}

				for (int zoom = rootMaxZoom; zoom >= rootMinZoom; zoom--) {
					final int[] count = {0, 0, 0};
					int finalZoom = zoom;
					final SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(
							0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, zoom,
							(types, index) -> true,
							new ResultMatcher<BinaryMapDataObject>() {
								@Override
								public boolean publish(BinaryMapDataObject obj) {
									count[0]++;
									for (int j = 0; j < obj.getTypes().length; j++) {
										int wholeType = obj.getTypes()[j];
										TagValuePair pair = obj.getMapIndex().decodeType(wholeType);
										request.setInitialTagValueZoom(pair.tag, pair.value, finalZoom, obj);
										request.search(RenderingRulesStorage.TEXT_RULES);
										int textOrder = request.getIntPropertyValue(request.ALL.R_TEXT_ORDER);
										if (textOrder > 0) {
											count[1]++;
										}
										request.search(RenderingRulesStorage.POINT_RULES);
										int iconOrder = request.getIntPropertyValue(request.ALL.R_ICON_ORDER);
										if (iconOrder > 0) {
											count[2]++;
										}
										if (textOrder > 0 || iconOrder > 0) {
											VisibleObject vo = new VisibleObject();
											vo.mapDataObject = obj;
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
					System.out.printf("zoom %d count %d text: %d icon: %d%n", zoom, count[0], count[1], count[2]);
				}
			}
		}
		printDiffs();
	}

	private void printDiffs() {
		List<VisibleObject> groupA = new ArrayList<>();
		List<VisibleObject> groupB = new ArrayList<>();
		for (int zoom = minZoom; zoom < maxZoom; zoom++) {
			List<VisibleObject> zoomList = mapObjectMap.get(zoom);
			List<VisibleObject> zoom1List = mapObjectMap.get(zoom + 1);
			groupA.clear();
			groupB.clear();
			System.out.println("zoom: " + zoom + " - " + (zoom + 1));
			for (VisibleObject oz1 : zoom1List) {
				for (VisibleObject oz : zoomList) {
					if (oz.mapDataObject.getId() == oz1.mapDataObject.getId()) {
						groupB.add(oz1);
						break;
					}
				}
				if (!groupB.contains(oz1)) {
					groupA.add(oz1);
				}
			}
			for (VisibleObject a : groupA) {
				for (VisibleObject b : groupB) {
					if (b.iconOrder > 0 && a.iconOrder > 0 && b.iconOrder < a.iconOrder) {
						System.out.printf("icon name=\"%s\" %d < a name=\"%s\" %d%n",
								b.mapDataObject.getName(), b.iconOrder,
								a.mapDataObject.getName(), a.iconOrder);
					}
					if (b.textOrder > 0 && a.textOrder > 0 && b.textOrder < a.textOrder) {
						System.out.printf("text b name=\"%s\" %d < a name=\"%s\" %d%n",
								b.mapDataObject.getName(), b.textOrder,
								a.mapDataObject.getName(), a.textOrder);
					}
				}
			}

		}
	}

	RenderingRulesStorage getRenderingStorage() throws IOException {
		final Map<String, String> renderingConstants = new LinkedHashMap<>();
		InputStream is = new FileInputStream(RENDER_FILE);
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
		is = new FileInputStream(RENDER_FILE);
		try {
			storage.parseRulesFromXmlInputStream(is, (nm, ref) -> null);
		} catch (XmlPullParserException e) {
			e.printStackTrace();
		} finally {
			is.close();
		}
		return storage;
	}

	static class VisibleObject {
		BinaryMapDataObject mapDataObject;
		int textOrder;
		int iconOrder;
	}
}
