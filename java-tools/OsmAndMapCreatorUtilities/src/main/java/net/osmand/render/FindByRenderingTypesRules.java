package net.osmand.render;

import net.osmand.PlatformUtil;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class FindByRenderingTypesRules {
	private final int SEARCH_ZOOM = 19;
	private final RenderingRulesStorage renderingRules;
	public final MapRenderingTypesEncoder renderingTypes;
	private final RenderingRuleSearchRequest searchRequest;

	public FindByRenderingTypesRules() {
		this(new String[]{"default.render.xml"}, null);
	}

	public FindByRenderingTypesRules(@Nonnull String[] styles, @Nullable Map<String, String> initCustomProperties) {
		renderingTypes = new MapRenderingTypesEncoder("basemap");
		renderingRules = initStylesFromResources(styles);

		searchRequest = new RenderingRuleSearchRequest(renderingRules);
		for (RenderingRuleProperty customProp : renderingRules.PROPS.getCustomRules()) {
			String custom = initCustomProperties != null ? initCustomProperties.get(customProp.getAttrName()) : null;
			boolean booleanValue = custom != null && "true".equals(custom);
			String stringValue = custom != null ? custom : "";
			if (customProp.isBoolean()) {
				searchRequest.setBooleanFilter(customProp, booleanValue);
			} else {
				searchRequest.setStringFilter(customProp, stringValue);
			}
		}

		searchRequest.setIntFilter(renderingRules.PROPS.R_MINZOOM, SEARCH_ZOOM);
		searchRequest.setIntFilter(renderingRules.PROPS.R_MAXZOOM, SEARCH_ZOOM);

		searchRequest.saveState();
	}

	public static RenderingRulesStorage initStylesFromResources(String... resourceFileNames) {
		try {
			RenderingRulesStorage storage = null;
			final String BASE_EXT = ".render.xml";
			final String ADDON_EXT = ".addon.render.xml";
			for (String resourceName : resourceFileNames) {
				boolean addon = resourceName.endsWith(ADDON_EXT);
				String styleName = resourceName.replace(ADDON_EXT, "").replace(BASE_EXT, "");
				Map<String, String> constants = readRenderingConstantsFromIS(
						RenderingRulesStorage.class.getResourceAsStream(resourceName));
				if (storage == null) {
					storage = new RenderingRulesStorage(styleName, constants);
				} else {
					storage.renderingConstants.putAll(constants);
				}
				final RenderingRulesStorage.RenderingRulesStorageResolver resolver = (name, ref) -> {
					final String resource = name + (addon ? ADDON_EXT : BASE_EXT);
					final RenderingRulesStorage depends = new RenderingRulesStorage(name,
							readRenderingConstantsFromIS(RenderingRulesStorage.class.getResourceAsStream(resource)));
					final InputStream depStream = RenderingRulesStorage.class.getResourceAsStream(resource);
					depends.parseRulesFromXmlInputStream(depStream, ref, false);
					depStream.close();
					return depends;
				};
				final InputStream xmlStream = RenderingRulesStorage.class.getResourceAsStream(resourceName);
				storage.parseRulesFromXmlInputStream(xmlStream, resolver, addon);
				xmlStream.close();
			}
			return storage;
		} catch (XmlPullParserException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static Map<String, String> readRenderingConstantsFromIS(InputStream is) throws XmlPullParserException, IOException {
		Map<String, String> renderingConstants = new LinkedHashMap<String, String>();
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
		} finally {
			is.close();
		}
		return renderingConstants;
	}

	@Nullable
	public Map<String, String> searchOsmcPropertiesByFinalTags(@Nonnull Map<String, String> tags) {
		final String FIRST = "_1", ROUTE_PREFIX = "route_";
		final String OSMC_TEXT_SUFFIX = "_1_osmc_text", OSMC_WAYCOLOR_SUFFIX = "_osmc_waycolor";
		// TODO tags-transfor _1_osmc_text => shield_text, etc

		String routeTypeTag = null;
		for (String key : tags.keySet()) {
			if (key.startsWith(ROUTE_PREFIX) && key.endsWith(FIRST)) {
				routeTypeTag = key.replace(FIRST, "");
			}
		}

		String routeNameTag = routeTypeTag != null ? routeTypeTag + OSMC_TEXT_SUFFIX : null;

		Map<String, String> searchTags = new LinkedHashMap<>();
		if (routeTypeTag != null) {
			searchTags.put(routeTypeTag, tags.get(routeTypeTag + FIRST));
		}
		searchTags.putAll(tags);

		for (Map.Entry<String, String> entry : searchTags.entrySet()) {
			final String tag = entry.getKey();
			final String value = entry.getValue();

			searchRequest.clearState();
			searchRequest.setStringFilter(renderingRules.PROPS.R_TAG, tag);
			searchRequest.setStringFilter(renderingRules.PROPS.R_VALUE, value);
			if (routeNameTag != null) {
				searchRequest.setStringFilter(renderingRules.PROPS.R_NAME_TAG, routeNameTag);
			}
			searchRequest.search(RenderingRulesStorage.TEXT_RULES);

			RenderingRuleProperty[] props = {
					renderingRules.PROPS.R_ICON,
					renderingRules.PROPS.R_ICON_2,
					renderingRules.PROPS.R_TEXT_SHIELD
			};

			Map<String, String> result = new HashMap<>();
			for (RenderingRuleProperty p : props) {
				String key = p.getAttrName();
				String val = searchRequest.getStringPropertyValue(p);
				String validated = substituteAndValidate(val, searchTags);
				if (validated != null) {
					result.put(key, validated);
				}
			}

			if (!result.isEmpty()) {
				for (String key : searchTags.keySet()) {
					if (key.endsWith(OSMC_WAYCOLOR_SUFFIX)) {
						result.put("color", searchTags.get(key));
					}
				}
				if (routeNameTag != null) {
					result.put("shield_text", searchTags.get(routeNameTag));
				}
				return result;
			}
		}

		return null;
	}

	@Nullable
	private String substituteAndValidate(@Nullable String in, @Nonnull Map<String, String> tags) {
		if (in != null && in.contains("?")) {
			for (String key : tags.keySet()) {
				if (in.contains("?" + key + "?")) {
					return in.replace("?" + key + "?", tags.get(key));
				}
			}
			return null; // invalid
		}
		return in;
	}

	@Nullable
	public String searchGpxIconByNode(@Nonnull Node node) {
		return searchBestPropertyByOsmTags(node.getTags(),
				Entity.EntityType.NODE, RenderingRulesStorage.POINT_RULES,
				renderingRules.PROPS.R_ICON, renderingRules.PROPS.R_ICON_ORDER);
	}

	@Nullable
	private String searchBestPropertyByOsmTags(@Nonnull Map<String, String> osmTags,
	                                           @Nonnull Entity.EntityType entityType, int searchRulesNumber,
	                                           @Nonnull RenderingRuleProperty mainStringProperty,
	                                           @Nullable RenderingRuleProperty orderIntProperty) {

		Map<String, Integer> resultOrderMap = new HashMap<>();
		final Map<String, String> transformedTags = renderingTypes.transformTags(
				osmTags, entityType, MapRenderingTypesEncoder.EntityConvertApplyType.MAP);

		for (Map.Entry<String, String> entry : transformedTags.entrySet()) {
			final String tag = entry.getKey();
			final String value = entry.getValue();
			searchRequest.clearState();
			searchRequest.setStringFilter(renderingRules.PROPS.R_TAG, tag);
			searchRequest.setStringFilter(renderingRules.PROPS.R_VALUE, value);
			searchRequest.clearValue(renderingRules.PROPS.R_ADDITIONAL); // parent - no additional

			int order = 0;
			String result = null;
			searchRequest.search(searchRulesNumber);

			if (searchRequest.isSpecified(mainStringProperty)) {
				result = searchRequest.getStringPropertyValue(mainStringProperty);
				order = orderIntProperty != null ? searchRequest.getIntPropertyValue(orderIntProperty) : 0;
			}

			// Cycle tags to visit "additional" rules. TODO: think how to optimize.
			for (Map.Entry<String, String> additional : transformedTags.entrySet()) {
				final String aTag = additional.getKey();
				final String aValue = additional.getValue();

				if (aTag.equals(tag) && aValue.equals(value)) {
					continue;
				}

				searchRequest.setStringFilter(renderingRules.PROPS.R_ADDITIONAL, aTag + "=" + aValue);
				searchRequest.search(searchRulesNumber);

				if (searchRequest.isSpecified(mainStringProperty)) {
					String childResult = searchRequest.getStringPropertyValue(mainStringProperty);
					if (childResult != null && (result == null || !result.equals(childResult))) {
						order = orderIntProperty != null ? searchRequest.getIntPropertyValue(orderIntProperty) : 0;
						result = childResult;
						break; // enough
					}
				}
			}

			if (result != null) {
				resultOrderMap.put(result, order);
			}
		}

		if (!resultOrderMap.isEmpty()) {
			Map.Entry<String, Integer> bestResult =
					Collections.min(resultOrderMap.entrySet(), Comparator.comparingInt(Map.Entry::getValue));
			return bestResult.getKey();
		}

		return null;
	}
}
