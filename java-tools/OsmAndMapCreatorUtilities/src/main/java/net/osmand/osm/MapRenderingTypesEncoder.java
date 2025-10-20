package net.osmand.osm;

import gnu.trove.list.array.TIntArrayList;
import net.osmand.PlatformUtil;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class MapRenderingTypesEncoder extends MapRenderingTypes {

	private static final String HIGH_CHARGING_OUT = "high";
	private static final String MEDIUM_CHARGING_OUT = "medium";
	private static final String LOW_CHARGING_OUT = "low";
	public static final char SOCKET_KEY_DELIMITER = ':';
	private static Log log = PlatformUtil.getLog(MapRenderingTypesEncoder.class);
	// stored information to convert from osm tags to int type
	private List<MapRouteTag> routeTags = new ArrayList<MapRouteTag>();
	private Map<String, List<EntityConvert>> convertTags = new LinkedHashMap<String, List<EntityConvert>>();
	private MapRulType coastlineRuleType;
	private String regionName;
	public static final String OSMAND_REGION_NAME_TAG = "osmand_region_name";

	private static final Collection<String> NODE_NETWORK_IDS = Arrays.asList("network:type", "expected_rcn_route_relations");
	private static final Collection<String> NODE_NETWORKS_REF_TYPES =
			new TreeSet<>(Arrays.asList("icn_ref", "ncn_ref", "rcn_ref", "lcn_ref", "iwn_ref", "nwn_ref", "rwn_ref", "lwn_ref"));
	private static final String NODE_NETWORK_TAG = "node_network_point";
	private static final String NODE_NETWORK_MULTIPLE_VALUE = "multiple";
	private static final boolean DELETE_AFTER_38_RELEASE = false;
	private static final Map<String, String> OSMC_NO_NAME_FOREGROUND = Map.of(
			"hiking", "black_hiker",
			"bicycle", "black_bicycle",
			"mtb", "black_bicycle",
			"horse", "black_horse",
			"fitness_trail", "black_runner",
			"running", "black_runner"
	);

	private Map<String, TIntArrayList> socketTypes;

	public MapRenderingTypesEncoder(String fileName, String regionName) {
		super(fileName != null && fileName.length() == 0 ? null : fileName);
		this.regionName = "$" + regionName.toLowerCase() + "^";
	}

	public MapRenderingTypesEncoder(String regionName) {
		super(null);
		this.regionName = "$" + regionName.toLowerCase() + "^";
	}

	private void initSocketTypes() {
		socketTypes = Map.ofEntries(Map.entry("socket:type2:output", new TIntArrayList(new int[]{20, 35})),
				Map.entry("socket:type2_combo:output", new TIntArrayList(new int[]{30, 70})),
				Map.entry("socket:type2_cable:output", new TIntArrayList(new int[]{30, 70})),
				Map.entry("socket:type3a:output", new TIntArrayList(new int[]{10, 20})),
				Map.entry("socket:type3c:output", new TIntArrayList(new int[]{10, 20})),
				Map.entry("socket:cee_blue:output", new TIntArrayList(new int[]{2, 5})),
				Map.entry("socket:chademo:output", new TIntArrayList(new int[]{30, 70})),
				Map.entry("socket:schuko:output", new TIntArrayList(new int[]{2, 3})),
				Map.entry("socket:type2:voltage", new TIntArrayList(new int[]{230, 500})),
				Map.entry("socket:type2_combo:voltage", new TIntArrayList(new int[]{500, 800})),
				Map.entry("socket:type2_cable:voltage", new TIntArrayList(new int[]{230, 500})),
				Map.entry("socket:cee_blue:voltage", new TIntArrayList(new int[]{220, 250})),
				Map.entry("socket:chademo:voltage", new TIntArrayList(new int[]{230, 500})),
				Map.entry("socket:schuko:voltage", new TIntArrayList(new int[]{220, 240})));
	}

	@Override
	protected MapRulType registerRuleType(MapRulType rt){
		rt = super.registerRuleType(rt);
		String tag = rt.tagValuePattern.tag;
		String val = rt.tagValuePattern.value;
		if("natural".equals(tag) && "coastline".equals(val)) {
			coastlineRuleType = rt;
		}
		return rt;
	}

	private MapRulType getMapRuleType(String tag, String val) {
		return getRuleType(tag, val, false, true);
	}

	public MapRulType getCoastlineRuleType() {
		getEncodingRuleTypes();
		return coastlineRuleType;
	}

	public List<MapRouteTag> getRouteTags() {
		checkIfInitNeeded();
		return routeTags;
	}


	@Override
	protected void parseEntityConvertXML(XmlPullParser parser) {

		String seq = parser.getAttributeValue("", "seq");
		if(Algorithms.isEmpty(seq)) {
			seq = "1:1";
		}
		String[] ls = seq.split(":");
		for (int ind = Integer.parseInt(ls[0]); ind <= Integer.parseInt(ls[1]); ind++) {
			Map<String, String> mp = new LinkedHashMap<String, String>();
			for (int i = 0; i < parser.getAttributeCount(); i++) {
				String at = parser.getAttributeName(i);
				mp.put(at, parser.getAttributeValue("", at).replace("*", ind + ""));
			}
			EntityConvert ec = new EntityConvert();
			String tg = mp.get("if_region_name"); //$NON-NLS-1$
			if (tg != null) {
				ec.ifRegionName.addAll(Arrays.asList(tg.split("\\,")));
			}
			tg = mp.get("if_not_region_name"); //$NON-NLS-1$
			if (tg != null) {
				ec.ifNotRegionName.addAll(Arrays.asList(tg.split("\\,")));
			}
			ec.lang = "true".equals(mp.get("lang"));
			ec.verbose = "true".equals(mp.get("verbose")); //$NON-NLS-1$
			parseConvertCol(mp, ec.ifTags, "if_");
			parseConvertCol(mp, ec.ifStartsTags, "if_starts_with_");
			parseConvertCol(mp, ec.ifNotStartsTags, "if_not_starts_with_");
			parseConvertCol(mp, ec.ifEndsTags, "if_ends_with_");
			parseConvertCol(mp, ec.ifNotEndsTags, "if_not_ends_with_");
			parseConvertCol(mp, ec.ifContainsTags, "if_contains_");
			parseConvertCol(mp, ec.ifNotContainsTags, "if_not_contains_");
			parseConvertCol(mp, ec.ifNotTags, "if_not_");
			parseConvertCol(mp, ec.ifTagsNotLess, "if_not_less_");
			parseConvertCol(mp, ec.ifTagsLess, "if_less_");
			ec.type = EntityConvertType.valueOf(mp.get("pattern").toUpperCase()); //$NON-NLS-1$
			ec.applyToType = EnumSet.allOf(EntityConvertApplyType.class);
			if ("no".equals(mp.get("routing"))
					|| "false".equals(mp.get("routing"))) {
				ec.applyToType.remove(EntityConvertApplyType.ROUTING);
			}
			if ("no".equals(mp.get("map")) || "false".equals(mp.get("map"))) {
				ec.applyToType.remove(EntityConvertApplyType.MAP);
			}
			if ("no".equals(mp.get("poi")) || "false".equals(mp.get("poi"))) {
				ec.applyToType.remove(EntityConvertApplyType.POI);
			}
			parseConvertCol(mp, ec.toTags, "to_", "");
			tg = mp.get("from_tag"); //$NON-NLS-1$
			String value = mp.get("from_value"); //$NON-NLS-1$
			if (tg != null) {
				ec.fromTag = new TagValuePattern(tg, "".equals(value) ? null : value);
				if (ec.type == EntityConvertType.TAG_COMBINE) {
					//from_tag1, from_tag2, from_tag3 etc.
					parseConvertCol(mp, ec.fromTagList, "from_");
					ec.separator = mp.get("separator") != null ? mp.get("separator") : " ";
				}
				if (!convertTags.containsKey(ec.fromTag.tag)) {
					convertTags.put(ec.fromTag.tag, new ArrayList<MapRenderingTypesEncoder.EntityConvert>());
				}
				convertTags.get(ec.fromTag.tag).add(ec);
			}
			String appTo = mp.get("apply_to"); //$NON-NLS-1$
			if (appTo != null) {
				ec.applyTo = EnumSet.noneOf(EntityType.class);
				String[] tps = appTo.split(",");
				for (String t : tps) {
					EntityType et = EntityType.valueOf(t.toUpperCase());
					ec.applyTo.add(et);
				}
			}
		}
	}



	protected void parseConvertCol(Map<String, String> mp, List<TagValuePattern> col, String prefix) {
		parseConvertCol(mp, col, prefix, null);
	}

	protected void parseConvertCol(Map<String, String> mp, List<TagValuePattern> col, String prefix,
			String emptyVal) {
		for (int i = 1; i <= 20; i++) {
			String tg = mp.get(prefix +"tag" + i); //$NON-NLS-1$
			String value = mp.get(prefix +"value" + i); //$NON-NLS-1$
			String tgPrefix = mp.get(prefix +"tag_prefix" + i); //$NON-NLS-1$
			if(tg == null) {
				tg = tgPrefix;
			}
			if (tg != null) {
				TagValuePattern pt = new TagValuePattern(tg, "".equals(value) ? emptyVal : value);
				pt.tagPrefix = tgPrefix;
				col.add(pt);
				String substr = mp.get(prefix +"substr" + i); //$NON-NLS-1$
				if(substr != null) {
					String[] ls = substr.split(":");
					if(ls.length > 0) {
						pt.substrSt = Integer.parseInt(ls[0]);
					}
					if(ls.length > 1) {
						pt.substrEnd = Integer.parseInt(ls[0]);
					}
				}
			}
		}
	}

	@Override
	protected void parseRouteTagFromXML(XmlPullParser parser) {
		String seq = parser.getAttributeValue("", "seq");
		if(Algorithms.isEmpty(seq)) {
			seq = "1:1";
		}
		String[] ls = seq.split(":");
		for (int ind = Integer.parseInt(ls[0]); ind <= Integer.parseInt(ls[1]); ind++) {
			MapRouteTag rtype = new MapRouteTag();
			String mode = parser.getAttributeValue("", "mode"); //$NON-NLS-1$
			rtype.tag = lc(parser.getAttributeValue("", "tag"), ind); //$NON-NLS-1$
			rtype.value = lc(parser.getAttributeValue("", "value"), ind); //$NON-NLS-1$
			rtype.tag2 = lc(parser.getAttributeValue("", "tag2"), ind); //$NON-NLS-1$
			rtype.value2 = lc(parser.getAttributeValue("", "value2"), ind); //$NON-NLS-1$
			rtype.base = Boolean.parseBoolean(parser.getAttributeValue("", "base"));
			rtype.replace = "replace".equalsIgnoreCase(mode);
			rtype.register = "register".equalsIgnoreCase(mode);
			rtype.type = lc(parser.getAttributeValue("", "type"), ind);
			rtype.amend = "amend".equalsIgnoreCase(mode);
			rtype.text = "text".equalsIgnoreCase(mode);
			routeTags.add(rtype);
		}
	}



	private String lc(String a, int seq) {
		if(a != null) {
			return a.toLowerCase().replace("*", seq+"");
		}
		return a;
	}

	@Override
	protected void parsePropagate(XmlPullParser parser, MapRulType parentType) {
		PropagateToNode ptype = parsePropagateType(parser);
		if (ptype != null && parentType != null) {
			parentType.propagateToNodes.add(ptype);
		}
	}


	@Override
	protected MapRulType parseAndRegisterTypeFromXML(XmlPullParser parser, MapRulType parent) {
		String seq = parser.getAttributeValue("", "seq");
		if (Algorithms.isEmpty(seq)) {
			seq = "1:1";
		}
		MapRulType mainType = null;
		String[] ls = seq.split(":");
		for (int ind = Integer.parseInt(ls[0]); ind <= Integer.parseInt(ls[1]); ind++) {
			String tag = lc(parser.getAttributeValue("", "tag"), ind);
			mainType = parseBaseRuleType(parser, parent, tag);
			registerMapRule(parser, mainType);
			if ("true".equals(parser.getAttributeValue("", "lang"))) {
				for (String lng : langs) {
					tag = lc(parser.getAttributeValue("", "tag"), ind) + ":" + lng;
					if (!types.containsKey(constructRuleKey(tag, null))) {
						MapRulType retype = parseBaseRuleType(parser, parent, tag);
						registerMapRule(parser, retype);
					}
				}
			}
		}
		return mainType;
	}


	public boolean encodeEntityWithType(Entity e, int zoom, TIntArrayList outTypes,
			TIntArrayList outAddTypes, TreeMap<MapRulType, String> namesToEncode, List<MapRulType> tempListNotUsed) {
		return encodeEntityWithType(e instanceof Node,
				e.getModifiableTags(), zoom, outTypes, outAddTypes, namesToEncode, tempListNotUsed);
	}

	public boolean encodeEntityWithType(boolean node, Map<String, String> tags, int zoom, TIntArrayList outTypes,
			TIntArrayList outAddTypes, TreeMap<MapRulType, String> namesToEncode, List<MapRulType> tempListNotUsed) {
		outTypes.clear();
		outAddTypes.clear();
		namesToEncode.clear();
		tags = transformTags(tags, node ? EntityType.NODE : EntityType.WAY, EntityConvertApplyType.MAP);
		boolean area = "yes".equals(tags.get("area"));
		for (String tag : tags.keySet()) {
			String val = tags.get(tag);
			if(tag.equals("seamark:notice:orientation")){
				val = simplifyValueTo45(val);
			}
			if(tag.equals("direction")) {
				val = simplifyDirection(val);
			}
			MapRulType rType = getMapRuleType(tag, val);
			if (rType != null) {
				if (rType.minzoom > zoom || rType.maxzoom < zoom) {
					continue;
				}
				if (rType.onlyPoint && !node) {
					continue;
				}
				if(rType == nameEnRuleType && Algorithms.objectEquals(val, tags.get(OSMTagKey.NAME.getValue()))) {
					continue;
				}
				rType.updateFreq();
				if (rType.isMain()) {
					outTypes.add(combineOrderAndId(rType));
				}
				if (rType.isAdditionalOrText() && rType.isMap()) {
					if (rType.isAdditional()) {
						outAddTypes.add(combineOrderAndId(rType));
					} else if (rType.isText()) {
						namesToEncode.put(rType, val);
					}
				}
			}
		}
        // sort to get most important features as first type (important for rendering)
        sortAndUpdateTypes(outTypes);
        sortAndUpdateTypes(outAddTypes);
		return area;
	}


	public Map<String, String> transformTags(Map<String, String> tags, EntityType entity,
			EntityConvertApplyType appType) {
		checkIfInitNeeded();
		tags = transformShieldTags(tags, entity, appType);
		tags = transformIntegrityTags(tags, entity, appType);
		tags = transformOpeningHoursTags(tags, appType);
		tags = transformChargingTags(tags, entity);
		tags = transformOsmcAndColorTags(tags);
		tags = transformAddMultipleNetwoksTag(tags);
		tags = transformRouteLimitationTags(tags);
		tags = transformTurnLanesTags(tags);
		tags = addEleFeetTags(tags);
		List<EntityConvert> listToTransform = getApplicableConverts(tags, entity, EntityConvertType.TAG_TRANSFORM, appType);
		List<EntityConvert> listToCombine = getApplicableConverts(tags, entity, EntityConvertType.TAG_COMBINE, appType);
		if (listToTransform == null && listToCombine == null) {
			return postTransform(tags);
		}
		Map<String, String> rtags = new LinkedHashMap<String, String>(tags);
		if (listToTransform != null) {
			for (EntityConvert ec : listToTransform) {
				applyTagTransforms(rtags, ec, tags);
			}
		}
		if (listToCombine != null) {
			for (EntityConvert ec : listToCombine) {
				applyTagCombines(rtags, ec, entity, tags);
			}
		}
		return postTransform(rtags);
	}

	private Map<String, String> postTransform(Map<String, String> tags) {
		if(DELETE_AFTER_38_RELEASE) {
			return tags;
		}
		boolean transform = false;
		for (String t : tags.keySet()) {
			if (t.startsWith("route_road_")) {
				transform = true;
			}
		}
		if (transform) {
			Map<String, String> rtags = new LinkedHashMap<String, String>(tags);
			for (String t : tags.keySet()) {
				if (t.startsWith("route_road_") && t.length() > "route_road_".length() + 3) {
					String tag = t.substring("route_road_".length() + 2);
					String ind = t.substring("route_road_".length(), "route_road_".length() + 1);
					rtags.put("road_" + tag + "_" + ind, tags.get(t));
				}
			}
			return rtags;
		}
		return tags;
	}

	private Map<String, String> transformOpeningHoursTags(Map<String, String> tags, EntityConvertApplyType appType) {
		String originalOH = tags.get("opening_hours");
		if (appType == EntityConvertApplyType.POI && originalOH != null) {
			String oh = originalOH;
			for (Entry<String, String> e : tags.entrySet()) {
				if (e.getKey().startsWith("opening_hours:")) {
					String subkey = e.getKey().substring("opening_hours:".length());
					if (!subkey.equals("lastcheck") && !subkey.equals("last_check") && !subkey.equals("signed")) {
						oh += " || " + e.getValue() + " \"" + Algorithms.capitalizeFirstLetter(subkey) + "\"";
					}
				}
			}
			if (oh.length() > originalOH.length()) {
				tags = new LinkedHashMap<>(tags);
				tags.put("opening_hours", oh);
			}
		}
		return tags;
	}

	private Map<String, String> transformChargingTags(Map<String, String> tags, EntityType entity) {
		if (entity == EntityType.NODE) {
			if (socketTypes == null) {
				initSocketTypes();
			}
			tags = new LinkedHashMap<>(tags);
			for (String key : socketTypes.keySet()) {
				String val = tags.get(key);
				String socketType = parseSocketType(key);
				String socketParam = parseSocketParam(key);
				if (val != null && socketType != null && socketParam != null) {
					String newKey = "osmand_socket_" + socketType + "_" + socketParam;
					tags.put(newKey, filterValues(val, socketTypes.get(key)));
				}
			}
		}
		return tags;
	}

	private String parseSocketParam(String key) {
		int lastColon = key.lastIndexOf(SOCKET_KEY_DELIMITER);
		if (lastColon != -1) {
			return key.substring(lastColon + 1);
		}
		return null;
	}

	private String parseSocketType(String string) {
		int firstColon = string.indexOf(SOCKET_KEY_DELIMITER);
		int secondColon = string.indexOf(SOCKET_KEY_DELIMITER, firstColon + 1);
		if (firstColon != -1 && secondColon != -1) {
			return string.substring(firstColon + 1, secondColon);
		}
		return null;
	}

	private String filterValues(String val, TIntArrayList limits) {
		String standard = val.toLowerCase().replaceAll(" ", "");
		if (standard.contains("x")) {
			standard = standard.substring(standard.indexOf("x"), (standard.length() - 1));
		}
		if (standard.contains(";")) {
			int out = 0;
			String[] vals = standard.split(";");
			for (String value : vals) {
				out = Math.max(out, getNumericValue(value));
			}
			return getAppropriateVal(out, limits);
		}
		return getAppropriateVal(getNumericValue(standard), limits);
	}

	private int getNumericValue(String value) {
		value = value.replaceAll("[^\\d.,]", "").replaceAll(",", ".");
		double tmp = 0d;
		if (!value.isEmpty()) {
			try {
				tmp = Double.valueOf(value);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		return (int) tmp;
	}

	private String getAppropriateVal(int output, TIntArrayList limits) {
		// to convert possible values in watts
		if (output > 1000) {
			output = (int) output / 1000;
		}
		if (output <= limits.get(0)) {
			return LOW_CHARGING_OUT;
		} else if (output <= limits.get(1)) {
			return MEDIUM_CHARGING_OUT;
		} else {
			return HIGH_CHARGING_OUT;
		}
	}

	private Map<String, String> transformIntegrityTags(Map<String, String> tags, EntityType entity,
	                                                   EntityConvertApplyType appType) {
		if (tags.containsKey("highway") && entity == EntityType.WAY) {
			tags = new LinkedHashMap<>(tags);
			int[] integrityResult = calculateIntegrity(tags);
			int integrity = integrityResult[0];
			int integrity_bicycle_routing = integrityResult[1];
			int max_integrity = 30;
			int normalised_integrity_brouting = 0;
			if (integrity_bicycle_routing >= 0) {
				normalised_integrity_brouting = (integrity_bicycle_routing * 10) / max_integrity;
			} else {
				normalised_integrity_brouting = -1;
			}
			int normalised_integrity = (integrity * 10) / max_integrity;
			if (integrity < 100) {
				tags.put("osmand_highway_integrity", normalised_integrity + "");
			}
			tags.put("osmand_highway_integrity_brouting", normalised_integrity_brouting + "");
			if (normalised_integrity_brouting > 4 && normalised_integrity_brouting <= 10) {
				tags.put("osmand_highway_integrity_brouting_low", "yes");
			}
		}
		return tags;
	}

	private Map<String, String> transformAddMultipleNetwoksTag(Map<String, String> tags) {
		int networkTypesCount = 0;
		boolean networkId = false;
		for (String tag : tags.keySet()) {
			if (NODE_NETWORKS_REF_TYPES.contains(tag)) {
				networkTypesCount++;
			}
			if (NODE_NETWORK_IDS.contains(tag)) {
				networkId = true;
			}
		}
		if (networkTypesCount > 1 && networkId) {
			Map<String, String> rtags = new LinkedHashMap<String, String>(tags);
			rtags.put(NODE_NETWORK_TAG, NODE_NETWORK_MULTIPLE_VALUE);
			return rtags;
		}
		return tags;
	}

	private Map<String, String> transformShieldTags(Map<String, String> tags, EntityType entity,
			EntityConvertApplyType appType) {

		if(entity == EntityType.WAY && !Algorithms.isEmpty(tags.get("ref")) && tags.containsKey("highway")) {
			String wayRef = tags.get("ref");
			String wayRefColor = tags.get("ref:colour");
			Set<String> wayRefs = new LinkedHashSet<String>();
			for(String r : Arrays.asList(wayRef.split(";"))) {
				wayRefs.add(r.trim());
			}
			Set<String> exisitingRefs = new LinkedHashSet<>();
			Map<String, String> missingColors = new LinkedHashMap<>();
			int maxModifier = 1;
			for(int modifier = 1; modifier < 10; modifier++) {
				String ref = tags.get("route_road_" + modifier + "_ref");
				if (!Algorithms.isEmpty(ref)) {
					exisitingRefs.add(ref);
					exisitingRefs.add(ref.replaceAll("-", "").replaceAll(" ", "")); // E 17, E-17, E17
					maxModifier = modifier + 1;
					String routeRoadRefColorTag = "route_road_" + modifier + "_ref:colour";
					if (!Algorithms.isEmpty(wayRefColor)
							&& tags.get(routeRoadRefColorTag) == null && wayRefs.contains(ref)) {
						missingColors.put(routeRoadRefColorTag, wayRefColor);
					}
				}
			}
			wayRefs.removeAll(exisitingRefs);
			if (wayRefs.size() > 0 || missingColors.size() > 0) {
				tags = new LinkedHashMap<String, String>(tags);
				tags.putAll(missingColors);
				for (String ref : wayRefs) {
					String s = ref.replaceAll("-", "").replaceAll(" ", "");
					if (ref.length() == 0 || exisitingRefs.contains(s)) {
						continue;
					}
					checkOrMainRule("route_road", "", 1);
					tags.put("route_road", "");

					String basePart = "route_road_" + maxModifier;
					checkOrCreateAdditional(basePart, "", null);
					tags.put(basePart, "");

					checkOrCreateTextRule(basePart + "_ref", getRuleType("ref", null, appType));
					tags.put(basePart + "_ref", ref);


					if (!Algorithms.isEmpty(wayRefColor)) {
						checkOrCreateTextRule(basePart + "_ref:colour", getRuleType("ref:colour", null, appType));
						tags.put(basePart + "_ref:colour", wayRefColor);
					}
					String wayRefNetwork = getNetwork(ref);
					if (!Algorithms.isEmpty(wayRefNetwork)) {
						checkOrCreateTextRule(basePart + "_network", getRuleType("network", null, appType));
						tags.put(basePart + "_network", wayRefNetwork);
					}
					maxModifier++;
				}
			}

		}

		if(entity == EntityType.WAY && tags.containsKey("highway") && tags.containsKey("name")) {
			tags = new LinkedHashMap<String, String>(tags);
			String name = tags.get("name");
			if (name.toLowerCase().contains("transcanad") || name.toLowerCase().contains("trans canad") || name.toLowerCase().contains("trans-canad") || name.toLowerCase().contains("yellowhead")) {
				tags.put("tch", "yes");
			}
		}
		if (entity == EntityType.WAY && tags.containsKey("highway")) {
			tags = new LinkedHashMap<String, String>(tags);
			int i = 1;
			while (i < 10) {
				String name = tags.get("road_name_" + i);
				if (name != null && (name.toLowerCase().contains("transcanad")
						|| name.toLowerCase().contains("trans canad") || name.toLowerCase().contains("trans-canad")
						|| name.toLowerCase().contains("yellowhead"))) {
					tags.put("tch", "yes");
				}
				i++;
			}
		}
		tags = transformRouteRoadTags(tags);
		return tags;
	}

    public Map<String, String> transformRouteLimitationTags(Map<String, String> tags) {
        String[] validatedTags = {"length", "maxspeed", "weight", "speed"};
        for (Entry<String, String> e : tags.entrySet()) {
            String val = e.getValue();
            String key = e.getKey();

            for (String valTag : validatedTags) {
                if (key.startsWith(valTag)) {
                    int i = Algorithms.findFirstNumberEndIndexLegacy(val);
                    if (i > 0) {
                        try {
                            Float.parseFloat(val.substring(0, i));
						} catch (Exception es) {
							System.err.println(String.format("! Error parsing float number %s", val));
							tags = new LinkedHashMap<>(tags);
							int ik = Algorithms.findFirstNumberEndIndex(val);
							String ending = "";
							if (val.indexOf(' ') != -1) {
								ending = val.substring(val.indexOf(' '));
							}
							float f = 0;
							if (ik >= 0) {
								f = Float.parseFloat(val.substring(0, ik));
							}
							System.err.println(String.format("! Parsed number -> '%s' ", f + ending));
							tags.put(key, f + ending);
						}
                    }
                }
            }
        }
        return tags;
    }

    public Map<String, String> transformTurnLanesTags(Map<String, String> tags) {
		if (tags.containsKey("turn:lanes:both_ways")) {
			tags = new LinkedHashMap<>(tags);
			String value = tags.get("turn:lanes:both_ways");
			if (tags.containsKey("turn:lanes:forward")) {
				addBothWayValue(tags, "turn:lanes:forward", value);
			}
			if (tags.containsKey("turn:lanes:backward")) {
				addBothWayValue(tags, "turn:lanes:backward", value);
			}
		}
		return tags;
	}

	private void addBothWayValue(Map<String, String> tags, String tag, String bothWayValue) {
		if (bothWayValue.equals("left")) {
			tags.put(tag, bothWayValue + "|" + tags.get(tag));
		} else if (bothWayValue.equals("right")) {
			tags.put(tag, tags.get(tag) + "|" + bothWayValue);
		}
	}

	protected MapRulType getRuleType(String tag, String val, EntityConvertApplyType appType) {
		return getRuleType(tag, val, appType == EntityConvertApplyType.POI, appType != EntityConvertApplyType.POI);
	}

	private Map<String, String> transformRouteRoadTags(Map<String, String> tags) {
		if(!tags.containsKey("route") && !"road".equals(tags.get("route"))) {
			return tags;
		}
		Map<String, String> rtags = new LinkedHashMap<String, String>(tags);
		String[] countyArray = new String[] { "luc", "herkimer", "montgomery", "guadalupe", "cumberland",
				"cass", "koochiching", "bergen", "saint lawrence", "schenectady", "log", "sullivan", "wil", "oneida",
				"le sueur", "way", "tus", "kandiyohi", "beltrami", "becker", "madison", "passaic", "douglas",
				"rensselaer", "dutchess", "freeborn", "ful", "crow wing", "hennepin", "orange", "clearwater", "sum",
				"hubbard", "hol", "otsego", "stearns", "carlton", "itasca", "anoka", "kanabec", "cook", "atlantic",
				"benton", "saratoga", "albany", "essex", "aitkin", "mah", "isanti", "faribault", "washington",
				"rockland", "cape_may", "ramsey", "lac qui parle", "warren", "greene", "chisago", "blue earth",
				"ulster", "somerset", "ott", "sussex", "morris", "kittson", "car", "pine", "big stone",
				"fillmore", "dakota", "monmouth", "col", "kane", "goodhue", "vin", "med",
				"middlesex", "lake", "columbia", "dodge", "hoc", "yellow medicine", "rice", "murray", "per", "steele",
				"outagamie", "asd", "lake of the woods", "mchenry", "fulton", "cottonwood", "carver",
				"ocean", "mille lacs", "redwood", "meeker", "winona", "renville", "brown", "swift", "pope", "martin",
				"delaware", "fay", "houston", "union", "chippewa", "nobles", "lyon", "wright", "sibley", "nicollet",
				"jef", "watonwan", "schoharie", "mcleod", "chenango", "hudson", "winnipeg", "pipestone", "mrw",
				"woodbury", "gue", "moe", "uni", "stevens", "wilkin", "traverse", "leelanau",
				"sen", "cook", "woo", "camden", "sta", "lic", "h", "cth", "ath", "burlington", "gonzales", "hamilton",
				"sauk", "colorado", "westchester", "story", "bel", "pau", "san", "lor", "ozaukee", "jasper", "waupaca",
				"dane", "belt", "oswego", "erie", "floyd", "bremer", "fond du lac", "sheboygan", "har", "macon",
				"chickasaw", "hays", "caldwell", "wya", "cos", "shelby", "monona",
				"clayton", "winnebago", "langlade", "hen", "gea", "eri", "chp", "but", "dupage", "ida",
				"hardin", "buena vista", "waushara", "walworth", "shawano", "saint croix", "rock",
				"portage", "milwaukee", "door", "put", "pre", "por", "odnr", "mei", "jac", "hur", "ham", "gac", "fra",
				"cli", "ash", "onondaga", "gloucester", "cape may", "charlotte", "waseca", "olmsted", "marquette",
				"fulton", "champaign", "worth", "sac", "pottawattamie", "polk", "lucas", "keokuk",
				"franklin", "cedar", "adams", "escambia", "kent", "santa clara", "chautauqua", "yates", "steuben",
				"chemung", "tioga", "tompkins", "schuyler", "allegany", "cattaraugus", "broome",
				"livingston" };
		if(rtags.containsKey("network")) {
			String network = rtags.get("network");
			if (network.startsWith("US:")) {
				if (!network.equalsIgnoreCase("US:I") && !network.startsWith("US:I:") && !network.toUpperCase().startsWith("US:US")) {
					if (((network.length() > 7) && network.substring(6,8).equals("CR")) || network.toLowerCase().contains("county") ||
						((network.length() > 7) && Arrays.asList(countyArray).contains((network.substring(6)).toLowerCase())) && ((network.split(":", -1).length-1) < 3))
					{
						rtags.put("us_county_network", "yes");
					} else {
						rtags.put("us_state_network", "yes");
					}
					if (network.length() > 5) {
						rtags.put("network", network);
					}
				}
				if (network.equalsIgnoreCase("US:I")) {
					rtags.put("network", "us:i");
				}
				if (network.toUpperCase().startsWith("US:US")) {
					rtags.put("network", "us:us");
				}
			}
		} else {
			String rf = rtags.get("ref");
			if(!Algorithms.isEmpty(rf)) {
				rf = rf.trim();
				String network = getNetwork(rf);
				if(!Algorithms.isEmpty(network)) {
					rtags.put("network", network);
				}
			}
		}
		if(rtags.containsKey("network") && !Algorithms.isEmpty(rtags.get("network"))) {
			rtags.put("network", rtags.get("network").toLowerCase());
		}
		if(rtags.containsKey("modifier") && !Algorithms.isEmpty(rtags.get("modifier"))) {
			rtags.put("modifier", rtags.get("modifier").toLowerCase());
		}
		if(rtags.containsKey("ref") && !Algorithms.isEmpty(rtags.get("ref"))) {
			rtags.put("ref", rtags.get("ref"));
		}
		return rtags;
	}

	private String getNetwork(String rf) {
		boolean numbers = true;
		String network = "";
		if(rf.length() != 0 && !Character.isDigit(rf.charAt(0))) {
			numbers = false;
		}
		if(numbers) {
			network = "#";
		}
// 		 else{
// 			int ind = 0;
// 			for(; ind < rf.length(); ind++) {
// 				if(Character.isDigit(rf.charAt(ind)) ||
// 						rf.charAt(ind) == ' ' || rf.charAt(ind) == '-') {
// 					break;
// 				}
// 			}
// 			network = rf.substring(0, ind).trim();
// 		}
		return network;
	}


	public List<Map<String, String>> splitTags(Map<String, String> tags, EntityType entity) {
		EntityConvertType filter = EntityConvertType.SPLIT;
		List<EntityConvert> listToConvert = getApplicableConverts(tags, entity, filter,
				EntityConvertApplyType.MAP);
		List<Map<String, String>> result = null;
		if(listToConvert == null) {
			return result;
		}
		result = new ArrayList<Map<String,String>>();
		Map<String, String> ctags = new LinkedHashMap<String, String>(tags);
		result.add(ctags);
		for (EntityConvert ec : listToConvert) {
			LinkedHashMap<String, String> mp = new LinkedHashMap<String, String>();
			for (TagValuePattern ift : ec.toTags) {
				String vl = ift.value;
				if (vl == null) {
					vl = ctags.get(ift.tag);
				}
				vl = processSubstr(ift, vl);
				if(vl != null) {
					mp.put(ift.tag, vl);
				}
			}
			ctags.remove(ec.fromTag.tag);
			result.add(mp);
		}
		return result;
	}


	protected List<EntityConvert> getApplicableConverts(Map<String, String> tags, EntityType entity,
			EntityConvertType filterTransform, EntityConvertApplyType filterProcessingType) {
		List<EntityConvert> listToConvert = null;
		for (Map.Entry<String, String> e : tags.entrySet()) {
			List<EntityConvert> list = convertTags.get(e.getKey());
			if (list != null) {
				for (EntityConvert ec : list) {
					String skipMsg = null;
					if (skipMsg == null && ec.type != filterTransform) {
						skipMsg = " transform " + filterTransform + "!= " + ec.type + ";";
					}
					if (skipMsg == null && !ec.applyToType.contains(filterProcessingType)) {
						skipMsg = " appFilter " + ec.applyToType + ";";
					}
					if (skipMsg == null && !checkConvertValue(ec.fromTag, e.getValue())) {
						skipMsg = " value mismatch " + e.getValue();
					}
					if (skipMsg != null) {
						if (ec.verbose) {
							log.info("Skip entity convert from '" + ec.fromTag + "' to " + tags + " in " + filterProcessingType + skipMsg);
						}
					} else {
						String verbose = null;
						if (ec.verbose) {
							verbose = "Apply entity convert from '" + ec.fromTag + "' to " + tags + " in " + filterProcessingType;
						}
						if (ec.type == EntityConvertType.TAG_COMBINE) {
							if (ec.fromTagList.size() > 0) {
								boolean tagPresent = false;
								String tagListVerbose = "";
								for (TagValuePattern ft : ec.fromTagList) {
									tagListVerbose += ft.tag + " ";
									if (tags.containsKey(ft.tag)) {
										tagPresent = true;
										break;
									}
								}
								if (!tagPresent) {
									if (verbose != null) {
										verbose = " - has failed due to tags are not contain any +'" + tagListVerbose
												+ "' for combine";
										log.info(verbose);
									}
									break;
								}
							} else {
								if (verbose != null) {
									verbose = " - has failed due to additional list of 'from_tag1, from_tag2, ...' for combine is empty";
									log.info(verbose);
								}
								break;
							}
						}
						if (checkConvert(tags, ec, entity)) {
							if (listToConvert == null) {
								listToConvert = new ArrayList<EntityConvert>();
							}
							listToConvert.add(ec);
							if (verbose != null) {
								verbose += " - has succeeded";
							}
						} else {
							if (verbose != null) {
								verbose += " - has failed due to if conditions";
							}
						}
						if (verbose != null) {
							log.info(verbose);
						}
					}
				}
			}
		}
		return listToConvert;
	}


	private void applyTagTransforms(Map<String, String> resultTags, EntityConvert ec, Map<String, String> originalTags) {
		applyTagTransforms(resultTags, ec, originalTags, "");
		if (ec.lang) {
			for (String lang : langs) {
				applyTagTransforms(resultTags, ec, originalTags, lang);
			}
			applyTagTransforms(resultTags, ec, originalTags, "en");
		}
	}

	private void applyTagTransforms(Map<String, String> tags, EntityConvert ec, Map<String, String> originaltags,
	                                String lang) {
		String langSuffix = lang.isEmpty() ? "" : ":" + lang;
		String fromTag = ec.fromTag.tag + langSuffix;
		String fromValue = originaltags.get(fromTag);
		if (tags.remove(fromTag) == null) {
			return;
		}
		for (TagValuePattern ift : ec.toTags) {
			String vl = ift.value;
			if (vl == null) {
				vl = fromValue;
			}
			vl = processSubstr(ift, vl);
			if (ift.tagPrefix != null) {
				for (String vlSplit : fromValue.split(";")) {
					tags.put(ift.tagPrefix + vlSplit.trim(), vl);
				}
			} else {
				tags.put(ift.tag + langSuffix, vl);
			}
		}
	}

	private void applyTagCombines(Map<String, String> tags, EntityConvert ec, EntityType entity,
								  Map<String, String> originaltags) {
		String fromValue = originaltags.get(ec.fromTag.tag);
		tags.remove(ec.fromTag.tag);
		//remove other from_tag1, from_tag2 ...
		for (TagValuePattern ft : ec.fromTagList) {
			if (tags.containsKey(ft.tag)) {
				tags.remove(ft.tag);
			}
		}
		for (TagValuePattern ift : ec.toTags) {
			String sep = ec.separator.isEmpty() ? " " : ec.separator;
			for (TagValuePattern ft : ec.fromTagList) {
				if (originaltags.containsKey(ft.tag)) {
					fromValue += sep + originaltags.get(ft.tag);
				}
			}
			tags.put(ift.tag, fromValue);
		}
	}

	private String processSubstr(TagValuePattern ift, String vl) {
		if (vl == null) {
			return null;
		}
		if (ift.substrSt != 0) {
			int s = ift.substrSt;
			if (s > 0) {
				if (vl.length() < s) {
					vl = "";
				} else {
					vl = vl.substring(s).trim();
				}
			} else {
				if (vl.length() > -s) {
					vl = vl.substring(vl.length() + s).trim();
				}
			}
		}
		if(ift.substrEnd != -1) {
			int s = ift.substrEnd;
			if (s > 0) {
				if (vl.length() > s) {
					vl = vl.substring(0, s).trim();
				}
			} else {
				if (vl.length() < -s) {
					vl = "";
				} else {
					vl = vl.substring(0, vl.length() + s).trim();
				}
			}
		}
		return vl;
	}


	protected boolean checkConvert(Map<String, String> tags, EntityConvert ec, EntityType entity) {
		if (ec.applyTo != null) {
			if (!ec.applyTo.contains(entity)) {
				return false;
			}
		}
		boolean empty = ec.ifRegionName.isEmpty();
		if (!empty) {
			boolean found = false;
			String rg = tags.get(OSMAND_REGION_NAME_TAG);
			if(Algorithms.isEmpty(rg)) {
				rg = regionName;
			} else {
				rg = "$" + rg + "^";
			}
			for (String s : ec.ifRegionName) {
				if (rg.contains(s)) {
					found = true;
					break;
				}
			}
			if (!found) {
				return false;
			}
		}
		for (String s : ec.ifNotRegionName) {
			String rg = tags.get(OSMAND_REGION_NAME_TAG);
			if(Algorithms.isEmpty(rg)) {
				rg = regionName;
			} else {
				rg = "$" + rg + "^";
			}
			if (rg.contains(s)) {
				return false;
			}
		}
		for (TagValuePattern ift : ec.ifTags) {
			String val = tags.get(ift.tag);
			if (!checkConvertValue(ift, val)) {
				return false;
			}
		}
		for (TagValuePattern ift : ec.ifNotTags) {
			String val = tags.get(ift.tag);
			if (checkConvertValue(ift, val)) {
				return false;
			}
		}
		for (TagValuePattern ift : ec.ifStartsTags) {
			String val = tags.get(ift.tag);
			if (!checkStartsWithValue(ift, val)) {
				return false;
			}
		}
		for (TagValuePattern ift : ec.ifNotStartsTags) {
			String val = tags.get(ift.tag);
			if (checkStartsWithValue(ift, val)) {
				return false;
			}
		}

		for (TagValuePattern ift : ec.ifEndsTags) {
			String val = tags.get(ift.tag);
			if (!checkEndsWithValue(ift, val)) {
				return false;
			}
		}
		for (TagValuePattern ift : ec.ifNotEndsTags) {
			String val = tags.get(ift.tag);
			if (checkEndsWithValue(ift, val)) {
				return false;
			}
		}
		for (TagValuePattern ift : ec.ifContainsTags) {
			String val = tags.get(ift.tag);
			if (!checkContainsValue(ift, val)) {
				return false;
			}
		}
		for (TagValuePattern ift : ec.ifNotContainsTags) {
			String val = tags.get(ift.tag);
			if (checkContainsValue(ift, val)) {
				return false;
			}
		}
		for (TagValuePattern ift : ec.ifTagsNotLess) {
			String val = tags.get(ift.tag);
			double nt = Double.parseDouble(ift.value);
			long vl = Algorithms.parseLongSilently(val, 0);
			if (vl < nt) {
				return false;
			}
		}
		for (TagValuePattern ift : ec.ifTagsLess) {
			String val = tags.get(ift.tag);
			double nt = Double.parseDouble(ift.value);
			long vl = Algorithms.parseLongSilently(val, 0);
			if (vl >= nt) {
				return false;
			}
		}
		return true;
	}



	private boolean checkConvertValue(TagValuePattern fromTag, String value) {
		if (value == null) {
			return false;
		}
		if (fromTag.value == null) {
			return true;
		}
		if (fromTag.value.equals(value)) {
			// fast check 1
			return true;
		}
		if(fromTag.value.length() != value.length()) {
			// fast check 2
			return false;
		}
		return fromTag.value.toLowerCase().equals(value.toLowerCase());
	}

	private boolean checkStartsWithValue(TagValuePattern fromTag, String value) {
		if (value == null) {
			return false;
		}
		if (fromTag.value == null) {
			return true;
		}
		return value.toLowerCase().startsWith(fromTag.value.toLowerCase());
	}


	private boolean checkContainsValue(TagValuePattern fromTag, String value) {
		if(value == null) {
			return false;
		}
		if(fromTag.value == null) {
			return true;
		}
		return value.toLowerCase().contains(fromTag.value.toLowerCase());
	}

	private boolean checkEndsWithValue(TagValuePattern fromTag, String value) {
		if(value == null) {
			return false;
		}
		if(fromTag.value == null) {
			return true;
		}
		return value.toLowerCase().endsWith(fromTag.value.toLowerCase());
	}

	private Map<String, String> addEleFeetTags(Map<String, String> tags) {
		final double FEET = 3.2808399;
		double eleMeters = Algorithms.parseDoubleSilently(tags.get("ele"), Double.NaN);
		double eleFeet = Algorithms.parseDoubleSilently(tags.get("ele_feet"), Double.NaN);

		if (!Double.isNaN(eleMeters) && Double.isNaN(eleFeet)) {
			eleFeet = eleMeters * FEET;
		} else if (!Double.isNaN(eleFeet) && Double.isNaN(eleMeters)) {
			eleMeters = eleFeet / FEET;
		} else if (Double.isNaN(eleMeters) && Double.isNaN(eleFeet)) {
			return tags;
		}

		tags = new LinkedHashMap<>(tags);
		tags.put("ele", String.valueOf(Math.round(eleMeters)));
		tags.put("ele_feet", String.valueOf(Math.round(eleFeet)));

		return tags;
	}


	protected String simplifyValueTo45(String val) {
		int rad = 8;
		//0, 45, 90, 135, 180, 225, 270, 315
		double circle = 360;
		try {
			double simple01 = Double.parseDouble(val) / circle;
			while (simple01 < 0) {
				simple01++;
			}
			while (simple01 >= 1) {
				simple01--;
			}
			int rnd = (int) (Math.round(simple01 * rad));
			val = "" + (rnd * circle / rad);
		} catch (NumberFormatException e) {
			log.error("Wrong value of \"seamark:notice:orientation\" " + val);
		}
		return val;
	}

	protected String simplifyDirection(String val) {
		if("down".equals(val) || "forward".equals(val) || "backward".equals(val) ||
				"anticlockwise".equals(val)  || "clockwise".equals(val)  ||
				"up".equals(val) || "all".equals(val)) {
			return val;
		}

		if ("N".equalsIgnoreCase(val) || "NNW".equalsIgnoreCase(val) || "NW".equalsIgnoreCase(val)
				|| "NNE".equalsIgnoreCase(val) || "NE".equalsIgnoreCase(val)
				|| "E".equalsIgnoreCase(val) || "ESE".equalsIgnoreCase(val) || "ENE".equalsIgnoreCase(val)
				|| "W".equalsIgnoreCase(val) || "WSW".equalsIgnoreCase(val) || "WNW".equalsIgnoreCase(val)
				|| "S".equalsIgnoreCase(val) || "SSW".equalsIgnoreCase(val) || "SW".equalsIgnoreCase(val)
				|| "SSE".equalsIgnoreCase(val) || "SE".equalsIgnoreCase(val)) {
			return val.toLowerCase();
		}

		int rad = 16;
		//0, 22.5, 45, 67.5, 90, 112.5, 135, 157.5, 180, 202.5, 225, 247.5, 270, 292.5, 315, 337.5, 360
		double circle = 360;
		try {
			double simple01 = Double.parseDouble(val) / circle;
			if(simple01 > Integer.MAX_VALUE || simple01 < Integer.MIN_VALUE) {
				return "n";
			}
			if (simple01 < 0) {
				simple01 = simple01 - ((int)simple01);
			}
			if (simple01 >= 1) {
				simple01 = simple01 - ((int)simple01);
			}
			int rnd = (int) (Math.round(simple01 * rad));
			val = "" + (int) (rnd * circle / rad);
			switch(val) {
				case "0": val = "n"; break;
				case "22": val = "nne"; break;
				case "45": val = "ne"; break;
				case "67": val = "ene"; break;
				case "90": val = "e"; break;
				case "112": val = "ese"; break;
				case "135": val = "se"; break;
				case "157": val = "sse"; break;
				case "180": val = "s"; break;
				case "202": val = "ssw"; break;
				case "225": val = "sw"; break;
				case "247": val = "wsw"; break;
				case "270": val = "w"; break;
				case "292": val = "wnw"; break;
				case "315": val = "nw"; break;
				case "337": val = "nnw"; break;
				case "360": val = "n"; break;
			}
		} catch (NumberFormatException e) {
//			log.error("Wrong value of \"direction\" " + val);
		}
		return val;
	}


	private void prepareColorTag(Map<String, String> tags, String tag) {
		String vl = tags.get(tag);
		vl = formatColorToPalette(vl, false);
		tags.put("colour_" + vl, "");
		tags.put("color_" + vl, "");
	}

	private void sortAndUpdateTypes(TIntArrayList outTypes) {
		outTypes.sort();
        for(int i = 0; i < outTypes.size(); i++) {
        	int k = outTypes.get(i) & ((1 << 15) - 1);
        	outTypes.set(i, k);
        }
	}



	private int combineOrderAndId(MapRulType rType) {
		if(rType.id > 1<<15) {
			throw new UnsupportedOperationException();
		}
		if(rType.order << 15 < 0) {
			throw new UnsupportedOperationException();
		}
		return (rType.order << 15) | rType.id;
	}

	public static double[] RGBtoHSV(double r, double g, double b){
		double h, s, v;
		double min, max, delta;
		min = Math.min(Math.min(r, g), b);
		max = Math.max(Math.max(r, g), b);

		// V
		v = max;
		delta = max - min;

		// S
		if( max != 0 )
			s = delta / max;
			else {
				s = 0;
				h = -1;
			return new double[]{h,s,v};
			}

		// H
		if( r == max )
			h = ( g - b ) / delta; // between yellow & magenta
			else if( g == max )
				h = 2 + ( b - r ) / delta; // between cyan & yellow
				else
				h = 4 + ( r - g ) / delta; // between magenta & cyan
			h *= 60;    // degrees

			if( h < 0 )
				h += 360;

		return new double[]{h,s,v};
	}

	public static String formatColorToPalette(String vl, boolean palette6){
		vl = vl.toLowerCase();
		int color = -1;
		int r = -1;
		int g = -1;
		int b = -1;
		if (vl.length() > 1 && vl.charAt(0) == '#') {
			try {
				color = Algorithms.parseColor(vl);
				r = (color >> 16) & 0xFF;
				g = (color >> 8) & 0xFF;
				b = (color >> 0) & 0xFF;
			} catch (RuntimeException e) {
			}
		}
		float[] hsv = new float[3];
		java.awt.Color.RGBtoHSB(r,g,b,hsv);
		float h = hsv[0];
		float s = hsv[1];
		float v = hsv[2];
		h *= 360;
		s *= 100;
		v *= 100;

		if ((h < 16 && s > 25 && v > 30) || (h > 326 && s > 25 && v > 30) || (h < 16 && s > 10 && s < 25 && v > 90) || (h > 326 && s > 10 && s < 25 && v > 90) ||
				vl.contains("red") || vl.equals("pink/white") || vl.equals("white-red") || vl.equals("ff0000") || vl.equals("800000") || vl.equals("red/tan") || vl.equals("tan/red") || vl.equals("rose") || vl.equals("salmon")) {
			vl = "red";
		} else if ((h >= 16 && h < 50 && s > 25 && v > 20 && v < 60) || vl.equals("brown") || vl.equals("darkbrown") || vl.equals("tan/brown") || vl.equals("tan_brown") || vl.equals("brown/tan") || vl.equals("light_brown") || vl.equals("brown/white") || vl.equals("tan")) {
			vl = palette6 ? "red" : "brown";
		} else if ((h >= 16 && h < 45 && v > 60) || vl.equals("orange") || vl.equals("cream") || vl.equals("gold") || vl.equals("yellow-red") || vl.equals("ff8c00") || vl.equals("peach")) {
			vl = palette6 ? "red" : "orange";
		} else if ((h >= 46 && h < 66 && s > 30 && v > 83) || vl.equals("yellow") || vl.equals("gelb") || vl.equals("ffff00") || vl.equals("beige") || vl.equals("lightyellow") || vl.equals("jaune")) {
			vl = "yellow";
		} else if ((h >= 46 && h < 66 && s > 30 && v > 30 && v < 82)) {
			vl = palette6 ? "yellow" : "darkyellow";
		} else if ((h >= 67 && h < 178 && s > 30 && v > 77) || vl.equals("lightgreen") || vl.equals("lime") || vl.equals("seagreen") || vl.equals("00ff00") || vl.equals("yellow/green")) {
			vl = palette6 ? "green" : "lightgreen";
		} else if ((h >= 74 && h < 174 && s > 30 && v > 30 && v < 77) || vl.contains("green") || vl.equals("darkgreen") || vl.equals("natural") || vl.equals("natur") || vl.equals("mediumseagreen") || vl.equals("green/white") || vl.equals("white/green") || vl.equals("blue/yellow") || vl.equals("vert") || vl.equals("green/blue") || vl.equals("olive")) {
			vl = "green";
		} else if ((h >= 178 && h < 210 && s > 40 && v > 80) || (h >= 178 && h < 265 && s > 25 && s < 61 && v > 90) || vl.equals("lightblue") || vl.equals("aqua") || vl.equals("cyan") || vl.equals("87ceeb") || vl.equals("turquoise")) {
			vl = palette6 ? "blue" : "lightblue";
		} else if ((h >= 178 && h < 210 && s > 40 && v > 35 && v <= 80) || (h >= 210 && h < 265 && s > 40 && v > 30) || vl.contains("blue") || vl.equals("0000ff") || vl.equals("darkblue") || vl.equals("blu") || vl.equals("navy")) {
			vl = "blue";
		} else if ((h >= 265 && h < 325 && s > 15 && v >= 27) || (h > 250 && h < 325 && s > 10 && s < 25 && v > 90) || vl.equals("purple") || vl.equals("violet") || vl.equals("magenta") || vl.equals("maroon") || vl.equals("fuchsia") || vl.equals("800080")) {
			vl = palette6 ? "blue" : "purple";
		} else if ((color != -1 & v < 27) || vl.contains("black") || vl.equals("darkgrey")) {
			vl = "black";
		} else if ((s < 32 && v > 30 && v < 90) || vl.equals("gray") || vl.equals("grey") || vl.equals("grey/tan") || vl.equals("silver") || vl.equals("srebrny") || vl.equals("lightgrey") || vl.equals("lightgray") || vl.equals("metal")) {
			vl = palette6 ? "white" : "gray";
		} else if ((s < 5 && v > 95) || vl.contains("white") /*|| vl.equals("white/tan")*/) {
			vl = "white";
		} else if (vl.contains("pink")) {
			vl = "pink";
		} else if (vl.contains("teal")) {
			vl = "teal";
		} else if (r != -1 && g != -1 && b != -1) {
			vl = "gray";
		}
		return vl;
	}

	final Set<String> barValues = new HashSet<String>(Arrays.asList(
		new String[]{
			"",
			"bar",
			"stripe",
			"cross",
			"x",
			"slash",
			"rectangle",
			"fork",
			"turned_t",
			"l",
			"lower",
			"corner",
			"backslash",
			"wolfshook",
			"rectangle_line",
	}));

	final java.util.Map<String, String> precolors = new java.util.HashMap<String, String>();
	{
            precolors.put("white_red_diamond","red");
            precolors.put("black_red_diamond","red");
            precolors.put("shell_modern","yellow");
            precolors.put("shell","yellow");
            precolors.put("wolfshook","white");
            precolors.put("ammonit","white");
            precolors.put("mine","white");
            precolors.put("hiker","black");
            precolors.put("heart","red");
            precolors.put("tower","black");
            precolors.put("bridleway","white");
	}

	public Map<String, String> transformOsmcAndColorTags(Map<String, String> tags) {
		if (tags.containsKey("osmc:symbol")) {

			tags = new LinkedHashMap<>(tags);
			String value = tags.get("osmc:symbol");
			OsmcSymbol osmcSymbol = new OsmcSymbol(value);
			osmcSymbol.addOsmcNewTags(tags);

		} else if (tags.containsKey("route") && !tags.containsKey("name") && !tags.containsKey("ref")) {
			String route = tags.get("route");
			String foreground = OSMC_NO_NAME_FOREGROUND.get(route);
			if (foreground != null) {
				OsmcSymbol osmcSymbol = new OsmcSymbol("white", "", "black");
				osmcSymbol.setForeground(foreground);
				String color = tags.containsKey("color") ? tags.get("color") : tags.get("colour");
				if (color != null) {
					osmcSymbol.setWaycolor(color);
				}
				osmcSymbol.addOsmcNewTags(tags);
				tags.put("osmc_order", "1");
			}
		} else if (tags.containsKey("route") && (tags.get("route").equals("hiking") || tags.get("route").equals("mtb") || tags.get("route").equals("bicycle") || tags.get("route").equals("horse") || tags.get("route").equals("running"))) {

			if (tags.containsKey("ref")) {
				tags = new LinkedHashMap<>(tags);
				String ref = tags.get("ref");
				if (!ref.isEmpty()) {
					OsmcSymbol osmcSymbol = new OsmcSymbol("white", ref.toUpperCase(), "black");
					osmcSymbol.addOsmcNewTags(tags);
				}
			} else if (tags.containsKey("name")) {
				tags = new LinkedHashMap<>(tags);
				String name = tags.get("name");
				int count = name.codePointCount(0, name.length());
				String text = "";
				for (int i = 0; i < count; i++) {
					int codePoint = name.codePointAt(i);
					if (Character.isUpperCase(codePoint)) {
						char[] c = Character.toChars(codePoint);
						text += new String(c);
					}
				}
				if (!text.isEmpty()) {
					OsmcSymbol osmcSymbol = new OsmcSymbol("white", text, "black");
					osmcSymbol.addOsmcNewTags(tags);
				}
			}
		}

		if (tags.containsKey("color")) {
			tags = new LinkedHashMap<>(tags);
			prepareColorTag(tags, "color");
		}
		if (tags.containsKey("colour")) {
			tags = new LinkedHashMap<>(tags);
			prepareColorTag(tags, "colour");
		}

		return tags;
	}

	private static int[] calculateIntegrity(Map<String, String> mp) {
		int result = 0;
		int result_bicycle_routing = 0;
		String surface = mp.get("surface");
		String smoothness = mp.get("smoothness");
		String tracktype = mp.get("tracktype");
		String highway = mp.get("highway");
		String bicycle = mp.get("bicycle");
		String foot = mp.get("foot");
		if (surface != null) {
			surface = surface.toLowerCase();
		}
		if (smoothness != null) {
			smoothness = smoothness.toLowerCase();
		}
		if (tracktype != null) {
			tracktype = tracktype.toLowerCase();
		}
		if ("paved".equals(surface) || "concrete".equals(surface) || "concrete:lanes".equals(surface)
				|| "concrete:plates".equals(surface) || "sett".equals(surface) || "paving_stones".equals(surface)
				|| "metal".equals(surface) || "wood".equals(surface) || "chipseal".equals(surface)) {
			result += 3;
			result_bicycle_routing += 3;
		} else if ("fine_gravel".equals(surface) || "grass_paver".equals(surface)) {
			result += 4;
			result_bicycle_routing += 4;
		} else if ("compacted".equals(surface)) {
			result += 8;
			result_bicycle_routing += 8;
		} else if ("cobblestone".equals(surface)) {
			result += 8;
			result_bicycle_routing += 8;
		} else if ("pebblestone".equals(surface)) {
			result += 9;
			result_bicycle_routing += 9;
		} else if ("ground".equals(surface) || "earth".equals(surface) || "dirt".equals(surface)) {
			result += 9;
			result_bicycle_routing += 9;
		} else if ("grass".equals(surface)) {
			result += 12;
			result_bicycle_routing += 12;
		} else if ("gravel".equals(surface)) {
			result += 12;
			result_bicycle_routing += 12;
		} else if ("stone".equals(surface) || "rock".equals(surface) || "rocky".equals(surface)) {
			result += 13;
			result_bicycle_routing += 13;
		} else if ("unpaved".equals(surface)) {
			result += 14;
			result_bicycle_routing += 14;
		} else if ("salt".equals(surface) || "ice".equals(surface) || "snow".equals(surface)) {
			result += 15;
			result_bicycle_routing += 15;
		} else if ("sand".equals(surface)) {
			result += 16;
			result_bicycle_routing += 16;
		} else if ("mud".equals(surface)) {
			result += 18;
			result_bicycle_routing += 18;
		}
		if ("excellent".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 7;
				result_bicycle_routing = 6;
			} else {
				result -= 5;
				result_bicycle_routing -= 5;
			}
		} else if ("very_good".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 6;
				result_bicycle_routing = 6;
			} else {
				result -= 4;
				result_bicycle_routing -= 6;
			}
		} else if ("good".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 8;
				result_bicycle_routing = 6;
			} else {
				result -= 2;
				result_bicycle_routing -= 2;
			}
		} else if ("intermediate".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 9;
				result_bicycle_routing = 9;
			}
		} else if ("bad".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 9;
				result_bicycle_routing = 9;
			} else if ("asphalt".equals(surface)) {
				result += 7;
				result_bicycle_routing += 7;
			} else {
				result += 6;
				result_bicycle_routing += 6;
			}
		} else if ("very_bad".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 12;
				result_bicycle_routing = 12;
			} else if ("asphalt".equals(surface)) {
				result += 12;
				result_bicycle_routing += 12;
			} else {
				result += 7;
				result_bicycle_routing += 7;
			}
		} else if ("horrible".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 15;
				result_bicycle_routing = 15;
			} else if ("asphalt".equals(surface)) {
				result += 19;
				result_bicycle_routing += 19;
			} else {
				result += 9;
				result_bicycle_routing += 9;
			}
		} else if ("very_horrible".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 18;
				result_bicycle_routing = 18;
			} else if ("asphalt".equals(surface)) {
				result += 22;
				result_bicycle_routing += 22;
			} else {
				result += 11;
				result_bicycle_routing += 11;
			}
		} else if ("impassable".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 24;
				result_bicycle_routing = 24;
			} else if ("asphalt".equals(surface)) {
				result += 26;
				result_bicycle_routing += 26;
			} else {
				result += 12;
				result_bicycle_routing += 12;
			}
		}
		if (surface == null) {
			if ("grade1".equals(tracktype)) {
				result += 1;
				result_bicycle_routing += 6;
			} else if ("grade2".equals(tracktype)) {
				result += 3;
				result_bicycle_routing += 9;
			} else if ("grade3".equals(tracktype)) {
				result += 7;
				result_bicycle_routing += 9;
			} else if ("grade4".equals(tracktype)) {
				result += 10;
				result_bicycle_routing += 12;
			} else if ("grade5".equals(tracktype)) {
				result += 15;
				result_bicycle_routing += 16;
			}
		}
		if (("motorway".equals(highway) || ("motorway_link".equals(highway)) || ("trunk".equals(highway))
				|| ("trunk_link".equals(highway)) || ("primary".equals(highway)) || ("primary_link".equals(highway))
				|| ("secondary".equals(highway)) || ("secondary_link".equals(highway)) || ("tertiary".equals(highway))
				|| ("tertiary_link".equals(highway)) || ("unclassified".equals(highway))
				|| ("residential".equals(highway)) || ("service".equals(highway)) || ("pedestrian".equals(highway))
				|| ("living_street".equals(highway)) || ("footway".equals(highway)) || ("cycleway".equals(highway)))
				&& (surface == null) && (smoothness == null)) {
			result = 100;
		}
		if (("track".equals(highway) || "path".equals(highway))
				&& ((surface == null) && (smoothness == null) && (tracktype == null))) {
			result = 100;
		}
		if ("track".equals(highway) && (surface == null) && (smoothness == null) && (tracktype == null)) {
			result_bicycle_routing = 9;
		}
		if (("path".equals(highway) && (surface == null) && (smoothness == null) && (tracktype == null))) {
			result_bicycle_routing = 12;
		}
		if (("footway".equals(highway) && (surface == null) && (smoothness == null) && (tracktype == null))) {
			result_bicycle_routing = 6;
		}
		if ("path".equals(highway)) {
			if ("designated".equals(bicycle)) {
				result = 0;
				result_bicycle_routing = 0;
			} else if ("designated".equals(foot)) {
				result = 2;
			}
		}
		if (result < 0) {
			result = 0;
		}
		int[] result_array = { result, result_bicycle_routing };
		return result_array;
	}

	public static class MapRouteTag {
		String tag;
		String value;
		String tag2;
		String value2;
		String type;
		boolean register;
		boolean amend;
		boolean base;
		boolean text;
		boolean replace;

	}


	public enum EntityConvertApplyType {
		MAP,
		ROUTING,
		POI
	}

	private enum EntityConvertType {
		TAG_TRANSFORM,
		SPLIT,
		TAG_COMBINE
	}
	public static class EntityConvert {
		public boolean verbose;
		public TagValuePattern fromTag ;
		public List<TagValuePattern> fromTagList = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public String separator;
		public EntityConvertType type;
		public EnumSet<EntityConvertApplyType> applyToType;
		public EnumSet<EntityType> applyTo ;
		public List<String> ifRegionName = new ArrayList<String>();
		public List<String> ifNotRegionName = new ArrayList<String>();
		public List<TagValuePattern> ifStartsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotStartsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifEndsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotEndsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifContainsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotContainsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifTagsLess = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifTagsNotLess = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> toTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public boolean lang;
	}

    public void addExternalAdditionalText(String tag, boolean lang) {
        checkIfInitNeeded();
        if (!types.containsKey(tag)) {
            registerRuleType(MapRulType.createText(tag, null));
        }
        if (lang) {
            for (String lng : langs) {
                String langTag = tag + ":" + lng;
                if (!types.containsKey(langTag)) {
                    registerRuleType(MapRulType.createText(langTag, null));
                }
            }
            String enTag = tag + ":en";
            if (!types.containsKey(enTag)) {
                registerRuleType(MapRulType.createText(enTag, null));
            }
        }
    }
}
