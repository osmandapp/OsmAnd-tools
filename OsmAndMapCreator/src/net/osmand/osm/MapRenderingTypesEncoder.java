package net.osmand.osm;

import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.osmand.PlatformUtil;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParser;

public class MapRenderingTypesEncoder extends MapRenderingTypes {

	private static Log log = PlatformUtil.getLog(MapRenderingTypesEncoder.class);
	// stored information to convert from osm tags to int type
	private List<MapRouteTag> routeTags = new ArrayList<MapRouteTag>();
	private Map<String, List<EntityConvert>> convertTags = new HashMap<String, List<EntityConvert>>();
	private MapRulType coastlineRuleType;
	private String regionName;
	
	
	public MapRenderingTypesEncoder(String fileName, String regionName) {
		super(fileName != null && fileName.length() == 0 ? null : fileName);
		this.regionName = "$" + regionName.toLowerCase();
	}
	
	public MapRenderingTypesEncoder(String regionName) {
		super(null);
		this.regionName = "$" + regionName.toLowerCase();
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

	private MapRulType getRelationalTagValue(String tag, String val) {
		MapRulType rType = getRuleType(tag, val);
		if(rType != null && rType.relation) {
			return rType;
		}
		return null;
	}
	
	public Map<MapRulType, Map<MapRulType, String>> getRelationPropogatedTags(Relation relation) {
		Map<MapRulType, Map<MapRulType, String>> propogated = new LinkedHashMap<MapRulType, Map<MapRulType, String>>();
		Map<String, String> ts = relation.getTags();
		ts = transformTags(ts, EntityType.RELATION, EntityConvertApplyType.MAP);
		ts = processExtraTags(ts);
		Iterator<Entry<String, String>> its = ts.entrySet().iterator();
		while(its.hasNext()) {
			Entry<String, String> ev = its.next();
			MapRulType rule = getRelationalTagValue(ev.getKey(), ev.getValue());
			if(rule != null) {
				String value = ev.getValue();
				LinkedHashMap<MapRulType, String> pr = new LinkedHashMap<MapRulType, String>();
				
				addRuleToPropogated(pr, ts, rule, value);
				if(pr.size() > 0) {
					propogated.put(rule, pr);
				}
			}
		}
		return propogated;
	}



	protected void addRuleToPropogated(Map<MapRulType, String> propogated, Map<String, String> ts, MapRulType rule,
			String value) {
		if (rule.names != null) {
			for (int i = 0; i < rule.names.length; i++) {
				String tag = rule.names[i].tagValuePattern.tag.substring(rule.namePrefix.length());
				if(ts.containsKey(tag)) {
					propogated.put(rule.names[i], ts.get(tag));
				} else if(rule.relationGroup) {
					propogated.put(rule.names[i], "");
				}
			}
		}
		propogated.put(rule, value);
	}
	

	private MapRulType getRuleType(String tag, String val) {
		return getRuleType(tag, val, false, false);
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
		EntityConvert ec = new EntityConvert();
		String tg = parser.getAttributeValue("", "if_region_name"); //$NON-NLS-1$
		if(tg != null) {
			ec.ifRegionName.addAll(Arrays.asList(tg.split("\\,")));
		}
		tg = parser.getAttributeValue("", "if_not_region_name"); //$NON-NLS-1$
		if(tg != null) {
			ec.ifNotRegionName.addAll(Arrays.asList(tg.split("\\,")));
		}
		ec.verbose = "true".equals(parser.getAttributeValue("", "verbose")); //$NON-NLS-1$
		parseConvertCol(parser, ec.ifTags, "if_");
		parseConvertCol(parser, ec.ifStartsTags, "if_starts_with_");
		parseConvertCol(parser, ec.ifNotStartsTags, "if_not_starts_with_");
		parseConvertCol(parser, ec.ifNotTags, "if_not_");
		parseConvertCol(parser, ec.ifTagsNotLess, "if_not_less_");
		parseConvertCol(parser, ec.ifTagsLess, "if_less_");
		ec.type = EntityConvertType.valueOf(parser.getAttributeValue("", "pattern" ).toUpperCase()); //$NON-NLS-1$
		ec.applyToType = EnumSet.allOf(EntityConvertApplyType.class);
		if("no".equals(parser.getAttributeValue("", "routing" )) || "false".equals(parser.getAttributeValue("", "routing" ))) {
			ec.applyToType.remove(EntityConvertApplyType.ROUTING);
		}
		if("no".equals(parser.getAttributeValue("", "map" )) || "false".equals(parser.getAttributeValue("", "map" ))) {
			ec.applyToType.remove(EntityConvertApplyType.MAP);
		}
		if("no".equals(parser.getAttributeValue("", "poi" )) || "false".equals(parser.getAttributeValue("", "poi" ))) {
			ec.applyToType.remove(EntityConvertApplyType.POI);
		}
		parseConvertCol(parser, ec.toTags, "to_");
		tg = parser.getAttributeValue("", "from_tag" ); //$NON-NLS-1$
		String value = parser.getAttributeValue("", "from_value"); //$NON-NLS-1$
		if (tg != null) {
			ec.fromTag = new TagValuePattern(tg, "".equals(value) ? null : value);
			if(!convertTags.containsKey(ec.fromTag.tag)) {
				convertTags.put(ec.fromTag.tag, new ArrayList<MapRenderingTypesEncoder.EntityConvert>());
			}
			convertTags.get(ec.fromTag.tag).add(ec);
		}
		String appTo = parser.getAttributeValue("", "apply_to" ); //$NON-NLS-1$
		if(appTo != null) {
			ec.applyTo = EnumSet.noneOf(EntityType.class);
			String[] tps = appTo.split(",");
			for(String t : tps) {
				EntityType et = EntityType.valueOf(t.toUpperCase());
				ec.applyTo.add(et);
			}
		}
	}



	protected void parseConvertCol(XmlPullParser parser, List<TagValuePattern> col, String prefix) {
		for (int i = 1; i <= 15; i++) {
			String tg = parser.getAttributeValue("", prefix +"tag" + i); //$NON-NLS-1$
			String value = parser.getAttributeValue("", prefix +"value" + i); //$NON-NLS-1$
			if (tg != null) {
				col.add(new TagValuePattern(tg, "".equals(value) ? null : value));
			}
		}
	}

	@Override
	protected void parseRouteTagFromXML(XmlPullParser parser) {
		MapRouteTag rtype = new MapRouteTag();
		String mode = parser.getAttributeValue("", "mode"); //$NON-NLS-1$
		rtype.tag = lc(parser.getAttributeValue("", "tag")); //$NON-NLS-1$
		rtype.value = lc(parser.getAttributeValue("", "value")); //$NON-NLS-1$
		rtype.tag2 = lc(parser.getAttributeValue("", "tag2")); //$NON-NLS-1$
		rtype.value2 = lc(parser.getAttributeValue("", "value2")); //$NON-NLS-1$
		rtype.base = Boolean.parseBoolean(parser.getAttributeValue("", "base"));
		rtype.replace = "replace".equalsIgnoreCase(mode);
		rtype.register = "register".equalsIgnoreCase(mode);
		rtype.amend = "amend".equalsIgnoreCase(mode);
		rtype.text = "text".equalsIgnoreCase(mode);
		rtype.relation = Boolean.parseBoolean(parser.getAttributeValue("", "relation"));
		routeTags.add(rtype);
	}
		


	private String lc(String a) {
		if(a != null) {
			return a.toLowerCase();
		}
		return a;
	}



	@Override
	protected void parseAndRegisterTypeFromXML(XmlPullParser parser, MapRulType parent) {
		String tag = lc(parser.getAttributeValue("", "tag"));
		MapRulType rtype = parseBaseRuleType(parser, parent, tag);
		registerOnlyMap(parser, rtype);
		if("true".equals(parser.getAttributeValue("", "lang"))) {
			for (String lng : langs) {
				tag = lc(parser.getAttributeValue("", "tag")) + ":" + lng;
				if(!types.containsKey(constructRuleKey(tag, null))){
					MapRulType retype = parseBaseRuleType(parser, parent, tag);
					registerOnlyMap(parser, retype);
				}
			}
		}
	}



	private void registerOnlyMap(XmlPullParser parser, MapRulType rtype) {
		String val = parser.getAttributeValue("", "minzoom"); //$NON-NLS-1$
		if (rtype.isMain()) {
			rtype.minzoom = 15;
		}
		if (val != null) {
			rtype.minzoom = Integer.parseInt(val);
		}
		val = parser.getAttributeValue("", "maxzoom"); //$NON-NLS-1$
		rtype.maxzoom = 31;
		if (val != null) {
			rtype.maxzoom = Integer.parseInt(val);
		}
		registerRuleType(rtype);
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
		tags = processExtraTags(tags);
		

		for (String tag : tags.keySet()) {
			String val = tags.get(tag);
			if(tag.equals("seamark:notice:orientation")){
				val = simplifyValueTo45(val);
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
		tags = transformShieldTags(tags, entity, appType);
		EntityConvertType filter = EntityConvertType.TAG_TRANSFORM;
		List<EntityConvert> listToConvert = getApplicableConverts(tags, entity, filter, appType);
		if(listToConvert == null) {
			return tags;
		} 
		Map<String, String> rtags = new LinkedHashMap<String, String>(tags);
		for(EntityConvert ec : listToConvert){
			applyTagTransforms(rtags, ec, entity, tags);
		}
		return rtags;
	}
	
	
	private Map<String, String> transformShieldTags(Map<String, String> tags, EntityType entity,
			EntityConvertApplyType appType) {
		if(entity == EntityType.WAY && !Algorithms.isEmpty(tags.get("ref"))) {
			String ref = tags.get("ref");
			boolean modify = false;
			List<String> rfs = new ArrayList<String>(Arrays.asList(ref.split(";")));
			Iterator<Entry<String, String>> it = tags.entrySet().iterator();
			int maxModifier = 1;
			while(it.hasNext()) {
				Entry<String, String> e = it.next();
				String tag = e.getKey();
				String vl = e.getValue();
				if(tag.startsWith("road_ref_")) {
					String sf = Algorithms.extractOnlyIntegerSuffix(tag);
					if(sf.length() > 0) {
						try {
							maxModifier = Math.max(maxModifier, 1 + Integer.parseInt(sf));
						} catch (NumberFormatException e1) {
							e1.printStackTrace();
						}
					}
					modify |= rfs.remove(vl);
					modify |= rfs.remove(vl.replace('-', ' ')); // E-17, E 17
					modify |= rfs.remove(vl.replace(' ', '-')); // E 17, E-17
					modify |= rfs.remove(vl.replaceAll(" ", "")); // E 17, E17
					modify |= rfs.remove(vl.replaceAll("-", "")); // E-17, E17
					modify |= rfs.remove("I " +vl); // I 5, 5
					modify |= rfs.remove("US " +vl); // US 5, 5
					modify |= rfs.remove("US " + vl + " Business"); // US 5 Business
				}
			}
			boolean SPLIT_REFS_TO_DIFFERENT_SHIELDS = true;
			// TODO THES LINE SHOULD NOT BE USED UNTIL MAJOR UPGRADE HAPPENS
			boolean MAJOR_UPGRADE_2_3_FINISHED = true;
			if ((modify || rfs.size() > 0) && MAJOR_UPGRADE_2_3_FINISHED) {
				tags = new LinkedHashMap<String, String>(tags);
				String rf = "";
				for (String r : rfs) {
					if (rf.length() == 0) {
						rf += r;
					} else {
						if(SPLIT_REFS_TO_DIFFERENT_SHIELDS) {
							tags.put("road_ref_"+maxModifier++, r);
						} else {
							rf += ", " + r;
						}
					}
				}
				if (rf.length() == 0) {
					tags.remove("ref");
				} else {
					tags.put("ref", rf);
				}
			}
			
		}
		tags = transformRouteRoadTags(tags);
		return tags;
	}

	private Map<String, String> transformRouteRoadTags(Map<String, String> tags) {
		if(!tags.containsKey("route") && !"road".equals(tags.get("route"))) {
			return tags;
		}
		Map<String, String> rtags = new LinkedHashMap<String, String>(tags);
		if(rtags.containsKey("network")) {
			String network = rtags.get("network");
			if (network.startsWith("US:")) {
				if (!network.equalsIgnoreCase("US:I") && !network.equalsIgnoreCase("US:US")) {
					rtags.put("us_state_network", "yes");
				}
				if (network.length() > 5) {
					network = network.substring(0, 5);
					rtags.put("network", network);
				}
			}
		} else {
			String rf = rtags.get("ref");
			if(!Algorithms.isEmpty(rf)) {
				rf = rf.trim();
				boolean allnumbers = true;
				for(int i = 0; i < rf.length(); i++) {
					if(!Character.isDigit(rf.charAt(i))) {
						allnumbers = false;
						break;
					}
				}
				if(allnumbers) {
					rtags.put("network", "#");
				} else{
					int ind = 0;
					for(; ind < rf.length(); ind++) {
						if(Character.isDigit(rf.charAt(ind)) || 
								rf.charAt(ind) == ' ' || rf.charAt(ind) == '-') {
							break;
						}
					}	
					rtags.put("network", rf.substring(0, ind).trim());
				}
				
			}
		}

		if(rtags.containsKey("network")) {
			rtags.put("network", rtags.get("network").toLowerCase());
		}
		if(rtags.containsKey("modifier")) {
			rtags.put("modifier", rtags.get("modifier").toLowerCase());
		}
		if(rtags.containsKey("ref")) {
			rtags.put("ref", rtags.get("ref").toUpperCase());
		}
		return rtags;
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
			EntityConvertType filter, EntityConvertApplyType appFilter) {
		List<EntityConvert> listToConvert = null;
		for(Map.Entry<String, String> e : tags.entrySet()) {
			List<EntityConvert> list = convertTags.get(e.getKey());
			if (list != null) {
				for (EntityConvert ec : list) {
					if (checkConvertValue(ec.fromTag, e.getValue())) {
						String verbose = null;
						if(ec.verbose) {
							verbose = "Apply entity convert from '"+ec.fromTag+"' to " + tags + " in " + 
									appFilter;
						}
						if (checkConvert(tags, ec, entity) && ec.type == filter && 
								ec.applyToType.contains(appFilter)) {
							if (listToConvert == null) {
								listToConvert = new ArrayList<EntityConvert>();
							}
							listToConvert.add(ec);
							if (verbose != null) {
								verbose += " - has succeeded";
							}
						} else {
							if (verbose != null) {
								verbose += " - has failed due to ";
								if(!checkConvert(tags, ec, entity)) {
									verbose += "if conditions;";
								}
								if(ec.type != filter ) {
									verbose += " transform " + filter + "!= " + ec.type + ";";
								}
								if(!ec.applyToType.contains(appFilter)) {
									verbose += " appFilter "+appFilter+";";
								}
								
							}
						}
						if(verbose != null) {
							log.info(verbose);
						}
					}
				}
			}
		}
		return listToConvert;
	}



	private void applyTagTransforms(Map<String, String> tags, EntityConvert ec, EntityType entity,
			Map<String, String> originaltags) {
		String fromValue =  originaltags.get(ec.fromTag.tag);
		tags.remove(ec.fromTag.tag);
		for(TagValuePattern ift : ec.toTags) {
			String vl = ift.value;
			if(vl == null) {
				vl = fromValue;
			}
			tags.put(ift.tag, vl);
		}
	}
	
	
	protected boolean checkConvert(Map<String, String> tags, EntityConvert ec, EntityType entity) {
		if(ec.applyTo != null) {
			if(!ec.applyTo.contains(entity)) {
				return false;
			}
		}
		boolean empty = ec.ifRegionName.isEmpty();
		if (!empty) {
			boolean found = false;
			for (String s : ec.ifRegionName) {
				if (regionName.contains(s)) {
					found = true;
					break;
				}
			}
			if (!found) {
				return false;
			}
		}
		for (String s : ec.ifNotRegionName) {
			if (regionName.contains(s)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifTags) {
			String val = tags.get(ift.tag);
			if(!checkConvertValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifNotTags) {
			String val = tags.get(ift.tag);
			if(checkConvertValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifStartsTags) {
			String val = tags.get(ift.tag);
			if(!checkStartsWithValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifNotStartsTags) {
			String val = tags.get(ift.tag);
			if(checkStartsWithValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifTagsNotLess) {
			String val = tags.get(ift.tag);
			double nt = Double.parseDouble(ift.value);
			long vl = Algorithms.parseLongSilently(val, 0);
			if(vl < nt) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifTagsLess) {
			String val = tags.get(ift.tag);
			double nt = Double.parseDouble(ift.value);
			long vl = Algorithms.parseLongSilently(val, 0);
			if(vl >= nt) {
				return false;
			}
		}
		return true;
	}



	private boolean checkConvertValue(TagValuePattern fromTag, String value) {
		if(value == null ) {
			return false;
		}
		if(fromTag.value == null) {
			return true;
		}
		return fromTag.value.toLowerCase().equals(value.toLowerCase());
	}
	
	private boolean checkStartsWithValue(TagValuePattern fromTag, String value) {
		if(value == null) {
			return false;
		}
		if(fromTag.value == null) {
			return true;
		}
		return value.toLowerCase().startsWith(fromTag.value.toLowerCase());
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
			System.err.println("Wrong value of \"seamark:notice:orientation\" " + val);
		}
		return val;
	}



	private void prepareColorTag(Map<String, String> tags, String tag) {
		String vl = tags.get(tag);
		vl = formatColorToPalette(vl, false);
		tags.put("colour_"+vl, "");
		tags.put("color_"+vl, "");
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

	public String formatColorToPalette(String vl, boolean palette6){
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
				vl.equals("pink") || vl.contains("red") || vl.equals("pink/white") || vl.equals("white-red") || vl.equals("ff0000") || vl.equals("800000") || vl.equals("red/tan") || vl.equals("tan/red") || vl.equals("rose")) {
			vl = "red";
		} else if ((h >= 16 && h < 50 && s > 25 && v > 20 && v < 60) || vl.equals("brown") || vl.equals("darkbrown") || vl.equals("tan/brown") || vl.equals("tan_brown") || vl.equals("brown/tan") || vl.equals("light_brown") || vl.equals("brown/white") || vl.equals("tan")) {
			vl = palette6 ? "red" : "brown";
		} else if ((h >= 16 && h < 45 && v > 60) || vl.equals("orange") || vl.equals("cream") || vl.equals("gold") || vl.equals("yellow-red") || vl.equals("ff8c00") || vl.equals("peach")) {
			vl = palette6 ? "red" : "orange";
		} else if ((h >= 46 && h < 73 && s > 30 && v > 80) || vl.equals("yellow") || vl.equals("gelb") || vl.equals("ffff00") || vl.equals("beige") || vl.equals("lightyellow") || vl.equals("jaune")) {
			vl = "yellow";
		} else if ((h >= 46 && h < 73 && s > 30 && v > 60 && v < 80)) {
			vl = palette6 ? "yellow" : "darkyellow";
		} else if ((h >= 74 && h < 150 && s > 30 && v > 77) || vl.equals("lightgreen") || vl.equals("lime") || vl.equals("seagreen") || vl.equals("00ff00") || vl.equals("yellow/green")) {
			vl = palette6 ? "green" : "lightgreen";
		} else if ((h >= 74 && h < 174 && s > 30 && v > 30 && v < 77) || vl.contains("green") || vl.equals("darkgreen") || vl.equals("natural") || vl.equals("natur") || vl.equals("mediumseagreen") || vl.equals("green/white") || vl.equals("white/green") || vl.equals("blue/yellow") || vl.equals("vert") || vl.equals("green/blue") || vl.equals("olive")) {
			vl = "green";
		} else if ((h >= 174 && h < 215 && s > 32 && v > 50) || vl.equals("lightblue") || vl.equals("aqua") || vl.equals("cyan") || vl.equals("87ceeb") || vl.equals("turquoise")) {
			vl = palette6 ? "blue" : "lightblue";
		} else if ((h >= 215 && h < 265 && s > 40 && v > 30) || vl.contains("blue") || vl.equals("0000ff") || vl.equals("teal") || vl.equals("darkblue") || vl.equals("blu") || vl.equals("navy")) {
			vl = "blue";
		} else if ((h >= 265 && h < 325 && s > 15 && v >= 27) || (h > 250 && h < 325 && s > 10 && s < 25 && v > 90) || vl.equals("purple") || vl.equals("violet") || vl.equals("magenta") || vl.equals("maroon") || vl.equals("fuchsia") || vl.equals("800080")) {
			vl = palette6 ? "blue" : "purple";
		} else if ((color != -1 & v < 27) || vl.contains("black") || vl.equals("darkgrey")) {
			vl = "black";
		} else if ((s < 32 && v > 30 && v < 90) || vl.equals("gray") || vl.equals("grey") || vl.equals("grey/tan") || vl.equals("silver") || vl.equals("srebrny") || vl.equals("lightgrey") || vl.equals("lightgray") || vl.equals("metal")) {
			vl = palette6 ? "white" : "gray";
		} else if ((s < 5 && v > 95) || vl.contains("white") /*|| vl.equals("white/tan")*/) {
			vl = "white";
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
	
	private boolean isColor(String s) {
		return s.equals("black")
				|| s.equals("blue")
				|| s.equals("green")
				|| s.equals("red")
				|| s.equals("white")
				|| s.equals("yellow")
				|| s.equals("orange")
				|| s.equals("purple")
				|| s.equals("brown");
	}
	
	
	private Map<String, String> processExtraTags(Map<String, String> tags) {
		if(tags.containsKey("osmc:symbol")) {
			tags = new LinkedHashMap<String, String>(tags);
			// osmc:symbol=black:red:blue_rectangle ->
			// 1.For backwards compatibility (already done) - osmc_shape=bar, osmc_symbol=black, osmc_symbol_red_blue_name=.
			// 2.New tags: osmc_waycolor=black, osmc_background=red, osmc_foreground=blue_rectangle, osmc_foreground2,
			// osmc_text, osmc_textcolor, osmc_stub_name=. ,
			String value = tags.get("osmc:symbol");
			String[] tokens = value.split(":", 6);
			osmcBackwardCompatility(tags, tokens);
			if (tokens != null) {
				if ((tokens.length == 4 && isColor(tokens[3])) || (tokens.length == 5 && isColor(tokens[4]))) {
					String[] ntokens = new String[] { tokens[0], tokens[1], tokens.length == 4 ? "" : tokens[2], "",
							tokens.length == 4 ? tokens[2] : tokens[3], tokens.length == 4 ? tokens[3] : tokens[4] };
					addOsmcNewTags(tags, ntokens);
				} else {
					addOsmcNewTags(tags, tokens);
				}
			}
		}
		if(tags.containsKey("color")) {
			tags = new LinkedHashMap<String, String>(tags);
			prepareColorTag(tags, "color");
		}
		if(tags.containsKey("colour")) {
			tags = new LinkedHashMap<String, String>(tags);
			prepareColorTag(tags, "colour");
		}
		return tags;
	}


	private void addOsmcNewTags(Map<String, String> propogated, String[] tokens) {
		if (tokens.length > 0) {
			String wayColor = tokens[0]; // formatColorToPalette(tokens[0], true);
			propogated.put("osmc_waycolor", wayColor);
			if (tokens.length > 1) {
				String bgColor = tokens[1]; // formatColorToPalette(tokens[1], true);
				propogated.put("osmc_background", bgColor);
				propogated.put("osmc_stub_name", ".");
				if (tokens.length > 2) {
					String shpVl = tokens[2]; // formatColorToPalette(tokens[1], true);
					propogated.put("osmc_foreground", shpVl);
					if (tokens.length > 3) {
						String shp2Vl = tokens[3];
						propogated.put("osmc_foreground2", shp2Vl);
						if (tokens.length > 4) {
							String txtVl = tokens[4];
							propogated.put("osmc_text", txtVl);
							propogated.put("osmc_text_symbol", txtVl);
							if (tokens.length > 5) {
								String txtcolorVl = tokens[5];
								propogated.put("osmc_textcolor", txtcolorVl);
							}
						}
					}
				}
			}
		}
	}



	private void osmcBackwardCompatility(Map<String, String> propogated, String[] tokens) {
		if (tokens.length > 0) {
			String wayColor = formatColorToPalette(tokens[0], true);
			propogated.put("osmc_symbol_" + wayColor, "");
			propogated.put("osmc_symbol", wayColor);
			if (tokens.length > 1) {
				String bgColor = tokens[1];
				String fgColorPrefix = "";
				String shape = "";
				if (tokens.length > 2) {
					String fgColor = tokens[2];
					if (precolors.containsKey(fgColor)) {
						shape = fgColor;
						fgColor = precolors.get(fgColor);
					} else if (fgColor.indexOf('_') >= 0) {
						final int i = fgColor.indexOf('_');
						shape = fgColor.substring(i + 1).toLowerCase();
						fgColor = fgColor.substring(0, i);
					}
					fgColorPrefix = "_" + fgColor;
				}
				String shpValue = "none";
				if (shape.length() != 0) {
					shpValue = barValues.contains(shape) ? "bar" : "circle";
				}
				propogated.put("osmc_shape", shpValue);
				String symbol = "osmc_symbol_" + formatColorToPalette(bgColor, true) + fgColorPrefix + "_name";
				String name = "."; // "\u00A0";
				// if (tokens.length > 3 && tokens[3].trim().length() > 0) {
				// name = tokens[3];
				// }

				propogated.put(symbol, name);
			}
		}
	}
	
	public static class MapRouteTag {
		boolean relation;
		String tag;
		String value;
		String tag2;
		String value2;
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
		SPLIT
	}
	public static class EntityConvert {
		public boolean verbose;
		public TagValuePattern fromTag ;
		public EntityConvertType type;
		public EnumSet<EntityConvertApplyType> applyToType;
		public EnumSet<EntityType> applyTo ;
		public List<String> ifRegionName = new ArrayList<String>();
		public List<String> ifNotRegionName = new ArrayList<String>();
		public List<TagValuePattern> ifStartsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotStartsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifTagsLess = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifTagsNotLess = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> toTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
	}

}
