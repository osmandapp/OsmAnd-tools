package net.osmand.osm;

import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation;
import net.osmand.util.Algorithms;

import org.xmlpull.v1.XmlPullParser;

public class MapRenderingTypesEncoder extends MapRenderingTypes {
	
	// stored information to convert from osm tags to int type
	private List<MapRouteTag> routeTags = new ArrayList<MapRouteTag>();
	private Map<String, List<EntityConvert>> convertTags = new HashMap<String, List<EntityConvert>>();
	private MapRulType coastlineRuleType;
	
	public MapRenderingTypesEncoder(String fileName) {
		super(fileName);
	}
	
	private static MapRenderingTypesEncoder DEFAULT_INSTANCE = null;
	
	public static MapRenderingTypesEncoder getDefault() {
		if(DEFAULT_INSTANCE == null){
			DEFAULT_INSTANCE = new MapRenderingTypesEncoder(null);
		}
		return DEFAULT_INSTANCE;
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
		MapRulType rType = getMapRuleType(tag, val);
		if(rType != null && rType.relation) {
			return rType;
		}
		return null;
	}
	
	public Map<MapRulType, String> getRelationPropogatedTags(Relation relation) {
		Map<MapRulType, String> propogated = new LinkedHashMap<MapRulType, String>();
		Map<String, String> ts = relation.getTags();
		ts = transformTags(ts, EntityType.RELATION);
		Iterator<Entry<String, String>> its = ts.entrySet().iterator();
		while(its.hasNext()) {
			Entry<String, String> ev = its.next();
			MapRulType rule = getRelationalTagValue(ev.getKey(), ev.getValue());
			if(rule != null) {
				String value = ev.getValue();
				addRuleToPropogated(propogated, ts, rule, value);
			}
			addOSMCSymbolsSpecialTags(propogated, ev);
		}
		return propogated;
	}



	protected void addRuleToPropogated(Map<MapRulType, String> propogated, Map<String, String> ts, MapRulType rule,
			String value) {
		if(rule.targetTagValue != null) {
			rule = rule.targetTagValue;
			if(rule.getValue() != null) {
				value = rule.getValue();
			}
		}
		if (rule.names != null) {
			for (int i = 0; i < rule.names.length; i++) {
				String tag = rule.names[i].tagValuePattern.tag.substring(rule.namePrefix.length());
				if(ts.containsKey(tag)) {
					propogated.put(rule.names[i], ts.get(tag));
				}
			}
		}
		propogated.put(rule, value);
	}
	



	private MapRulType getMapRuleType(String tag, String val) {
		return getRuleType(tag, val, false);
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
		parseConvertCol(parser, ec.ifTags, "if_");
		parseConvertCol(parser, ec.ifNotTags, "if_not_");
		ec.type = EntityConverType.valueOf(parser.getAttributeValue("", "pattern" ).toUpperCase()); //$NON-NLS-1$
		parseConvertCol(parser, ec.toTags, "to_");
		String tg = parser.getAttributeValue("", "from_tag" ); //$NON-NLS-1$
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
			ec.applyTo = new HashSet<Entity.EntityType>();
			String[] tps = appTo.split(",");
			for(String t : tps) {
				EntityType et = EntityType.valueOf(t.toUpperCase());
				ec.applyTo.add(et);
			}
		}
	}



	protected void parseConvertCol(XmlPullParser parser, List<TagValuePattern> col, String prefix) {
		for (int i = 1; i <= 5; i++) {
			String tg = parser.getAttributeValue("", prefix +"tag" + i); //$NON-NLS-1$
			String value = parser.getAttributeValue("", prefix +"value" + i); //$NON-NLS-1$
			if (tg != null) {
				col.add(new TagValuePattern(tg, "".equals(value) ? null : value));
			}
		}
	}

	@Override
	protected void parseRouteTagFromXML(XmlPullParser parser) {
		super.parseRouteTagFromXML(parser);
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
	protected MapRulType parseTypeFromXML(XmlPullParser parser, MapRulType parent) {
		MapRulType rtype = parseBaseRuleType(parser, parent, false);
		rtype.onlyPoi = "true".equals(parser.getAttributeValue("", "only_poi"));
		if(!rtype.onlyPoi) {
			String val = parser.getAttributeValue("", "minzoom"); //$NON-NLS-1$
			if(rtype.isMain()) {
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
			if(rtype.onlyMap) {
				registerRuleType(rtype);
			}
		}
		return rtype;
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
		tags = transformTags(tags, node ? EntityType.NODE : EntityType.WAY);
		boolean area = "yes".equals(tags.get("area"));
		if(tags.containsKey("color")) {
			prepareColorTag(tags, "color");
		}
		if(tags.containsKey("colour")) {
			prepareColorTag(tags, "colour");
		}

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
				if(rType.targetTagValue != null) {
					rType = rType.targetTagValue;
				}
				rType.updateFreq();
				if (rType.isMain()) {
					outTypes.add(combineOrderAndId(rType));
				}
				if (rType.isAdditionalOrText()) {
					boolean applied = rType.applyToTagValue == null;
					if(!applied) {
						Iterator<TagValuePattern> it = rType.applyToTagValue.iterator();
						while(!applied && it.hasNext()) {
							TagValuePattern nv = it.next();
							applied = nv.isApplicable(tags);
						}
					}
					if (applied) {
						if (rType.isAdditional()) {
							outAddTypes.add(combineOrderAndId(rType));
						} else if (rType.isText()) {
							namesToEncode.put(rType, val);
						}
					}
				}
			}
		}
        // sort to get most important features as first type (important for rendering)
        sortAndUpdateTypes(outTypes);
        sortAndUpdateTypes(outAddTypes);
		return area;
	}



	private Map<String, String> transformTags(Map<String, String> tags, EntityType entity) {
		EntityConverType filter = EntityConverType.TAG_TRANSFORM;
		List<EntityConvert> listToConvert = getApplicableConverts(tags, entity, filter);
		if(listToConvert == null) {
			return tags;
		} 
		Map<String, String> rtags = new LinkedHashMap<String, String>(tags);
		for(EntityConvert ec : listToConvert){
			applyTagTransforms(rtags, ec, entity, tags);
		}
		return rtags;
	}
	
	
	public List<Map<String, String>> splitTags(Map<String, String> tags, EntityType entity) {
		EntityConverType filter = EntityConverType.SPLIT;
		List<EntityConvert> listToConvert = getApplicableConverts(tags, entity, filter);
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
			EntityConverType filter) {
		List<EntityConvert> listToConvert = null;
		for(Map.Entry<String, String> e : tags.entrySet()) {
			List<EntityConvert> list = convertTags.get(e.getKey());
			if (list != null) {
				for (EntityConvert ec : list) {
					if (checkConvertValue(ec.fromTag, e.getValue())) {
						if (checkConvert(tags, ec, entity) && ec.type == filter) {
							if (listToConvert == null) {
								listToConvert = new ArrayList<EntityConvert>();
							}
							listToConvert.add(ec);
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
		} else if ((h >= 174 && h < 215 && s > 15 && v > 50) || vl.equals("lightblue") || vl.equals("aqua") || vl.equals("cyan") || vl.equals("87ceeb") || vl.equals("turquoise")) {
			vl = palette6 ? "blue" : "lightblue";
		} else if ((h >= 215 && h < 239 && s > 40 && v > 30) || vl.contains("blue") || vl.equals("0000ff") || vl.equals("teal") || vl.equals("darkblue") || vl.equals("blu") || vl.equals("navy")) {
			vl = "blue";
		} else if ((h >= 239 && h < 325 && s > 15 && v > 45) || (h > 250 && h < 325 && s > 10 && s < 25 && v > 90) || vl.equals("purple") || vl.equals("violet") || vl.equals("magenta") || vl.equals("maroon") || vl.equals("fuchsia") || vl.equals("800080")) {
			vl = palette6 ? "blue" : "purple";
		} else if ((color != -1 & v < 20) || vl.contains("black") || vl.equals("darkgrey")) {
			vl = "black";
		} else if ((s < 5 && v > 30 && v < 90) || vl.equals("gray") || vl.equals("grey") || vl.equals("grey/tan") || vl.equals("silver") || vl.equals("srebrny") || vl.equals("lightgrey") || vl.equals("lightgray") || vl.equals("metal")) {
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

	public void addOSMCSymbolsSpecialTags(Map<MapRulType,String> propogated, Entry<String,String> ev) {
		if ("osmc:symbol".equals(ev.getKey())) {
			// osmc:symbol=black:red:blue_rectangle ->
//			1.For backwards compatibility (already done) - osmc_shape=bar, osmc_symbol=black, osmc_symbol_red_blue_name=.
//			2.New tags: osmc_waycolor=black, osmc_background=red, osmc_foreground=blue_rectangle, osmc_foreground2, osmc_text, osmc_textcolor, osmc_stub_name=. ,
			String[] tokens = ev.getValue().split(":", 6);
			osmcBackwardCompatility(propogated, tokens);
			if(tokens != null) {
				if(tokens.length > 1) {
					if(isColor(tokens[3])) {
						tokens[3] = tokens[5];
						tokens[2] = tokens[4];
						tokens[2] = "";
						tokens[3] = "";
					} else if(isColor(tokens[4])) {
						tokens[4] = tokens[5];
						tokens[3] = tokens[4];
						tokens[3] = "";
					}
				}
				addOsmcNewTags(propogated, tokens);
			}
		}
		if ("color".equals(ev.getKey()) || "colour".equals(ev.getKey())) {
			String vl = ev.getValue().toLowerCase();
			String nm = "color_"+formatColorToPalette(vl, false);
			MapRulType rt = getMapRuleType(nm, "");
			if(rt != null) {
				propogated.put(rt, "");
			}
			nm = "colour_"+formatColorToPalette(vl, false);
			rt = getMapRuleType(nm, "");
			if(rt != null) {
				propogated.put(rt, "");
			}
		}
	}



	private void addOsmcNewTags(Map<MapRulType, String> propogated, String[] tokens) {
		if (tokens.length > 0) {
			String wayColor = tokens[0]; // formatColorToPalette(tokens[0], true);
			MapRulType rt = getMapRuleType("osmc_waycolor", wayColor);
			if (rt != null) {
				propogated.put(rt, wayColor);
			}
			if (tokens.length > 1) {
				String bgColor = tokens[1]; // formatColorToPalette(tokens[1], true);
				rt = getMapRuleType("osmc_background", bgColor);
				if (rt != null) {
					propogated.put(rt, bgColor);
					rt = getMapRuleType("osmc_stub_name", "");
					if(rt != null) {
						propogated.put(rt, ".");
					}
				}
				if (tokens.length > 2) {
					String shpVl = tokens[2]; // formatColorToPalette(tokens[1], true);
					rt = getMapRuleType("osmc_foreground", shpVl);
					if (rt != null) {
						propogated.put(rt, shpVl);
					}
					if (tokens.length > 3) {
						String shp2Vl = tokens[3];
						rt = getMapRuleType("osmc_foreground2", shp2Vl);
						if (rt != null) {
							propogated.put(rt, shp2Vl);
						}
						if (tokens.length > 4) {
							String txtVl = tokens[4];
							rt = getMapRuleType("osmc_text", txtVl);
							if (rt != null) {
								propogated.put(rt, txtVl);
							}
							rt = getMapRuleType("osmc_text_symbol", txtVl);
							if (rt != null) {
								propogated.put(rt, txtVl);
							}
							if (tokens.length > 5) {
								String txtcolorVl = tokens[5];
								rt = getMapRuleType("osmc_textcolor", txtcolorVl);
								if (rt != null) {
									propogated.put(rt, txtcolorVl);
								}
							}
						}
					}
				}
			}
		}
	}



	private void osmcBackwardCompatility(Map<MapRulType, String> propogated, String[] tokens) {
		if (tokens.length > 0) {
			String wayColor = formatColorToPalette(tokens[0], true);
			MapRulType rt = getMapRuleType("osmc_symbol_" + wayColor, "");
			if(rt != null) {
				propogated.put(rt, wayColor);
			}
			rt = getMapRuleType("osmc_symbol", wayColor);
			if(rt != null) {
				propogated.put(rt, wayColor);
			}
			if(rt != null) {
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
					String shpValue ="none";
					if(shape.length() != 0) {
						shpValue = barValues.contains(shape) ? "bar" : "circle";
					}
					MapRulType shp = getMapRuleType("osmc_shape", shpValue);
					if (shp != null) {
						propogated.put(shp, shpValue);
					}
					String symbol = "osmc_symbol_" + formatColorToPalette(bgColor, true) +  fgColorPrefix + "_name";
					String name = "."; //"\u00A0";
//						if (tokens.length > 3 && tokens[3].trim().length() > 0) {
//							name = tokens[3];
//						}

					MapRulType textRule = getMapRuleType(symbol, "");
					if (textRule != null) {
						propogated.put(textRule, name);
					}
				}
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
	
	
	

	private enum EntityConverType {
		TAG_TRANSFORM,
		SPLIT
	}
	public static class EntityConvert {
		public TagValuePattern fromTag ;
		public EntityConverType type;
		public Set<EntityType> applyTo ;
		public List<TagValuePattern> ifTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> toTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
	}

}
