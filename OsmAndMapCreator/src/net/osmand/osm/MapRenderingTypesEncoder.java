package net.osmand.osm;

import gnu.trove.list.array.TIntArrayList;

import java.util.*;
import java.util.Map.Entry;

import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.util.Algorithms;

import org.xmlpull.v1.XmlPullParser;

public class MapRenderingTypesEncoder extends MapRenderingTypes {
	
	// stored information to convert from osm tags to int type
	private List<MapRouteTag> routeTags = new ArrayList<MapRouteTag>();
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
		Iterator<Entry<String, String>> its = ts.entrySet().iterator();
		while(its.hasNext()) {
			Entry<String, String> ev = its.next();
			MapRulType rule = getRelationalTagValue(ev.getKey(), ev.getValue());
			if(rule != null) {
				String value = ev.getValue();
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
			addOSMCSymbolsSpecialTags(propogated, ev);
		}
		return propogated;
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
	protected MapRulType parseTypeFromXML(XmlPullParser parser, String poiParentCategory, String poiParentPrefix, String order) {
		MapRulType rtype = parseBaseRuleType(parser, poiParentCategory, poiParentPrefix, order, false);
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
		if(splitIsNeeded(e.getTags())) {
			if(splitTagsIntoDifferentObjects(e.getTags()).size() > 1) {
				throw new UnsupportedOperationException("Split is needed for tag/values " + e.getTags() );
			}
		}
		return encodeEntityWithType(e instanceof Node, 
				e.getModifiableTags(), zoom, outTypes, outAddTypes, namesToEncode, tempListNotUsed);
	}
	
	public boolean encodeEntityWithType(boolean node, Map<String, String> tags, int zoom, TIntArrayList outTypes, 
			TIntArrayList outAddTypes, TreeMap<MapRulType, String> namesToEncode, List<MapRulType> tempListNotUsed) {
		outTypes.clear();
		outAddTypes.clear();
		namesToEncode.clear();
		boolean area = "yes".equals(tags.get("area")) || "true".equals(tags.get("area")) || tags.containsKey("area:highway");
		if(tags.containsKey("color")) {
			prepareColorTag(tags, "color");
		}
		if(tags.containsKey("colour")) {
			prepareColorTag(tags, "colour");
		}

		for (String tag : tags.keySet()) {
			String val = tags.get(tag);
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

		if ((h < 16 && s > 25 && v > 30) || (h > 326 && s > 25 && v > 30) || (h < 16 && s > 10 && s < 25 && v > 90) || (h > 326 && s > 10 && s < 25 && v > 90) || vl.equals("pink") || vl.equals("red") || vl.equals("red;white") || vl.equals("red/white") || vl.equals("white/red") || vl.equals("pink/white") || vl.equals("white-red") || vl.equals("ff0000") || vl.equals("800000") || vl.equals("red/tan") || vl.equals("tan/red") || vl.equals("rose")) {
			vl = "red";
		} else if ((h > 16 && h < 50 && s > 25 && v > 20 && v < 60) || vl.equals("brown") || vl.equals("darkbrown") || vl.equals("tan/brown") || vl.equals("tan_brown") || vl.equals("brown/tan") || vl.equals("light_brown") || vl.equals("brown/white")) {
			vl = palette6 ? "red" : "brown";
		} else if ((h > 16 && h < 45 && v > 60) || vl.equals("orange") || vl.equals("cream") || vl.equals("gold") || vl.equals("yellow-red") || vl.equals("ff8c00") || vl.equals("peach")) {
			vl = palette6 ? "red" : "orange";
		} else if ((h > 46 && h < 73 && s > 30 && v > 60) || vl.equals("yellow") || vl.equals("tan") || vl.equals("gelb") || vl.equals("ffff00") || vl.equals("beige") || vl.equals("lightyellow") || vl.equals("jaune") || vl.equals("olive")) {
			vl = "yellow";
		} else if ((h > 74 && h < 150 && s > 30 && v > 77) || vl.equals("lightgreen") || vl.equals("lime") || vl.equals("seagreen") || vl.equals("00ff00") || vl.equals("yellow/green")) {
			vl = palette6 ? "green" : "lightgreen";
		} else if ((h > 74 && h < 174 && s > 30 && v > 30 && v < 77) || vl.equals("green") || vl.equals("darkgreen") || vl.equals("natural") || vl.equals("natur") || vl.equals("mediumseagreen") || vl.equals("green/white") || vl.equals("white/green") || vl.equals("blue/yellow") || vl.equals("vert") || vl.equals("green/blue")) {
			vl = "green";
		} else if ((h > 174 && h < 215 && s > 15 && v > 50) || vl.equals("lightblue") || vl.equals("aqua") || vl.equals("cyan") || vl.equals("87ceeb") || vl.equals("turquoise")) {
			vl = palette6 ? "blue" : "lightblue";
		} else if ((h > 215 && h < 250 && s > 40 && v > 30) || vl.equals("blue") || vl.equals("blue/white") || vl.equals("blue/tan") || vl.equals("0000ff") || vl.equals("teal") || vl.equals("darkblue") || vl.equals("blu") || vl.equals("navy")) {
			vl = "blue";
		} else if ((h > 250 && h < 325 && s > 15 && v > 45) || (h > 250 && h < 325 && s > 10 && s < 25 && v > 90) || vl.equals("purple") || vl.equals("violet") || vl.equals("magenta") || vl.equals("maroon") || vl.equals("fuchsia") || vl.equals("800080")) {
			vl = palette6 ? "blue" : "purple";
		} else if ((color != -1 & v < 20) || vl.equals("black") || vl.equals("darkgrey")) {
			vl = "black";
		} else if ((s < 5 && v > 30 && v < 90) || vl.equals("gray") || vl.equals("grey") || vl.equals("grey/tan") || vl.equals("silver") || vl.equals("srebrny") || vl.equals("lightgrey") || vl.equals("lightgray") || vl.equals("metal")) {
			vl = palette6 ? "white" : "gray";
		} else if ((s < 5 && v > 95) || vl.equals("white") || vl.equals("white/tan")) {
			vl = "white";
		} else if (r != -1 && g != -1 && b != -1) {
			vl = "gray";
		}
		return vl;
	}

	public void addOSMCSymbolsSpecialTags(Map<MapRulType,String> propogated, Entry<String,String> ev) {
		if ("osmc:symbol".equals(ev.getKey())) {
			String[] tokens = ev.getValue().split(":", 6);
			if (tokens.length > 0) {
				String symbol_name = "osmc_symbol_" + formatColorToPalette(tokens[0], true);
				MapRulType rt = getMapRuleType(symbol_name, "");
				if(rt != null) {
					propogated.put(rt, "");
					if (tokens.length > 2 && rt.names != null) {
						String symbol = "osmc_symbol_" + formatColorToPalette(tokens[1], true) + "_" + formatColorToPalette(tokens[2], true) + "_name";
						String name = "\u00A0";
						if (tokens.length > 3 && tokens[3].trim().length() > 0) {
							name = tokens[3];
						}
						for(int k = 0; k < rt.names.length; k++) {
							if(rt.names[k].tagValuePattern.tag.equals(symbol)) {
								propogated.put(rt.names[k], name);
							}
						}
					}
				}
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
	
	
	
	

}
