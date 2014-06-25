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
		rtype.tag = parser.getAttributeValue("", "tag"); //$NON-NLS-1$
		rtype.value = parser.getAttributeValue("", "value"); //$NON-NLS-1$
		rtype.tag2 = parser.getAttributeValue("", "tag2"); //$NON-NLS-1$
		rtype.value2 = parser.getAttributeValue("", "value2"); //$NON-NLS-1$
		rtype.base = Boolean.parseBoolean(parser.getAttributeValue("", "base"));
		rtype.replace = "replace".equalsIgnoreCase(mode);
		rtype.register = "register".equalsIgnoreCase(mode);
		rtype.amend = "amend".equalsIgnoreCase(mode);
		rtype.text = "text".equalsIgnoreCase(mode);
		rtype.relation = Boolean.parseBoolean(parser.getAttributeValue("", "relation"));
		routeTags.add(rtype);
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
			TIntArrayList outAddTypes, Map<MapRulType, String> namesToEncode, List<MapRulType> tempListNotUsed) {
		if(splitIsNeeded(e.getTags())) {
			if(splitTagsIntoDifferentObjects(e.getTags()).size() > 1) {
				throw new UnsupportedOperationException("Split is needed for tag/values " + e.getTags() );
			}
		}
		return encodeEntityWithType(e instanceof Node, 
				e.getTags(), zoom, outTypes, outAddTypes, namesToEncode, tempListNotUsed);
	}
	
	public boolean encodeEntityWithType(boolean node, Map<String, String> tags, int zoom, TIntArrayList outTypes, 
			TIntArrayList outAddTypes, Map<MapRulType, String> namesToEncode, List<MapRulType> tempListNotUsed) {
		outTypes.clear();
		outAddTypes.clear();
		namesToEncode.clear();
		boolean area = "yes".equals(tags.get("area")) || "true".equals(tags.get("area"));

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
	
	public void addOSMCSymbolsSpecialTags(Map<MapRulType,String> propogated, Entry<String,String> ev) {
		if ("osmc:symbol".equals(ev.getKey())) {
			String[] tokens = ev.getValue().split(":", 6);
			if (tokens.length > 0) {
				String symbol_name = "osmc_symbol_" + tokens[0];
				MapRulType rt = getMapRuleType(symbol_name, "");
				if(rt != null) {
					propogated.put(rt, "");
					if (tokens.length > 2 && rt.names != null) {
						String symbol = "osmc_symbol_" + tokens[1] + "_" + tokens[2] + "_name";
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
			if(vl.equals("#ffff00")){
				vl = "yellow";
			} else if(vl.equals("#ff0000")){
				vl = "red";
			} else if(vl.equals("#00ff00")){
				vl = "green";
			} else if(vl.equals("#0000ff")){
				vl = "blue";
			} else if(vl.equals("#000000")){
				vl = "black";
			}
			String nm = "color_"+vl;
			MapRulType rt = getMapRuleType(nm, "");
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
