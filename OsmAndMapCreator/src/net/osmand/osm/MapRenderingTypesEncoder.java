package net.osmand.osm;

import gnu.trove.list.array.TIntArrayList;

import java.util.*;
import java.util.Map.Entry;

import net.osmand.osm.edit.Entity;
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
		super.registerRuleType(rt);
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
	protected MapRulType parseTypeFromXML(XmlPullParser parser, String poiParentCategory, String poiParentPrefix) {
		MapRulType rtype = parseBaseRuleType(parser, poiParentCategory, poiParentPrefix, false);
		rtype.onlyPoi = "true".equals(parser.getAttributeValue("", "only_poi"));
		if(!rtype.onlyPoi) {
			String val = parser.getAttributeValue("", "minzoom"); //$NON-NLS-1$
			rtype.minzoom = 15;
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
		outTypes.clear();
		outAddTypes.clear();
		namesToEncode.clear();
		boolean area = "yes".equals(e.getTag("area")) || "true".equals(e.getTag("area"));

		Collection<String> tagKeySet = e.getTagKeySet();
		for (String tag : tagKeySet) {
			String val = e.getTag(tag);
			MapRulType rType = getMapRuleType(tag, val);
			if (rType != null) {
				if (rType.minzoom > zoom || rType.maxzoom < zoom) {
					continue;
				}
				if (rType.onlyPoint && !(e instanceof net.osmand.osm.edit.Node)) {
					continue;
				}
				if(rType == nameEnRuleType && Algorithms.objectEquals(val, e.getTag(OSMTagKey.NAME))) {
					continue;
				}
				if(rType.targetTagValue != null) {
					rType = rType.targetTagValue;
				}
				rType.updateFreq();
				if (!rType.isAdditionalOrText()) {
					outTypes.add(rType.id);
				} else {
					boolean applied = rType.applyToTagValue == null;
					if(!applied) {
						Iterator<TagValuePattern> it = rType.applyToTagValue.iterator();
						while(!applied && it.hasNext()) {
							TagValuePattern nv = it.next();
							applied = nv.isApplicable(e.getTags());
						}
					}
					if (applied) {
						if (rType.isAdditional()) {
							outAddTypes.add(rType.id);
						} else if (rType.isText()) {
							namesToEncode.put(rType, val);
						}
					}
				}
			}
		}
        // sort to get most important features as first type (important for rendering)
        outTypes.sort();
        outAddTypes.sort();
		return area;
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
