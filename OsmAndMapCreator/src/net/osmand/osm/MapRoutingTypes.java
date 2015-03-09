package net.osmand.osm;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.osmand.binary.RouteDataObject;
import net.osmand.osm.MapRenderingTypesEncoder.MapRouteTag;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;


public class MapRoutingTypes {

	private static Set<String> TAGS_TO_SAVE = new HashSet<String>();
	private static Set<String> TAGS_TO_ACCEPT = new HashSet<String>();
	private static Map<String, String> TAGS_TO_REPLACE = new HashMap<String, String>();
	private static Set<String> TAGS_RELATION_TO_ACCEPT = new HashSet<String>();
	private static Set<String> TAGS_TEXT = new HashSet<String>();
	private static Set<String> BASE_TAGS_TEXT = new HashSet<String>();
	private static Set<String> BASE_TAGS_TO_SAVE = new HashSet<String>();
	private static Map<String, String> BASE_TAGS_TO_REPLACE = new HashMap<String, String>();
	private static char TAG_DELIMETER = '/'; //$NON-NLS-1$
	
	private Map<String, MapRouteType> types = new LinkedHashMap<String, MapRoutingTypes.MapRouteType>();
	private List<MapRouteType> listTypes = new ArrayList<MapRoutingTypes.MapRouteType>();
	private MapRouteType refRuleType;
	private MapRouteType nameRuleType; 
	
	public MapRoutingTypes(MapRenderingTypesEncoder baseTypes) {
		for(MapRouteTag tg :  baseTypes.getRouteTags() ) {
			String t = tg.tag;
			if(tg.value != null) {
				t += TAG_DELIMETER + tg.value;
			}
			if(tg.register) {
				if(tg.relation) {
					TAGS_RELATION_TO_ACCEPT.add(t);
				}
				TAGS_TO_ACCEPT.add(t);
			} else if (tg.replace) {
				String t2 = tg.tag2;
				if (tg.value2 != null) {
					t2 += TAG_DELIMETER + tg.value2;
				}
				if (tg.base) {
					BASE_TAGS_TO_REPLACE.put(t, t2);
				}
				TAGS_TO_REPLACE.put(t, t2);
			} else if(tg.text) {
				if(tg.base) {
					BASE_TAGS_TEXT.add(t);
				}
				TAGS_TEXT.add(t);
			} else if(tg.amend) {
				if(tg.base) {
					BASE_TAGS_TO_SAVE.add(t);
				}
				TAGS_TO_SAVE.add(t);
			}
		}
	}
	
	public static String constructRuleKey(String tag, String val) {
		if(val == null || val.length() == 0){
			return tag;
		}
		return tag + TAG_DELIMETER + val;
	}
	
	protected static String getTagKey(String tagValue) {
		int i = tagValue.indexOf(TAG_DELIMETER);
		if(i >= 0){
			return tagValue.substring(0, i);
		}
		return tagValue;
	}
	
	protected static String getValueKey(String tagValue) {
		int i = tagValue.indexOf(TAG_DELIMETER);
		if(i >= 0){
			return tagValue.substring(i + 1);
		}
		return null;
	}
	
	public MapRouteType getRefRuleType() {
		return refRuleType;
	}
	
	
	public MapRouteType getNameRuleType() {
		return nameRuleType;
	}
	
	public Map<String, String> getRouteRelationPropogatedTags(Entity e) {
		Map<String, String> propogated = null; 
		for(Entry<String, String> es : e.getTags().entrySet()) {
			String tag = es.getKey();
			String value = converBooleanValue(es.getValue());
			if(contains(TAGS_RELATION_TO_ACCEPT, tag, value)) {
				propogated = new LinkedHashMap<String, String>();
				propogated.put(tag, value);
				break;
			}
		}
		if(propogated == null) {
			return propogated;
		}
		
		for(Entry<String, String> es : e.getTags().entrySet()) {
			String tag = es.getKey();
			String value = converBooleanValue(es.getValue());
			// do not propogate text tags they could be wrong
//			if(TAGS_TEXT.contains(tag)) {
//				propogated.put(tag, value);
//			}
			if(contains(TAGS_TO_ACCEPT, tag, value) ||
					startsWith(TAGS_TO_SAVE, tag, value)) {
				propogated.put(tag, value);
			}
		}
		return propogated;
	}
	
	private boolean contains(Set<String> s, String tag, String value) {
		if(s.contains(tag) || s.contains(tag + TAG_DELIMETER + value)){
			return true;
		}
		return false;
	}
	
	private String getMap(Map<String, String> s, String tag, String value) {
		String r = s.get(tag);
		if (r != null) {
			return r;
		}
		r = s.get(tag + TAG_DELIMETER + value);
		return r;
	}
	
	
	
	private boolean startsWith(Set<String> s, String tag, String value) {
		for(String st : s) {
			if(tag.startsWith(st)) {
				return true;
			}
		}
		return false;
	}
	
	private boolean testNonParseableRules(String tag, String value) {
		// fix possible issues (i.e. non arabic digits)
		if(tag.equals("maxspeed") && value != null) {
			try {
				RouteDataObject.parseSpeed(value, 0);
			} catch (Exception e) {
				return false;
			}
		}
		return true;
	}
	
	
	public boolean encodeEntity(Way et, TIntArrayList outTypes, Map<MapRouteType, String> names){
		Way e = et;
		boolean init = false;
		for(Entry<String, String> es : e.getTags().entrySet()) {
			String tag = es.getKey();
			String value = es.getValue();
			if(!testNonParseableRules(tag, value)){
				continue;
			}
			if (contains(TAGS_TO_ACCEPT, tag, value)) {
				init = true;
				break;
			}
		}
		if(!init) {
			return false;
		}
		outTypes.clear();
		names.clear();
		for(Entry<String, String> es : e.getTags().entrySet()) {
			String tag = es.getKey();
			System.out.println(tag + " " + e.getId());
			String value = converBooleanValue(es.getValue());
			if(!testNonParseableRules(tag, value)){
				continue;
			}
			String tvl = getMap(TAGS_TO_REPLACE, tag.toLowerCase(), value.toLowerCase());
			if(tvl != null) {
				int i = tvl.indexOf(TAG_DELIMETER);
				tag = tvl.substring(0, i);
				value = tvl.substring(i + 1);
			}
            if(TAGS_TEXT.contains(tag)) {
                names.put(registerRule(tag, null), value);
            } else if(contains(TAGS_TO_ACCEPT, tag, value) || startsWith(TAGS_TO_SAVE, tag, value) || getMap(TAGS_TO_REPLACE, tag, value) != null) {
				outTypes.add(registerRule(tag, value).id);
			}
		}
		return true;
	}
	
	public boolean encodeBaseEntity(Way et, TIntArrayList outTypes, Map<MapRouteType, String> names){
		Way e = et;
		boolean init = false;
		for(Entry<String, String> es : e.getTags().entrySet()) {
			String tag = es.getKey();
			String value = es.getValue();
			if (contains(TAGS_TO_ACCEPT, tag, value)) {
				if(value.startsWith("trunk") || value.startsWith("motorway")
						|| value.startsWith("primary") || value.startsWith("secondary")
						|| value.startsWith("tertiary")
						|| value.startsWith("ferry")
						) {
					init = true;
					break;
				}
			}
		}
		if(!init) {
			return false;
		}
		outTypes.clear();
		names.clear();
		for(Entry<String, String> es : e.getTags().entrySet()) {
			String tag = es.getKey();
			String value = converBooleanValue(es.getValue());
			String tvl = getMap(BASE_TAGS_TO_REPLACE, tag, value);
			if(tvl != null) {
				int i = tvl.indexOf(TAG_DELIMETER);
				tag = tvl.substring(0, i);
				value = tvl.substring(i + 1);
			}
			if(BASE_TAGS_TEXT.contains(tag)) {
				names.put(registerRule(tag, null), value);
			}
			if(contains(TAGS_TO_ACCEPT, tag, value) ||
					startsWith(BASE_TAGS_TO_SAVE, tag, value)) {
				outTypes.add(registerRule(tag, value).id);
			}
		}
		return true;
	}
	
	private String converBooleanValue(String value){
		if(value.equals("true")) {
			return "yes";
		} else if(value.equals("false")) {
			return "no";
		}
		return value;
	}
	
	public void encodePointTypes(Way e, TLongObjectHashMap<TIntArrayList> pointTypes, boolean base){
		pointTypes.clear();
		for(Node nd : e.getNodes() ) {
			if (nd != null) {
				for (Entry<String, String> es : nd.getTags().entrySet()) {
					String tag = es.getKey();
					String value = converBooleanValue(es.getValue());
					String tvl = getMap(base ? BASE_TAGS_TO_REPLACE : TAGS_TO_REPLACE, tag, value);
					if(tvl != null) {
						int i = tvl.indexOf(TAG_DELIMETER);
						tag = tvl.substring(0, i);
						value = tvl.substring(i + 1);
					}
					if (contains(TAGS_TO_ACCEPT, tag, value) || startsWith(base? BASE_TAGS_TO_SAVE : TAGS_TO_SAVE, tag, value)) {
						if (!pointTypes.containsKey(nd.getId())) {
							pointTypes.put(nd.getId(), new TIntArrayList());
						}
						pointTypes.get(nd.getId()).add(registerRule(tag, value).id);
					}
				}
			}
		}
	}
	
	public MapRouteType getTypeByInternalId(int id) {
		return listTypes.get(id - 1);
	}
	
	private MapRouteType registerRule(String tag, String val) {
		String id = constructRuleKey(tag, val);
		if(!types.containsKey(id)) {
			MapRouteType rt = new MapRouteType();
			// first one is always 1
			rt.id = types.size() + 1;
			rt.tag = tag;
			rt.value = val;
			types.put(id, rt);
			listTypes.add(rt);
			if(tag.equals("ref")){
				refRuleType = rt;
			}
			if(tag.equals("name")){
				nameRuleType = rt;
			}
		}
		MapRouteType type = types.get(id);
		type.freq ++;
		return type;
	}
	
	public static class MapRouteType {
		int freq = 0;
		int id;
		int targetId;
		String tag;
		String value;
		
		public int getInternalId() {
			return id;
		}
		
		public int getFreq() {
			return freq;
		}
		
		public int getTargetId() {
			return targetId;
		}
		
		public String getTag() {
			return tag;
		}
		
		public String getValue() {
			return value;
		}
		
		public void setTargetId(int targetId) {
			this.targetId = targetId;
		}
		
		@Override
		public String toString() {
			if (value == null) {
				return "'" + tag + "'";
			}
			return tag + "='" + value + "'";
		}

	}

	public List<MapRouteType> getEncodingRuleTypes() {
		return listTypes;
	}
}
