package net.osmand.osm;

import gnu.trove.list.array.TIntArrayList;

import java.util.*;
import java.util.Map.Entry;

import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Relation;
import net.osmand.util.Algorithms;

import org.xmlpull.v1.XmlPullParser;

public class MapRenderingTypesEncoder extends MapRenderingTypes {
	
	// stored information to convert from osm tags to int type
	private Map<String, MapRulType> types = null;
	private List<MapRulType> typeList = new ArrayList<MapRulType>();
	private List<MapRouteTag> routeTags = new ArrayList<MapRouteTag>();
	private MapRulType nameRuleType;
	private MapRulType nameEnRuleType;
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
	
	public Map<String, MapRulType> getEncodingRuleTypes(){
		checkIfInitNeeded();
		return types;
	}
	
	@Override
	protected void checkIfInitNeeded() {
		if (types == null) {
			types = new LinkedHashMap<String, MapRulType>();
			typeList.clear();
			nameRuleType = new MapRulType();
			nameRuleType.tag = "name";
			nameRuleType.onlyNameRef = true;
			nameRuleType.additional = false; 
			registerRuleType("name", null, nameRuleType);
			nameEnRuleType = new MapRulType();
			nameEnRuleType.tag = "name:en";
			nameEnRuleType.onlyNameRef = true;
			nameEnRuleType.additional = false; 
			registerRuleType("name:en", null, nameRuleType);
			super.checkIfInitNeeded();
		}
	}
	
	
	
	private MapRulType registerRuleType(String tag, String val, MapRulType rt){
		String keyVal = constructRuleKey(tag, val);
		if("natural".equals(tag) && "coastline".equals(val)) {
			coastlineRuleType = rt;
		}
		if(types.containsKey(keyVal)){
			if(types.get(keyVal).onlyNameRef ) {
				rt.id = types.get(keyVal).id;
				types.put(keyVal, rt);
				typeList.set(rt.id, rt);
				return rt;
			} else {
				throw new RuntimeException("Duplicate " + keyVal);
			}
		} else {
			rt.id = types.size();
			types.put(keyVal, rt);
			typeList.add(rt);
			return rt;
		}
	}

	private MapRulType getRelationalTagValue(String tag, String val) {
		MapRulType rType = getRuleType(tag, val);
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
						String tag = rule.names[i].tag.substring(rule.namePrefix.length());
						if(ts.containsKey(tag)) {
							propogated.put(rule.names[i], ts.get(tag));
						}
					}
				}
				propogated.put(rule, value);
			}
			addParsedSpecialTags(propogated, ev);
		}
		return propogated;
	}
	
	private MapRulType getRuleType(String tag, String val) {
		Map<String, MapRulType> types = getEncodingRuleTypes();
		MapRulType rType = types.get(constructRuleKey(tag, val));
		if (rType == null) {
			rType = types.get(constructRuleKey(tag, null));
		}
		return rType;
	}
	
	public MapRulType getNameRuleType() {
		getEncodingRuleTypes();
		return nameRuleType;
	}
	
	public MapRulType getNameEnRuleType() {
		getEncodingRuleTypes();
		return nameRuleType;
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
	protected void parseTypeFromXML(XmlPullParser parser, String poiParentCategory, String poiParentPrefix) {
		super.parseTypeFromXML(parser, poiParentCategory, poiParentPrefix);
		MapRulType rtype = new MapRulType();
		String val = parser.getAttributeValue("", "minzoom"); //$NON-NLS-1$
		rtype.minzoom = 15;
		if (val != null) {
			rtype.minzoom = Integer.parseInt(val);
		}
		rtype.tag = parser.getAttributeValue("", "tag"); //$NON-NLS-1$
		rtype.value = parser.getAttributeValue("", "value"); //$NON-NLS-1$
		if (rtype.value != null && rtype.value.length() == 0) { //$NON-NLS-1$
			rtype.value = null;
		}
		registerRuleType(rtype.tag, rtype.value, rtype);
		rtype.additional = Boolean.parseBoolean(parser.getAttributeValue("", "additional")); //$NON-NLS-1$
		rtype.onlyPoint = Boolean.parseBoolean(parser.getAttributeValue("", "point")); //$NON-NLS-1$
		rtype.relation = Boolean.parseBoolean(parser.getAttributeValue("", "relation")); //$NON-NLS-1$
		rtype.namePrefix = parser.getAttributeValue("", "namePrefix"); //$NON-NLS-1$
		rtype.nameCombinator = parser.getAttributeValue("", "nameCombinator"); //$NON-NLS-1$
		if(rtype.namePrefix == null){
			rtype.namePrefix = "";
		}
		
		String v = parser.getAttributeValue("", "nameTags");
		if (v != null) {
			String[] names = v.split(",");
			if (names.length == 0) {
				names = new String[] { "name" };
			}
			rtype.names = new MapRulType[names.length];
			for (int i = 0; i < names.length; i++) {
				String tagName = names[i];
				if(rtype.namePrefix.length() > 0) {
					tagName = rtype.namePrefix + tagName;
				}
				MapRulType mt = types.get(constructRuleKey(tagName, null));
				if (mt == null) {
					mt = new MapRulType();
					mt.tag = tagName;
					mt.onlyNameRef = true;
					mt.additional = false;
					registerRuleType(tagName, null, mt);
				}
				rtype.names[i] = mt;
			}
		}
		String targetTag = parser.getAttributeValue("", "target_tag");
		String targetValue = parser.getAttributeValue("", "target_value");
		if (targetTag != null || targetValue != null) {
			if (targetTag == null) {
				targetTag = rtype.tag;
			}
			if (targetValue == null) {
				targetValue = rtype.value;
			}
			rtype.targetTagValue = types.get(constructRuleKey(targetTag, targetValue));
			if (rtype.targetTagValue == null) {
				throw new RuntimeException("Illegal target tag/value " + targetTag + " " + targetValue);
			}
		}
	}

	
	
	public MapRulType getTypeByInternalId(int id) {
		return typeList.get(id);
	}
	

	public boolean encodeEntityWithType(Entity e, int zoom, TIntArrayList outTypes, 
			TIntArrayList outaddTypes, Map<MapRulType, String> namesToEncode, List<MapRulType> tempList) {
		outTypes.clear();
		outaddTypes.clear();
		namesToEncode.clear();
		tempList.clear();
		tempList.add(getNameRuleType());
		tempList.add(getNameEnRuleType());

		boolean area = "yes".equals(e.getTag("area")) || "true".equals(e.getTag("area"));

		Collection<String> tagKeySet = e.getTagKeySet();
		for (String tag : tagKeySet) {
			String val = e.getTag(tag);
			MapRulType rType = getRuleType(tag, val);
			if (rType != null) {
				if (rType.minzoom > zoom) {
					continue;
				}
				if (rType.onlyPoint && !(e instanceof net.osmand.osm.edit.Node)) {
					continue;
				}
				if(rType.targetTagValue != null) {
					rType = rType.targetTagValue;
				}
				rType.updateFreq();
				if (rType.names != null) {
                    Collections.addAll(tempList, rType.names);
				}

				if (!rType.onlyNameRef) {
					if (rType.additional) {
						outaddTypes.add(rType.id);
					} else {
						outTypes.add(rType.id);
					}
				}
			}
		}
        // sort to get most important features as first type (important for rendering)
        outTypes.sort();
        outaddTypes.sort();
		for(MapRulType mt : tempList){
			String val = e.getTag(mt.tag);
			if(mt == nameEnRuleType && Algorithms.objectEquals(val, e.getTag(nameRuleType.tag))) {
				continue;
			}
			if(val != null && val.length() > 0){
				namesToEncode.put(mt, val);
			}
		}
		return area;
	}
	
	public void addParsedSpecialTags(Map<MapRulType,String> propogated, Entry<String,String> ev) {
		if ("osmc:symbol".equals(ev.getKey())) {
			String[] tokens = ev.getValue().split(":", 6);
			if (tokens.length > 0) {
				String symbol_name = "osmc_symbol_" + tokens[0];
				MapRulType rt = getRuleType(symbol_name, "");
				if(rt != null) {
					propogated.put(rt, "");
					if (tokens.length > 2 && rt.names != null) {
						String symbol = "osmc_symbol_" + tokens[1] + "_" + tokens[2] + "_name";
						String name = "\u00A0";
						if (tokens.length > 3 && tokens[3].trim().length() > 0) {
							name = tokens[3];
						}
						for(int k = 0; k < rt.names.length; k++) {
							if(rt.names[k].tag.equals(symbol)) {
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
	
	public static class MapRulType {
		protected MapRulType[] names;
		protected String tag;
		protected String value;
		protected int minzoom;
		protected boolean additional;
		protected boolean relation;
		protected MapRulType targetTagValue;
		protected boolean onlyNameRef;
		protected boolean onlyPoint;
		
		// inner id
		protected int id;
		protected int freq;
		protected int targetId;
		
		protected String namePrefix ="";
		protected String nameCombinator = null;
		
		
		
		public MapRulType(){
		}
		
		
		public String getTag() {
			return tag;
		}
		
		public int getTargetId() {
			return targetId;
		}
		
		public int getInternalId() {
			return id;
		}
		
		public void setTargetId(int targetId) {
			this.targetId = targetId;
		}
		
		public MapRulType getTargetTagValue() {
			return targetTagValue;
		}
		
		public String getValue() {
			return value;
		}
		
		public int getMinzoom() {
			return minzoom;
		}
		
		public boolean isAdditional() {
			return additional;
		}
		
		public boolean isOnlyNameRef() {
			return onlyNameRef;
		}
		
		public boolean isOnlyPoint() {
			return onlyPoint;
		}
		
		public boolean isRelation() {
			return relation;
		}
		
		public int getFreq() {
			return freq;
		}
		
		public int updateFreq(){
			return ++freq;
		}
		
		@Override
		public String toString() {
			return tag + " " + value;
		}
	}

}
