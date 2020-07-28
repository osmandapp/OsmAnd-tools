package net.osmand.osm;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import net.osmand.obf.preparation.OsmDbAccessorContext;
import net.osmand.osm.MapRenderingTypes.MapRulType;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.util.Algorithms;

public class TagsTransformer {
	Map<EntityId, Map<String, String>> propogatedTags = new LinkedHashMap<Entity.EntityId, Map<String, String>>();
	final static String SPLIT_VALUE= "SPLITVL";
	private static final List<String> NODE_NETWORK_IDS = Arrays.asList("network:type", "expected_rcn_route_relations");
	private static final List<String> NODE_NETWORKS_REF_TYPES = Arrays.asList("icn_ref", "ncn_ref", "rcn_ref", "lcn_ref", "iwn_ref", "nwn_ref", "rwn_ref", "lwn_ref");
	private static final String multipleNodeNetworksKey = "multiple_node_networks";
	
	private static final int MAX_RELATION_GROUP = 10;
	private static final String RELATION_SORT_TAG = "relation_sort:";
	
	private static class RenderingSortGroup {
		String relationGroupKeyString;
		String relationGroupValueString;
	}
	
	private Map<String, String> processNameTags(Map<String, String> relationTags, MapRenderingTypesEncoder renderingTypes,
			Map<String, String> relationNameTags, Map<String, String> relationNames) {
		if (relationNames != null) {
			for (Entry<String, String> e : relationNames.entrySet()) {
				String sourceTag = e.getKey();
				String targetTag = e.getValue();
				String vl = relationTags.get(sourceTag);
				if (!Algorithms.isEmpty(vl)) {
					renderingTypes.checkOrCreateTextRule(targetTag);
					relationNameTags.put(targetTag, vl);
				}
			}
		}
		return relationNameTags;
	}
	
	private void processRelationTags(MapRenderingTypesEncoder renderingTypes, Map<String, String> relationNameTags,
			Map<String, String> relationPutTags, Map<String, String> relationGroupNameTags,
			Map<String, String> relationTags, RenderingSortGroup g) {
		for (Entry<String, String> ev : relationTags.entrySet()) {
			String key = ev.getKey();
			String value = ev.getValue();
			MapRulType rule = renderingTypes.getRuleType(key, value, false, false);
			if (rule != null && rule.relation) {
				if (rule.isAdditionalOrText()) {
					relationPutTags.put(key, value);
					g.relationGroupKeyString = key;
					g.relationGroupValueString = value;
				} else {
					// for main tags propagate "route_hiking", "route_road", etc
					String mainTag = key + "_" + value;
					relationPutTags.put(mainTag, "");
					g.relationGroupKeyString = mainTag;
					String sortValue = "";
					if (rule.relationSortTags != null) {
						for (Entry<String, List<String>> sortTag : rule.relationSortTags.entrySet()) {
							String vl = relationTags.get(sortTag.getKey());
							int index = sortTag.getValue().indexOf(vl);
							if (index < 0) {
								index = sortTag.getValue().size();
							}
							char ch = (char) ('a' + index);
							sortValue += ch;
						}
					}
					g.relationGroupValueString = sortValue;
					
					processNameTags(relationTags, renderingTypes, relationNameTags, rule.relationNames);
					processNameTags(relationTags, renderingTypes, relationGroupNameTags,
							rule.relationGroupNameTags);
					if (rule.additionalTags != null) {
						for (Entry<String, String> atag : rule.additionalTags.entrySet()) {
							String tvalue = relationTags.get(atag.getKey());
							if (!Algorithms.isEmpty(tvalue)) {
								renderingTypes.checkOrCreateAdditional(atag.getValue(), tvalue);
								relationPutTags.put(atag.getValue(), tvalue);
							}
						}
					}
					if (rule.relationGroupAdditionalTags != null) {
						for (Entry<String, String> atag : rule.relationGroupAdditionalTags.entrySet()) {
							String tvalue = relationTags.get(atag.getKey());
							if (!Algorithms.isEmpty(tvalue)) {
								renderingTypes.checkOrCreateAdditional(atag.getValue(), tvalue);
								relationGroupNameTags.put(atag.getValue(), tvalue);
							}
						}
					}
				}
			}
		}
	}
	
	public void handleRelationPropogatedTags(Relation relation, MapRenderingTypesEncoder renderingTypes, OsmDbAccessorContext ctx, 
			EntityConvertApplyType at) throws SQLException {
		Map<String, String> relationNameTags = new LinkedHashMap<String, String>();
		Map<String, String> relationPutTags = new LinkedHashMap<String, String>();
		Map<String, String> relationGroupNameTags = new LinkedHashMap<String, String>();
		
		Map<String, String> relationTags = relation.getTags();
		relationTags = renderingTypes.transformTags(relationTags, EntityType.RELATION, at);
		RenderingSortGroup rsg = new RenderingSortGroup();
		processRelationTags(renderingTypes, 
				relationNameTags, relationPutTags, relationGroupNameTags, relationTags, rsg);
		if (!Algorithms.isEmpty(relationPutTags)) {
			if (ctx != null) {
				ctx.loadEntityRelation(relation);
			}
			for (RelationMember ids : relation.getMembers()) {
				Map<String, String> map = getPropogateTagForEntity(ids.getEntityId());
				boolean overwriteAdditionalTags = true;
				if (map.containsKey(RELATION_SORT_TAG + rsg.relationGroupKeyString)) {
					String vl = map.get(RELATION_SORT_TAG + rsg.relationGroupKeyString);
//					System.out.println(String.format("CC %s: %s %s - %d",rsg.relationGroupKeyString,
//							rsg.relationGroupValueString, vl,
//							rsg.relationGroupValueString.compareTo(vl)));
					if (rsg.relationGroupValueString.compareTo(vl) <= 0) {
						overwriteAdditionalTags = false;
					}
				}

				if (overwriteAdditionalTags) {
					map.put(RELATION_SORT_TAG + rsg.relationGroupKeyString, rsg.relationGroupValueString);
					map.putAll(relationPutTags);
					if (relationNameTags.size() > 0) {
						for (Entry<String, String> es : relationNameTags.entrySet()) {
							String key = es.getKey();
							String res = sortAndAttachUniqueValue(map.get(key), es.getValue());
							map.put(key, res);
						}
					}
				}
				if (relationGroupNameTags.size() > 0) {
					int modifier = 1;
					// TODO add group names according to sort
					while (modifier < MAX_RELATION_GROUP) {
						String sortKey = RELATION_SORT_TAG + modifier + ":" + rsg.relationGroupKeyString;
						if (!map.containsKey(sortKey)) {
							map.put(sortKey, rsg.relationGroupValueString);
							break;
						}
						modifier++;
					}
					for (Entry<String, String> es : relationGroupNameTags.entrySet()) {
						String key = es.getKey();
						String value = es.getValue();
						String newKey = addModifier(key, modifier);
						if (renderingTypes.getRuleType(key, value, false, false).isAdditional()) {
							renderingTypes.checkOrCreateAdditional(newKey, value);
						} else {
							renderingTypes.checkOrCreateTextRule(newKey);
						}
						map.put(newKey, value);
					}
				}
			}
		}
	}

	private String addModifier(String key, int modifier) {
		if (key.indexOf(":") >= 0) {
			for (String lang : MapRenderingTypes.langs) {
				if (key.endsWith(":" + lang)) {
					return key.substring(0, key.length() - lang.length() - 1) + "_" + modifier + ":" + lang;
				}
			}
		}
		return key + "_" + modifier;
	}

	public Map<String, String> getPropogateTagForEntity(EntityId entityId) {
		if(!propogatedTags.containsKey(entityId)) {
			propogatedTags.put(entityId, new LinkedHashMap<String, String>());
		}
		return propogatedTags.get(entityId);
	}
	
	public void addMultipleNetwoksTag(Entity e) {
		if (e instanceof Node && !Algorithms.isEmpty(e.getTags()) 
				&& NODE_NETWORK_IDS.contains(e.getTags().entrySet().iterator().next().getKey())) {
			int networkTypesCount = 0;
			for (Entry<String, String> tag : e.getTags().entrySet()) {
				if (NODE_NETWORKS_REF_TYPES.contains(tag.getKey())) {
					networkTypesCount++;
				} 
			}
			if (networkTypesCount > 1) {
				e.putTag(multipleNodeNetworksKey, "true");
			}
		}
	}
	
	public void addPropogatedTags(EntityConvertApplyType tp, Entity e) {
		EntityId eid = EntityId.valueOf(e);
		Map<String, String> proptags = propogatedTags.get(eid);
		if (proptags != null) {
			Iterator<Entry<String, String>> iterator = proptags.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<String, String> ts = iterator.next();
				if (e.getTag(ts.getKey()) == null) {
					String vl = ts.getValue();
					if(vl != null) {
						vl = vl.replaceAll(SPLIT_VALUE, ", ");
					}
					e.putTag(ts.getKey(), vl);
				}
			}
		}		
	}
	
	
	
	private static String sortAndAttachUniqueValue(String list, String value) {
		if(list == null) {
			list = "";
		}
		String[] ls = list.split(SPLIT_VALUE);
		Set<String> set = new TreeSet<String>(new Comparator<String>() {

			@Override
			public int compare(String s1, String s2) {
				int i1 = Algorithms.extractFirstIntegerNumber(s1);
				int i2 = Algorithms.extractFirstIntegerNumber(s2);
				if(i1 == i2) {
					String t1 = Algorithms.extractIntegerSuffix(s1);
					String t2 = Algorithms.extractIntegerSuffix(s2);
					return t1.compareToIgnoreCase(t2);
				}
				return i1 - i2;
			}
		});
		set.add(value.trim());
		for(String l : ls) {
			set.add(l.trim());
		}
		String r = "";
		for(String a : set) {
			if(r.length() > 0) {
				r += SPLIT_VALUE;
			}
			r+= a;
		}
		return r;
	}

	
}
