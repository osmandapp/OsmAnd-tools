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
	
	//////// TODO BLOCK ////////
	private int networkWayOrder(String tag) {
		switch (tag) {
		case "network_iwn":
			return 4;
		case "network_nwn":
			return 3;
		case "network_rwn":
			return 2;
		case "network_lwn":
			return 1;
		default:
			return 0;
		}
	}

	private int networkCycleOrder(String tag, String value) {
		if ("network".equals(tag)) {
			switch (value) {
			case "icn":
				return 4;
			case "ncn":
				return 3;
			case "rcn":
				return 2;
			case "lcn":
				return 1;
			default:
				return 0;
			}
		}
		return 0;
	}

	private boolean skipPropagationOfDuplicate(Map<MapRulType, Map<MapRulType, String>> propogated,
			Map<String, String> existing) {
		int existingWayNetwork = 0, newWayNetwork = 0, existingCycleNetwork = 0, newCycleNetwork = 0;
		for (String t : existing.keySet()) {
			existingWayNetwork = Math.max(existingWayNetwork, networkWayOrder(t));
			existingCycleNetwork = Math.max(existingCycleNetwork, networkCycleOrder(t, existing.get(t)));
		}
		for (MapRulType t : propogated.keySet()) {
			newWayNetwork = Math.max(newWayNetwork, networkWayOrder(t.getTag()));
			newCycleNetwork = Math.max(newCycleNetwork, networkCycleOrder(t.getTag(), t.getValue()));
		}
		if (newWayNetwork > 0 && newWayNetwork < existingWayNetwork) {
			return true;
		}
		if (newCycleNetwork > 0 && newCycleNetwork < existingCycleNetwork) {
			return true;
		}
		return false;
	}
	//////// TODO BLOCK ////////
	
	private Map<String, String> processNameTags(Relation relation, MapRenderingTypesEncoder renderingTypes,
			Map<String, String> relationNameTags, Map<String, String> relationNames) {
		if (relationNames != null) {
			for (Entry<String, String> e : relationNames.entrySet()) {
				String sourceTag = e.getKey();
				String targetTag = e.getValue();
				String vl = relation.getTag(sourceTag);
				if (!Algorithms.isEmpty(vl)) {
					if (relationNameTags == null) {
						relationNameTags = new LinkedHashMap<String, String>();
					}
					renderingTypes.checkOrCreateTextRule(targetTag);
					relationNameTags.put(targetTag, vl);
				}
			}
		}
		return relationNameTags;
	}
	
	public void handleRelationPropogatedTags(Relation relation, MapRenderingTypesEncoder renderingTypes, OsmDbAccessorContext ctx, 
			EntityConvertApplyType at) throws SQLException {
		Map<String, String> relationNameTags = null;
		Map<String, String> relationPutTags = null;
		Map<String, String> relationGroupNameTags = null;
		
		Map<String, String> relationTags = relation.getTags();
		relationTags = renderingTypes.transformTags(relationTags, EntityType.RELATION, at);
		relationTags = renderingTypes.processExtraTags(relationTags);
		
		for (Entry<String, String> ev : relationTags.entrySet()) {
			String key = ev.getKey();
			String value = ev.getValue();
			MapRulType rule = renderingTypes.getRuleType(key, value, false, false);
			if (rule != null && rule.relation) {
				if (relationPutTags == null) {
					relationPutTags = new LinkedHashMap<String, String>();
				}
				if (rule.isAdditionalOrText()) {
					relationPutTags.put(key, value);
				} else {
					// for main tags propagate "route_hiking", "route_road", etc
					relationPutTags.put(key + "_" + value, "");
					relationNameTags = processNameTags(relation, renderingTypes, relationNameTags, rule.relationNames);
					relationGroupNameTags = processNameTags(relation, renderingTypes, relationGroupNameTags,
							rule.relationGroupNameTags);
				}
			}
		}
		if (!Algorithms.isEmpty(relationPutTags)) {
			if (ctx != null) {
				ctx.loadEntityRelation(relation);
			}
			for (RelationMember ids : relation.getMembers()) {
				Map<String, String> map = getPropogateTagForEntity(ids.getEntityId());
				// TODO 
//				if (skipPropagationOfDuplicate(relationNameTags, map)) {
//					continue;
//				}
				if(relationPutTags != null) {
					// here we could sort but this is exceptional case of overwriting
					map.putAll(relationPutTags);
//					for (Entry<String, String> es : relationAdditionalTags.entrySet()) {
//						String key = es.getKey();
//						String value = es.getValue();
//						map.put(key, value);
//					}
				}
				if (relationNameTags != null) {
					for (Entry<String, String> es : relationNameTags.entrySet()) {
						String key = es.getKey();
						String res = sortAndAttachUniqueValue(map.get(key), es.getValue());
						map.put(key, res);
					}
				}
				if (relationGroupNameTags != null) {
					int modifier = 0;
					boolean modifierExists = true;
					while (modifierExists && modifier++ < MAX_RELATION_GROUP) {
						modifierExists = false;
						for (Entry<String, String> es : relationGroupNameTags.entrySet()) {
							String key = es.getKey();
							String s = addModifier(key, modifier);
							if (map.containsKey(s)) {
								modifierExists = true;
								break;
							}
						}
					}
					if (!modifierExists) {
						for (Entry<String, String> es : relationGroupNameTags.entrySet()) {
							String key = es.getKey();
							String value = es.getValue();
							String newKey = addModifier(key, modifier);
							renderingTypes.checkOrCreateTextRule(newKey);
							map.put(newKey, value);
						}
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
