package net.osmand.osm;

import java.sql.SQLException;
import java.util.ArrayList;
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
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.util.Algorithms;

public class RelationTagsPropagation {
	final static String SPLIT_VALUE= "SPLITVL";
	private static final String RELATION_SORT_TAG = "relation_sort:";
	private Map<EntityId, PropagateEntityTags> propogatedTags = new LinkedHashMap<Entity.EntityId, PropagateEntityTags>();
	
	private static class RenderingSortGroup {
		String relationGroupKeyString;
		String relationGroupValueString;
	}
	
	public static class PropagateTagGroup {
		public Map<String, String> tags = new LinkedHashMap<String, String>();
		public String orderValue = "";
		public String groupKey = "";
	}
	
	public static class PropagateEntityTags {
		public Map<String, String> putThroughTags = new LinkedHashMap<String, String>();
		public Map<String, List<PropagateTagGroup>> relationGroupTags = new LinkedHashMap<String, List<PropagateTagGroup>>();
		
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
	
	private void processRelationTags(MapRenderingTypesEncoder renderingTypes, Map<String, String> propagateRelationNameTags,
			Map<String, String> propagateRelationAdditionalTags, List<PropagateTagGroup>  propagateRelationGroups,
			Map<String, String> relationTags, RenderingSortGroup g) {
		for (Entry<String, String> ev : relationTags.entrySet()) {
			String key = ev.getKey();
			String value = ev.getValue();
			MapRulType rule = renderingTypes.getRuleType(key, value, false, false);
			if (rule != null && rule.relation) {
				if (rule.isAdditionalOrText()) {
					propagateRelationAdditionalTags.put(key, value);
					g.relationGroupKeyString = key;
					g.relationGroupValueString = value;
				} else {
					// for main tags propagate "route_hiking", "route_road", etc
					String mainTag = key + "_" + value;
					propagateRelationAdditionalTags.put(mainTag, "");
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
					
					processNameTags(relationTags, renderingTypes, propagateRelationNameTags, rule.relationNames);
					if (rule.additionalTags != null) {
						for (Entry<String, String> atag : rule.additionalTags.entrySet()) {
							// TODO create additional or not
							String tvalue = relationTags.get(atag.getKey());
							if (!Algorithms.isEmpty(tvalue)) {
								renderingTypes.checkOrCreateAdditional(atag.getValue(), tvalue);
								propagateRelationAdditionalTags.put(atag.getValue(), tvalue);
							}
						}
					}
					// handle relation groups
					if (rule.relationGroupAdditionalTags != null || rule.relationGroupNameTags != null) {
						PropagateTagGroup gr = new PropagateTagGroup();
						gr.orderValue = sortValue;
						gr.groupKey = Algorithms.isEmpty(rule.relationGroupPrefix) ? mainTag : rule.relationGroupPrefix;
						processNameTags(relationTags, renderingTypes, gr.tags, rule.relationGroupNameTags);
						if (rule.relationGroupAdditionalTags != null) {
							for (Entry<String, String> atag : rule.relationGroupAdditionalTags.entrySet()) {
								String tvalue = relationTags.get(atag.getKey());
								if (!Algorithms.isEmpty(tvalue)) {
									renderingTypes.checkOrCreateAdditional(atag.getValue(), tvalue);
									gr.tags.put(atag.getValue(), tvalue);
								}
							}
						}
						if (!gr.tags.isEmpty()) {
							propagateRelationGroups.add(gr);
						}
					}
				}
			}
		}
	}
	
	public void handleRelationPropogatedTags(Relation relation, MapRenderingTypesEncoder renderingTypes, OsmDbAccessorContext ctx, 
			EntityConvertApplyType at) throws SQLException {
		Map<String, String> propagateRelationNameTags = new LinkedHashMap<String, String>();
		Map<String, String> propagateRelationAdditionalTags = new LinkedHashMap<String, String>();
		List<PropagateTagGroup> propagateRelationGroups = new ArrayList<>();
		
		Map<String, String> relationTags = relation.getTags();
		relationTags = renderingTypes.transformTags(relationTags, EntityType.RELATION, at);
		RenderingSortGroup rsg = new RenderingSortGroup();
		processRelationTags(renderingTypes, 
				propagateRelationNameTags, propagateRelationAdditionalTags, propagateRelationGroups, relationTags, rsg);
		if (!Algorithms.isEmpty(propagateRelationAdditionalTags)) {
			if (ctx != null) {
				ctx.loadEntityRelation(relation);
			}
			for (RelationMember ids : relation.getMembers()) {
				PropagateEntityTags entityTags = getPropogateTagForEntity(ids.getEntityId());
				String sortKey = RELATION_SORT_TAG + rsg.relationGroupKeyString;
				if (!entityTags.putThroughTags.containsKey(sortKey) || 
						rsg.relationGroupValueString.compareTo(entityTags.putThroughTags.get(sortKey)) > 0) {
//					System.out.println(String.format("CC %s: %s %s - %d",rsg.relationGroupKeyString,
//							rsg.relationGroupValueString, vl,
//							rsg.relationGroupValueString.compareTo(vl)));
					entityTags.putThroughTags.put(sortKey, rsg.relationGroupValueString);
					entityTags.putThroughTags.putAll(propagateRelationAdditionalTags);
					
				}
				if (propagateRelationNameTags.size() > 0) {
					for (Entry<String, String> es : propagateRelationNameTags.entrySet()) {
						String key = es.getKey();
						String oldValue = entityTags.putThroughTags.get(key);
						String res = sortAndAttachUniqueValue(oldValue, es.getValue());
						entityTags.putThroughTags.put(key, res);
					}
				}
				for(PropagateTagGroup g : propagateRelationGroups) {
					if(!entityTags.relationGroupTags.containsKey(g.groupKey)) {
						entityTags.relationGroupTags.put(g.groupKey, new ArrayList<RelationTagsPropagation.PropagateTagGroup>());
					}
					entityTags.relationGroupTags.get(g.groupKey).add(g);
				}
			}
		}
	}

	public PropagateEntityTags getPropogateTagForEntity(EntityId entityId) {
		if (!propogatedTags.containsKey(entityId)) {
			propogatedTags.put(entityId, new PropagateEntityTags());
		}
		return propogatedTags.get(entityId);
	}
	
	
	
	public void addPropogatedTags(MapRenderingTypesEncoder renderingTypes, EntityConvertApplyType tp, Entity e) {
		EntityId eid = EntityId.valueOf(e);
		PropagateEntityTags proptags = propogatedTags.get(eid);
		if (proptags != null) {
			Iterator<Entry<String, String>> iterator = proptags.putThroughTags.entrySet().iterator();
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
			for (List<PropagateTagGroup> groups : proptags.relationGroupTags.values()) {
				groups.sort(new Comparator<PropagateTagGroup>() {

					@Override
					public int compare(PropagateTagGroup o1, PropagateTagGroup o2) {
						return o1.orderValue.compareTo(o2.orderValue);
					}
				});
				int mod = 1;
				for (PropagateTagGroup g : groups) {
					String groupPart = g.groupKey + "_" + mod ;
					renderingTypes.checkOrCreateAdditional(groupPart, "");
					e.putTag(groupPart, "");
					for (Entry<String, String> te : g.tags.entrySet()) {
						String targetTag = groupPart+ "_" + te.getKey();
						String targetValue = te.getValue();
						// TODO register additional according to main rule tag
						MapRulType rt = renderingTypes.getRuleType(te.getKey(), targetValue, false, false);
						if (rt == null) {
							// ignore non existing
						} else if(rt.isAdditional()) {
							renderingTypes.checkOrCreateAdditional(targetTag, targetValue);
						} else {
							renderingTypes.checkOrCreateTextRule(targetTag);
						}
						e.putTag(targetTag, targetValue);
					}
					mod++;
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
