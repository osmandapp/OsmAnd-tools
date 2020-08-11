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
	
	private static class RelationRulePropagation {
		String relationGroupKeyString;
		String relationGroupValueString;
		
		Map<String, String> relationNameTags = new LinkedHashMap<String, String>();
		Map<String, String> relationAdditionalTags = new LinkedHashMap<String, String>();
		List<PropagateTagGroup> relationGroups = new ArrayList<>();
		@Override
		public String toString() {
			return String.format("%s - %s: %s %s %s", relationGroupKeyString, relationGroupValueString,
					relationNameTags, relationAdditionalTags, relationGroups);
		}
	}
	
	public static class PropagateTagGroup {
		public Map<String, String> tags = new LinkedHashMap<String, String>();
		public String orderValue = "";
		public String groupKey = "";
		
		@Override
		public String toString() {
			return groupKey + " " + orderValue + " - " + tags;
		}
	}
	
	public static class PropagateEntityTags {
		public Map<String, String> putThroughTags = new LinkedHashMap<String, String>();
		public Map<String, List<PropagateTagGroup>> relationGroupTags = new LinkedHashMap<String, List<PropagateTagGroup>>();
		
		@Override
		public String toString() {
			return putThroughTags + " " + relationGroupTags;
		}
	}
	
	private Map<String, String> processNameTags(Map<String, String> relationTags, MapRenderingTypesEncoder renderingTypes,
			Map<String, String> relationNameTags, Map<String, String> relationNames, EntityConvertApplyType at) {
		if (relationNames != null) {
			for (Entry<String, String> e : relationNames.entrySet()) {
				String sourceTag = e.getKey();
				String targetTag = e.getValue();
				String vl = relationTags.get(sourceTag);
				if (!Algorithms.isEmpty(vl)) {
					MapRulType rt = renderingTypes.getRuleType(sourceTag, null, at);
					if (rt != null) {
						renderingTypes.checkOrCreateTextRule(targetTag, rt);
					}
					relationNameTags.put(targetTag, vl);
				}
			}
		}
		return relationNameTags;
	}
	
	private List<RelationRulePropagation> processRelationTags(MapRenderingTypesEncoder renderingTypes, Map<String, String> relationTags, EntityConvertApplyType at) {
		List<RelationRulePropagation> res = null;
		for (Entry<String, String> ev : relationTags.entrySet()) {
			String key = ev.getKey();
			String value = ev.getValue();
			MapRulType rule = renderingTypes.getRuleType(key, value, false, false);
			if (rule != null && (rule.relation || rule.relationGroup)) {
				RelationRulePropagation rrp = new RelationRulePropagation();
				if(res == null) {
					res = new ArrayList<RelationTagsPropagation.RelationRulePropagation>();
				}
				res.add(rrp);
				if (rule.relation) {
					rrp.relationAdditionalTags.put(key, value);
					rrp.relationGroupKeyString = key;
					rrp.relationGroupValueString = value;
					processNameTags(relationTags, renderingTypes, rrp.relationNameTags, rule.relationNames, at);
				} else if (rule.relationGroup) {
					// for main tags propagate "route_hiking", "route_road", etc
					String mainTag = Algorithms.isEmpty(value) ? key : key + "_" + value;
					rrp.relationAdditionalTags.put(mainTag, "");
					rrp.relationGroupKeyString = mainTag;
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
					rrp.relationGroupValueString = sortValue;

					processNameTags(relationTags, renderingTypes, rrp.relationNameTags, rule.relationNames, at);
					if (rule.additionalTags != null) {
						for (Entry<String, String> atag : rule.additionalTags.entrySet()) {
							String tvalue = relationTags.get(atag.getKey());
							if (!Algorithms.isEmpty(tvalue)) {
								MapRulType rt = renderingTypes.getRuleType(atag.getKey(), tvalue, at);
								if (rt != null) {
									renderingTypes.checkOrCreateAdditional(atag.getValue(), tvalue, rt);
									rrp.relationAdditionalTags.put(atag.getValue(), tvalue);
								}
							}
						}
					}
					// handle relation groups
					if (rule.relationGroupAdditionalTags != null || rule.relationGroupNameTags != null) {
						PropagateTagGroup gr = new PropagateTagGroup();
						gr.orderValue = sortValue;
						gr.groupKey = Algorithms.isEmpty(rule.relationGroupPrefix) ? mainTag : rule.relationGroupPrefix;
						processNameTags(relationTags, renderingTypes, gr.tags, rule.relationGroupNameTags, at);
						if (rule.relationGroupAdditionalTags != null) {
							for (Entry<String, String> atag : rule.relationGroupAdditionalTags.entrySet()) {
								String tvalue = relationTags.get(atag.getKey());
								if (!Algorithms.isEmpty(tvalue)) {
									gr.tags.put(atag.getValue(), tvalue);
								}
							}
						}
						if (!gr.tags.isEmpty()) {
							rrp.relationGroups.add(gr);
						}
					}
				}
			}
		}
		return res;
	}
	
	public void handleRelationPropogatedTags(Relation relation, MapRenderingTypesEncoder renderingTypes, OsmDbAccessorContext ctx, 
			EntityConvertApplyType at) throws SQLException {
		Map<String, String> relationTags = relation.getTags();
		relationTags = renderingTypes.transformTags(relationTags, EntityType.RELATION, at);
		List<RelationRulePropagation> lst = processRelationTags(renderingTypes, relationTags, at);
		if (lst != null) {
			if (ctx != null) {
				ctx.loadEntityRelation(relation);
			}
			for (RelationMember ids : relation.getMembers()) {
				PropagateEntityTags entityTags = getPropogateTagForEntity(ids.getEntityId());
				for (RelationRulePropagation p : lst) {
					String sortKey = RELATION_SORT_TAG + p.relationGroupKeyString;
					if (!entityTags.putThroughTags.containsKey(sortKey)
							|| p.relationGroupValueString.compareTo(entityTags.putThroughTags.get(sortKey)) < 0) {
						entityTags.putThroughTags.put(sortKey, p.relationGroupValueString);
						entityTags.putThroughTags.putAll(p.relationAdditionalTags);

					}
					if (p.relationNameTags.size() > 0) {
						for (Entry<String, String> es : p.relationNameTags.entrySet()) {
							String key = es.getKey();
							String oldValue = entityTags.putThroughTags.get(key);
							String res = sortAndAttachUniqueValue(oldValue, es.getValue());
							entityTags.putThroughTags.put(key, res);
						}
					}
					for (PropagateTagGroup g : p.relationGroups) {
						if (!entityTags.relationGroupTags.containsKey(g.groupKey)) {
							entityTags.relationGroupTags.put(g.groupKey,
									new ArrayList<RelationTagsPropagation.PropagateTagGroup>());
						}
						entityTags.relationGroupTags.get(g.groupKey).add(g);
					}
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
					renderingTypes.checkOrCreateAdditional(groupPart, "", null);
					e.putTag(groupPart, "");
					for (Entry<String, String> te : g.tags.entrySet()) {
						String targetTag = groupPart+ "_" + te.getKey();
						String targetValue = te.getValue();
						MapRulType rt = renderingTypes.getRuleType(te.getKey(), targetValue, false, false);
						if (rt == null) {
							// ignore non existing
						} else if(rt.isAdditional()) {
							renderingTypes.checkOrCreateAdditional(targetTag, targetValue, rt);
						} else {
							renderingTypes.checkOrCreateTextRule(targetTag, rt);
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
