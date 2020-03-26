package net.osmand.obf.preparation;

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

import net.osmand.osm.MapRenderingTypes.MapRulType;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.util.Algorithms;

public class TagsTransformer {
	Map<EntityId, Map<String, String>> propogatedTags = new LinkedHashMap<Entity.EntityId, Map<String, String>>();
	final static String SPLIT_VALUE= "SPLITVL";
	private static final List<String> NODE_NETWORK_IDS = Arrays.asList("network:type", "expected_rcn_route_relations");
	private static final List<String> NODE_NETWORKS_REF_TYPES = Arrays.asList("icn_ref", "ncn_ref", "rcn_ref", "lcn_ref", "iwn_ref", "nwn_ref", "rwn_ref", "lwn_ref");
	private static final String multipleNodeNetworksKey = "multiple_node_networks";
	
	public void handleRelationPropogatedTags(Relation e, MapRenderingTypesEncoder renderingTypes, OsmDbAccessorContext ctx, 
			EntityConvertApplyType at) throws SQLException {

		Map<MapRulType, Map<MapRulType, String>> propogated =
				renderingTypes.getRelationPropogatedTags((Relation)e, at);
		if(propogated != null && propogated.size() > 0) {
			if(ctx != null) {
				ctx.loadEntityRelation((Relation) e);
			}
			for(RelationMember ids : ((Relation) e).getMembers()) {
				Map<String, String> map = getPropogateTagForEntity(ids);
				if (skipPropagationOfDuplicate(propogated, map)) {
					continue;
				}
				Iterator<Entry<MapRulType, Map<MapRulType, String>>> itMain = propogated.entrySet().iterator();
				while (itMain.hasNext()) {
					Entry<MapRulType, Map<MapRulType, String>> ev = itMain.next();
					Map<MapRulType, String> pr = ev.getValue();
					MapRulType propagateRule = ev.getKey();
					if (propagateRule.isRelationGroup()) {
						Iterator<Entry<MapRulType, String>> it = pr.entrySet().iterator();
						int modifier = 1;
						String s = propagateRule.getTag() + "__" + propagateRule.getValue() + "_";
						while (map.containsKey(s + modifier)) {
							modifier++;
						}
						map.put(s + modifier, s);
						while (it.hasNext()) {
							Entry<MapRulType, String> es = it.next();
							String key = es.getKey().getTag();
							map.put(key + "_" + modifier, es.getValue());
						}
					} else {
						Iterator<Entry<MapRulType, String>> it = pr.entrySet().iterator();
						while (it.hasNext()) {
							Entry<MapRulType, String> es = it.next();
							String key = es.getKey().getTag();
							if (es.getKey().isText() && map.containsKey(key)) {
								String res = sortAndAttachUniqueValue(map.get(key), es.getValue());
								map.put(key, res);
							} else {
								map.put(key, es.getValue());
							}
						}
					}
				}
			}
		}
	}
	
	private int networkWayOrder(String tag) {
		switch(tag) {
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
	
	private int networkCycleOrder(String tag) {
		switch(tag) {
		case "network_icn":
			return 4;
		case "network_ncn":
			return 3;
		case "network_rcn":
			return 2;
		case "network_lcn":
			return 1;
		default:
			return 0;
		}
	}

	private boolean skipPropagationOfDuplicate(Map<MapRulType, Map<MapRulType, String>> propogated,
			Map<String, String> existing) {
		int existingWayNetwork = 0, newWayNetwork = 0, existingCycleNetwork = 0, newCycleNetwork = 0;
		for (String t : existing.keySet()) {
			existingWayNetwork = Math.max(existingWayNetwork, networkWayOrder(t));
			existingCycleNetwork = Math.max(existingWayNetwork, networkCycleOrder(t));
		}
		for (MapRulType t : propogated.keySet()) {
			newWayNetwork = Math.max(newWayNetwork, networkWayOrder(t.getTag()));
			newCycleNetwork = Math.max(newCycleNetwork, networkCycleOrder(t.getTag()));
		}
		if (newWayNetwork > 0 && newWayNetwork < existingWayNetwork) {
			return true;
		}
		if (newCycleNetwork > 0 && newCycleNetwork < existingCycleNetwork) {
			return true;
		}
		return false;
	}

	Map<String, String> getPropogateTagForEntity(RelationMember id) {
		if(!propogatedTags.containsKey(id.getEntityId())) {
			propogatedTags.put(id.getEntityId(), new LinkedHashMap<String, String>());
		}
		Map<String, String> map = propogatedTags.get(id.getEntityId());
		return map;
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
	
	public void addPropogatedTags(Entity e) {
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
	
	public void registerPropogatedTag(EntityId id, String t, String value ) {
		if (!propogatedTags.containsKey(id)) {
			propogatedTags.put(id, new LinkedHashMap<String, String>());
		}
		propogatedTags.get(id).put(t, value);
	}
	
	
	private static String sortAndAttachUniqueValue(String list, String value) {
		String[] ls = list.split(SPLIT_VALUE);
		Set<String> set = new TreeSet<String>(new Comparator<String>() {

			@Override
			public int compare(String s1, String s2) {
				int i1 = Algorithms.extractFirstIntegerNumber(s1);
				int i2 = Algorithms.extractFirstIntegerNumber(s2);
				if(i1 == i2) {
					String t1 = Algorithms.extractIntegerSuffix(s1);
					String t2 = Algorithms.extractIntegerSuffix(s2);
					return t1.compareTo(t2);
				}
				return i1 - i2;
			}
		});
		set.add(value);
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
