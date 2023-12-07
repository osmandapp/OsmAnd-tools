package net.osmand.obf.preparation;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRenderingTypes.MapRulType.PropagateToNodesType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PropagateToNodes {

    private MapRenderingTypesEncoder renderingTypes;
    private TLongObjectHashMap<Map<String, String>> nodePropagatedTags = new TLongObjectHashMap<>();
    private Map<String, List<PropagateRule>> propagateRulesByTag = new HashMap<>();

    public PropagateToNodes(MapRenderingTypesEncoder renderingTypes) {
        this.renderingTypes = renderingTypes;
        initPropagateToNodes();
    }


    private void initPropagateToNodes() {
        Map<String, MapRenderingTypes.MapRulType> ruleTypes = renderingTypes.getEncodingRuleTypes();
        for (Map.Entry<String, MapRenderingTypes.MapRulType> entry : ruleTypes.entrySet()) {
            MapRenderingTypes.MapRulType ruleType = entry.getValue();
            if (ruleType.isPropagateToNodes()) {
                PropagateToNodesType type = ruleType.getPropagateToNodesType();
                String prefix = ruleType.getPropagateToNodesPrefix();
                Map<String, String> propIf = ruleType.getPropagateIf();
                PropagateRule rule = new PropagateRule(type, prefix, propIf);
                String[] split = entry.getKey().split("/");
    			rule.tag = split[0];
    			rule.value = split[1];
    			if(propagateRulesByTag.containsKey(rule.tag)) {
    				propagateRulesByTag.put(rule.tag, new ArrayList<PropagateToNodes.PropagateRule>());
    			}
                propagateRulesByTag.get(rule.tag).add(rule);
            }
        }
    }

	public TLongArrayList propagateTagsFromWays(Way w) {
		List<PropagateRule> rulesToApply = getRulesToApply(w);
		if (rulesToApply == null) {
			return null;
		}
		TLongArrayList allIds = w.getNodeIds();
		if (allIds.size() == 0) {
			return null;
		}
		TLongArrayList resultIds = new TLongArrayList();
		for (PropagateRule rule : rulesToApply) {
			String propagateTag = rule.tag;
			if (rule.tagPrefix != null) {
				propagateTag = rule.tagPrefix + propagateTag;
			}
			TLongArrayList ids = new TLongArrayList();
			switch (rule.type) {
			case ALL:
				ids.addAll(allIds);
				break;
			case START:
				ids.add(allIds.get(0));
				break;
			case END:
				ids.add(allIds.get(allIds.size() - 1));
				break;
			case CENTER:
				ids.add(allIds.get(allIds.size() / 2));
				break;
			case BORDER:
				long start = allIds.get(0);
				long end = allIds.get(allIds.size() - 1);
				if (start != end) {
					ids.add(start);
					ids.add(end);
				}
				for (int i = 1; i < allIds.size() - 1; i++) {
					Map<String, String> tags = nodePropagatedTags.get(allIds.get(i));
					if (tags != null && tags.containsKey(propagateTag)) {
						tags.put(propagateTag, null);
					}
				}
				break;
			}
			resultIds.addAll(ids);
			for (long id : ids.toArray()) {
				setPropagation(id, propagateTag, rule.value, rule.type);
			}
		}
		return resultIds;
	}


	private List<PropagateRule> getRulesToApply(Way w) {
		List<PropagateRule> rulesToApply = null;
		for (String tag : w.getTagKeySet()) {
			List<PropagateRule> list = propagateRulesByTag.get(tag);
			if(list == null) {
				continue;
			}
			for (PropagateRule rule : list) {
				String entityTag = w.getTag(tag);
				if (entityTag != null && entityTag.equals(rule.value)) {
					boolean propIf = true;
					if (rule.propIf != null) {
						propIf = false;
						for (Map.Entry<String, String> entry : rule.propIf.entrySet()) {
							String ifTag = w.getTag(entry.getKey());
							if (ifTag != null) {
								if (entry.getValue() == null || ifTag.equals(entry.getValue())) {
									propIf = true;
									break;
								}
							}
						}
					}
					if (propIf) {
						if (rulesToApply == null) {
							rulesToApply = new ArrayList<>();

						}
						if (!rulesToApply.contains(rule)) {
							rulesToApply.add(rule);
						}
					}
				}
			}
		}
		return rulesToApply;
	}

    public void propagateTagsToWayNodes(Way w) {
        if (nodePropagatedTags.isEmpty()) {
            return;
        }
        List<Node> nodes = w.getNodes();
        for (Node n : nodes) {
            propagateTagsToNode(n);
        }
    }

	public void propagateTagsToNode(Node n) {
		if (nodePropagatedTags.isEmpty()) {
            return;
        }
		long nodeId = n.getId();
		Map<String, String> tags = nodePropagatedTags.get(nodeId);
		for (Map.Entry<String, String> entry : tags.entrySet()) {
			if (n.getTag(entry.getKey()) == null && entry.getValue() != null) {
				n.putTag(entry.getKey(), entry.getValue());
			}
		}
	}

    private void setPropagation(long id, String propagateTag, String propagateValue, PropagateToNodesType type) {
        Map<String, String> tags = nodePropagatedTags.get(id);
        if (tags == null) {
            tags = new HashMap<>();
            nodePropagatedTags.put(id, tags);
        } 
        if (type == PropagateToNodesType.BORDER && tags.containsKey(propagateTag)) {
            tags.put(propagateTag, null); // 2nd iteration null
        } else {
        	tags.put(propagateTag, propagateValue);
        }
    }
    
	private static class PropagateRule {
		public String tag;
		String value;
		PropagateToNodesType type;
		String tagPrefix;
		Map<String, String> propIf;

		public PropagateRule(PropagateToNodesType type, String tagPrefix, Map<String, String> propIf) {
			this.type = type;
			this.tagPrefix = tagPrefix;
			this.propIf = propIf;
		}
	}

}
