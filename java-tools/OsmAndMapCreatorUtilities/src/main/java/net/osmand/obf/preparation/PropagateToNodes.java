package net.osmand.obf.preparation;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.data.LatLon;
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
    private TLongObjectHashMap<List<PropagateFromWayToNode>> propagateTagsByNodeId = new TLongObjectHashMap<>();
    private TLongObjectHashMap<List<PropagateFromWayToNode>> propagateTagsByOsmNodeId = new TLongObjectHashMap<>();
    private Map<String, List<PropagateRule>> propagateRulesByTag = new HashMap<>();

	public static class PropagateFromWayToNode {
		public long id; // negative ids - artificial node
		public long osmId; // first point (main point)
		public Map<String, String> tags = new HashMap<>();
		public long wayId;
		public PropagateToNodesType type;
		public int start;
		public int end;
		public boolean ignoreBorderPoint;

		public PropagateFromWayToNode(Way way, int start, int end) {
			this.end = end;
			this.start = start;
			wayId = way.getId();
			osmId = way.getNodeIds().get(start);
			if (start == end) {
				this.id = osmId;
			} else {
				this.id = -1;
			}
		}
		
		public LatLon getLatLon(Node st, Node en) {
			if (st == null || en == null) {
				return null;
			}
			return new LatLon(st.getLatitude() / 2 + en.getLatitude() / 2,
					st.getLongitude() / 2 + en.getLongitude() / 2);
		}

		public void addTag(String tag, String value, PropagateToNodesType type) {
			tags.put(tag, value);
			this.type = type;
		}
	}

	public PropagateToNodes(MapRenderingTypesEncoder renderingTypes) {
		this.renderingTypes = renderingTypes;
		initPropagateToNodes();
	}
	
	public boolean isEmpty() {
		return propagateTagsByNodeId.isEmpty();
	}

	public void registerNode(PropagateFromWayToNode node) {
		List<PropagateFromWayToNode> lst = propagateTagsByNodeId.get(node.id);
		if (lst == null) {
			lst = new ArrayList<PropagateToNodes.PropagateFromWayToNode>();
			propagateTagsByNodeId.put(node.id, lst);
		}
		lst.add(node);

		lst = propagateTagsByOsmNodeId.get(node.osmId);
		if (lst == null) {
			lst = new ArrayList<PropagateToNodes.PropagateFromWayToNode>();
			propagateTagsByOsmNodeId.put(node.id, lst);
		}
		lst.add(node);
	}

	public List<PropagateFromWayToNode> getLinkedPropagate(long nodeId, boolean osmId) {
		if (osmId) {
			return propagateTagsByOsmNodeId.get(nodeId);
		}
		return propagateTagsByNodeId.get(nodeId);
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
				if (!propagateRulesByTag.containsKey(rule.tag)) {
					propagateRulesByTag.put(rule.tag, new ArrayList<PropagateToNodes.PropagateRule>());
				}
				propagateRulesByTag.get(rule.tag).add(rule);
			}
		}
	}

	public PropagateFromWayToNode getNode(List<PropagateFromWayToNode> resultIds, Way way, int start, int end) {
		for (PropagateFromWayToNode w : resultIds) {
			if (w.start == start && w.end == end) {
				return w;
			}
		}
		PropagateFromWayToNode pwn = new PropagateFromWayToNode(way, start, end);
		resultIds.add(pwn);
		return pwn;
	}

	public List<PropagateFromWayToNode> propagateTagsFromWays(Way w) {
		List<PropagateRule> rulesToApply = getRulesToApply(w);
		if (rulesToApply == null) {
			return null;
		}
		TLongArrayList allIds = w.getNodeIds();
		if (allIds.size() == 0) {
			return null;
		}

		List<PropagateFromWayToNode> resultIds = new ArrayList<>();
		for (PropagateRule rule : rulesToApply) {
			String propagateTag = rule.tag;
			if (rule.tagPrefix != null) {
				propagateTag = rule.tagPrefix + propagateTag;
			}
			switch (rule.type) {
			case ALL:
				for (int i = 0; i < w.getNodes().size(); i++) {
					getNode(resultIds, w, i, i).addTag(propagateTag, rule.value, rule.type);
				}
				break;
			case START:
				getNode(resultIds, w, 0, 1).addTag(propagateTag, rule.value, rule.type);
				break;
			case END:
				getNode(resultIds, w, allIds.size() - 2, allIds.size() - 1).addTag(propagateTag, rule.value, rule.type);
				break;
			case CENTER:
				if (allIds.size() == 2) {
					getNode(resultIds, w, 0, 1).addTag(propagateTag, rule.value, rule.type);
				} else {
					getNode(resultIds, w, allIds.size() / 2, allIds.size() / 2).addTag(propagateTag, rule.value,
							rule.type);
				}
				break;
			case BORDER:
				getNode(resultIds, w, 0, 1).addTag(propagateTag, rule.value, rule.type);
				getNode(resultIds, w, allIds.size() - 1, allIds.size() - 2).addTag(propagateTag, rule.value, rule.type);
				break;
			case NONE:
				break;
			}
		}
		return resultIds;
	}

	private List<PropagateRule> getRulesToApply(Way w) {
		List<PropagateRule> rulesToApply = null;
		for (String tag : w.getTagKeySet()) {
			List<PropagateRule> list = propagateRulesByTag.get(tag);
			if (list == null) {
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
		if (propagateTagsByNodeId.isEmpty()) {
			return;
		}
		List<Node> nodes = w.getNodes();
		for (Node n : nodes) {
			propagateTagsToNode(n);
		}
	}

	public void propagateTagsToNode(Node n) {
		if (propagateTagsByNodeId.isEmpty()) {
			return;
		}
		long nodeId = n.getId();
		List<PropagateFromWayToNode> list = propagateTagsByNodeId.get(nodeId);
		if (list == null) {
			return;
		}
		for (PropagateFromWayToNode l : list) {
			for (Map.Entry<String, String> entry : l.tags.entrySet()) {
				if (n.getTag(entry.getKey()) == null && entry.getValue() != null) {
					n.putTag(entry.getKey(), entry.getValue());
				}
			}
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
