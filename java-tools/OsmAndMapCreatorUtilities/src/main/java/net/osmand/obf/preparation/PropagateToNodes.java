package net.osmand.obf.preparation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.data.LatLon;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.MapRenderingTypes.MapRulType.PropagateToNodesType;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;

public class PropagateToNodes {

    private MapRenderingTypesEncoder renderingTypes;
    private TLongObjectHashMap<List<PropagateFromWayToNode>> propagateTagsByNodeId = new TLongObjectHashMap<>();
    private TLongObjectHashMap<List<PropagateFromWayToNode>> propagateTagsByOsmNodeId = new TLongObjectHashMap<>();
    private Map<String, List<PropagateRule>> propagateRulesByTag = new HashMap<>();

    
    public static class PropagateWayWithNodes {
    	
    	PropagateFromWayToNode[] points;
    	boolean empty = true;
    	
		public PropagateWayWithNodes(int oldNodes) {
			points = new PropagateFromWayToNode[2 *oldNodes - 1];
		}
    }
    
    public static class PropagateRuleFromWayToNode {
    	public final PropagateFromWayToNode way;
    	public final PropagateRule rule;
		public final Map<String, String> tags = new HashMap<>();
		public boolean ignoreBorderPoint;
		
		public PropagateRuleFromWayToNode(PropagateFromWayToNode propagateFromWayToNode, PropagateRule rule) {
			this.way = propagateFromWayToNode;
			this.rule = rule;
		}
		
    }
    
	public static class PropagateFromWayToNode {
		public long id; // negative ids - artificial node
		public long osmId; // first point (main point)
		public long wayId;
		public int start;
		public int end;
		public List<PropagateRuleFromWayToNode> rls = new ArrayList<>();

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

		public void applyRule(PropagateRule rule) {
			PropagateRuleFromWayToNode rl = new PropagateRuleFromWayToNode(this, rule);
			String propagateTag = rule.tag;
			if (rule.tagPrefix != null) {
				propagateTag = rule.tagPrefix + propagateTag;
			}
			rl.tags.put(propagateTag, rule.value);
			if (rule.type == PropagateToNodesType.BORDER) {
				rl.ignoreBorderPoint = true;
			}
			this.rls.add(rl);

		}
	}

	public PropagateToNodes(MapRenderingTypesEncoder renderingTypes) {
		this.renderingTypes = renderingTypes;
		initPropagateToNodes();
	}
	
	public boolean isNoRegisteredNodes() {
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
			propagateTagsByOsmNodeId.put(node.osmId, lst);
		}
		lst.add(node);
	}
	
	public List<PropagateFromWayToNode> getPropagateByEndpoint(long nodeId) {
		return propagateTagsByOsmNodeId.get(nodeId);
	}

	public List<PropagateFromWayToNode> getPropagateByNodeId(long nodeId) {
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

	public PropagateFromWayToNode getNode(PropagateWayWithNodes rWay, Way way, int start, int end) {
		if (rWay.points[start + end] == null) {
			rWay.points[start + end] = new PropagateFromWayToNode(way, start, end);
		}
		rWay.empty = false;
		return rWay.points[start + end];
	}

	public PropagateWayWithNodes propagateTagsFromWays(Way w) {
		List<PropagateRule> rulesToApply = getRulesToApply(w);
		if (rulesToApply == null) {
			return null;
		}
		TLongArrayList allIds = w.getNodeIds();
		if (allIds.size() == 0) {
			return null;
		}

		PropagateWayWithNodes resultWay = new PropagateWayWithNodes(allIds.size());
		for (PropagateRule rule : rulesToApply) {
			switch (rule.type) {
			case ALL:
				for (int i = 0; i < w.getNodes().size(); i++) {
					getNode(resultWay, w, i, i).applyRule(rule);
				}
				break;
			case START:
				getNode(resultWay, w, 0, 1).applyRule(rule);
				break;
			case END:
				getNode(resultWay, w, allIds.size() - 2, allIds.size() - 1).applyRule(rule);
				break;
			case CENTER:
				if (allIds.size() == 2) {
					getNode(resultWay, w, 0, 1).applyRule(rule);
				} else {
					getNode(resultWay, w, allIds.size() / 2, allIds.size() / 2).applyRule(rule);
				}
				break;
			case BORDER:
				getNode(resultWay, w, 0, 1).applyRule(rule);
				getNode(resultWay, w, allIds.size() - 1, allIds.size() - 2).applyRule(rule);
				break;
			case NONE:
				break;
			}
		}
		return resultWay;
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
					boolean propIf = rule.applicable(w);
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
			// TODO doesn't work correctly with border points (can't ignore them)
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
		for (PropagateFromWayToNode ways : list) {
			for (PropagateRuleFromWayToNode w : ways.rls) {
				for (Map.Entry<String, String> entry : w.tags.entrySet()) {
					if (n.getTag(entry.getKey()) == null && entry.getValue() != null) {
						n.putTag(entry.getKey(), entry.getValue());
					}
				}
			}
		}
	}

	public static class PropagateRule {
		public String tag;
		public String value;
		public final PropagateToNodesType type;
		public String tagPrefix;
		public Map<String, String> propIf;

		public PropagateRule(PropagateToNodesType type, String tagPrefix, Map<String, String> propIf) {
			this.type = type;
			this.tagPrefix = tagPrefix;
			this.propIf = propIf;
		}

		
		public boolean applicable(Way w) {
			boolean res = true;
			if (propIf != null) {
				res = false;
				for (Map.Entry<String, String> entry : propIf.entrySet()) {
					String tagValue = w.getTag(entry.getKey());
					if (tagValue != null) {
						if (entry.getValue() == null) {
							res = true;
						} else {
							String[] allValues = entry.getValue().split(",");
							boolean allNegs = true;
							for (String v : allValues) {
								if (v.startsWith("~")) {
									if (v.equals("~" + tagValue)) {
										return false;
									}
								} else {
									allNegs = false;
									if (v.equals(tagValue)) {
										res = true;
									}
								}
							}
							if (allNegs) {
								res = true;
							}
						}
					}
				}
			}
			return res;
		}
	}

}
