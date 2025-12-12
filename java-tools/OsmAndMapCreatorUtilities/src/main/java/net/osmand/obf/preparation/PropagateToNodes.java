package net.osmand.obf.preparation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.data.LatLon;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.MapRenderingTypes.MapRulType;
import net.osmand.osm.MapRenderingTypes.PropagateToNode;
import net.osmand.osm.MapRenderingTypes.PropagateToNodesType;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.PoiType;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;

public class PropagateToNodes {

    private MapRenderingTypesEncoder renderingTypes;
    private TLongObjectHashMap<List<PropagateFromWayToNode>> propagateTagsByNodeId = new TLongObjectHashMap<>();
    private TLongObjectHashMap<List<PropagateRuleFromWayToNode>> propagateTagsByOsmNodeId = new TLongObjectHashMap<>();
    private Map<String, List<PropagateRule>> propagateRulesByTag = new HashMap<>();
    private Map<PropagateRule, Map<String, String>> convertedPropagatedTags = new HashMap<>();

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
		public long osmId; // connected point

		public PropagateRuleFromWayToNode(PropagateFromWayToNode propagateFromWayToNode, PropagateRule rule) {
			this.way = propagateFromWayToNode;
			this.rule = rule;
		}

    }

	public static class PropagateFromWayToNode {
		public long id; // negative ids - artificial node
		public long wayId;
		public int start;
		public long startId;
		public long endId;
		public int end;
		public List<PropagateRuleFromWayToNode> rls = new ArrayList<>();

		public PropagateFromWayToNode(Way way, int start, int end) {
			this.end = end;
			this.start = start;
			wayId = way.getId();
			this.startId = way.getNodeIds().get(start);
			this.endId = way.getNodeIds().get(end);
			if (start == end) {
				this.id = way.getNodeIds().get(start);
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

		public void applyRule(PropagateRule rule, Map<String, String> parentTags) {
			applyRule(rule, parentTags, false);
		}

		public void applyRule(PropagateRule rule, Map<String, String> parentTags, boolean end) {
			PropagateRuleFromWayToNode rl = new PropagateRuleFromWayToNode(this, rule);
			rl.osmId = end ? endId : startId;
			if (parentTags != null && rule.getPropAlso() != null) {
				Iterator<Entry<String, String>> it = parentTags.entrySet().iterator();
				while (it.hasNext()) {
					Entry<String, String> wTag = it.next();
					if (rule.getPropAlso().contains(wTag.getKey())) {
						rl.tags.put(wTag.getKey(), wTag.getValue());
					}
				}
			}
			rl.tags.put(rule.getPropagateTag(), rule.getPropagateValue());
			if (rule.type.isBorder()) {
				rl.ignoreBorderPoint = true;
			}
			this.rls.add(rl);

		}
	}

	public PropagateToNodes(MapRenderingTypesEncoder renderingTypes) {
		this.renderingTypes = renderingTypes;
		initPropagateToNodes();
		initConvertedTags();
	}

	private void initConvertedTags() {
		for (List<PropagateRule> rules : propagateRulesByTag.values()) {
			 for (PropagateRule r : rules) {
				Map<String, String> tags = new HashMap<>();
				String tag = r.tag;
				if (!Algorithms.isEmpty(r.tagPrefix)) {
					tag = r.tagPrefix + r.tag;
				}
				tags.put(tag, r.value);
				tags = renderingTypes.transformTags(tags, Entity.EntityType.NODE, MapRenderingTypesEncoder.EntityConvertApplyType.MAP);
				if (!Algorithms.isEmpty(tags)) {
					convertedPropagatedTags.put(r, tags);
				}
			}
		}
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

		for (PropagateRuleFromWayToNode pn : node.rls) {
			List<PropagateRuleFromWayToNode> l = propagateTagsByOsmNodeId.get(pn.osmId);
			if (l == null) {
				l = new ArrayList<PropagateRuleFromWayToNode>();
				propagateTagsByOsmNodeId.put(pn.osmId, l);
			}
			l.add(pn);
		}
	}


	public List<PropagateFromWayToNode> getPropagateByNodeId(long nodeId) {
		return propagateTagsByNodeId.get(nodeId);
	}

	private void initPropagateToNodes() {
		Map<String, MapRenderingTypes.MapRulType> ruleTypes = renderingTypes.getEncodingRuleTypes();
		for (Map.Entry<String, MapRenderingTypes.MapRulType> entry : ruleTypes.entrySet()) {
			MapRenderingTypes.MapRulType ruleType = entry.getValue();
			for (PropagateToNode d : ruleType.getPropagateToNodes()) {
				PropagateRule rule = new PropagateRule(d.propagateToNodes, d.propagateToNodesPrefix,
						d.propagateNetworkIf, d.propagateIf, d.propagateAlsoTags, d.propagateAvoidPolygons);
				String[] split = entry.getKey().split("/");
				rule.tag = split[0];
				rule.value = split.length > 1 ? split[1] : "";
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
				for (int i = 0; i < allIds.size(); i++) {
					getNode(resultWay, w, i, i).applyRule(rule, w.getTags());
				}
				break;
			case START:
				getNode(resultWay, w, 0, 1).applyRule(rule, w.getTags());
				break;
			case END:
				getNode(resultWay, w, allIds.size() - 2, allIds.size() - 1).applyRule(rule, w.getTags(), true);
				break;
			case CENTER:
				if (allIds.size() == 2) {
					getNode(resultWay, w, 0, 1).applyRule(rule, w.getTags());
				} else if (!rule.avoidPolygons || allIds.get(0) != allIds.get(allIds.size() - 1)) {
					// Propagate to the approximate midpoint ("closed" ways regulated by propagateAvoidPolygons)
					getNode(resultWay, w, allIds.size() / 2, allIds.size() / 2).applyRule(rule, w.getTags());
				}
				break;
			case BORDERIN:
				// possible fix for all interconnected roads assign on each point (not needed & more computational power)
				for (int i = 0; i < allIds.size() - 1; i++) {
					getNode(resultWay, w, i, i + 1).applyRule(rule, w.getTags(), false);
					getNode(resultWay, w, i, i + 1).applyRule(rule, w.getTags(), true);
				}
//				getNode(resultWay, w, 0, 1).applyRule(rule);
//				getNode(resultWay, w, allIds.size() - 2, allIds.size() - 1).applyRule(rule, true);
				break;
			case BORDEROUT:
				// fix for all interconnected roads assign on each point (not needed & more computational power)
				for (int i = 0; i < allIds.size(); i++) {
					getNode(resultWay, w, i, i).applyRule(rule, w.getTags());
				}
				break;
			}
		}
		return resultWay;
	}

	private List<PropagateRule> getRulesToApply(Way w) {
		List<PropagateRule> rulesToApply = null;
		Map<String, String> trTags = renderingTypes.transformTags(w.getTags(), EntityType.WAY, EntityConvertApplyType.MAP);
//		System.out.println("WA " + (w.getId()));
		for (String tag : trTags.keySet()) {
            if (tag.equals("bicycle")) {
                int r = 0;
            }
			List<PropagateRule> list = propagateRulesByTag.get(tag);
			if (list == null) {
				continue;
			}
            PropagateRule defaultRule = null;
			for (PropagateRule rule : list) {
				String entityTag = trTags.get(tag);
                if (rule.value.isEmpty() && !renderingTypes.isMapRenderingType(tag, entityTag)) {
                    defaultRule = rule;
                }
                if (tag.equals("hazard")) {
                    int y = 0;
                }
				if (entityTag != null && entityTag.equals(rule.value)) {
                    rulesToApply = applyRule(rulesToApply, rule, trTags);
                    defaultRule = null;
				}
			}
            if (defaultRule != null) {
                rulesToApply = applyRule(rulesToApply, defaultRule, trTags);
            }
		}
		return rulesToApply;
	}

    private List<PropagateRule> applyRule(List<PropagateRule> rulesToApply, PropagateRule rule, Map<String, String> trTags) {
        boolean propIf = rule.applicableBorder(trTags);
        if (propIf) {
            if (rulesToApply == null) {
                rulesToApply = new ArrayList<>();
            }
            if (!rulesToApply.contains(rule)) {
                rulesToApply.add(rule);
            }
        }
        return rulesToApply;
    }


	public void propagateTagsToWayNodesNoBorderRule(Way w) {
		if (propagateTagsByNodeId.isEmpty()) {
			return;
		}
		List<Node> nodes = w.getNodes();
		for (Node n : nodes) {
			propagateTagsToNode(n, false);
		}
	}

	public void propagateTagsToNode(Node n, boolean includeBorder) {
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
				if (!includeBorder && w.rule.type.isBorder()) {
					continue;
				}
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
		public final boolean avoidPolygons;
		public Map<String, String> propMapIf;
		public Map<String, String> propNetworkIf;
		public Set<String> propAlso;

		public PropagateRule(PropagateToNodesType type, String tagPrefix,
				Map<String, String> propNetworkIf, Map<String, String> propMapIf,
				String[] propAlsoTags, boolean avoidPolygons) {
			this.type = type;
			this.tagPrefix = tagPrefix;
			this.propMapIf = propMapIf;
			if (propAlsoTags != null) {
				this.propAlso = new HashSet<>(Arrays.asList(propAlsoTags));
			}
			this.propNetworkIf = propNetworkIf;
			this.avoidPolygons = avoidPolygons;
		}

		public String getPropagateValue() {
			return value;
		}

		public String getWayTag() {
			return tag;
		}

		public String getWayValue() {
			return value;
		}

		public String getPropagateTag() {
			String propagateTag = tag;
			if (tagPrefix != null) {
				propagateTag = tagPrefix + propagateTag;
			}
			return propagateTag;
		}

		public Set<String> getPropAlso() {
			return propAlso;
		}

		public boolean applicableBorder(Map<String, String> tags) {
			if (!value.equals(tags.get(tag)) && !value.isEmpty()) {
				return false;
			}
			if (propNetworkIf != null && !applicable(tags, propNetworkIf)) {
				return false;
			}
			if (propMapIf != null && !applicable(tags, propMapIf)) {
				return false;
			}
			return true;
		}

		public boolean applicableNetwork(Map<String, String> tags) {
			if (propNetworkIf != null) {
				return applicable(tags, propNetworkIf);
			}
			return true;
		}

		private boolean applicable(Map<String, String> tags, Map<String, String> propMap) {
			for (Map.Entry<String, String> entry : propMap.entrySet()) {
				String propTag = entry.getKey();
				if (propTag.startsWith("~")) {
					String tagValue = tags.get(propTag.substring(1));
					boolean noValue = Algorithms.isEmpty(tagValue) || "no".equals(tagValue);
					if (!noValue) {
						return false;
					}
					continue;
				}
				String tagValue = tags.get(propTag);
				if (tagValue == null) {
					return false;
				} else {
					if (entry.getValue() == null) {
						// value present
					} else {
						String[] allValues = entry.getValue().split(",");
						boolean allNegs = true;
						boolean match = false;
						for (String v : allValues) {
							if (v.startsWith("~")) {
								if (v.equals("~" + tagValue)) {
									return false;
								}
							} else {
								allNegs = false;
								if (v.equals(tagValue)) {
									match = true;
								}
							}
						}
						if (!allNegs && !match) {
							return false;
						}
					}
				}
			}
			return true;
		}
	}

	public void calculateBorderPoints(Way w) {
		if (propagateTagsByOsmNodeId.isEmpty()) {
			return;
		}
		Map<String, String> convertTags = null;
		for (long nodeId : w.getNodeIds().toArray()) {
			List<PropagateRuleFromWayToNode> linkedPropagate = propagateTagsByOsmNodeId.get(nodeId);
			if (linkedPropagate != null) {
				if (convertTags == null) {
					convertTags = renderingTypes.transformTags(w.getTags(), EntityType.WAY, EntityConvertApplyType.MAP);
				}
				Map<PropagateRule, List<PropagateRuleFromWayToNode>> rules = new HashMap<>();
				for (PropagateRuleFromWayToNode n : linkedPropagate) {
					if (!rules.containsKey(n.rule)) {
						rules.put(n.rule, new ArrayList<>());
					}
					rules.get(n.rule).add(n);
				}
//				System.out.println("W" + (w.getId() >> OsmDbCreator.SHIFT_ID));
				for (PropagateRule rule : rules.keySet()) {
					if (!rule.type.isBorder()) {
						continue;
					}
					List<PropagateRuleFromWayToNode> propagatedBorders = rules.get(rule);
					for (PropagateRuleFromWayToNode p : propagatedBorders) {
						if (p.rule.applicableNetwork(convertTags) && !p.rule.applicableBorder(convertTags)) {
							p.ignoreBorderPoint = false;
						}
					}
				}
			}
		}
	}

	public void calculateBorderPointMainTypes(long nodeId, List<MapRulType> mainTypes) {
		List<PropagateFromWayToNode> linkedPropagate = getPropagateByNodeId(nodeId);
		if (linkedPropagate == null) {
			return;
		}
		Iterator<MapRulType> it = mainTypes.iterator();
		while (it.hasNext()) {
			MapRulType type = it.next();
			int delete = 0;
			for (PropagateFromWayToNode p : linkedPropagate) {
				for (PropagateRuleFromWayToNode n : p.rls) {
					boolean isEqual = type.getTag().equals(n.rule.getPropagateTag()) && type.getValue().equals(n.rule.getPropagateValue());
					boolean isConverted = false;
					if (!isEqual) {
						Map<String, String> convertedTags = convertedPropagatedTags.get(n.rule);
						if (convertedTags != null) {
							String value = convertedTags.get(type.getTag());
							if (!Algorithms.isEmpty(value) && value.equals(type.getValue())) {
								isConverted = true;
							}
						}
					}
					if (isEqual || isConverted) {
						if (n.ignoreBorderPoint && delete == 0) {
							delete = 1;
						} else if (!n.ignoreBorderPoint) {
							delete = -1;
						}
					}

				}
			}
			if (delete == 1) {
				it.remove();
			}
		}
	}

	public boolean ignoreBorderPoint(long nodeId, PoiType poiType) {
		List<PropagateFromWayToNode> linkedPropagate = getPropagateByNodeId(nodeId);
		if (linkedPropagate == null) {
			return false;
		}
		for (PropagateFromWayToNode p : linkedPropagate) {
			for (PropagateRuleFromWayToNode n : p.rls) {
				if (poiType.getOsmTag().equals(n.rule.getPropagateTag()) && poiType.getOsmValue().equals(n.rule.getPropagateValue())) {
					return n.ignoreBorderPoint;
				}
			}
		}
		return false;
	}

}
