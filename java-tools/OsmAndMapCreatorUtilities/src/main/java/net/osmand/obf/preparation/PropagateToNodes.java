package net.osmand.obf.preparation;

import gnu.trove.list.array.TLongArrayList;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRenderingTypes.MapRulType.PropagateToNodesType;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PropagateToNodes {

    private MapRenderingTypesEncoder renderingTypes;
    private TLongArrayList nodePropagatedIds = new TLongArrayList();
    private Map<Long, Map<String, String>> nodePropagatedTags = new HashMap<>();
    private Map<String, PropagateRule> propagateToNodes = new HashMap<>();
    private TLongArrayList removedIds = new TLongArrayList();

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
                propagateToNodes.put(entry.getKey(), rule);
            }
        }
    }

    public TLongArrayList registerRestrictionNodes(Entity entity) {
        TLongArrayList resultIds = new TLongArrayList();
        if (entity instanceof Way) {
            Way w = (Way) entity;
            TLongArrayList allIds = w.getNodeIds();
            if (allIds.size() == 0) {
                return resultIds;
            }
            for (Map.Entry<String, PropagateRule> propagate : propagateToNodes.entrySet()) {
                String[] tagValue = propagate.getKey().split("/");
                String propagateTag = tagValue[0];
                String propagateValue = tagValue[1];
                String entityTag = entity.getTag(propagateTag);
                PropagateRule rule = propagate.getValue();
                if (entityTag != null && entityTag.equals(propagateValue)) {
                    boolean propIf = true;
                    if (rule.propIf != null) {
                        propIf = false;
                        for (Map.Entry<String, String> entry : rule.propIf.entrySet()) {
                            String ifTag = entity.getTag(entry.getKey());
                            if (ifTag != null) {
                                if (entry.getValue() == null || ifTag.equals(entry.getValue())) {
                                    propIf = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (!propIf) {
                        continue;
                    }
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
                            if (allIds.size() > 2) {
                                for (int i = 1; i < allIds.size() - 1; i++) {
                                    long n = (allIds.get(i));
                                    removedIds.add(n);
                                    Map<String, String> tags = nodePropagatedTags.get(n);
                                    if (tags != null && tags.containsKey(propagateTag)) {
                                        nodePropagatedTags.remove(n);
                                        nodePropagatedIds.remove(n);
                                    }
                                }
                            }
                            break;
                    }
                    resultIds.addAll(ids);
                    for (long id : ids.toArray()) {
                        setPropagation(id, propagateTag, propagateValue, rule.type);
                    }
                }
            }
        }
        return resultIds;
    }

    private void propagateWay(Way w) {
        if (nodePropagatedIds.size() == 0) {
            return;
        }
        List<Node> nodes = w.getNodes();
        for (Node n : nodes) {
            propagateNode(n);
        }
    }

    private void propagateNode(Node n) {
        if (nodePropagatedIds.size() == 0) {
            return;
        }
        long nodeId = n.getId();
        if (nodePropagatedIds.contains(nodeId)) {
            Map<String, String> tags = nodePropagatedTags.get(nodeId);
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                if (n.getTag(entry.getKey()) == null) {
                    n.putTag(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    public void propagateRestrictionNodeTags(Entity e) {
        if (e instanceof Way) {
            propagateWay((Way) e);
        } else if (e instanceof Node) {
            propagateNode((Node) e);
        }
    }

    public TLongArrayList getNodePropagatedIds() {
        return nodePropagatedIds;
    }

    private class PropagateRule {
        PropagateToNodesType type;
        String tagPrefix;
        Map<String, String> propIf;
        public PropagateRule(PropagateToNodesType type, String tagPrefix, Map<String, String> propIf) {
            this.type = type;
            this.tagPrefix = tagPrefix;
            this.propIf = propIf;
        }
    }

    private void setPropagation(long id, String propagateTag, String propagateValue, PropagateToNodesType type) {
        Map<String, String> tags = nodePropagatedTags.get(id);
        if (tags == null) {
            tags = new HashMap<>();
        } else if (type == PropagateToNodesType.BORDER && tags.containsKey(propagateTag)) {
            nodePropagatedTags.remove(id);
            nodePropagatedIds.remove(id);
            removedIds.add(id);
            return;
        }
        if (removedIds.contains(id)) {
            if (tags.containsKey(propagateTag)) {
                nodePropagatedTags.remove(id);
                nodePropagatedIds.remove(id);
            }
            return;
        }
        tags.put(propagateTag, propagateValue);
        nodePropagatedTags.put(id, tags);
        if (!nodePropagatedIds.contains(id)) {
            nodePropagatedIds.add(id);
        }
    }
}
