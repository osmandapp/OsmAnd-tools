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
    private Map<String, PropagateToNodesType> propagateToNodes = new HashMap<>();

    public PropagateToNodes(MapRenderingTypesEncoder renderingTypes) {
        this.renderingTypes = renderingTypes;
        initPropagateToNodes();
    }


    private void initPropagateToNodes() {
        Map<String, MapRenderingTypes.MapRulType> ruleTypes = renderingTypes.getEncodingRuleTypes();
        for (Map.Entry<String, MapRenderingTypes.MapRulType> entry : ruleTypes.entrySet()) {
            MapRenderingTypes.MapRulType ruleType = entry.getValue();
            if (ruleType.isPropagateToNodes()) {
                propagateToNodes.put(entry.getKey(), ruleType.getPropagateToNodesType());
            }
        }
    }

    public void registerRestrictionNodes(Entity entity) {
        if (entity instanceof Way) {
            Way w = (Way) entity;
            TLongArrayList allIds = w.getNodeIds();
            if (allIds.size() == 0) {
                return;
            }
            for (Map.Entry<String, PropagateToNodesType> propagate : propagateToNodes.entrySet()) {
                String[] tagValue = propagate.getKey().split("/");
                String propagateTag = tagValue[0];
                String propagateValue = tagValue[1];
                String entityTag = entity.getTag(propagateTag);
                if (entityTag != null && entityTag.equals(propagateValue)) {
                    TLongArrayList ids = new TLongArrayList();
                    switch (propagate.getValue()) {
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
                    }
                    nodePropagatedIds.addAll(ids);
                    for (long id : ids.toArray()) {
                        Map<String, String> tags = nodePropagatedTags.get(id);
                        if (tags == null) {
                            tags = new HashMap<>();
                        }
                        tags.put(propagateTag, propagateValue);
                        nodePropagatedTags.put(id, tags);
                    }
                }
            }
        }
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
        long nodeId = n.getId() >> OsmDbCreator.SHIFT_ID;
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
}
