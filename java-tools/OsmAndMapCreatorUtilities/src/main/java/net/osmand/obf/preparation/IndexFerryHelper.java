package net.osmand.obf.preparation;

import net.osmand.osm.edit.Entity;
import static net.osmand.osm.edit.OSMSettings.OSMTagKey.*;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;

import java.util.Map;

public class IndexFerryHelper {

    public static boolean hasFerryTags(Entity e) {
        // "ferry=*" or "route=ferry"
        return e.getTag(FERRY) != null || (e.getTag(ROUTE) != null && e.getTag(ROUTE).equals(FERRY));
    }

    public static void collectFoundedFerryWays(Way way, Map<String, Way> collectedFerryWays) {
        if (hasFerryTags(way)) {
            String key = way.getFirstNodeId() + " " + way.getLastNodeId();
            collectedFerryWays.put(key, way);
        }
    }

    public static void removeDuplicatedFerryWays(Relation rel, Map<String, Way> collectedFerryWays) {
        if (hasFerryTags(rel)) {
            long startStopId = rel.getMembers().get(0).getEntity().getId();
            long endStopId = rel.getMembers().get(1).getEntity().getId();
            collectedFerryWays.remove(startStopId + " " + endStopId);
            collectedFerryWays.remove(endStopId + " " + startStopId);
        }
    }

    public static Relation createSyntheticFerryRelation(Way way) {
        //create relation based on ferry-way dato. it will be used for generation forward and backward public transport routes.
        Relation syntethicRelation = new Relation(way.getId());
        
        if (way.getTag(NAME) != null) {
            syntethicRelation.putTag(NAME.getValue(), way.getTag(NAME));
        }
        if (way.getTag(REF) != null) {
            syntethicRelation.putTag(REF.getValue(), way.getTag(REF));
        }
        if (way.getTag(DURATION) != null) {
            syntethicRelation.putTag(DURATION.getValue(), way.getTag(DURATION));
        }
        if (way.getTag(OPERATOR) != null) {
            syntethicRelation.putTag(OPERATOR.getValue(), way.getTag(OPERATOR));
        }
        syntethicRelation.putTag(PT_VERSION.getValue(), "1");
        syntethicRelation.putTag(TYPE.getValue(), ROUTE.getValue());
        syntethicRelation.putTag(ROUTE.getValue(), FERRY.getValue());

        syntethicRelation.addMember(way.getFirstNodeId(), Entity.EntityType.NODE, STOP.getValue());
        syntethicRelation.addMember(way.getLastNodeId(), Entity.EntityType.NODE, STOP.getValue());
        syntethicRelation.addMember(way.getId(), Entity.EntityType.WAY, "");
        return syntethicRelation;
    }
}
