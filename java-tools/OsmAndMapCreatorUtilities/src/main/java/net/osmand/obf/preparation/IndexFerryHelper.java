package net.osmand.obf.preparation;

import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.OSMSettings;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;

import java.util.Map;

public class IndexFerryHelper {

    public static boolean hasFerryTags(Entity e) {
        // "ferry=*" or "route=ferry"
        return e.getTag(OSMSettings.OSMTagKey.FERRY.getValue()) != null || (e.getTag(OSMSettings.OSMTagKey.ROUTE.getValue()) != null && e.getTag(OSMSettings.OSMTagKey.ROUTE.getValue()).equals(OSMSettings.OSMTagKey.FERRY.getValue()));
    }

    public static void saveFoundedFerryWay(Way way, Map<String, Way> foundFerryWays) {
        if (hasFerryTags(way)) {
            String key = way.getFirstNodeId() + " " + way.getLastNodeId();
            foundFerryWays.put(key, way);
        }
    }

    public static void removeDuplicatedFerryWays(Relation rel, Map<String, Way> foundFerryWays) {
        if (hasFerryTags(rel)) {
            long startStopId = rel.getMembers().get(0).getEntity().getId();
            long endStopId = rel.getMembers().get(1).getEntity().getId();
            foundFerryWays.remove(startStopId + " " + endStopId);
            foundFerryWays.remove(endStopId + " " + startStopId);
        }
    }

    public static Relation createSyntheticFerryRelation(Way way) {
        Relation syntethicRelation = new Relation(way.getId());
        if (way.getTag(OSMSettings.OSMTagKey.NAME.getValue()) != null) {
            syntethicRelation.putTag(OSMSettings.OSMTagKey.NAME.getValue(), way.getTag(OSMSettings.OSMTagKey.NAME.getValue()));
        }
        if (way.getTag(OSMSettings.OSMTagKey.REF.getValue()) != null) {
            syntethicRelation.putTag(OSMSettings.OSMTagKey.REF.getValue(), way.getTag(OSMSettings.OSMTagKey.REF.getValue()));
        }
        if (way.getTag(OSMSettings.OSMTagKey.DURATION.getValue()) != null) {
            syntethicRelation.putTag(OSMSettings.OSMTagKey.DURATION.getValue(), way.getTag(OSMSettings.OSMTagKey.DURATION.getValue()));
        }
        if (way.getTag(OSMSettings.OSMTagKey.OPERATOR.getValue()) != null) {
            syntethicRelation.putTag(OSMSettings.OSMTagKey.OPERATOR.getValue(), way.getTag(OSMSettings.OSMTagKey.OPERATOR.getValue()));
        }
        syntethicRelation.putTag(OSMSettings.OSMTagKey.PT_VERSION.getValue(), "1");
        syntethicRelation.putTag(OSMSettings.OSMTagKey.TYPE.getValue(), OSMSettings.OSMTagKey.ROUTE.getValue());
        syntethicRelation.putTag(OSMSettings.OSMTagKey.ROUTE.getValue(), OSMSettings.OSMTagKey.FERRY.getValue());

        syntethicRelation.addMember(way.getFirstNodeId(), Entity.EntityType.NODE, OSMSettings.OSMTagKey.STOP.getValue());
        syntethicRelation.addMember(way.getLastNodeId(), Entity.EntityType.NODE, OSMSettings.OSMTagKey.STOP.getValue());
        syntethicRelation.addMember(way.getId(), Entity.EntityType.WAY, "");
        return syntethicRelation;
    }
}
