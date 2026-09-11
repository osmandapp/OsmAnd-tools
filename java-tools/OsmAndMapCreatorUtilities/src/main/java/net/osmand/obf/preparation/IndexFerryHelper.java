package net.osmand.obf.preparation;

import net.osmand.binary.ObfConstants;
import net.osmand.osm.edit.Entity;

import static net.osmand.obf.preparation.IndexRouteRelationCreator.MAX_RELATION_ID_BITS;
import static net.osmand.osm.edit.OSMSettings.OSMTagKey.*;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.edit.Node;

import java.util.Map;
import java.util.Set;

public class IndexFerryHelper {
    
    // Keep synthetic IDs in the top limit of relation ID range (between 26 and 27 bit).
    private static final long MIN_SYNTHETIC_RELATION_ID = 1L << (MAX_RELATION_ID_BITS - 1); // 26
    private static final long MAX_SYNTHETIC_RELATION_ID = (1L << MAX_RELATION_ID_BITS) - 1; // 27

    public static void collectUsedRelationId(Relation relation, Set<Long> usedRelationIds) {
        long id = relation.getId();
        if (id >= MIN_SYNTHETIC_RELATION_ID && id <= MAX_SYNTHETIC_RELATION_ID) {
            usedRelationIds.add(id);
        }
    }

    private static long generateSyntheticRelationId(Way way, Set<Long> usedRelationIds) {
        // generatedId = 26bit + wayId. Should be less than 27bit
        long wayId = way.getId() >> ObfConstants.SHIFT_ID;
        wayId = Math.floorMod(wayId, MIN_SYNTHETIC_RELATION_ID); // less than 26bit
        long generatedId = MIN_SYNTHETIC_RELATION_ID + wayId;
        
        while (usedRelationIds.contains(generatedId)) {
            generatedId += 1;
            if (generatedId >= MAX_SYNTHETIC_RELATION_ID) {
                generatedId = MIN_SYNTHETIC_RELATION_ID;
            }
        }
        return generatedId;
    }
    
    public static boolean hasFerryTags(Entity e) {
        // "ferry=*" or "route=ferry"
        return e.getTag(FERRY) != null || (e.getTag(ROUTE) != null && e.getTag(ROUTE).equals(FERRY.getValue()));
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

    public static Relation createSyntheticFerryRelation(Way way, Set<Long> usedRelationIds) {
        //create relation based on ferry-way dato. it will be used for generation forward and backward public transport routes.
        long syntheticRelationId = generateSyntheticRelationId(way, usedRelationIds);
        Relation syntethicRelation = new Relation(syntheticRelationId);
        
        if (way.getTag(NAME) != null) {
            syntethicRelation.putTag(NAME.getValue(), way.getTag(NAME));
        }
        if (way.getTag(REF) != null) {
            syntethicRelation.putTag(REF.getValue(), way.getTag(REF));
        } else {
            syntethicRelation.putTag(REF.getValue(), Long.toString(way.getId())); //can't create pt routes without ref
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

        for (Node node : way.getNodes()) {
            if (node.getTag("amenity") != null && node.getTag("amenity").equals(FERRY_TERMINAL.getValue())) {
                syntethicRelation.addMember(node.getId(), Entity.EntityType.NODE, STOP.getValue());
            }
        }
        
        syntethicRelation.addMember(way.getId(), Entity.EntityType.WAY, "");
        return syntethicRelation;
    }
}
