package net.osmand.reviews.mangrove;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.reviews.OsmElementType;
import org.apache.commons.logging.Log;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class OsmCoding {
    private static final Log log = PlatformUtil.getLog(OsmCoding.class);

    /**
     * Map a review to information about an OSM node/way. Currently relies on the information provided in the review itself, so
     * only works for reviews that contain OSM ids in their metadata.
     *
     * @param reviews a set of reviews
     * @return a map of review to a {@code ReviewedPlace} describing the OpenStreetMap POI to which the review applies. The {@code ReviewedPlace} will have an empty list of reviews.
     * only reviews whose OSM details were successfully resolved are returned.
     */
    public static Map<Review, OsmPoi> resolveOsmPois(Set<Review> reviews) {
        Map<Review, OsmPoi> result = new HashMap<>();
        // we conflate the ids for nodes/relations/ways in this map, because the poi database's unique constraint does not consider the element type
        Map<Long, Set<OsmPoi>> poisById = new HashMap<>();
        Map<OsmPoi, Long> poiLatestReviewIat = new HashMap<>();
        for (Review review : reviews) {
            OsmPoi poi = resolvePoi(review);
            if (poi != null) {
                result.put(review, poi);

                if (!poisById.containsKey(poi.osmId())) {
                    poisById.put(poi.osmId(), new HashSet<>());
                }
                poisById.get(poi.osmId()).add(poi);
                poiLatestReviewIat.put(poi, Long.max(review.payload().iat(), poiLatestReviewIat.getOrDefault(poi, Long.MIN_VALUE)));
            }
        }

        Set<OsmPoi> poisToRemove = oldPoisSharingId(poisById, poiLatestReviewIat);
        log.info(String.format("found %d old version(s) of of POIs", poisToRemove.size()));
        for (Review review : reviews) {
            if (result.containsKey(review) && poisToRemove.contains(result.get(review))) {
                result.remove(review);
            }
        }

        return ImmutableMap.copyOf(result);
    }

    private static Set<OsmPoi> oldPoisSharingId(Map<Long, Set<OsmPoi>> poisById, Map<OsmPoi, Long> poiLatestReviewIat) {
        ImmutableSet.Builder<OsmPoi> result = ImmutableSet.builder();
        for (Set<OsmPoi> poisSharingId : poisById.values()) {
            if (poisSharingId.size() > 1) {
                List<OsmPoi> orderedPois = new ArrayList<>(poisSharingId);
                orderedPois.sort(Comparator.comparing(poiLatestReviewIat::get).reversed());
                result.addAll(orderedPois.subList(1, orderedPois.size()));
            }
        }
        return result.build();
    }

    private static final Pattern MAPCOMPLETE_CLIENT_ID_ELEMENT_PATTERN = Pattern.compile("#(?<type>node|relation|way)/(?<id>\\d+)$");
    private static final Pattern GEO_SUB_NAME_PATTERN = Pattern.compile("[?&]q=(?<qValue>[^&]*)");

    private static OsmPoi resolvePoi(Review review) {
        if (!review.scheme().equals("geo") || review.payload().metadata() == null || review.payload().metadata().clientId() == null) {
            return null;
        }

        OsmElementType elementType;
        long osmId;
        Matcher osmIdMatcher = MAPCOMPLETE_CLIENT_ID_ELEMENT_PATTERN.matcher(review.payload().metadata().clientId());
        if (!osmIdMatcher.find()) {
            return null;
        }

        String elementTypeStr = osmIdMatcher.group("type");
        String osmIdStr = osmIdMatcher.group("id");

        switch (elementTypeStr) {
            case "node" -> elementType = OsmElementType.NODE;
            case "relation" -> elementType = OsmElementType.RELATION;
            case "way" -> elementType = OsmElementType.WAY;
            default -> {
                log.error(String.format("unexpected element type: '%s'", osmIdMatcher.group(1)));
                return null;
            }
        }

        osmId = Long.parseLong(osmIdStr);

        String name = nameFromSub(review.payload().sub());
        if (name == null) {
            return null;
        }

        return new OsmPoi(
                new LatLon(
                        review.geo().coordinates().y(),
                        review.geo().coordinates().x()
                ),
                name,
                elementType,
                osmId
        );
    }

    private static String nameFromSub(String sub) {
        Matcher matcher = GEO_SUB_NAME_PATTERN.matcher(sub);
        if (matcher.find()) {
            try {
                String encodedValue = matcher.group("qValue");
                return URLDecoder.decode(encodedValue, StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                log.error(String.format("can't decode name from sub '%s'", sub), e);
                return null;
            }
        }
        log.error(String.format("name not found in sub '%s'", sub));
        return null;
    }

    public record OsmPoi(LatLon location,
                         String name,
                         OsmElementType elementType,
                         Long osmId) {
    }

    private OsmCoding() {
    }
}
