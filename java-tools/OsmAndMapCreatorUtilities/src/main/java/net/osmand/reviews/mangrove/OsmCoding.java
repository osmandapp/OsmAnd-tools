package net.osmand.reviews.mangrove;

import com.google.common.collect.ImmutableMap;
import net.osmand.PlatformUtil;
import net.osmand.reviews.OsmElementType;
import org.apache.commons.logging.Log;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
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
        ImmutableMap.Builder<Review, OsmPoi> result = ImmutableMap.builder();
        for (Review review : reviews) {
            OsmPoi poi = resolvePoi(review);
            if (poi != null) {
                result.put(review, poi);
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
                review.geo().coordinates().y(),
                review.geo().coordinates().x(),
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

    public record OsmPoi(double lat,
                         double lon,
                         String name,
                         OsmElementType elementType,
                         Long osmId) {
    }

    private OsmCoding() {}
}
