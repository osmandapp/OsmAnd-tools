package net.osmand.obf.preparation;

import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.obf.OsmGpxWriteContext;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.RelationTagsPropagation;
import net.osmand.osm.edit.*;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.*;

public class IndexRouteRelationCreator {
	private static final String[] FILTERED_TAGS = {
			"hiking", // 244k
			"bicycle", // 119k
			"foot", // 63k
			"mtb", // 29k
			"piste", // 14k
			"ski", // 8k
			"horse", // 4k
			"running", // 1k
			"snowmobile", // 1k
			"fitness_trail", // 1k
			"canoe", // 0.8k
			"canyoning", // 0.6k
			"motorboat", // 0.4k
			"boat", // 0.3k
			"waterway", // 0.3k
			"inline_skates", // 0.2k
			"via_ferrata", // 0.2k
			"walking", // 0.2k
			"ferrata", // proposed
			// Ignored: bus detour emergency_access evacuation ferry funicular historic light_rail motorcycle
			// Ignored: power railway road share_taxi subway taxi tracks train tram transhumance trolleybus worship
	};

	private static long INTERNAL_NEGATIVE_BASE_ID = -(1 << 20); // used for Node(s) inside Way(s)

	private static final Log log = LogFactory.getLog(IndexRouteRelationCreatorOld.class);

	private final IndexCreatorSettings settings;
	private final IndexPoiCreator indexPoiCreator;
	private final IndexVectorMapCreator indexMapCreator;

	private final RelationTagsPropagation transformer;
	private final MapRenderingTypesEncoder renderingTypes;

	public IndexRouteRelationCreator(@Nonnull IndexCreatorSettings settings,
	                                 @Nonnull IndexPoiCreator indexPoiCreator,
	                                 @Nonnull IndexVectorMapCreator indexMapCreator) {
		this.settings = settings;
		this.indexPoiCreator = indexPoiCreator;
		this.indexMapCreator = indexMapCreator;
		this.transformer = indexMapCreator.tagsTransformer;
		this.renderingTypes = indexMapCreator.renderingTypes;
	}

	public void iterateRelation(Relation relation, OsmDbAccessorContext ctx, IndexCreationContext icc)
			throws SQLException {
		if ("route".equals(relation.getTag("type")) && isSupportedRouteType(relation.getTag("route"))) {
			List<Way> joinedWays = new ArrayList<>();
			Map<String, String> shieldTags = new LinkedHashMap<>();
			collectJoinedWaysAndShieldTags(relation, joinedWays, shieldTags);

			Map<String, String> mapSectionTags = new LinkedHashMap<>();
			Map<String, String> poiSectionTags = new LinkedHashMap<>();
			collectMapAndPoiSectionTags(relation, shieldTags, mapSectionTags, poiSectionTags);

			// TODO create pointsForPoiSearch split in POI_SEARCH_POINTS_DISTANCE_M based on joinedWays (ID ???)

			for (Way way : joinedWays) {
				way.replaceTags(mapSectionTags);
				indexMapCreator.iterateMainEntity(way, ctx, icc);
			}

		}
	}

	private static final String ROUTE = "route";

	// TODO move all const from OsmGpxWriteContext to this
	private static final String ROUTE_TYPE = OsmGpxWriteContext.ROUTE_TYPE;
	private static final String ROUTE_ID_TAG = OsmGpxWriteContext.ROUTE_ID_TAG;
	private static final String TRACK_COLOR = OsmGpxWriteContext.TRACK_COLOR;

	public static final int MIN_REF_LENGTH_TO_USE_FOR_SEARCH = 3;

	private void collectMapAndPoiSectionTags(@Nonnull Relation relation,
	                                         @Nonnull Map<String, String> preparedTags,
	                                         @Nonnull Map<String, String> mapSectionTags,
	                                         @Nonnull Map<String, String> poiSectionTags) {
		// TODO consider XML_COLON (compare with RouteRelationExtractor obf)
		Map<String, String> commonTags = new LinkedHashMap<>(relation.getTags());

		// route-related tags
		commonTags.remove(ROUTE); // see also OsmGpxWriteContext.alwaysExtraTags
		commonTags.put(ROUTE_ID_TAG, Amenity.ROUTE_ID_OSM_PREFIX + relation.getId());

		// appearance tags
		commonTags.put("width", "roadstyle");
		commonTags.put("translucent_line_colors", "yes");

		// shield tags, etc
		commonTags.putAll(preparedTags);

		String ref = commonTags.get("ref");
		if (ref != null && ref.length() >= MIN_REF_LENGTH_TO_USE_FOR_SEARCH) {
			commonTags.put("name:ref", ref);
		}

		// TODO finalizeActivityTypeAndColors() - TRACK_COLOR, ROUTE_TYPE, ROUTE_ACTIVITY_TYPE -- test with ROUTE_TYPE="other", TRACK_COLOR="red"
		finalizeRouteShieldTags(commonTags);

		// prepare section tags
		mapSectionTags.putAll(commonTags);
		poiSectionTags.putAll(commonTags);
		mapSectionTags.put(ROUTE, "segment");
		mapSectionTags.remove(ROUTE_TYPE); // avoid creation of POI-data when indexing Ways
		poiSectionTags.remove(TRACK_COLOR); // track_color is required for Rendering only
	}

	private static final String SHIELD_FG = "shield_fg";
	private static final String SHIELD_BG = "shield_bg";
	private static final String SHIELD_TEXT = "shield_text";
	private static final String SHIELD_STUB_NAME = "shield_stub_name";

	public static void finalizeRouteShieldTags(Map<String, String> tags) {
		if (tags.containsKey("ref") || tags.containsKey(SHIELD_TEXT)) {
			tags.remove(SHIELD_STUB_NAME);
		} else if (tags.containsKey(SHIELD_FG) || tags.containsKey(SHIELD_BG)) {
			tags.put(SHIELD_STUB_NAME, ".");
		}
		if (tags.containsKey(SHIELD_TEXT)) {
			String text = tags.get(SHIELD_TEXT);
			if (text.length() >= MIN_REF_LENGTH_TO_USE_FOR_SEARCH && !text.equals(tags.get("ref"))) {
				tags.put("name:sym", text);
			}
		}
	}

	private void collectJoinedWaysAndShieldTags(@Nonnull Relation relation,
	                                            @Nonnull List<Way> joinedWays,
	                                            @Nonnull Map<String, String> shieldTags) {
		List<Way> waysToJoin = new ArrayList<>();
		for (Relation.RelationMember member : relation.getMembers()) {
			if (member.getEntity() instanceof Way way) {
				if ("yes".equals(way.getTag("area"))) {
					continue; // skip (eg https://www.openstreetmap.org/way/746544031)
				}
				waysToJoin.add(way);
				transformer.addPropogatedTags(renderingTypes,
						MapRenderingTypesEncoder.EntityConvertApplyType.MAP, way, way.getModifiableTags());
				shieldTags.putAll(getShieldTagsFromOsmcTags(way.getTags(), relation.getId()));
			}
		}
		spliceWaysIntoSegments(waysToJoin, joinedWays, relation.getId());
		// TODO add route_bbox_radius based on joinedWays (bbox -> radius), calc distance (if not exists in relation)
	}

	public static void spliceWaysIntoSegments(@Nonnull List<Way> waysToJoin,
	                                          @Nonnull List<Way> joinedWays,
	                                          long relationId) {
		boolean[] done = new boolean[waysToJoin.size()];
		while (true) {
			List<Node> nodes = new ArrayList<>();
			for (int i = 0; i < waysToJoin.size(); i++) {
				if (!done[i]) {
					done[i] = true;
					addWayToNodes(nodes, false, waysToJoin.get(i), false); // "head" way
					while (true) {
						boolean stop = true;
						for (int j = 0; j < waysToJoin.size(); j++) {
							if (!done[j] && considerWayToJoin(nodes, waysToJoin.get(j))) {
								done[j] = true;
								stop = false;
							}
						}
						if (stop) {
							break; // nothing joined
						}
					}
					break; // segment is done
				}
			}
			if (nodes.isEmpty()) {
				break; // all done
			}
			long generatedId = calcSyntheticId(relationId, joinedWays.size(), nodes);
			joinedWays.add(new Way(generatedId, nodes)); // ID = relationId + counter + hash(nodes)
		}
	}

	private static long calcSyntheticId(long relationId, long counter, @Nonnull List<Node> nodes) {
		final long MAX_RELATION_ID_BITS = 27;
		final long MAX_COUNTER_BITS = 9;

		if (counter < 0 || counter >= (1L << MAX_COUNTER_BITS) ||
				relationId < 0 || relationId >= (1L << MAX_RELATION_ID_BITS)) {
			log.error(String.format(
					"calcSyntheticId() relation %d/%d overflow (%d/%d bits)",
					relationId, counter, MAX_RELATION_ID_BITS, MAX_COUNTER_BITS));
			throw new UnsupportedOperationException();
		}

		LatLon center = OsmMapUtils.getWeightCenterForNodes(nodes);

		int hash = center == null
				? nodes.size() % 64
				: (int) (1000 * (center.getLatitude() + center.getLongitude())) % 64; // 0-63 = 6 bits

		// Max OSM Relation ID has 25 bits @ 2025/02/05 = 18655715
		return (1L << (ObfConstants.SHIFT_MULTIPOLYGON_IDS - 1)) // 43rd bit = 1 (42 bits left for numbers)
				+ (relationId << 15)                             // 27 bits (15 left) (with 4x reserve)
				+ (counter << 6)                                 // 9 bits (6 left)
				+ hash;                                          // 6 bits
	}

	private static boolean considerWayToJoin(List<Node> nodes, Way candidate) {
		if (nodes.isEmpty() || candidate.getNodes().isEmpty()) {
			return true;
		}

		LatLon firstNodeLL = nodes.get(0).getLatLon();
		LatLon lastNodeLL = nodes.get(nodes.size() - 1).getLatLon();
		LatLon firstCandidateLL = candidate.getNodes().get(0).getLatLon();
		LatLon lastCandidateLL = candidate.getNodes().get(candidate.getNodes().size() - 1).getLatLon();

		if (MapUtils.areLatLonEqual(lastNodeLL, firstCandidateLL)) {
			addWayToNodes(nodes, false, candidate, false); // nodes + Candidate
		} else if (MapUtils.areLatLonEqual(lastNodeLL, lastCandidateLL)) {
			addWayToNodes(nodes, false, candidate, true); // nodes + etadidnaC
		} else if (MapUtils.areLatLonEqual(firstNodeLL, firstCandidateLL)) {
			addWayToNodes(nodes, true, candidate, true); // etadidnaC + nodes
		} else if (MapUtils.areLatLonEqual(firstNodeLL, lastCandidateLL)) {
			addWayToNodes(nodes, true, candidate, false); // Candidate + nodes
		} else {
			return false;
		}

		return true;
	}

	private static void addWayToNodes(List<Node> nodes, boolean insert, Way way, boolean reverse) {
		List<Node> points = new ArrayList<>();
		for (Node n : way.getNodes()) {
			points.add(new Node(n.getLatitude(), n.getLongitude(), INTERNAL_NEGATIVE_BASE_ID--));
		}
		if (reverse) {
			Collections.reverse(points);
		}
		nodes.addAll(insert ? 0 : nodes.size(), points);
	}

	public static boolean isSupportedRouteType(@Nullable String routeType) {
		if (routeType != null) {
			for (String tag : routeType.split("[;, ]")) {
				for (String value : IndexRouteRelationCreator.FILTERED_TAGS) {
					if (tag.startsWith(value) || tag.endsWith(value)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private static final Map<String, String> OSMC_TAGS_TO_SHIELD_PROPS = Map.of(
			"osmc_text", "shield_text",
			"osmc_background", "shield_bg",
			"osmc_foreground", "shield_fg",
			"osmc_foreground2", "shield_fg_2",
			"osmc_textcolor", "shield_textcolor",
			"osmc_waycolor", "shield_waycolor" // waycolor is a part of osmc:symbol and must be applied to whole way
	);

	private static final String OSMC_ICON_PREFIX = "osmc_";
	private static final String OSMC_ICON_BG_SUFFIX = "_bg";
	private static final Set<String> SHIELD_BG_ICONS = Set.of("shield_bg");
	private static final Set<String> SHIELD_FG_ICONS = Set.of("shield_fg", "sheld_fg_2");
	private static final String RELATION_ID = OSMSettings.OSMTagKey.RELATION_ID.getValue();

	@Nonnull
	public static Map<String, String> getShieldTagsFromOsmcTags(@Nonnull Map<String, String> tags, long relationId) {
		String requiredGroupPrefix = "route_"; // default prefix for generated OSMC-related tags
		if (relationId != 0) {
			for (String tag : tags.keySet()) {
				if (tag.endsWith(RELATION_ID) && tags.get(tag).equals(Long.toString(relationId))) {
					requiredGroupPrefix = tag.replace(RELATION_ID, "");
					break; // use relation prefix to catch tags from distinct group
				}
			}
		}
		Map<String, String> result = new LinkedHashMap<>();
		for (String tag : tags.keySet()) {
			for (String match : OSMC_TAGS_TO_SHIELD_PROPS.keySet()) {
				if (tag.startsWith(requiredGroupPrefix) && tag.endsWith(match)) {
					final String key = OSMC_TAGS_TO_SHIELD_PROPS.get(match);
					final String prefix =
							(SHIELD_BG_ICONS.contains(key) || SHIELD_FG_ICONS.contains(key)) ? OSMC_ICON_PREFIX : "";
					final String suffix = SHIELD_BG_ICONS.contains(key) ? OSMC_ICON_BG_SUFFIX : "";
					final String val = prefix + tags.get(tag) + suffix;
					result.putIfAbsent(key, val); // prefer 1st
				}
			}
		}
		return result;
	}
}
