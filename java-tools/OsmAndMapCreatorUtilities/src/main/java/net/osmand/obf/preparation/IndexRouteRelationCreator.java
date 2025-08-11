package net.osmand.obf.preparation;

import gnu.trove.set.hash.TLongHashSet;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.obf.ToolsOsmAndContextImpl;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.OsmRouteType;
import net.osmand.osm.RelationTagsPropagation;
import net.osmand.osm.edit.*;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.RouteActivityHelper;
import net.osmand.shared.gpx.primitives.RouteActivity;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.*;

import static net.osmand.shared.gpx.GpxUtilities.ACTIVITY_TYPE;

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

	private static final boolean DEBUG_GENERATE_ROUTE_SEGMENT = false;

	private static final String SHIELD_FG = "shield_fg";
	private static final String SHIELD_BG = "shield_bg";
	private static final String SHIELD_TEXT = "shield_text";
	private static final String SHIELD_STUB_NAME = "shield_stub_name";

	private static final String ROUTE = "route";

	public static final int MIN_REF_LENGTH_TO_USE_FOR_SEARCH = 3;

	public static final int MAX_JOINED_POINTS_PER_SEGMENT = 2000; // ~25m * 2000 = ~50 km (optimize Map-section)

	public static final int POI_SEARCH_POINTS_INTERVAL_M = 5000; // store segments as POI-points every 5 km
	public static final int POI_SEARCH_POINTS_EDGE_DISTANCE_M = 100; // distance POI-points from edges of the Way (100m)

	public static final String ROUTE_ID_TAG = Amenity.ROUTE_ID;
	public static final String ROUTE_TYPE = "route_type";
	public static final String TRACK_COLOR = "track_color"; // Map-section tag

	private static final String SHIELD_WAYCOLOR = "shield_waycolor"; // shield-specific
	public static final String COLOR = "color"; // osmand:color
	private static final String COLOUR = "colour"; // osmand:colour
	public static final String DISPLAYCOLOR = "displaycolor"; // osmand:displaycolor / original gpxx:DisplayColor

	private static final String OSMAND_ACTIVITY = ACTIVITY_TYPE;
	private static final String ROUTE_ACTIVITY_TYPE = "route_activity_type";
	private static final String[] COLOR_TAGS_FOR_MAP_SECTION = {TRACK_COLOR, SHIELD_WAYCOLOR, COLOR, COLOUR, DISPLAYCOLOR};

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
	private static final Set<String> SHIELD_FG_ICONS = Set.of("shield_fg", "shield_fg_2");
	private static final String RELATION_ID = OSMSettings.OSMTagKey.RELATION_ID.getValue();

	private static long INTERNAL_NEGATIVE_BASE_ID = -(1 << 20); // used for Node(s) inside Way(s)
	private static final RouteActivityHelper routeActivityHelper = RouteActivityHelper.INSTANCE;
	private static final Log log = LogFactory.getLog(IndexRouteRelationCreator.class);

	private final IndexPoiCreator indexPoiCreator;
	private final IndexVectorMapCreator indexMapCreator;

	private final RelationTagsPropagation transformer;
	private final MapRenderingTypesEncoder renderingTypes;
	private final Long lastModifiedDate;

	private final static NumberFormat distanceKmFormat = new DecimalFormat("0.0", new DecimalFormatSymbols(Locale.US));

	public IndexRouteRelationCreator(@Nonnull IndexPoiCreator indexPoiCreator,
	                                 @Nonnull IndexVectorMapCreator indexMapCreator,
									 Long lastModifiedDate) {
		this.indexPoiCreator = indexPoiCreator;
		this.indexMapCreator = indexMapCreator;
		this.transformer = indexMapCreator.tagsTransformer;
		this.renderingTypes = indexMapCreator.renderingTypes;
		this.lastModifiedDate = lastModifiedDate;
		net.osmand.shared.util.PlatformUtil.INSTANCE.initialize(new ToolsOsmAndContextImpl());
	}

	public void iterateRelation(Relation relation, OsmDbAccessorContext ctx, IndexCreationContext icc)
			throws SQLException {
		if (!isSupportedRouteType(relation.getTag(Amenity.ROUTE))) {
			return;
		}
		if ("route".equals(relation.getTag("type"))) {
			List<Way> joinedWays = new ArrayList<>();
			List<Node> pointsForPoiSearch = new ArrayList<>();
			Map<String, String> preparedTags = new LinkedHashMap<>();

			TLongHashSet geometryBeforeCompletion = new TLongHashSet();
			fillRelationWaysGeometrySet(relation, geometryBeforeCompletion);

			OverpassFetcher.getInstance().fetchCompleteGeometryRelation(relation, ctx, lastModifiedDate);

			int hash = getRelationHash(relation);
			if (hash == -1) {
				log.error(String.format("Route relation %d is incomplete", relation.getId()));
				return; // incomplete relation
			}

			collectJoinedWaysAndShieldTags(relation, joinedWays, preparedTags, hash);
			calcRadiusDistanceAndPoiSearchPoints(relation.getId(), joinedWays, pointsForPoiSearch, preparedTags, hash);

			Map<String, String> mapSectionTags = new LinkedHashMap<>();
			Map<String, String> poiSectionTags = new LinkedHashMap<>();
			collectMapAndPoiSectionTags(relation, preparedTags, mapSectionTags, poiSectionTags);

			for (Way way : joinedWays) {
				for (Node node : way.getNodes()) {
					if (geometryBeforeCompletion.contains(getNodeLongId(node))) {
						way.replaceTags(mapSectionTags);
						indexMapCreator.iterateMainEntity(way, ctx, icc);
						break; // one-off
					}
				}
			}
			for (Node node : pointsForPoiSearch) {
				if (geometryBeforeCompletion.contains(getNodeLongId(node))) {
					poiSectionTags.forEach(node::putTag); // append tags
					indexPoiCreator.iterateEntity(node, ctx, icc);
				}
			}
			indexPoiCreator.excludeFromMainIteration(relation.getId());
		}
		if (OsmMapUtils.isSuperRoute(relation.getTags())) {
			Map<String, String> mapSectionTags = new LinkedHashMap<>();
			Map<String, String> poiSectionTags = new LinkedHashMap<>();
			Map<String, String> preparedTags = new LinkedHashMap<>();
			collectMapAndPoiSectionTags(relation, preparedTags, mapSectionTags, poiSectionTags);
			for (Map.Entry<String, String> entry : poiSectionTags.entrySet()) {
				relation.putTag(entry.getKey(), entry.getValue());
			}
			indexPoiCreator.iterateEntity(relation, ctx, icc);
			indexPoiCreator.excludeFromMainIteration(relation.getId());
		}
	}

	private void fillRelationWaysGeometrySet(Relation relation, TLongHashSet geometryBeforeCompletion) {
		for (Relation.RelationMember member : relation.getMembers()) {
			if (member.getEntity() instanceof Way way) {
				for (Node node : way.getNodes()) {
					geometryBeforeCompletion.add(getNodeLongId(node));
				}
			}
		}
	}

	private long getNodeLongId(Node node) {
		long y31 = MapUtils.get31TileNumberY(node.getLatitude());
		long x31 = MapUtils.get31TileNumberX(node.getLongitude());
		return (x31 << 31) + y31;
	}

	private void calcRadiusDistanceAndPoiSearchPoints(long relationId,
	                                                  @Nonnull List<Way> joinedWays,
	                                                  @Nonnull List<Node> pointsForPoiSearch,
	                                                  @Nonnull Map<String, String> tagsToFill,
	                                                  int hash) {
		final int MIN_RADIUS_FOR_SHORT_LINK = 50 * 1000; // 50 km
		final int SHORT_LINK_ZOOM = 9; // z9 = 3 chars ~50x50km
		Set<String> shortLinkTiles = new TreeSet<>();

		double distance = 0;
		QuadRect bbox = new QuadRect();
		int searchPointsCounter = 0; // 512 * 5 km = 2560 km max (in case of 9-bit limit)...

		for (int segmentIndex = 0; segmentIndex < joinedWays.size(); segmentIndex++) {
			Way way = joinedWays.get(segmentIndex);
			QuadRect wayBbox = way.getLatLonBBox();
			bbox.expand(wayBbox.left, wayBbox.top, wayBbox.right, wayBbox.bottom);
			List<Node> localPoints = new ArrayList<>();
			List<Node> nodes = way.getNodes();
			if (nodes.size() >= 2) {
				// place the very first point in the approx middle
				LatLon middle = nodes.get(nodes.size() / 2).getLatLon();
				long nodeId = calcEntityIdFromRelationId(relationId, searchPointsCounter++, hash);
				localPoints.add(new Node(middle.getLatitude(), middle.getLongitude(), nodeId));

				for (int i = 1; i < nodes.size(); i++) {
					LatLon currentLatLon = nodes.get(i).getLatLon();
					LatLon previousLatLon = nodes.get(i - 1).getLatLon();
					distance += MapUtils.getDistance(currentLatLon, previousLatLon);

					// place the very next points close to start/end
					// afterward, spread points evenly along the geometry
					int alternateIndex = i % 2 == 0 ? i : nodes.size() - i - 1;
					LatLon candidate = nodes.get(alternateIndex).getLatLon();
					double distStart = MapUtils.getDistance(candidate, nodes.get(0).getLatLon());
					double distEnd = MapUtils.getDistance(candidate, nodes.get(nodes.size() - 1).getLatLon());
					if (distStart > POI_SEARCH_POINTS_EDGE_DISTANCE_M && distEnd > POI_SEARCH_POINTS_EDGE_DISTANCE_M) {
						if (localPoints.stream().noneMatch(node ->
								MapUtils.getDistance(candidate, node.getLatLon()) < POI_SEARCH_POINTS_INTERVAL_M)) {
							nodeId = calcEntityIdFromRelationId(relationId, searchPointsCounter++, hash);
							localPoints.add(new Node(candidate.getLatitude(), candidate.getLongitude(), nodeId));
						}
					}
				}
			}
			for (Node node : localPoints) {
				node.putTag("route_segment_index", String.valueOf(segmentIndex));
			}
			pointsForPoiSearch.addAll(localPoints);
		}

		if (distance > 0) {
			tagsToFill.put("distance", distanceKmFormat.format(distance / 1000));
		}

		if (!bbox.hasInitialState()) {
			int radius = (int) MapUtils.getDistance(bbox.left, bbox.top, bbox.right, bbox.bottom);
			String routeBboxRadius = MapUtils.convertDistToChar(
					radius,
					GpxUtilities.TRAVEL_GPX_CONVERT_FIRST_LETTER,
					GpxUtilities.TRAVEL_GPX_CONVERT_FIRST_DIST,
					GpxUtilities.TRAVEL_GPX_CONVERT_MULT_1,
					GpxUtilities.TRAVEL_GPX_CONVERT_MULT_2
			);

			if (radius > MIN_RADIUS_FOR_SHORT_LINK) {
				pointsForPoiSearch.forEach(node -> shortLinkTiles.add(MapUtils
						.createShortLinkString(node.getLatitude(), node.getLongitude(), SHORT_LINK_ZOOM - 8)));
				shortLinkTiles.add(MapUtils.createShortLinkString(bbox.bottom, bbox.left, SHORT_LINK_ZOOM - 8));
				shortLinkTiles.add(MapUtils.createShortLinkString(bbox.top, bbox.right, SHORT_LINK_ZOOM - 8));
				tagsToFill.put("route_shortlink_tiles", String.join(",", shortLinkTiles));
			}

			tagsToFill.put("route_bbox_radius", routeBboxRadius);
		}
	}

	private void collectMapAndPoiSectionTags(@Nonnull Relation relation,
	                                         @Nonnull Map<String, String> preparedTags,
	                                         @Nonnull Map<String, String> mapSectionTags,
	                                         @Nonnull Map<String, String> poiSectionTags) {
		Map<String, String> commonTags = new LinkedHashMap<>(relation.getTags());

		commonTags.putAll(relation.getTags());

		// route_id and appearance tags
		commonTags.put("width", "roadstyle");
		commonTags.put("translucent_line_colors", "yes");
		commonTags.put(ROUTE_ID_TAG, Amenity.ROUTE_ID_OSM_PREFIX + relation.getId());

		// shield tags, etc
		commonTags.putAll(preparedTags);

		String ref = commonTags.get("ref");
		if (ref != null && ref.length() >= MIN_REF_LENGTH_TO_USE_FOR_SEARCH) {
			commonTags.put("name:ref", ref);
		}

		finalizeRouteShieldTags(commonTags);
		finalizeActivityTypeAndColors(commonTags, null, null, null);

		// prepare section tags
		mapSectionTags.putAll(commonTags);
		poiSectionTags.putAll(commonTags);

		if (DEBUG_GENERATE_ROUTE_SEGMENT) {
			mapSectionTags.put(ROUTE, "segment"); // enable to debug as TravelGpx data
		}
		// mapSectionTags.remove(ROUTE_TYPE); // avoid creation of POI-data when indexing Ways

		poiSectionTags.remove(TRACK_COLOR); // track_color is required for Rendering only
		poiSectionTags.remove(ROUTE); // see also OsmGpxWriteContext.alwaysExtraTags
	}

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

	public static void finalizeActivityTypeAndColors(@Nonnull Map<String, String> commonTags,
	                                                 @Nullable Map<String, String> metadataExtraTags,
	                                                 @Nullable Map<String, String> extensionsExtraTags,
	                                                 @Nullable String[] gpxInfoTags) {
		// route_activity_type (user-defined) - osmand:activity (OsmAnd) - route (OSM)
		final String[] activityTags = {ROUTE_ACTIVITY_TYPE, OSMAND_ACTIVITY, "route"};

		// OsmGpxFile.tags compatibility (might be used by DownloadOsmGPX)
		if (gpxInfoTags != null) {
			OsmRouteType compatibleOsmRouteType = OsmRouteType.getTypeFromTags(gpxInfoTags);
			if (extensionsExtraTags != null) {
				for (String tg : gpxInfoTags) {
					extensionsExtraTags.put("tag_" + tg, "yes");
				}
			}
			if (compatibleOsmRouteType != null) {
				if (compatibleOsmRouteType.getColor() != null) {
					commonTags.putIfAbsent(TRACK_COLOR, compatibleOsmRouteType.getColor());
				}
				commonTags.putIfAbsent(ROUTE_ACTIVITY_TYPE, compatibleOsmRouteType.getName().toLowerCase());
			}
		}

		Map<String, String> allTags = new LinkedHashMap<>(commonTags);
		if (metadataExtraTags != null) {
			allTags.putAll(metadataExtraTags);
		}
		if (extensionsExtraTags != null) {
			allTags.putAll(extensionsExtraTags);
		}

		for (String tag : COLOR_TAGS_FOR_MAP_SECTION) {
			if (allTags.containsKey(tag)) {
				commonTags.put(TRACK_COLOR,
						MapRenderingTypesEncoder.formatColorToPalette(allTags.get(tag), false));
				break;
			}
		}

		for (String tag : activityTags) {
			String values = allTags.get(tag);
			if (values != null) {
				// "hiking;horse" "mountain_bike, bicycle"
				for (String val : values.split("[;, ]")) {
					RouteActivity activity = routeActivityHelper.findRouteActivity(val); // find by id
					if (activity == null) {
						activity = routeActivityHelper.findActivityByTag(val); // try to find by tags
					}
					if (activity != null) {
						commonTags.put(ROUTE_TYPE, activity.getGroup().getId());
						commonTags.put(ROUTE_ACTIVITY_TYPE, activity.getId()); // to split into poi_additional_category
						return; // success
					}
				}
			}
		}

		commonTags.putIfAbsent(ROUTE_TYPE, "other"); // unknown / default
	}

	private void collectJoinedWaysAndShieldTags(@Nonnull Relation relation,
	                                            @Nonnull List<Way> joinedWays,
	                                            @Nonnull Map<String, String> shieldTags, int hash) {
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
		spliceWaysIntoSegments(waysToJoin, joinedWays, relation.getId(), hash);
	}

	public static void spliceWaysIntoSegments(@Nonnull List<Way> waysToJoin,
	                                          @Nonnull List<Way> joinedWays,
	                                          long relationId,
	                                          int hash) {
		boolean[] done = new boolean[waysToJoin.size()];
		while (true) {
			List<Node> nodes = new ArrayList<>();
			for (int i = 0; i < waysToJoin.size(); i++) {
				if (!done[i]) {
					done[i] = true;
					if (!waysToJoin.get(i).getNodeIds().isEmpty()) {
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
			}
			if (nodes.isEmpty()) {
				break; // all done
			}
			long generatedId = calcEntityIdFromRelationId(relationId, joinedWays.size(), hash);
			joinedWays.add(new Way(generatedId, nodes)); // ID = relationId + counter + hash(all-relation)
		}
	}

	private int getRelationHash(@Nonnull Relation relation) {
		List<Node> allNodes = new ArrayList<>();
		for (Relation.RelationMember member : relation.getMembers()) {
			if (member.getEntity() instanceof Node node) {
				allNodes.add(node);
			}
			if (member.getEntity() instanceof Way way) {
				if (way.getNodes().isEmpty()) {
					return -1; // incomplete
				}
				allNodes.addAll(way.getNodes());
			}
		}
		LatLon center = OsmMapUtils.getWeightCenterForNodes(allNodes);
		return center == null
				? allNodes.size() % 64
				: (int) (1000.0 * (Math.abs(center.getLatitude() + center.getLongitude()))) % 64;
	}

	private static long calcEntityIdFromRelationId(long relationId, long counter, int hash) {
		final long MAX_RELATION_ID_BITS = 27;
		final long MAX_COUNTER_BITS = 9;
		final long MAX_HASH_BITS = 6;

		if (relationId < 0 || relationId >= (1L << MAX_RELATION_ID_BITS)
				|| counter < 0 || counter >= (1L << MAX_COUNTER_BITS)
				|| hash < 0 || hash >= (1L << MAX_HASH_BITS)) {
			log.error(String.format(
					"calcEntityIdFromRelationId() relation %d/%d/%d overflow (%d/%d/%d bits)",
					relationId, counter, hash, MAX_RELATION_ID_BITS, MAX_COUNTER_BITS, MAX_HASH_BITS));
		}

		// Max OSM Relation ID has 25 bits @ 2025/02/05 = 18655715
		return (1L << (ObfConstants.SHIFT_MULTIPOLYGON_IDS - 1)) // 43rd bit = 1 (42 bits left for numbers)
				+ (relationId << 15)                             // 27 bits (15 left) (with 4x reserve)
				+ (counter << 6)                                 // 9 bits (6 left)
				+ hash;                                          // 6 bits
	}

	private static boolean considerWayToJoin(List<Node> result, Way candidate) {
		if (result.isEmpty() || result.size() > MAX_JOINED_POINTS_PER_SEGMENT) {
			return false;
		}

		if (candidate.getNodes().isEmpty()) {
			return true;
		}

		LatLon firstNodeLL = result.get(0).getLatLon();
		LatLon lastNodeLL = result.get(result.size() - 1).getLatLon();
		LatLon firstCandidateLL = candidate.getNodes().get(0).getLatLon();
		LatLon lastCandidateLL = candidate.getNodes().get(candidate.getNodes().size() - 1).getLatLon();

		if (MapUtils.areLatLonEqual(lastNodeLL, firstCandidateLL)) {
			addWayToNodes(result, false, candidate, false); // result + Candidate
		} else if (MapUtils.areLatLonEqual(lastNodeLL, lastCandidateLL)) {
			addWayToNodes(result, false, candidate, true); // result + etadidnaC
		} else if (MapUtils.areLatLonEqual(firstNodeLL, firstCandidateLL)) {
			addWayToNodes(result, true, candidate, true); // etadidnaC + result
		} else if (MapUtils.areLatLonEqual(firstNodeLL, lastCandidateLL)) {
			addWayToNodes(result, true, candidate, false); // Candidate + result
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
		if (!nodes.isEmpty() && !points.isEmpty()) {
			List<Node> skipLeadingPoint = points.subList(insert ? 0 : 1, points.size() - (insert ? 1 : 0));
			nodes.addAll(insert ? 0 : nodes.size(), skipLeadingPoint); // avoid duplicate point at joints
		} else {
			nodes.addAll(insert ? 0 : nodes.size(), points); // first addition to the result
		}
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

	public void closeAllStatements() {

	}
}
