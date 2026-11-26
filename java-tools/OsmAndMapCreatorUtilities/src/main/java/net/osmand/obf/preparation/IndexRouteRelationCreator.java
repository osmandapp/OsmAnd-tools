package net.osmand.obf.preparation;

import com.google.gson.Gson;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.gpx.clickable.ClickableWayTags;
import net.osmand.obf.ToolsOsmAndContextImpl;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.OsmRouteType;
import net.osmand.osm.RelationTagsPropagation;
import net.osmand.osm.edit.*;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.render.RenderingRuleSearchRequest;
import net.osmand.render.RenderingRulesStorage;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.RouteActivityHelper;
import net.osmand.shared.gpx.primitives.RouteActivity;
import net.osmand.util.Algorithms;
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

import static net.osmand.data.Amenity.*;
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
	private static final boolean COLLECT_OSM_ROUTE_RELATION_NODES = false; // Don't forget to implement unique node.id before use !!

	private static final String SHIELD_FG = "shield_fg";
	private static final String SHIELD_BG = "shield_bg";
	private static final String SHIELD_TEXT = "shield_text";
	public static final String SHIELD_STUB_NAME = "shield_stub_name";

	private static final String ROUTE = "route";

	public static final int MIN_REF_LENGTH_TO_USE_FOR_SEARCH = 3;

	public static final int MAX_JOINED_POINTS_PER_SEGMENT = 2000; // ~25m * 2000 = ~50 km (optimize Map-section)

	public static final int POI_SEARCH_POINTS_INTERVAL_M = 5000; // store segments as POI-points every 5 km
	public static final int POI_SEARCH_POINTS_EDGE_DISTANCE_M = 100; // distance POI-points from edges of the Way (100m)

	public static final String ROUTE_ID_TAG = Amenity.ROUTE_ID;
	public static final String ROUTE_TYPE = "route_type";
	public static final String TRACK_COLOR = "track_color"; // Map-section tag
	public static final String ROUTE_LANE = "route_lane"; // Map-section tag: parallel-lane index
	public static final String ROUTE_LANE_END = "route_lane_end"; // lane at the far end, for ramps
	public static final String ROUTE_LANE_SIDE = "route_lane_side"; // same order, stacked to one side

	// A ramp slides the line sideways along its own length, so on a short connector the sideways
	// move dominates and draws a spike instead of a transition. Such ways keep the lane of the
	// bundle they belong to - dropping them onto the path instead tears the bundle apart mid-way -
	// but the lane change is deferred to a longer piece.
	private static final double MIN_LANE_LENGTH_M = 40.0;
	public static final String ROUTE_SHARED_LANE = "route_shared_lane"; // lane shared with a look-alike
	public static final String WPT_EXTRA_TAGS = "wpt_extra_tags"; // pass tags to WptPt using JSON

	private static final String SHIELD_WAYCOLOR = "shield_waycolor"; // shield-specific
	public static final String COLOR = "color"; // osmand:color
	private static final String COLOUR = "colour"; // osmand:colour
	public static final String DISPLAYCOLOR = "displaycolor"; // osmand:displaycolor / original gpxx:DisplayColor
	public static final String OSMC_SYMBOL = "osmc:symbol";

	private static final String OSMAND_ACTIVITY = ACTIVITY_TYPE;
	private static final String ROUTE_ACTIVITY_TYPE = "route_activity_type";
	private static final String[] COLOR_TAGS_FOR_MAP_SECTION = {TRACK_COLOR, SHIELD_WAYCOLOR, COLOR, COLOUR, DISPLAYCOLOR};

	public static final Map<String, String> SKIP_RELATION_NODE_BY_TAGS = Map.of(
			"information", "guidepost"
			// ...
	);

	private static final Map<String, String> OSMC_TAGS_TO_SHIELD_PROPS = Map.of(
			"osmc_text", "shield_text",
			"osmc_background", "shield_bg",
			"osmc_foreground", "shield_fg",
			"osmc_foreground2", "shield_fg_2",
			"osmc_textcolor", "shield_textcolor",
			"osmc_waycolor", "shield_waycolor" // waycolor is a part of osmc:symbol and must be applied to whole way
	);

	private static final Map<String, String> NO_SYMBOL_ROUTE_SHIELD_COLORS = Map.of(
			"default", "black",
			"fitness_trail", "blue",
			"hiking", "green",
			"mtb", "red"
			// ...
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

	// Lane assignment. Filled by a pre-pass over all route relations (collectRouteMembership) and
	// used while emitting geometry, because a route's lane depends on which other routes share the
	// very same way - knowledge a single relation does not have.
	private final Map<Long, List<Long>> routesByWay = new LinkedHashMap<>();
	private final Map<Long, String> appearanceByRoute = new LinkedHashMap<>();
	private final Map<Long, long[]> endNodesByWay = new LinkedHashMap<>();
	private Map<Long, Map<String, Integer>> laneByWay = null; // built lazily from the two above
	private Map<Long, Map<String, Integer>> sideLaneByWay = null; // same ranking, stacked one side
	private Map<Long, Boolean> forwardByWay = null; // orientation shared by every route on the way

	private final Gson gson = new Gson();
	private final int ICON_SEARCH_ZOOM = 19;
	private final RenderingRulesStorage renderingRules;
	private final RenderingRuleSearchRequest searchRequest;

	public static final String[] CUSTOM_STYLES = {
			"default.render.xml",
			"routes.addon.render.xml"
			// "skimap.render.xml" // ski-style could work instead of default.render.xml but not together
	};
	public static final Map<String, String> CUSTOM_PROPERTIES = Map.of(
			// default.render.xml:
			"whiteWaterSports", "true",
			// routes.addon.render.xml:
			"showCycleRoutes", "true",
			"showMtbRoutes", "true",
			"hikingRoutesOSMC", "walkingRoutesOSMC",
			"showDirtbikeTrails", "true",
			"horseRoutes", "true",
			"showFitnessTrails", "true",
			"showRunningRoutes", "true"
			// "pisteRoutes", "true" // skimap.render.xml conflicts with default
	);
	public static final String DEFAULT_CLICKABLE_WAY_ACTIVITY_COLOR = "red";

	public IndexRouteRelationCreator(@Nonnull IndexPoiCreator indexPoiCreator,
	                                 @Nonnull IndexVectorMapCreator indexMapCreator,
									 Long lastModifiedDate) {
		this.indexPoiCreator = indexPoiCreator;
		this.indexMapCreator = indexMapCreator;
		this.transformer = indexMapCreator.tagsTransformer;
		this.renderingTypes = indexMapCreator.renderingTypes;
		this.renderingRules = RenderingRulesStorage.initWithStylesFromResources(CUSTOM_STYLES);
		this.searchRequest = RenderingRuleSearchRequest
				.initWithCustomProperties(renderingRules, ICON_SEARCH_ZOOM, CUSTOM_PROPERTIES);
		this.lastModifiedDate = lastModifiedDate;
		net.osmand.shared.util.PlatformUtil.INSTANCE.initialize(new ToolsOsmAndContextImpl());
	}

	/**
	 * Pre-pass over every route relation, recording which routes run over which way and what each
	 * route looks like on the map. Must run before iterateRelation() emits any geometry.
	 */
	/**
	 * Route types drawn as a coloured line next to each other, and therefore the only ones that
	 * should take up a lane. A piste or horse route sharing a path is either drawn by different
	 * rules or hidden behind a rendering property that is off by default; letting it occupy a lane
	 * pushes the one visible route off its path with nothing beside it - measured on Krkonoše,
	 * that was 14% of all ways carrying more than one route.
	 */
	private static final Set<String> LANE_ROUTE_TYPES =
			Set.of("hiking", "foot", "walking", "running", "bicycle", "mtb");

	/** -Dosmand.routeLanes=false turns lane assignment off entirely, for A/B against stock output. */
	public static boolean isLaneAssignmentEnabled() {
		return !"false".equals(System.getProperty("osmand.routeLanes"));
	}

	public void collectRouteMembership(Relation relation, OsmDbAccessorContext ctx) throws SQLException {
		if (!isLaneAssignmentEnabled()) {
			return;
		}
		if (!"route".equals(relation.getTag("type"))
				|| !isSupportedRouteType(relation.getTag(Amenity.ROUTE))
				|| !LANE_ROUTE_TYPES.contains(relation.getTag(Amenity.ROUTE))) {
			return;
		}
		ctx.loadEntityRelation(relation); // members are not loaded for us
		appearanceByRoute.put(relation.getId(), routeAppearance(relation.getTags()));
		for (Relation.RelationMember member : relation.getMembers()) {
			if (member.getEntityId() != null && member.getEntityId().getType() == Entity.EntityType.WAY) {
				long wayId = member.getEntityId().getId();
				routesByWay.computeIfAbsent(wayId, k -> new ArrayList<>()).add(relation.getId());
				if (member.getEntity() instanceof Way way && way.getNodeIds().size() >= 2) {
					TLongArrayList ids = way.getNodeIds();
					endNodesByWay.put(wayId, new long[] { ids.get(0), ids.get(ids.size() - 1) });
				}
			}
		}
	}

	/**
	 * Assigns lanes per corridor instead of per way.
	 * <p>
	 * A corridor is a maximal set of ways that carry the same routes and hang together end to end.
	 * Ranking route by route on each way separately makes a route hop sideways wherever the set of
	 * its neighbours changes - measured on the High Tatras, a quarter of all routes took more than
	 * one offset along their length, and one took four. Within a corridor the set is constant by
	 * construction, so the lane cannot change; between corridors a route keeps the lane it already
	 * had wherever that lane is still free, which is what stops the remaining hops at junctions.
	 */
	private void buildLanes() {
		laneByWay = new LinkedHashMap<>();
		sideLaneByWay = new LinkedHashMap<>();
		forwardByWay = new LinkedHashMap<>();

		// group ways by the set of routes on them, then split each group into connected corridors
		Map<String, List<Long>> waysBySignature = new LinkedHashMap<>();
		for (Map.Entry<Long, List<Long>> entry : routesByWay.entrySet()) {
			waysBySignature.computeIfAbsent(signatureOf(entry.getValue()), k -> new ArrayList<>())
					.add(entry.getKey());
		}

		List<List<Long>> corridors = new ArrayList<>();
		for (List<Long> group : waysBySignature.values()) {
			corridors.addAll(splitIntoConnected(group));
		}
		// largest first: long trails claim their lane before short spurs do
		corridors.sort((a, b) -> b.size() - a.size());

		for (List<Long> corridor : corridors) {
			List<String> present = appearancesOn(corridor.get(0));
			Map<String, Integer> assigned = new LinkedHashMap<>();
			// Centred on the path: 2*index-(n-1), so a lone route is at 0 and a bundle straddles
			// the path evenly. Where the lane changes between corridors the renderer slides the
			// line across instead of jumping, so recentring costs nothing visually now.
			Map<String, Integer> stacked = new LinkedHashMap<>();
			for (int index = 0; index < present.size(); index++) {
				assigned.put(present.get(index), 2 * index - (present.size() - 1));
				stacked.put(present.get(index), index);
			}
			for (Long wayId : corridor) {
				laneByWay.put(wayId, assigned);
				sideLaneByWay.put(wayId, stacked);
			}
			orientCorridor(corridor);
		}
	}

	/**
	 * Picks one direction for a whole corridor and records, per way, whether its own node order
	 * already follows it. The renderer takes the offset side from the direction of the geometry it
	 * draws, and 21% of consecutive member ways in OSM are stored reversed relative to their
	 * neighbour - without a shared orientation the line jumps to the other side of the path at
	 * every one of those joints, which is what made the one-sided mode unusable.
	 */
	private void orientCorridor(List<Long> corridor) {
		Map<Long, List<Long>> waysByNode = new LinkedHashMap<>();
		for (Long wayId : corridor) {
			long[] ends = endNodesByWay.get(wayId);
			if (ends != null) {
				waysByNode.computeIfAbsent(ends[0], k -> new ArrayList<>()).add(wayId);
				waysByNode.computeIfAbsent(ends[1], k -> new ArrayList<>()).add(wayId);
			}
		}
		Set<Long> visited = new HashSet<>();
		for (Long start : corridor) {
			if (!visited.add(start) || !endNodesByWay.containsKey(start)) {
				continue;
			}
			forwardByWay.put(start, Boolean.TRUE);
			Deque<Long> queue = new ArrayDeque<>();
			queue.add(start);
			while (!queue.isEmpty()) {
				Long wayId = queue.poll();
				long[] ends = endNodesByWay.get(wayId);
				boolean forward = forwardByWay.getOrDefault(wayId, Boolean.TRUE);
				long head = forward ? ends[0] : ends[1];
				long tail = forward ? ends[1] : ends[0];
				for (long node : new long[] { head, tail }) {
					for (Long other : waysByNode.getOrDefault(node, Collections.emptyList())) {
						long[] otherEnds = endNodesByWay.get(other);
						if (otherEnds == null || !visited.add(other)) {
							continue;
						}
						// keep its own order when it continues the corridor, flip it otherwise
						boolean sameDirection = (node == tail && otherEnds[0] == node)
								|| (node == head && otherEnds[1] == node);
						forwardByWay.put(other, sameDirection);
						queue.add(other);
					}
				}
			}
		}
	}

	/** Ways too short to carry a visible offset without turning into a sideways stub. */
	private static boolean isTooShortForLane(Way way) {
		List<Node> nodes = way.getNodes();
		if (nodes.size() < 2) {
			return true;
		}
		double length = 0;
		for (int i = 1; i < nodes.size(); i++) {
			length += MapUtils.getDistance(nodes.get(i - 1).getLatLon(), nodes.get(i).getLatLon());
			if (length >= MIN_LANE_LENGTH_M) {
				return false;
			}
		}
		return true;
	}

	/** True when the way should be emitted in its own node order for lanes to line up. */
	private boolean isForwardOnWay(long wayId) {
		if (laneByWay == null) {
			buildLanes();
		}
		return forwardByWay.getOrDefault(wayId, Boolean.TRUE);
	}

	/** A copy of the way with its geometry reversed; tags and id are kept. */
	private static Way reversedWay(Way way) {
		List<Node> nodes = new ArrayList<>(way.getNodes());
		Collections.reverse(nodes);
		Way copy = new Way(way.getId(), nodes);
		copy.replaceTags(way.getTags());
		return copy;
	}

	private String signatureOf(List<Long> routeIds) {
		return new TreeSet<>(routeIds).toString();
	}

	/** Distinct appearances on a way, ordered by relation id so builds agree. */
	private List<String> appearancesOn(long wayId) {
		List<String> distinct = new ArrayList<>();
		for (Long id : new TreeSet<>(routesByWay.getOrDefault(wayId, Collections.emptyList()))) {
			String appearance = appearanceByRoute.get(id);
			if (appearance != null && !distinct.contains(appearance)) {
				distinct.add(appearance);
			}
		}
		return distinct;
	}

	/** Splits ways carrying the same routes into pieces that actually touch each other. */
	private List<List<Long>> splitIntoConnected(List<Long> ways) {
		Map<Long, List<Long>> waysByNode = new LinkedHashMap<>();
		for (Long wayId : ways) {
			long[] ends = endNodesByWay.get(wayId);
			if (ends != null) {
				waysByNode.computeIfAbsent(ends[0], k -> new ArrayList<>()).add(wayId);
				waysByNode.computeIfAbsent(ends[1], k -> new ArrayList<>()).add(wayId);
			}
		}
		List<List<Long>> components = new ArrayList<>();
		Set<Long> seen = new HashSet<>();
		for (Long start : ways) {
			if (!seen.add(start)) {
				continue;
			}
			List<Long> component = new ArrayList<>();
			Deque<Long> queue = new ArrayDeque<>();
			queue.add(start);
			while (!queue.isEmpty()) {
				Long wayId = queue.poll();
				component.add(wayId);
				long[] ends = endNodesByWay.get(wayId);
				if (ends == null) {
					continue;
				}
				for (long node : ends) {
					for (Long neighbour : waysByNode.getOrDefault(node, Collections.emptyList())) {
						if (seen.add(neighbour)) {
							queue.add(neighbour);
						}
					}
				}
			}
			components.add(component);
		}
		return components;
	}

	/**
	 * How the route will be drawn. Routes agreeing on this are indistinguishable on the map unless
	 * they carry different shields, so they share one lane instead of being drawn twice.
	 */
	private static String routeAppearance(Map<String, String> tags) {
		String symbol = tags.get(OSMC_SYMBOL);
		String color = symbol != null ? symbol.split(":")[0].trim()
				: Algorithms.isEmpty(tags.get(COLOUR)) ? tags.get(COLOR) : tags.get(COLOUR);
		String route = tags.get(Amenity.ROUTE);
		String group = "bicycle".equals(route) || "mtb".equals(route) ? "cycling" : "foot";
		return group + "|" + (color == null ? "" : color.toLowerCase());
	}

	/**
	 * Lane of this route on this way: its rank among the distinct appearances present, ordered by
	 * relation id so the result is stable between builds.
	 * <p>
	 * Lanes grow to one side instead of being centred on the path. Centring looks tidier on a
	 * single way but recentres wherever the number of routes changes, so a route hops sideways at
	 * every way boundary - measured on Krkonoše, one blue route took lanes +1, +2, +4, -1, -2 and
	 * -3 along one trail. Ranking from a fixed side keeps a route still unless the set of
	 * lower-ranked routes beside it actually changes, and leaves a solitary route on its path.
	 * <p>
	 * The renderer derives the offset side from the geometry direction, so the index is relative to
	 * the way's own node order and the way must not be reversed when emitted.
	 */
	private int laneOnWay(long wayId, long relationId) {
		List<Long> routes = routesByWay.get(wayId);
		if (routes == null || routes.size() < 2) {
			return 0;
		}
		if (laneByWay == null) {
			buildLanes();
		}
		Map<String, Integer> lanes = laneByWay.get(wayId);
		Integer lane = lanes == null ? null : lanes.get(appearanceByRoute.get(relationId));
		return lane == null ? 0 : lane;
	}

	/** More than one route of the same appearance here, so the lane stands for several routes. */
	/** Same order as the centred lane, but stacked to one side of the path, 0 being on it. */
	private int sideLaneOnWay(long wayId, long relationId) {
		List<Long> routes = routesByWay.get(wayId);
		if (routes == null || routes.size() < 2) {
			return 0;
		}
		if (laneByWay == null) {
			buildLanes();
		}
		Map<String, Integer> lanes = sideLaneByWay.get(wayId);
		Integer lane = lanes == null ? null : lanes.get(appearanceByRoute.get(relationId));
		return lane == null ? 0 : lane;
	}

	private boolean sharedLaneOnWay(long wayId, long relationId) {
		List<Long> routes = routesByWay.get(wayId);
		if (routes == null || routes.size() < 2) {
			return false;
		}
		String own = appearanceByRoute.get(relationId);
		int sameLooking = 0;
		for (Long id : routes) {
			if (own != null && own.equals(appearanceByRoute.get(id))) {
				sameLooking++;
			}
		}
		return sameLooking > 1;
	}

	public void iterateRelation(Relation relation, OsmDbAccessorContext ctx, IndexCreationContext icc)
			throws SQLException {
		if (!isSupportedRouteType(relation.getTag(Amenity.ROUTE))) {
			return;
		}
		if ("proposed".equals(relation.getTag("state")) || "yes".equals(relation.getTag("proposed"))) {
			return;
		}
		if ("route".equals(relation.getTag("type"))) {
			List<Way> joinedWays = new ArrayList<>();
			List<Node> pointsForPoiSearch = new ArrayList<>();
			List<Node> pointsOfRelationNodes = new ArrayList<>();
			Map<String, String> preparedTags = new LinkedHashMap<>();

			TLongHashSet geometryBeforeCompletion = new TLongHashSet();
			fillRelationWaysGeometrySet(relation, geometryBeforeCompletion);

			OverpassFetcher.getInstance().fetchCompleteGeometryRelation(relation, ctx, lastModifiedDate);

			int hash = getRelationHash(relation);
			if (hash == -1) {
				log.error(String.format("Route relation %d has no usable geometry", relation.getId()));
				return;
			}

			if (COLLECT_OSM_ROUTE_RELATION_NODES) {
				collectOsmRouteRelationNodes(relation, pointsOfRelationNodes);
			}
			Map<Long, int[]> laneByJoinedWay = new LinkedHashMap<>();
			collectJoinedWaysAndShieldTags(relation, joinedWays, preparedTags, hash, laneByJoinedWay);
			calcRadiusDistanceAndPoiSearchPoints(relation.getId(), joinedWays, pointsForPoiSearch, preparedTags, hash);

			Map<String, String> mapSectionTags = new LinkedHashMap<>();
			Map<String, String> poiSectionTags = new LinkedHashMap<>();
			collectElevationStatsForWays(joinedWays, preparedTags, icc);
			collectMapAndPoiSectionTags(relation, preparedTags, mapSectionTags, poiSectionTags);

			for (Way way : joinedWays) {
				for (Node node : way.getNodes()) {
					if (geometryBeforeCompletion.contains(getNodeLongId(node))) {
						int[] lane = laneByJoinedWay.get(way.getId());
						if (lane == null || (lane[0] == 0 && lane[1] == 0 && lane[2] == 0 && lane[3] == 0)) {
							way.replaceTags(mapSectionTags);
						} else {
							Map<String, String> tags = new LinkedHashMap<>(mapSectionTags);
							boolean ramp = lane[2] != lane[0];
							if (lane[0] != 0 || ramp) {
								tags.put(ROUTE_LANE, String.valueOf(lane[0]));
							}
							if (ramp) {
								tags.put(ROUTE_LANE_END, String.valueOf(lane[2]));
							}
							if (lane[3] != 0) {
								tags.put(ROUTE_LANE_SIDE, String.valueOf(lane[3]));
							}
							if (lane[1] == 1) {
								tags.put(ROUTE_SHARED_LANE, "yes");
							}
							way.replaceTags(tags);
						}
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
			if (COLLECT_OSM_ROUTE_RELATION_NODES) {
				for (Node node : pointsOfRelationNodes) {
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

	private void applyShieldTagsBySymbolOrActivity(Map<String, String> shieldTags, Map<String, String> relationTags) {
		String routeType = relationTags.get(ROUTE);
		if (routeType == null || shieldTags.containsKey(SHIELD_FG) || shieldTags.containsKey(SHIELD_BG)) {
			return; // shield is already calculated based on Ways of v1 routes
		}

		String osmcSymbol = relationTags.get(OSMC_SYMBOL);
		if (osmcSymbol != null) {
			Map<String, String> osmcTags = renderingTypes.transformOsmcAndColorTags(Map.of(OSMC_SYMBOL, osmcSymbol));
			for (String tag : osmcTags.keySet()) {
				for (String match : OSMC_TAGS_TO_SHIELD_PROPS.keySet()) {
					if (tag.equals(match)) {
						final String key = OSMC_TAGS_TO_SHIELD_PROPS.get(match);
						final String prefix =
								(SHIELD_BG_ICONS.contains(key) || SHIELD_FG_ICONS.contains(key)) ? OSMC_ICON_PREFIX : "";
						final String suffix = SHIELD_BG_ICONS.contains(key) ? OSMC_ICON_BG_SUFFIX : "";
						final String val = prefix + osmcTags.get(tag) + suffix;
						shieldTags.putIfAbsent(key, val);
					}
				}
			}
			if (shieldTags.containsKey(SHIELD_FG) || shieldTags.containsKey(SHIELD_BG)) {
				return; // got shield based on osmc:symbol
			}
		}

		RouteActivity activity = routeActivityHelper.findActivityByTag(routeType);
		if (activity != null && !Algorithms.isEmpty(activity.getIconName())) {
			String color = NO_SYMBOL_ROUTE_SHIELD_COLORS.get(routeType);
			if (color == null) {
				color = NO_SYMBOL_ROUTE_SHIELD_COLORS.get("default");
			}
			shieldTags.put(SHIELD_BG, "osmc_" + color + "_bg");
			shieldTags.put(SHIELD_FG, activity.getIconName());
		}
	}

	protected void applyActivityMapShieldToClickableWay(Map<String, String> tags, boolean applyOnNamelessOnly) {
		if (applyOnNamelessOnly) {
			for (String nameTag : ClickableWayTags.REQUIRED_TAGS_ANY) {
				if (tags.containsKey(nameTag)) {
					return;
				}
			}
		}
		RouteActivity activity = null;
		for (String clickableTagValue : ClickableWayTags.CLICKABLE_TAGS) {
			String[] tagValue = clickableTagValue.split("=");
			boolean found = tagValue.length < 2
					? tags.containsKey(clickableTagValue) // tag or tag:value
					: tagValue[1].equals(tags.get(tagValue[0])); // tag=value (snowmobile=yes)
			if (found) {
				activity = routeActivityHelper.findActivityByTag(clickableTagValue);
				if (activity != null) {
					break;
				}
			}
		}
		if (activity != null && !Algorithms.isEmpty(activity.getIconName())) {
			String color = ClickableWayTags.getGpxColorByTags(tags);
			if (color == null) {
				color = DEFAULT_CLICKABLE_WAY_ACTIVITY_COLOR;
			}
			tags.put(SHIELD_BG, "osmc_" + color + "_bg");
			tags.put(SHIELD_FG, activity.getIconName());
			tags.put(SHIELD_STUB_NAME, ".");
		}
	}

	protected void collectElevationStatsForWays(List<Way> ways, Map<String, String> tags, IndexCreationContext icc) {
		int eleCount = 0;
		double distance = 0;
		double upHill = 0, downHill = 0, sumEle = 0;
		double minEle = Double.POSITIVE_INFINITY, maxEle = Double.NEGATIVE_INFINITY;

		if (icc.getIndexHeightData() != null) {
			for (Way way : ways) {
                if (way.getNodes().isEmpty()) {
                    continue;
                }
				IndexHeightData.WayGeneralStats wg = icc.getIndexHeightData()
						.calculateWayGeneralStats(way, IndexRouteRelationCreatorV1.DIST_STEP);
				if (wg.eleCount > 0) {
					upHill += wg.up;
					downHill += wg.down;
					minEle = Math.min(minEle, wg.minEle);
					maxEle = Math.max(maxEle, wg.maxEle);
					eleCount += wg.eleCount;
					sumEle += wg.sumEle;
					distance += wg.dist;
				}
			}
		}

		if (eleCount > 0) {
			tags.put("min_ele", String.valueOf((int) minEle));
			tags.put("max_ele", String.valueOf((int) maxEle));
			tags.put("diff_ele_up", String.valueOf((int) upHill));
			tags.put("diff_ele_down", String.valueOf((int) downHill));
			tags.put("avg_ele", String.valueOf((int) (sumEle / eleCount)));
			tags.putIfAbsent("distance", distanceKmFormat.format(distance / 1000.0));
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
			tagsToFill.put("distance", distanceKmFormat.format(distance / 1000.0));
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

		if ("node_network".equals(commonTags.get("network:type")) && commonTags.containsKey("network")) {
			mapSectionTags.putIfAbsent("node_network", commonTags.get("network"));
		}

		if (DEBUG_GENERATE_ROUTE_SEGMENT) {
			mapSectionTags.put(ROUTE, "segment"); // enable to debug as TravelGpx data
		}
		// mapSectionTags.remove(ROUTE_TYPE); // avoid creation of POI-data when indexing Ways

		poiSectionTags.remove(TRACK_COLOR); // track_color is required for Rendering only
		poiSectionTags.remove(ROUTE); // see also OsmGpxWriteContext.alwaysExtraTags
	}

	public static void finalizeRouteShieldTags(Map<String, String> tags) {
		if (tags.containsKey(SHIELD_FG) || tags.containsKey(SHIELD_BG)) {
			tags.put(SHIELD_STUB_NAME, ".");
		}
		if (tags.containsKey(SHIELD_TEXT)) {
			tags.remove(SHIELD_STUB_NAME);
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

	private void collectOsmRouteRelationNodes(Relation relation, List<Node> pointsOfRelationNodes) {
		for (Relation.RelationMember member : relation.getMembers()) {
			if (member.getEntity() instanceof Node node) {
				boolean allowThisNode = true;
				for (Map.Entry<String, String> skip : SKIP_RELATION_NODE_BY_TAGS.entrySet()) {
					if (skip.getValue().equals(node.getTag(skip.getKey()))) {
						allowThisNode = false;
						break;
					}
				}
				if (allowThisNode) {
					Node routeTrackPoint = new Node(node.getLatitude(), node.getLongitude(), node.getId());

					final Map<String, String> transformedTags = renderingTypes.transformTags(node.getTags(),
							Entity.EntityType.NODE, MapRenderingTypesEncoder.EntityConvertApplyType.MAP);
					String gpxIcon = searchRequest.searchIconByTags(transformedTags);

					if (gpxIcon != null) {
						Map<String, String> combinedTags = new LinkedHashMap<>(transformedTags);

						Map<String, String> importantRelationTags = new LinkedHashMap<>(relation.getTags());
						importantRelationTags.keySet().retainAll(Set.of(OPERATOR));
						combinedTags.putAll(importantRelationTags);

						Map<String, String> directlyPassedTags = new LinkedHashMap<>(transformedTags);
						directlyPassedTags.keySet().retainAll(Set.of(NAME, DESCRIPTION));

						routeTrackPoint.putTag(ROUTE_ID_TAG, Amenity.ROUTE_ID_OSM_PREFIX + relation.getId());
						routeTrackPoint.putTag(WPT_EXTRA_TAGS, gson.toJson(combinedTags));
						routeTrackPoint.getModifiableTags().putAll(directlyPassedTags);
						routeTrackPoint.putTag(ROUTE_TYPE, "track_point");
						routeTrackPoint.putTag("icon", gpxIcon);

						pointsOfRelationNodes.add(routeTrackPoint);
					}
				}
			}
		}
	}

	private void collectJoinedWaysAndShieldTags(@Nonnull Relation relation,
	                                            @Nonnull List<Way> joinedWays,
	                                            @Nonnull Map<String, String> shieldTags, int hash,
	                                            @Nonnull Map<Long, int[]> laneByJoinedWay) {
		List<Way> waysToJoin = new ArrayList<>();
		List<Way> sharedWays = new ArrayList<>();

		for (Relation.RelationMember member : relation.getMembers()) {
			if (member.getEntity() instanceof Way way) {
				if ("yes".equals(way.getTag(OSMTagKey.AREA))) {
					continue; // skip (eg https://www.openstreetmap.org/way/746544031)
				}
				if (laneOnWay(way.getId(), relation.getId()) != 0
						|| sideLaneOnWay(way.getId(), relation.getId()) != 0
						|| sharedLaneOnWay(way.getId(), relation.getId())) {
					if (!isForwardOnWay(way.getId()) && way.getNodes().size() >= 2) {
						way = reversedWay(way);
					}
					// Carries other routes too. Splicing would reorient it to build a chain, and the
					// renderer takes the offset side from the geometry direction, so it is emitted
					// on its own in the corridor's orientation - the same one every other route
					// here uses, which is what keeps them on consistent sides of the path.
					sharedWays.add(way);
				} else {
					waysToJoin.add(way);
				}
				transformer.addPropogatedTags(renderingTypes,
						MapRenderingTypesEncoder.EntityConvertApplyType.MAP, way, way.getModifiableTags());
				shieldTags.putAll(getShieldTagsFromOsmcTags(way.getTags(), relation.getId()));
			}
		}
		applyShieldTagsBySymbolOrActivity(shieldTags, relation.getTags());

		// A way whose lane differs from the one before it on this route is the ramp: it carries the
		// previous lane at its start and its own at the end, and the renderer slides between them.
		Map<Long, Integer> rampFrom = new LinkedHashMap<>();
		Integer previousLane = null;
		for (Relation.RelationMember member : relation.getMembers()) {
			if (!(member.getEntity() instanceof Way way)) {
				continue;
			}
			int lane = laneOnWay(way.getId(), relation.getId());
			// no ramp on a short way: sliding several pixels sideways over a few metres of line
			// draws a spike, not a transition
			if (previousLane != null && previousLane != lane && !isTooShortForLane(way)) {
				rampFrom.put(way.getId(), previousLane);
			}
			previousLane = lane;
		}

		List<Way> plainToJoin = new ArrayList<>();
		List<Way> rampWays = new ArrayList<>();
		for (Way way : waysToJoin) {
			(rampFrom.containsKey(way.getId()) ? rampWays : plainToJoin).add(way);
		}
		spliceWaysIntoSegments(plainToJoin, joinedWays, relation.getId(), hash);

		List<List<Way>> chains = new ArrayList<>();
		for (Way way : rampWays) {
			chains.add(Collections.singletonList(way));
		}
		for (List<Way> chain : chainSharedWays(sharedWays, relation.getId())) {
			// a chain that starts on a ramp gets that first way split off, so the ramp stays short
			if (chain.size() > 1 && rampFrom.containsKey(chain.get(0).getId())) {
				chains.add(Collections.singletonList(chain.get(0)));
				chains.add(chain.subList(1, chain.size()));
			} else {
				chains.add(chain);
			}
		}
		for (List<Way> chain : chains) {
			int before = joinedWays.size();
			spliceWaysIntoSegments(chain, joinedWays, relation.getId(), hash);
			Way first = chain.get(0);
			int lane = laneOnWay(first.getId(), relation.getId());
			int shared = sharedLaneOnWay(first.getId(), relation.getId()) ? 1 : 0;
			int from = rampFrom.getOrDefault(first.getId(), lane);
			int side = sideLaneOnWay(first.getId(), relation.getId());
			for (int i = before; i < joinedWays.size(); i++) {
				laneByJoinedWay.put(joinedWays.get(i).getId(), new int[] { from, shared, lane, side });
			}
		}
	}

	/**
	 * Groups shared ways into chains that may safely be spliced into one object: same lane, same
	 * shared flag, and connected tail-to-head in their own node order, so nothing gets reversed and
	 * every route on the underlying path still agrees on which side is which. Without this a trail
	 * cut into many OSM ways becomes many tiny objects, which renders as stubs and restarts the
	 * dash pattern at every one of them.
	 */
	private List<List<Way>> chainSharedWays(List<Way> sharedWays, long relationId) {
		List<List<Way>> chains = new ArrayList<>();
		Map<Long, Way> byHeadNode = new LinkedHashMap<>();
		List<Way> remaining = new ArrayList<>(sharedWays);

		for (Way way : remaining) {
			if (!way.getNodeIds().isEmpty()) {
				byHeadNode.put(way.getNodeIds().get(0), way);
			}
		}
		Set<Long> used = new HashSet<>();
		for (Way way : remaining) {
			if (used.contains(way.getId()) || way.getNodeIds().isEmpty()) {
				continue;
			}
			List<Way> chain = new ArrayList<>();
			Way current = way;
			while (current != null && used.add(current.getId())) {
				chain.add(current);
				TLongArrayList ids = current.getNodeIds();
				Way next = byHeadNode.get(ids.get(ids.size() - 1));
				boolean sameSlot = next != null && !used.contains(next.getId())
						&& laneOnWay(next.getId(), relationId) == laneOnWay(current.getId(), relationId)
						&& sharedLaneOnWay(next.getId(), relationId) == sharedLaneOnWay(current.getId(), relationId);
				current = sameSlot ? next : null;
			}
			chains.add(chain);
		}
		return chains;
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
		// The hash only spreads the generated object ids (6 bits of calcEntityIdFromRelationId); it
		// has nothing to do with shields, names or geometry. Refusing to emit anything when a single
		// member way failed to resolve threw away whole long-distance routes - Szlak Warowni
		// Jurajskich, 510 member ways, disappeared because a handful never came back from Overpass.
		// Fall back to a hash of what did resolve and keep the parts we have.
		List<Node> allNodes = new ArrayList<>();
		int unresolved = 0;
		for (Relation.RelationMember member : relation.getMembers()) {
			if (member.getEntity() instanceof Node node) {
				allNodes.add(node);
			}
			if (member.getEntity() instanceof Way way) {
				if (way.getNodes().isEmpty()) {
					unresolved++;
					continue;
				}
				allNodes.addAll(way.getNodes());
			}
		}
		if (allNodes.isEmpty()) {
			return -1; // nothing at all to draw
		}
		if (unresolved > 0) {
			log.warn(String.format("Route relation %d is partial: %d member ways unresolved",
					relation.getId(), unresolved));
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

		boolean keepWayDirection = "yes".equals(candidate.getTag("oneway"))
				|| "downhill".equals(candidate.getTag("piste:type"))
				|| "yes".equals(candidate.getTag("piste:oneway"));

		LatLon firstNodeLL = result.get(0).getLatLon();
		LatLon lastNodeLL = result.get(result.size() - 1).getLatLon();
		LatLon firstCandidateLL = candidate.getNodes().get(0).getLatLon();
		LatLon lastCandidateLL = candidate.getNodes().get(candidate.getNodes().size() - 1).getLatLon();

		if (MapUtils.areLatLonEqual(lastNodeLL, firstCandidateLL)) {
			addWayToNodes(result, false, candidate, false); // result + Candidate
		} else if (!keepWayDirection && MapUtils.areLatLonEqual(lastNodeLL, lastCandidateLL)) {
			addWayToNodes(result, false, candidate, true); // result + etadidnaC
		} else if (!keepWayDirection && MapUtils.areLatLonEqual(firstNodeLL, firstCandidateLL)) {
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
		Map<String, String> result = new LinkedHashMap<>();
		if (relationId != 0) {
			boolean relationPrefixFound = false;
			for (String tag : tags.keySet()) {
				if (tag.endsWith(RELATION_ID) && tags.get(tag).equals(Long.toString(relationId))) {
					// mandatory prefix of this relation to catch tags from the distinct group
					requiredGroupPrefix = tag.replace(RELATION_ID, "");
					relationPrefixFound = true;
					break;
				}
			}
			if (!relationPrefixFound) {
				return result; // empty
			}
		}
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

	public Map<String, String> addClickableWayTags(IndexCreationContext icc, Entity entity,
	                                               Map<String, String> tags, boolean collectElevationMetrics) {
		if (entity instanceof Way way && ClickableWayTags.isClickableWayTags(SHIELD_STUB_NAME, tags)) {
			tags = new LinkedHashMap<>(tags);
			applyActivityMapShieldToClickableWay(tags, false);
			if (collectElevationMetrics) {
				collectElevationStatsForWays(List.of(way), tags, icc);
			}
		}
		return tags;
	}
}
