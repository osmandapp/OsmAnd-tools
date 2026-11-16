package net.osmand.tester;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.ObfConstants;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.router.GeneralRouter;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.router.RoutingContext;
import net.osmand.router.TurnType;
import net.osmand.util.MapUtils;

/**
 * Turn-lane datasets for /admin/turn-lanes: picks junctions of an obf, routes drives through them and records
 * every instruction ("turn:lanes", "[MUTE] " prefix when not spoken). The recorded values are what OsmAnd says
 * today - a regression net, not the correct answer.
 *
 * <pre>
 * generate(obf)
 *   1. Graph.read          the roads the car router drives on
 *   2. pickJunctions       candidates spread over the map, biggest roads first
 *   3. recordDrives        for each junction until enough drives:
 *        new Junction        its cluster of nodes and its arms (start and end points)
 *        junction.pairs      way in x way out, sharpest turn first
 *        drive               route it, drop loops and detours, read the instructions
 * </pre>
 */
public class GenerateTurnLanesTest {

	// ================================================================== constants
	public static final List<String> HIGHWAYS = List.of("motorway", "trunk", "primary", "secondary", "tertiary");
	public static final int MEMORY_ROUTING_LIMIT_MB = 512;
	public static final String[] CSV_HEADER = {"num", "name", "start", "end", "segment", "expected", "left_side", "obf",
			"point"};
	private static final int TRUNK = HIGHWAYS.indexOf("trunk"), PRIMARY = HIGHWAYS.indexOf("primary"),
			TERTIARY = HIGHWAYS.indexOf("tertiary"), SMALLER = HIGHWAYS.size();
	private static final Set<String> NOT_JUNCTION = Set.of("service", "track", "road");
	private static final double RESERVE_MULTIPLY = 2; // routes can be rejected, so need reserve
	private static final double CELL_DEGREES = 0.006; // for pickJunctions

	// ================================================================== public API

	public static class Options {
		public int junctions = 1000; // drives to record per obf, "Junctions" on the admin page
		public int perJunction = 2; // for admin page, count of drives per junction
		public String profile = "car";
		public boolean linksOnly;
		public boolean anyTurn;
		public boolean noneLanes;
		public String regionsPath;
		public String highway = "secondary";

		int lowest() {
			int i = HIGHWAYS.indexOf(highway);
			if (i < 0) {
				throw new IllegalArgumentException("Highway level: one of " + HIGHWAYS);
			}
			return i;
		}
	}

	public interface Progress {
		void drives(int done, int total, int points);

		boolean isCancelled();
	}

	public static class Case {
		public final String name;
		public final LatLon start;
		public final LatLon end;
		public final String map;
		public final boolean leftSide;
		public final Map<String, String> results;
		public final Map<String, LatLon> points;

		Case(String name, LatLon start, LatLon end, String map, boolean leftSide, Map<String, String> results,
				Map<String, LatLon> points) {
			this.name = name;
			this.start = start;
			this.end = end;
			this.map = map;
			this.leftSide = leftSide;
			this.results = results;
			this.points = points;
		}
	}

	public static class Replay {
		public final Map<String, String> instructions;
		public final Set<Long> roads;

		Replay(Map<String, String> instructions, Set<Long> roads) {
			this.instructions = instructions;
			this.roads = roads;
		}
	}

	private final Options options;
	private final Progress progress;

	public GenerateTurnLanesTest(Options options, Progress progress) {
		this.options = options;
		this.progress = progress;
	}

	public List<Case> generate(File obf, OsmandRegions regions) throws IOException, InterruptedException {
		Graph graph = Graph.read(obf, routingConfig().router);
		List<Long> centres = pickJunctions(graph);
		return recordDrives(obf, graph, centres, regions);
	}

	/** the roads tell "instruction gone" from "drive went elsewhere" in a compare */
	public Replay replay(BinaryMapIndexReader[] readers, LatLon from, LatLon to, boolean leftSide)
			throws IOException, InterruptedException {
		List<RouteSegmentResult> route = searchRoute(readers, from, to, leftSide);
		if (route == null || route.isEmpty()) {
			return null;
		}
		Set<Long> roads = new HashSet<>();
		for (RouteSegmentResult segment : route) {
			roads.add(ObfConstants.getOsmObjectId(segment.getObject()));
		}
		return new Replay(instructions(route, null), roads);
	}

	/** @return the next drive number */
	public static int writeCsv(List<Case> cases, Appendable out, int firstNum) throws IOException {
		int num = firstNum;
		for (Case c : cases) {
			for (Map.Entry<String, String> e : c.results.entrySet()) {
				LatLon at = c.points.get(e.getKey());
				writeCsvRow(out, String.valueOf(num), c.name, latLon(c.start), latLon(c.end), e.getKey(), e.getValue(),
						String.valueOf(c.leftSide), c.map, at == null ? "" : latLon(at));
			}
			num++;
		}
		return num;
	}

	public static void writeCsvRow(Appendable out, String... values) throws IOException {
		for (int i = 0; i < values.length; i++) {
			if (i > 0) {
				out.append(',');
			}
			String v = values[i] == null ? "" : values[i];
			if (v.indexOf(',') >= 0 || v.indexOf('"') >= 0 || v.indexOf('\n') >= 0) {
				out.append('"').append(v.replace("\"", "\"\"")).append('"');
			} else {
				out.append(v);
			}
		}
		out.append('\n');
	}

	private static String latLon(LatLon p) {
		return String.format(Locale.US, "%.6f,%.6f", p.getLatitude(), p.getLongitude());
	}

	// ================================================================== step 2: which junctions

	private List<Long> pickJunctions(Graph graph) {
		int lowest = options.lowest();
		List<long[]> candidates = new ArrayList<>();
		for (Map.Entry<Long, List<Long>> e : graph.at.entrySet()) {
			long key = e.getKey();
			if (e.getValue().size() < 2) {
				continue;
			}
			int arms = graph.arms(key);
			int rank = graph.rankOf(key);
			if (arms < 3 || rank > lowest || (options.linksOnly && !graph.hasLink(key))
					|| (options.noneLanes && !graph.hasNoneLanes(key))) {
				continue;
			}
			candidates.add(new long[] {rank, -arms, key});
		}
		candidates.sort(Comparator.<long[]>comparingLong(c -> c[0]).thenComparingLong(c -> c[1]));
		Map<Long, List<Long>> grid = new HashMap<>();
		List<Long> picked = new ArrayList<>();
		for (long[] c : candidates) {
			long key = c[2];
			if (nearPicked(grid, key, minJunctionDistance((int) c[0]))) {
				continue;
			}
			grid.computeIfAbsent(cellOf(lat(key), lon(key)), k -> new ArrayList<>()).add(key);
			picked.add(key);
			if (picked.size() >= Math.ceil(options.junctions * RESERVE_MULTIPLY)) {
				break;
			}
		}
		return picked;
	}

	private static long cellOf(double lat, double lon) {
		long i = (long) Math.floor(lat / CELL_DEGREES), j = (long) Math.floor(lon / CELL_DEGREES);
		return (i << 32) | (j & 0xffffffffL);
	}

	private static boolean nearPicked(Map<Long, List<Long>> grid, long key, double apart) {
		long gi = (long) Math.floor(lat(key) / CELL_DEGREES), gj = (long) Math.floor(lon(key) / CELL_DEGREES);
		int reach = (int) Math.ceil(apart / (CELL_DEGREES * 111000)) + 1;
		for (long i = gi - reach; i <= gi + reach; i++) {
			for (long j = gj - reach; j <= gj + reach; j++) {
				for (long other : grid.getOrDefault((i << 32) | (j & 0xffffffffL), List.of())) {
					if (metres(key, other) < apart) {
						return true;
					}
				}
			}
		}
		return false;
	}

	// ================================================================== step 3: drives through them

	private List<Case> recordDrives(File obf, Graph graph, List<Long> centres, OsmandRegions regions)
			throws IOException, InterruptedException {
		List<Case> cases = new ArrayList<>();
		int points = 0;
		BinaryMapIndexReader reader = new BinaryMapIndexReader(new RandomAccessFile(obf, "r"), obf);
		BinaryMapIndexReader[] readers = {reader};
		try {
			for (long centre : centres) {
				if (cases.size() >= options.junctions || (progress != null && progress.isCancelled())) {
					break;
				}
				if (progress != null) {
					progress.drives(cases.size(), options.junctions, points);
				}
				Junction junction = new Junction(graph, centre, regions);
				int written = 0;
				Set<Long> usedIn = new HashSet<>(), usedOut = new HashSet<>();
				for (Arm[] pair : junction.pairs()) {
					if (written >= options.perJunction || cases.size() >= options.junctions) {
						break;
					}
					Arm from = pair[0], to = pair[1];
					if (!usedIn.add(from.key) || !usedOut.add(to.key)) {
						continue;
					}
					Map<String, LatLon> at = new LinkedHashMap<>();
					Map<String, String> results = drive(readers, junction, from, to, at);
					if (results == null) {
						continue;
					}
					written++;
					cases.add(new Case(junction.name + " " + written, from.at, to.at, obf.getName(), junction.leftSide,
							results, at));
					points += results.size();
				}
			}
		} finally {
			reader.close();
		}
		if (progress != null) {
			progress.drives(cases.size(), options.junctions, points);
		}
		return cases;
	}

	private Map<String, String> drive(BinaryMapIndexReader[] readers, Junction junction, Arm from, Arm to,
			Map<String, LatLon> points) throws IOException {
		List<RouteSegmentResult> route;
		try {
			route = searchRoute(readers, from.at, to.at, junction.leftSide);
		} catch (RuntimeException | InterruptedException e) {
			return null;
		}
		if (route == null || route.isEmpty() || !passes(route, junction.cluster) || loops(route)
				|| length(route) > junction.maxRoute(from, to)) {
			return null;
		}
		if (options.noneLanes && !readsFromNone(route)) {
			return null;
		}
		Map<String, String> results = instructions(route, points);
		return results.isEmpty() || !worthKeeping(results) ? null : results;
	}

	/** a u-turn first means the start landed on the wrong side of the road */
	private boolean worthKeeping(Map<String, String> results) {
		String first = results.values().iterator().next();
		if (first.startsWith("TU") || first.startsWith("[MUTE] TU")) {
			return false;
		}
		return options.anyTurn || results.values().stream().anyMatch(v -> v.indexOf(':') >= 0);
	}

	// ------------------------------------------------------------------ route checks

	private static boolean passes(List<RouteSegmentResult> route, Set<Long> junction) {
		for (RouteSegmentResult segment : route) {
			RouteDataObject road = segment.getObject();
			for (int p = 0; p < road.getPointsLength(); p++) {
				if (junction.contains(place(road, p))) {
					return true;
				}
			}
		}
		return false;
	}

	private static boolean loops(List<RouteSegmentResult> route) {
		Set<Long> seen = new HashSet<>();
		long last = 0;
		for (RouteSegmentResult segment : route) {
			RouteDataObject road = segment.getObject();
			int start = segment.getStartPointIndex(), end = segment.getEndPointIndex();
			int step = start < end ? 1 : -1;
			for (int p = start; p != end; p += step) {
				long place = place(road, p);
				// two consecutive points of a road can share coordinates in the obf
				if (place != last && !seen.add(place)) {
					return true;
				}
				last = place;
			}
		}
		return false;
	}

	private static double length(List<RouteSegmentResult> route) {
		double length = 0;
		for (RouteSegmentResult segment : route) {
			length += segment.getDistance();
		}
		return length;
	}

	private static boolean readsFromNone(List<RouteSegmentResult> route) {
		for (int i = 1; i < route.size(); i++) {
			if (route.get(i).getTurnType() != null && hasNoneLane(turnLanesOf(route.get(i - 1)))) {
				return true;
			}
		}
		return false;
	}

	private static String turnLanesOf(RouteSegmentResult segment) {
		RouteDataObject road = segment.getObject();
		if (road.getOneway() == 0) {
			return segment.isForwardDirection() ? road.getValue("turn:lanes:forward")
					: road.getValue("turn:lanes:backward");
		}
		return road.getValue("turn:lanes");
	}

	private static boolean hasNoneLane(String turnLanes) {
		if (turnLanes == null) {
			return false;
		}
		for (String lane : turnLanes.split("\\|", -1)) {
			if (lane.isEmpty() || "none".equals(lane)) {
				return true;
			}
		}
		return false;
	}

	// ------------------------------------------------------------------ routing and instructions

	private RoutingConfiguration routingConfig() {
		RoutingMemoryLimits limits = new RoutingMemoryLimits(
				MEMORY_ROUTING_LIMIT_MB, RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		Map<String, String> params = new HashMap<>();
		params.put(options.profile, "true");
		return RoutingConfiguration.getDefault().build(options.profile, limits, params);
	}

	private List<RouteSegmentResult> searchRoute(BinaryMapIndexReader[] readers, LatLon from, LatLon to,
			boolean leftSide) throws IOException, InterruptedException {
		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();
		RoutingContext ctx = fe.buildRoutingContext(routingConfig(), null, readers,
				RoutePlannerFrontEnd.RouteCalculationMode.NORMAL);
		ctx.leftSideNavigation = leftSide;
		return fe.searchRoute(ctx, from, to, null).getList();
	}

	/**
	 * Keyed by osm id, "id:startPoint" when a road carries two instructions. Mirrored by TurnLanesRunner -
	 * keep the two in sync.
	 */
	private static Map<String, String> instructions(List<RouteSegmentResult> route, Map<String, LatLon> points) {
		Map<String, String> results = new LinkedHashMap<>();
		Map<Long, Integer> seen = new HashMap<>();
		// the first segment counts as an instruction too
		seen.put(ObfConstants.getOsmObjectId(route.get(0).getObject()), route.get(0).getStartPointIndex());
		for (int i = 1; i < route.size(); i++) {
			RouteSegmentResult segment = route.get(i);
			TurnType turn = segment.getTurnType();
			if (turn == null) {
				continue;
			}
			long id = ObfConstants.getOsmObjectId(segment.getObject());
			// no lanes: the bare maneuver, as RouteResultPreparationTest compares it
			String lanes = turn.getLanes() == null ? null : TurnType.lanesToString(turn.getLanes());
			String value = lanes == null ? turn.toXmlString()
					: (turn.isSkipToSpeak() ? "[MUTE] " : "") + turn.toXmlString() + ":" + lanes;
			Integer had = seen.put(id, segment.getStartPointIndex());
			String key = had == null ? String.valueOf(id) : id + ":" + segment.getStartPointIndex();
			if (had != null && results.containsKey(String.valueOf(id))) {
				results.put(id + ":" + had, results.remove(String.valueOf(id)));
				if (points != null) {
					points.put(id + ":" + had, points.remove(String.valueOf(id)));
				}
			}
			results.put(key, value);
			if (points != null) {
				points.put(key, segment.getStartPoint());
			}
		}
		return results;
	}

	// ================================================================== one junction

	private static class Junction {
		private final Graph graph;
		final long centre;
		final int rank;
		final Set<Long> cluster;
		final boolean leftSide;
		final String name;
		final List<Arm> in;
		final List<Arm> out;

		Junction(Graph graph, long centre, OsmandRegions regions) throws IOException {
			this.graph = graph;
			this.centre = centre;
			this.rank = graph.rankOf(centre);
			this.cluster = cluster();
			this.leftSide = leftHandAt(regions, lat(centre), lon(centre));
			this.name = name();
			this.in = arms(false);
			this.out = arms(true);
		}

		List<Arm[]> pairs() {
			List<Arm[]> pairs = new ArrayList<>();
			for (Arm from : in) {
				for (Arm to : out) {
					// same arm: the drive would not pass the junction
					if (metres(from.key, to.key) >= minArm(rank)) {
						pairs.add(new Arm[] {from, to});
					}
				}
			}
			pairs.sort(Comparator.comparingDouble((Arm[] p) -> turnOf(p[0], p[1])).reversed());
			return pairs;
		}

		/**
		 * Longest acceptable drive: the arms walked twice plus crossing the junction. Longer means the router could
		 * not take the turn the arms suggest (oneway link, turn restriction) and made a loop to reach the end.
		 */
		double maxRoute(Arm from, Arm to) {
			return 2 * (from.away + to.away + junctionRadius(rank));
		}

		private Set<Long> cluster() {
			double radius = junctionRadius(rank);
			Set<Long> found = new HashSet<>();
			List<Long> stack = new ArrayList<>();
			found.add(centre);
			stack.add(centre);
			while (!stack.isEmpty()) {
				long key = stack.remove(stack.size() - 1);
				for (long next : graph.neighbours(key, true)) {
					if (found.contains(next) || metres(centre, next) > radius || graph.arms(next) < 3) {
						continue;
					}
					found.add(next);
					stack.add(next);
				}
			}
			return found;
		}

		private List<Arm> arms(boolean forward) {
			double limit = maxArm(rank) * 1.3;
			Map<Long, Double> seen = new HashMap<>();
			Map<Long, Long> firstStep = new HashMap<>();
			Map<Long, Long> cameFrom = new HashMap<>();
			PriorityQueue<long[]> queue = new PriorityQueue<>(11, Comparator.comparingLong(x -> x[1]));
			for (long key : cluster) {
				seen.put(key, 0.0);
				queue.add(new long[] {key, 0});
			}
			Map<Long, Arm> best = new HashMap<>();
			while (!queue.isEmpty()) {
				long key = queue.poll()[0];
				double away = seen.get(key);
				if (away > limit) {
					continue;
				}
				Long arm = firstStep.get(key);
				if (arm != null && away >= minArm(rank) && away <= maxArm(rank)
						&& !graph.inTunnel(cameFrom.get(key), key)) {
					Arm cur = best.get(arm);
					if (cur == null || away > cur.away) {
						best.put(arm, new Arm(key, cameFrom.get(key), away, bearing(centre, key)));
					}
				}
				for (long next : graph.neighbours(key, forward)) {
					if (cluster.contains(next)) {
						continue;
					}
					double step = away + metres(key, next);
					if (step > limit) {
						continue;
					}
					Double had = seen.get(next);
					if (had == null || step < had - 1e-6) {
						seen.put(next, step);
						firstStep.put(next, arm != null ? arm : next);
						cameFrom.put(next, key);
						queue.add(new long[] {next, (long) step});
					}
				}
			}
			return new ArrayList<>(best.values());
		}

		/** a named or ref'd road first: motorway_link says nothing, A10 does */
		private String name() {
			RouteDataObject best = null;
			String bestCalled = null;
			int bestScore = Integer.MAX_VALUE;
			for (long key : cluster) {
				for (long p : graph.at.get(key)) {
					RouteDataObject road = graph.roads.get(roadOf(p));
					String called = called(road);
					boolean link = road.getHighway() != null && road.getHighway().endsWith("_link");
					// tertiary scored as SMALLER: keeps the names of older datasets
					int r = rank(road.getHighway());
					int score = (called == null ? 100 : 0) + (r >= TERTIARY ? SMALLER : r) * 2 + (link ? 1 : 0);
					if (score < bestScore) {
						bestScore = score;
						best = road;
						bestCalled = called;
					}
				}
			}
			if (best == null) {
				return "junction " + Math.abs(centre % 100000);
			}
			return (bestCalled == null ? best.getHighway() : bestCalled) + " " + ObfConstants.getOsmObjectId(best);
		}

		private static String called(RouteDataObject road) {
			String name = road.getName();
			if (name != null && !name.isEmpty()) {
				return name;
			}
			String ref = road.getRef(null, false, true);
			return ref == null || ref.isEmpty() ? null : ref;
		}

		private static boolean leftHandAt(OsmandRegions regions, double lat, double lon) throws IOException {
			if (regions == null) {
				return false;
			}
			for (BinaryMapDataObject o : regions.getRegionsToDownload(lat, lon)) {
				for (WorldRegion r = regions.getRegionData(regions.getFullName(o)); r != null; r = r.getSuperregion()) {
					String value = r.getParams().getRegionLeftHandDriving();
					if (value != null) {
						return "yes".equals(value) || "true".equals(value);
					}
				}
			}
			return false;
		}

		private static double turnOf(Arm from, Arm to) {
			return Math.abs(MapUtils.degreesDiff(from.bearing + 180, to.bearing));
		}

		private static double bearing(long from, long to) {
			return Math.toDegrees(Math.atan2(lon(to) - lon(from), lat(to) - lat(from)));
		}
	}

	/**
	 * One way out of a junction. The route point is the middle of the last step, not a road node: a start on a
	 * shared node can attach to any road there and give a zero-length first segment.
	 */
	private static class Arm {
		final long key;
		final double away;
		final double bearing;
		final LatLon at;

		Arm(long key, long before, double away, double bearing) {
			this.key = key;
			this.away = away;
			this.bearing = bearing;
			this.at = new LatLon((lat(key) + lat(before)) / 2, (lon(key) + lon(before)) / 2);
		}
	}

	// ================================================================== step 1: the road graph

	/** place = a point's 31-bit coordinates, pin = road index and point index of a road at that place */
	private static class Graph {
		final List<RouteDataObject> roads = new ArrayList<>();
		final Map<Long, List<Long>> at = new HashMap<>();

		static Graph read(File file, GeneralRouter router) throws IOException {
			Graph graph = new Graph();
			BinaryMapIndexReader reader = new BinaryMapIndexReader(new RandomAccessFile(file, "r"), file);
			try {
				for (RouteRegion region : reader.getRoutingIndexes()) {
					SearchRequest<RouteDataObject> request = BinaryMapIndexReader.buildSearchRouteRequest(
							0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
					List<RouteSubregion> subregions = reader.searchRouteIndexTree(request, region.getSubregions());
					reader.loadRouteIndexData(subregions, new ResultMatcher<RouteDataObject>() {
						@Override
						public boolean publish(RouteDataObject road) {
							if (road.getHighway() != null && road.getPointsLength() >= 2 && router.acceptLine(road)) {
								graph.roads.add(road);
							}
							return false;
						}

						@Override
						public boolean isCancelled() {
							return false;
						}
					});
				}
			} finally {
				reader.close();
			}
			for (int r = 0; r < graph.roads.size(); r++) {
				RouteDataObject road = graph.roads.get(r);
				for (int p = 0; p < road.getPointsLength(); p++) {
					graph.at.computeIfAbsent(place(road, p), k -> new ArrayList<>(2)).add(pin(r, p));
				}
			}
			return graph;
		}

		/** a road passing through counts twice */
		int arms(long key) {
			int arms = 0;
			for (long p : at.get(key)) {
				RouteDataObject road = roads.get(roadOf(p));
				if (NOT_JUNCTION.contains(road.getHighway())) {
					continue;
				}
				int point = pointOf(p);
				arms += point > 0 && point < road.getPointsLength() - 1 ? 2 : 1;
			}
			return arms;
		}

		int rankOf(long key) {
			int best = SMALLER;
			for (long p : at.get(key)) {
				best = Math.min(best, rank(roads.get(roadOf(p)).getHighway()));
			}
			return best;
		}

		boolean hasLink(long key) {
			for (long p : at.get(key)) {
				String highway = roads.get(roadOf(p)).getHighway();
				if (highway != null && highway.endsWith("_link")) {
					return true;
				}
			}
			return false;
		}

		boolean hasNoneLanes(long key) {
			for (long p : at.get(key)) {
				RouteDataObject road = roads.get(roadOf(p));
				if (hasNoneLane(road.getValue("turn:lanes")) || hasNoneLane(road.getValue("turn:lanes:forward"))
						|| hasNoneLane(road.getValue("turn:lanes:backward"))) {
					return true;
				}
			}
			return false;
		}

		List<Long> neighbours(long key, boolean forward) {
			List<Long> out = new ArrayList<>(4);
			for (long p : at.getOrDefault(key, List.of())) {
				RouteDataObject road = roads.get(roadOf(p));
				int point = pointOf(p);
				int oneway = road.getOneway();
				if (point + 1 < road.getPointsLength() && (forward ? oneway >= 0 : oneway <= 0)) {
					out.add(place(road, point + 1));
				}
				if (point - 1 >= 0 && (forward ? oneway <= 0 : oneway >= 0)) {
					out.add(place(road, point - 1));
				}
			}
			return out;
		}

		/** a point in a tunnel snaps to roads above or beside it */
		boolean inTunnel(long a, long b) {
			for (long p : at.get(b)) {
				RouteDataObject road = roads.get(roadOf(p));
				int point = pointOf(p);
				boolean joins = (point > 0 && place(road, point - 1) == a)
						|| (point + 1 < road.getPointsLength() && place(road, point + 1) == a);
				if (joins && road.tunnel()) {
					return true;
				}
			}
			return false;
		}
	}

	private static long place(RouteDataObject road, int point) {
		return (((long) road.getPoint31XTile(point)) << 32) | (road.getPoint31YTile(point) & 0xffffffffL);
	}

	private static long pin(int road, int point) {
		return (((long) road) << 20) | point;
	}

	private static int roadOf(long pin) {
		return (int) (pin >>> 20);
	}

	private static int pointOf(long pin) {
		return (int) (pin & 0xfffff);
	}

	private static double lat(long place) {
		return MapUtils.get31LatitudeY((int) (place & 0xffffffffL));
	}

	private static double lon(long place) {
		return MapUtils.get31LongitudeX((int) (place >>> 32));
	}

	private static double metres(long a, long b) {
		return MapUtils.getDistance(lat(a), lon(a), lat(b), lon(b));
	}

	// ================================================================== road classes and sizes

	private static int rank(String highway) {
		if (highway == null) {
			return SMALLER;
		}
		int i = HIGHWAYS.indexOf(highway.endsWith("_link") ? highway.substring(0, highway.length() - 5) : highway);
		return i < 0 ? SMALLER : i;
	}

	private static double junctionRadius(int rank) {
		return rank <= TRUNK ? 1000 : rank == PRIMARY ? 300 : 120;
	}

	/** nearest start/end: beyond the lane warning look-ahead (~270 m on a motorway) */
	private static double minArm(int rank) {
		return rank <= TRUNK ? 300 : rank == PRIMARY ? 180 : 120;
	}

	private static double maxArm(int rank) {
		return rank <= TRUNK ? 900 : rank == PRIMARY ? 500 : 400;
	}

	private static double minJunctionDistance(int rank) {
		return junctionRadius(rank) * 3;
	}
}
