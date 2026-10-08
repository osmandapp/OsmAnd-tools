package net.osmand.tester;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
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
 */
public class GenerateTurnLanesTest {

	// road classes, smaller is bigger: a motorway interchange and a secondary crossing are read at different radii
	private static final int MOTORWAY = 0, TRUNK = 1, PRIMARY = 2, SECONDARY = 3, TERTIARY = 4, SMALLER = 9;

	/** the classes {@link Options#highway} can name, biggest first */
	public static final List<String> HIGHWAYS = List.of("motorway", "trunk", "primary", "secondary", "tertiary");

	private static int rank(String highway) {
		if (highway == null) {
			return SMALLER;
		}
		String base = highway.endsWith("_link") ? highway.substring(0, highway.length() - 5) : highway;
		if (base.equals("motorway")) {
			return MOTORWAY;
		} else if (base.equals("trunk")) {
			return TRUNK;
		} else if (base.equals("primary")) {
			return PRIMARY;
		} else if (base.equals("secondary")) {
			return SECONDARY;
		} else if (base.equals("tertiary")) {
			return TERTIARY;
		}
		return SMALLER;
	}

	/** radius of one junction */
	private static double clusterOf(int rank) {
		return rank <= TRUNK ? 1000 : rank == PRIMARY ? 300 : 120;
	}

	/** nearest start/end: beyond the lane warning look-ahead (~270 m on a motorway) */
	private static double minArm(int rank) {
		return rank <= TRUNK ? 300 : rank == PRIMARY ? 180 : 120;
	}

	private static double maxArm(int rank) {
		return rank <= TRUNK ? 900 : rank == PRIMARY ? 500 : 400;
	}

	private static double armLimit(int rank) {
		return maxArm(rank) * 1.3;
	}

	/** min distance between two picked junctions */
	private static double apart(int rank) {
		return clusterOf(rank) * 3;
	}

	public static class Options {
		public int junctions = 100;
		public int perJunction = 2;
		public String profile = "car";
		public boolean linksOnly;
		/** keep drives without any lanes too */
		public boolean anyTurn;
		/** only drives with an instruction given off a "none"/empty lane */
		public boolean noneLanes;
		/** regions.ocbf for left-hand traffic */
		public String regionsPath;
		/** the smallest road class to pick junctions on, one of {@link #HIGHWAYS} */
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
		void junctions(int done, int total, int cases);

		boolean isCancelled();
	}

	/** one drive and its instructions, keyed by segment */
	public static class Case {
		public final String name;
		public final LatLon start;
		public final LatLon end;
		public final String map;
		public final boolean leftSide;
		public final Map<String, String> results;
		/** where each instruction is given, same keys as results */
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

	/** router heap per drive: a big city overflows the default and reloads tiles at every step */
	public static final int MEMORY_LIMIT_MB = 512;

	public static final String[] CSV_HEADER = {"num", "name", "start", "end", "segment", "expected", "left_side", "obf",
			"point"};

	private final Options options;
	private final Progress progress;

	public GenerateTurnLanesTest(Options options, Progress progress) {
		this.options = options;
		this.progress = progress;
	}

	public List<Case> generate(File obf, OsmandRegions regions) throws IOException, InterruptedException {
		Graph graph = read(obf);
		List<Long> picked = pick(graph);
		return record(obf, graph, picked, regions);
	}

	/**
	 * One row per instruction, drives numbered from {@code firstNum}.
	 *
	 * @return the next drive number
	 */
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

	// ------------------------------------------------------------------ road graph

	private static class Graph {
		final List<RouteDataObject> roads = new ArrayList<>();
		/** place -> pins (road index, point index) at it */
		final Map<Long, List<Long>> at = new HashMap<>();
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

	private static Graph read(File file) throws IOException {
		final Graph graph = new Graph();
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, file);
		try {
			for (RouteRegion region : reader.getRoutingIndexes()) {
				SearchRequest<RouteDataObject> request = BinaryMapIndexReader.buildSearchRouteRequest(
						0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
				List<RouteSubregion> subregions = reader.searchRouteIndexTree(request, region.getSubregions());
				reader.loadRouteIndexData(subregions, new ResultMatcher<RouteDataObject>() {
					@Override
					public boolean publish(RouteDataObject road) {
						if (road.getHighway() != null && road.getPointsLength() >= 2) {
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
				long key = place(road, p);
				List<Long> pins = graph.at.get(key);
				if (pins == null) {
					pins = new ArrayList<>(2);
					graph.at.put(key, pins);
				}
				pins.add(pin(r, p));
			}
		}
		return graph;
	}

	/** ways out of a place: a road passing through counts twice */
	private static int arms(Graph graph, long key) {
		int arms = 0;
		for (long p : graph.at.get(key)) {
			RouteDataObject road = graph.roads.get(roadOf(p));
			int point = pointOf(p);
			arms += point > 0 && point < road.getPointsLength() - 1 ? 2 : 1;
		}
		return arms;
	}

	/** the biggest road class meeting here */
	private static int rankOf(Graph graph, long key) {
		int best = SMALLER;
		for (long p : graph.at.get(key)) {
			best = Math.min(best, rank(graph.roads.get(roadOf(p)).getHighway()));
		}
		return best;
	}

	private static boolean hasLink(Graph graph, long key) {
		for (long p : graph.at.get(key)) {
			String highway = graph.roads.get(roadOf(p)).getHighway();
			if (highway != null && highway.endsWith("_link")) {
				return true;
			}
		}
		return false;
	}

	private static boolean hasNoneLanes(Graph graph, long key) {
		for (long p : graph.at.get(key)) {
			RouteDataObject road = graph.roads.get(roadOf(p));
			if (hasNoneLane(road.getValue("turn:lanes"))
					|| hasNoneLane(road.getValue("turn:lanes:forward"))
					|| hasNoneLane(road.getValue("turn:lanes:backward"))) {
				return true;
			}
		}
		return false;
	}

	private static double lat(long key) {
		return MapUtils.get31LatitudeY((int) (key & 0xffffffffL));
	}

	private static double lon(long key) {
		return MapUtils.get31LongitudeX((int) (key >>> 32));
	}

	private static double metres(long a, long b) {
		return MapUtils.getDistance(lat(a), lon(a), lat(b), lon(b));
	}

	// ------------------------------------------------------------------ junction picking

	/** biggest road and most arms first, never two within {@link #apart} */
	private List<Long> pick(Graph graph) {
		int lowest = options.lowest();
		List<long[]> candidates = new ArrayList<>();
		for (Map.Entry<Long, List<Long>> e : graph.at.entrySet()) {
			if (e.getValue().size() < 2) {
				continue;
			}
			int a = arms(graph, e.getKey());
			int rank = rankOf(graph, e.getKey());
			if (a < 3 || rank > lowest || (options.linksOnly && !hasLink(graph, e.getKey()))
					|| (options.noneLanes && !hasNoneLanes(graph, e.getKey()))) {
				continue;
			}
			candidates.add(new long[] {rank, -a, e.getKey()});
		}
		Collections.sort(candidates, new Comparator<long[]>() {
			@Override
			public int compare(long[] x, long[] y) {
				return x[0] != y[0] ? Long.compare(x[0], y[0]) : Long.compare(x[1], y[1]);
			}
		});
		// coarse grid for the "anything picked nearby" check
		double cell = 0.006;
		Map<Long, List<Long>> grid = new HashMap<>();
		List<Long> picked = new ArrayList<>();
		for (long[] c : candidates) {
			long key = c[2];
			if (near(grid, cell, key, apart((int) c[0]))) {
				continue;
			}
			long gi = (long) Math.floor(lat(key) / cell), gj = (long) Math.floor(lon(key) / cell);
			long g = (gi << 32) | (gj & 0xffffffffL);
			List<Long> cellList = grid.get(g);
			if (cellList == null) {
				cellList = new ArrayList<>();
				grid.put(g, cellList);
			}
			cellList.add(key);
			picked.add(key);
			if (picked.size() >= options.junctions) {
				break;
			}
		}
		return picked;
	}

	private static boolean near(Map<Long, List<Long>> grid, double cell, long key, double apart) {
		long gi = (long) Math.floor(lat(key) / cell), gj = (long) Math.floor(lon(key) / cell);
		int reach = (int) Math.ceil(apart / (cell * 111000)) + 1;
		for (long i = gi - reach; i <= gi + reach; i++) {
			for (long j = gj - reach; j <= gj + reach; j++) {
				List<Long> cellList = grid.get((i << 32) | (j & 0xffffffffL));
				if (cellList == null) {
					continue;
				}
				for (long other : cellList) {
					if (metres(key, other) < apart) {
						return true;
					}
				}
			}
		}
		return false;
	}

	/** places of one junction: everything with 3+ arms within {@code radius} of the seed */
	private static Set<Long> cluster(Graph graph, long seed, double radius) {
		Set<Long> found = new HashSet<>();
		List<Long> stack = new ArrayList<>();
		found.add(seed);
		stack.add(seed);
		while (!stack.isEmpty()) {
			long key = stack.remove(stack.size() - 1);
			for (long next : neighbours(graph, key, true, true)) {
				if (found.contains(next) || metres(seed, next) > radius || arms(graph, next) < 3) {
					continue;
				}
				found.add(next);
				stack.add(next);
			}
		}
		return found;
	}

	/** places one step away, respecting oneway */
	private static List<Long> neighbours(Graph graph, long key, boolean forward, boolean backward) {
		List<Long> out = new ArrayList<>(4);
		List<Long> pins = graph.at.get(key);
		if (pins == null) {
			return out;
		}
		for (long p : pins) {
			RouteDataObject road = graph.roads.get(roadOf(p));
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

	// ------------------------------------------------------------------ start and end points

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

	/**
	 * The furthest place on each way out within {@link #minArm}..{@link #maxArm}. Forward finds route ends,
	 * backward finds route starts.
	 */
	private static List<Arm> armsOut(Graph graph, Set<Long> cluster, long centre, boolean forward,
			int rank) {
		Map<Long, Double> seen = new HashMap<>();
		Map<Long, Long> firstStep = new HashMap<>();
		Map<Long, Long> cameFrom = new HashMap<>();
		PriorityQueue<long[]> queue = new PriorityQueue<>(11, new Comparator<long[]>() {
			@Override
			public int compare(long[] x, long[] y) {
				return Long.compare(x[1], y[1]);
			}
		});
		for (long key : cluster) {
			seen.put(key, 0.0);
			queue.add(new long[] {key, 0});
		}
		Map<Long, Arm> best = new HashMap<>();
		while (!queue.isEmpty()) {
			long[] top = queue.poll();
			long key = top[0];
			double away = seen.containsKey(key) ? seen.get(key) : Double.MAX_VALUE;
			if (away > armLimit(rank)) {
				continue;
			}
			Long arm = firstStep.get(key);
			if (arm != null && away >= minArm(rank) && away <= maxArm(rank)) {
				Arm cur = best.get(arm);
				if (cur == null || away > cur.away) {
					best.put(arm, new Arm(key, cameFrom.get(key), away, bearing(centre, key)));
				}
			}
			for (long next : neighbours(graph, key, forward, !forward)) {
				if (cluster.contains(next)) {
					continue;
				}
				double step = away + metres(key, next);
				if (step > armLimit(rank)) {
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

	private static double bearing(long from, long to) {
		return Math.toDegrees(Math.atan2(lon(to) - lon(from), lat(to) - lat(from)));
	}

	// ------------------------------------------------------------------ recording

	private List<Case> record(File obf, Graph graph, List<Long> picked, OsmandRegions regions)
			throws IOException, InterruptedException {
		List<Case> cases = new ArrayList<>();
		RandomAccessFile raf = new RandomAccessFile(obf, "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, obf);
		BinaryMapIndexReader[] readers = {reader};
		int done = 0, points = 0;
		try {
			for (long centre : picked) {
				// a cancelled run keeps what is recorded so far
				if (progress != null && progress.isCancelled()) {
					break;
				}
				if (progress != null) {
					progress.junctions(done, picked.size(), points);
				}
				done++;
				int rank = rankOf(graph, centre);
				Set<Long> cluster = cluster(graph, centre, clusterOf(rank));
				boolean leftSide = leftHandAt(regions, lat(centre), lon(centre));
				List<Arm> in = armsOut(graph, cluster, centre, false, rank);
				List<Arm> out = armsOut(graph, cluster, centre, true, rank);
				// every way in x every way out, sharpest turn first
				List<Arm[]> pairs = new ArrayList<>();
				for (Arm from : in) {
					for (Arm to : out) {
						if (metres(from.key, to.key) < minArm(rank)) {
							continue; // same arm: the drive would not pass the junction
						}
						pairs.add(new Arm[] {from, to});
					}
				}
				Collections.sort(pairs, new Comparator<Arm[]>() {
					@Override
					public int compare(Arm[] x, Arm[] y) {
						return Double.compare(turnOf(y), turnOf(x));
					}
				});
				int written = 0;
				Set<Long> usedIn = new HashSet<>(), usedOut = new HashSet<>();
				for (Arm[] pair : pairs) {
					if (written >= options.perJunction) {
						break;
					}
					Arm from = pair[0], to = pair[1];
					if (!usedIn.add(from.key) || !usedOut.add(to.key)) {
						continue; // each arm used once per junction
					}
					Map<String, LatLon> at = new LinkedHashMap<>();
					Map<String, String> results = run(readers, from.at, to.at, cluster, leftSide, at);
					if (results == null || results.isEmpty() || !worthKeeping(results)) {
						continue;
					}
					cases.add(new Case(name(graph, centre, cluster) + " " + (written + 1), from.at, to.at, obf.getName(),
							leftSide, results, at));
					points += results.size();
					written++;
				}
			}
		} finally {
			reader.close();
		}
		if (progress != null) {
			progress.junctions(done, picked.size(), points);
		}
		return cases;
	}

	/** left-hand traffic from the most specific region that says so, as the app does */
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

	private static double turnOf(Arm[] pair) {
		return Math.abs(MapUtils.degreesDiff(pair[0].bearing + 180, pair[1].bearing));
	}

	/** a named or ref'd road first (motorway_link says nothing, A10 does), road class breaks the tie */
	private static String name(Graph graph, long centre, Set<Long> cluster) {
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
		return (bestCalled == null ? best.getHighway() : bestCalled)
				+ " " + ObfConstants.getOsmObjectId(best);
	}

	private static String called(RouteDataObject road) {
		String name = road.getName();
		if (name != null && !name.isEmpty()) {
			return name;
		}
		String ref = road.getRef(null, false, true);
		return ref == null || ref.isEmpty() ? null : ref;
	}

	/** has lanes (unless anyTurn), and does not start with a u-turn (start on the wrong side of the road) */
	private boolean worthKeeping(Map<String, String> results) {
		boolean first = true;
		for (String value : results.values()) {
			if (first && (value.startsWith("TU") || value.startsWith("[MUTE] TU"))) {
				return false;
			}
			first = false;
		}
		if (options.anyTurn) {
			return true;
		}
		for (String value : results.values()) {
			if (value.indexOf(':') >= 0) {
				return true;
			}
		}
		return false;
	}

	/** "none" or empty between the bars */
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

	/** turn:lanes on a oneway, the direction's own tag otherwise */
	private static String turnLanesOf(RouteSegmentResult segment) {
		RouteDataObject road = segment.getObject();
		if (road.getOneway() == 0) {
			return segment.isForwardDirection() ? road.getValue("turn:lanes:forward")
					: road.getValue("turn:lanes:backward");
		}
		return road.getValue("turn:lanes");
	}

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

	/** the drive's instructions, or null when it does not route or misses the junction */
	private Map<String, String> run(BinaryMapIndexReader[] readers, LatLon from, LatLon to,
			Set<Long> junction, boolean leftSide, Map<String, LatLon> points) throws IOException {
		List<RouteSegmentResult> route;
		try {
			route = searchRoute(readers, from, to, leftSide);
		} catch (RuntimeException | InterruptedException e) {
			return null;
		}
		if (route == null || route.isEmpty() || !passes(route, junction)) {
			return null;
		}
		if (options.noneLanes && !readsFromNone(route)) {
			return null;
		}
		return instructions(route, points);
	}

	/** a drive routed again: its instructions and the osm ids of its roads */
	public static class Replay {
		public final Map<String, String> instructions;
		public final Set<Long> roads;

		Replay(Map<String, String> instructions, Set<Long> roads) {
			this.instructions = instructions;
			this.roads = roads;
		}
	}

	/**
	 * Routes a dataset drive again for a compare. The roads tell "instruction gone" from "drive went elsewhere".
	 * Null when there is no route.
	 */
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

	private List<RouteSegmentResult> searchRoute(BinaryMapIndexReader[] readers, LatLon from, LatLon to,
			boolean leftSide) throws IOException, InterruptedException {
		RoutingMemoryLimits limits = new RoutingMemoryLimits(
				MEMORY_LIMIT_MB, RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		Map<String, String> params = new HashMap<>();
		params.put(options.profile, "true");
		RoutingConfiguration config = RoutingConfiguration.getDefault().build(options.profile, limits, params);
		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();
		RoutingContext ctx = fe.buildRoutingContext(config, null, readers,
				RoutePlannerFrontEnd.RouteCalculationMode.NORMAL);
		ctx.leftSideNavigation = leftSide;
		return fe.searchRoute(ctx, from, to, null).getList();
	}

	/** some instruction is given off a "none" lane of the road the turn is taken from */
	private static boolean readsFromNone(List<RouteSegmentResult> route) {
		for (int i = 1; i < route.size(); i++) {
			if (route.get(i).getTurnType() != null && hasNoneLane(turnLanesOf(route.get(i - 1)))) {
				return true;
			}
		}
		return false;
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
}
