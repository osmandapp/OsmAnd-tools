package net.osmand.tester;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import com.google.gson.GsonBuilder;

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
 * Builds turn-lane test cases out of an obf: picks junctions spread over the map, puts a start and
 * an end on two of the arms of each, and records what the current preparation says between them.
 *
 * The recorded strings are the reference, in the same form test_turn_lanes.json already uses -
 * "turn:lanes", with "[MUTE] " in front when the instruction is not spoken. They describe what
 * OsmAnd does today, which is what makes them useful as a regression net; they are not a statement
 * about what the right instruction is.
 */
public class GenerateTurnLanesTest {

	/**
	 * How big a place is depends on what kind of road it is. An interchange of two motorways is a
	 * kilometre of ramps and is one junction; a crossing of two secondaries is a hundred metres and
	 * is another. Reading both at the same radius either tears the interchange into a dozen
	 * junctions or swallows a whole neighbourhood into one.
	 */
	private static final int MOTORWAY = 0, TRUNK = 1, PRIMARY = 2, SECONDARY = 3, SMALLER = 9;

	/** the class of road a junction is worth testing on at all */
	private static final int LOWEST = SECONDARY;

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
		}
		return SMALLER;
	}

	/** nodes of one junction: anything with three arms within this of the seed is the same place */
	private static double clusterOf(int rank) {
		return rank <= TRUNK ? 1000 : rank == PRIMARY ? 300 : 120;
	}

	/**
	 * How far out the start and the end are put. The near end is what the lane warning needs to be
	 * visible at all: the preparation looks ahead a hundred metres, and further when a hundred
	 * metres is less than eight seconds of driving - which on a motorway is some 270 m. A start
	 * closer than that is inside the warning it is meant to test.
	 */
	private static double minArm(int rank) {
		return rank <= TRUNK ? 300 : rank == PRIMARY ? 180 : 120;
	}

	private static double maxArm(int rank) {
		return rank <= TRUNK ? 900 : rank == PRIMARY ? 500 : 400;
	}

	private static double armLimit(int rank) {
		return maxArm(rank) * 1.3;
	}

	/** two junctions closer than this are one place, so only the first is taken */
	private static double apart(int rank) {
		return clusterOf(rank) * 3;
	}

	private static int junctions = 100;
	private static int perJunction = 2;
	private static String profile = "car";
	private static boolean linksOnly;
	/** a case with no lane picture in it says nothing about lanes */
	private static boolean anyTurn;
	/**
	 * Only drives that actually read an unpainted lane: somewhere along the route an instruction is
	 * given off markings that leave a lane blank - "||right", "none|none|through". A junction with
	 * such a road nearby is not enough; the drive has to pass through it.
	 */
	private static boolean noneLanes;
	/** where to read which side traffic keeps to; regions.ocbf beside the obf when not given */
	private static String regionsPath;

	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length < 2) {
			System.out.println("generate-turn-lanes-test <file.obf> <out.json>"
					+ " [--junctions=100] [--per-junction=2] [--profile=car] [--links-only] [--any-turn]"
					+ " [--none-lanes] [--regions=regions.ocbf]");
			return;
		}
		File obf = new File(args[0]);
		File out = new File(args[1]);
		for (int i = 2; i < args.length; i++) {
			String a = args[i];
			if (a.startsWith("--junctions=")) {
				junctions = Integer.parseInt(a.substring(a.indexOf('=') + 1));
			} else if (a.startsWith("--per-junction=")) {
				perJunction = Integer.parseInt(a.substring(a.indexOf('=') + 1));
			} else if (a.startsWith("--profile=")) {
				profile = a.substring(a.indexOf('=') + 1);
			} else if (a.equals("--links-only")) {
				linksOnly = true;
			} else if (a.equals("--any-turn")) {
				anyTurn = true;
			} else if (a.equals("--none-lanes")) {
				noneLanes = true;
			} else if (a.startsWith("--regions=")) {
				regionsPath = a.substring(a.indexOf('=') + 1);
			}
		}
		Graph graph = read(obf);
		System.out.printf("roads %d, nodes %d%n", graph.roads.size(), graph.at.size());
		List<Long> picked = pick(graph);
		System.out.printf("junctions picked %d%n", picked.size());
		File regionsFile = regionsPath != null ? new File(regionsPath)
				: new File(obf.getAbsoluteFile().getParentFile(), OsmandRegions.REGIONS_OCBF);
		OsmandRegions regions = null;
		if (regionsFile.exists()) {
			regions = new OsmandRegions(regionsFile.getAbsolutePath());
		} else {
			System.out.println("no " + regionsFile + ": every case is recorded as right-hand traffic");
		}
		List<Object> cases = record(obf, graph, picked, regions);
		System.out.printf("cases written %d%n", cases.size());
		FileWriter w = new FileWriter(out);
		new GsonBuilder().setPrettyPrinting().create().toJson(cases, w);
		w.close();
	}

	// ------------------------------------------------------------------ the roads as a graph

	/** a point of a road, as the key of the place it is at and the road it belongs to */
	private static class Graph {
		final List<RouteDataObject> roads = new ArrayList<>();
		/** place -> the points sitting on it, packed as road index and point index */
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
		reader.close();
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

	/** how many ways out a place offers: a road passing through it counts twice */
	private static int arms(Graph graph, long key) {
		int arms = 0;
		for (long p : graph.at.get(key)) {
			RouteDataObject road = graph.roads.get(roadOf(p));
			int point = pointOf(p);
			arms += point > 0 && point < road.getPointsLength() - 1 ? 2 : 1;
		}
		return arms;
	}

	/** the biggest road meeting here, as a class number: smaller is bigger */
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

	/** a road meeting here leaves at least one lane unpainted, so the junction says something about "none" */
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

	// ------------------------------------------------------------------ which junctions to take

	/**
	 * The junctions worth a test, spread over the map: the ones offering the most ways out first,
	 * and never two within {@link #APART}, so a single interchange does not fill the file.
	 */
	private static List<Long> pick(Graph graph) {
		List<long[]> candidates = new ArrayList<>();
		for (Map.Entry<Long, List<Long>> e : graph.at.entrySet()) {
			if (e.getValue().size() < 2) {
				continue;
			}
			int a = arms(graph, e.getKey());
			int rank = rankOf(graph, e.getKey());
			if (a < 3 || rank > LOWEST || (linksOnly && !hasLink(graph, e.getKey()))
					|| (noneLanes && !hasNoneLanes(graph, e.getKey()))) {
				continue;
			}
			// the biggest road first, then the most ways out: an interchange before a crossroads
			candidates.add(new long[] {rank, -a, e.getKey()});
		}
		Collections.sort(candidates, new Comparator<long[]>() {
			@Override
			public int compare(long[] x, long[] y) {
				return x[0] != y[0] ? Long.compare(x[0], y[0]) : Long.compare(x[1], y[1]);
			}
		});
		// a coarse grid, so "is anything picked near this" does not walk the whole list
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
			if (picked.size() >= junctions) {
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

	/** the places of one junction: everything with three arms within {@link #CLUSTER_R} of the seed */
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

	/** the places one step away, following the road's direction when it is one-way */
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

	// ------------------------------------------------------------------ where to start and finish

	/**
	 * one way out of a junction: the place to stand on it, and how far out that is. The route is
	 * started and finished halfway along the last step of the walk rather than on its point: a point
	 * of a road can be where other roads join it, and a start put there is attached to any of them,
	 * which leaves the route a first segment of no length and no bearing.
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
	 * Walks outwards from the junction and keeps the furthest place on each way out that is still
	 * between {@link #MIN_ARM} and {@link #MAX_ARM} from it. Going forward finds where a route
	 * leaving the junction can end; going backward finds where one entering it can start.
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

	// ------------------------------------------------------------------ what the route says today

	private static List<Object> record(File obf, Graph graph, List<Long> picked, OsmandRegions regions)
			throws IOException, InterruptedException {
		List<Object> cases = new ArrayList<>();
		RandomAccessFile raf = new RandomAccessFile(obf, "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, obf);
		BinaryMapIndexReader[] readers = {reader};
		for (long centre : picked) {
			int rank = rankOf(graph, centre);
			Set<Long> cluster = cluster(graph, centre, clusterOf(rank));
			boolean leftSide = leftHandAt(regions, lat(centre), lon(centre));
			List<Arm> in = armsOut(graph, cluster, centre, false, rank);
			List<Arm> out = armsOut(graph, cluster, centre, true, rank);
			// every way in against every way out, the sharpest turns first: two tests of one
			// junction should be two different drives through it, not the same one twice
			List<Arm[]> pairs = new ArrayList<>();
			for (Arm from : in) {
				for (Arm to : out) {
					if (metres(from.key, to.key) < minArm(rank)) {
						continue; // both ends on the same arm: the drive would not pass the junction
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
				if (written >= perJunction) {
					break;
				}
				Arm from = pair[0], to = pair[1];
				if (!usedIn.add(from.key) || !usedOut.add(to.key)) {
					continue; // an arm already used: vary the drive rather than repeat it
				}
				Map<String, String> results = run(readers, from.at, to.at, cluster, leftSide);
				if (results == null || results.isEmpty() || !worthKeeping(results)) {
					continue;
				}
				Map<String, Object> entry = new LinkedHashMap<>();
				entry.put("testName", name(graph, centre, cluster) + " " + (written + 1));
				entry.put("startPoint", point(from.at));
				entry.put("endPoint", point(to.at));
				Map<String, String> params = new LinkedHashMap<>();
				params.put("map", obf.getName());
				if (leftSide) {
					params.put("leftSide", "true");
				}
				entry.put("params", params);
				entry.put("expectedResults", results);
				cases.add(entry);
				written++;
			}
		}
		reader.close();
		return cases;
	}

	/**
	 * Which side traffic keeps to at a place. The obf does not say: the app takes it from the region the place is in,
	 * and so does this, from the most specific region that says anything about it.
	 */
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

	/** how far the drive turns at the junction, so the sharpest turns are tested first */
	private static double turnOf(Arm[] pair) {
		return Math.abs(MapUtils.degreesDiff(pair[0].bearing + 180, pair[1].bearing));
	}

	private static Map<String, Object> point(LatLon at) {
		Map<String, Object> p = new LinkedHashMap<>();
		p.put("latitude", at.getLatitude());
		p.put("longitude", at.getLongitude());
		return p;
	}

	/**
	 * What to call the junction. A road that says what it is called is worth more here than a
	 * bigger one that does not: an interchange is mostly unnamed slip roads, and calling it
	 * "motorway_link" tells a reader nothing, while the motorway it leaves carries a ref like A10.
	 * So a name or a ref comes first, the class of road only breaks the tie.
	 */
	private static String name(Graph graph, long centre, Set<Long> cluster) {
		RouteDataObject best = null;
		String bestCalled = null;
		int bestScore = Integer.MAX_VALUE;
		for (long key : cluster) {
			for (long p : graph.at.get(key)) {
				RouteDataObject road = graph.roads.get(roadOf(p));
				String called = called(road);
				boolean link = road.getHighway() != null && road.getHighway().endsWith("_link");
				int score = (called == null ? 100 : 0) + rank(road.getHighway()) * 2 + (link ? 1 : 0);
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

	/** what the road is called on a sign: its name, or the ref it is signed with */
	private static String called(RouteDataObject road) {
		String name = road.getName();
		if (name != null && !name.isEmpty()) {
			return name;
		}
		String ref = road.getRef(null, false, true);
		return ref == null || ref.isEmpty() ? null : ref;
	}

	/**
	 * A case is kept when it says something about lanes. A drive whose every instruction is a bare
	 * maneuver belongs in the routing tests, not here; and one that starts with a u-turn is the
	 * router turning the car round because the start landed on the wrong side of the road.
	 */
	private static boolean worthKeeping(Map<String, String> results) {
		boolean first = true;
		for (String value : results.values()) {
			if (first && (value.startsWith("TU") || value.startsWith("[MUTE] TU"))) {
				return false;
			}
			first = false;
		}
		if (anyTurn) {
			return true;
		}
		for (String value : results.values()) {
			if (value.indexOf(':') >= 0) {
				return true;
			}
		}
		return false;
	}

	/** the turn:lanes a segment is driven with: the unsuffixed tag on a one-way, the direction's own otherwise */
	/** an unpainted lane: "none" or nothing at all between the bars. Not every branch has this on TurnType. */
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

	private static String turnLanesOf(RouteSegmentResult segment) {
		RouteDataObject road = segment.getObject();
		if (road.getOneway() == 0) {
			return segment.isForwardDirection() ? road.getValue("turn:lanes:forward")
					: road.getValue("turn:lanes:backward");
		}
		return road.getValue("turn:lanes");
	}

	/** a drive that misses the junction is not a test of it, however well it routed */
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

	/**
	 * The instructions the preparation gives between the two points, keyed the way the test reads
	 * them: the osm id of the road carrying the instruction, and its start point when that road
	 * carries more than one.
	 */
	private static Map<String, String> run(BinaryMapIndexReader[] readers, LatLon from, LatLon to,
			Set<Long> junction, boolean leftSide) throws IOException, InterruptedException {
		RoutingMemoryLimits limits = new RoutingMemoryLimits(
				RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3, RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		Map<String, String> params = new HashMap<>();
		params.put(profile, "true");
		RoutingConfiguration config = RoutingConfiguration.getDefault().build(profile, limits, params);
		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();
		RoutingContext ctx = fe.buildRoutingContext(config, null, readers,
				RoutePlannerFrontEnd.RouteCalculationMode.NORMAL);
		ctx.leftSideNavigation = leftSide;
		List<RouteSegmentResult> route;
		try {
			route = fe.searchRoute(ctx, from, to, null).getList();
		} catch (RuntimeException | InterruptedException e) {
			return null;
		}
		if (route == null || !passes(route, junction)) {
			return null;
		}
		Map<String, String> results = new LinkedHashMap<>();
		Map<Long, Integer> seen = new HashMap<>();
		// the route opens with a "carry on" that the test sees like any other instruction: a later
		// one on this same road has to name its start point, or the two cannot be told apart
		seen.put(ObfConstants.getOsmObjectId(route.get(0).getObject()), route.get(0).getStartPointIndex());
		boolean readFromNone = false;
		for (int i = 1; i < route.size(); i++) {
			RouteSegmentResult segment = route.get(i);
			TurnType turn = segment.getTurnType();
			if (turn == null) {
				continue;
			}
			// the markings that gave this instruction are on the road the turn is taken from
			if (hasNoneLane(turnLanesOf(route.get(i - 1)))) {
				readFromNone = true;
			}
			long id = ObfConstants.getOsmObjectId(segment.getObject());
			// RouteResultPreparationTest compares three forms - the instruction with its lanes, the
			// lanes alone, the manoeuvre alone - and a turn without lanes can only take the last one:
			// the first would have to carry the "null" that test's own string concatenation produces
			String lanes = turn.getLanes() == null ? null : TurnType.lanesToString(turn.getLanes());
			String value = lanes == null ? turn.toXmlString()
					: (turn.isSkipToSpeak() ? "[MUTE] " : "") + turn.toXmlString() + ":" + lanes;
			Integer had = seen.put(id, segment.getStartPointIndex());
			// a road carrying two instructions needs the start point to tell them apart
			String key = had == null ? String.valueOf(id) : id + ":" + segment.getStartPointIndex();
			if (had != null && results.containsKey(String.valueOf(id))) {
				results.put(id + ":" + had, results.remove(String.valueOf(id)));
			}
			results.put(key, value);
		}
		if (noneLanes && !readFromNone) {
			return null; // the drive never took an instruction off an unpainted lane
		}
		return results;
	}
}
