package net.osmand.router;

import static net.osmand.router.HHRoutingUtilities.addWay;
import static net.osmand.router.HHRoutingUtilities.calculateRoutePointInternalId;
import static net.osmand.router.HHRoutingUtilities.getPoint;
import static net.osmand.router.HHRoutingUtilities.makePositiveDir;
import static net.osmand.router.HHRoutingUtilities.saveOsmFile;
import static net.osmand.router.HHRoutingUtilities.visualizeWays;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.router.BinaryRoutePlanner.MultiFinalRouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBSegment;
import net.osmand.router.HHRoutingPreparationDB.NetworkRouteRegion;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.util.MapUtils;

// TODO 
// 1st phase - bugs routing
// 1.3 TODO for long distance causes bugs if (pnt.index != 2005) { 2005-> 1861 } - 3372.75 vs 2598
// 1.4 BinaryRoutePlanner TODO routing 1/-1/0 FIX routing time 7288 / 7088 / 7188 (43.15274, 19.55169 -> 42.955495, 19.0972263)
// 1.5 BinaryRoutePlanner TODO double checkfix correct at all?  https://github.com/osmandapp/OsmAnd/issues/14148
// 1.6 BinaryRoutePlanner TODO ?? we don't stop here in order to allow improve found *potential* final segment - test case on short route
// 1.7 BinaryRoutePlanner TODO test that routing time is different with on & off!

// TODO Important try different recursive algorithm for road separation
// 2nd phase - points selection
// 2.0 print stats about initial routing graph (points / edges / distribution % edges / point)
// 2.1 Create tests: 1) Straight parallel roads -> 4 points 2) parking slots -> exit points 3) road and suburb -> exit points including road?
// 2.2 Merge single point exit islands (up to a limit?)
// 2.3 Exclude & merge non exit islands (up to a limit)
// 2.4 Play with Heuristics [MAX_VERT_DEPTH_LOOKUP, MAX_NEIGHBOORS_N_POINTS, ... ] to achieve Better points distribution:
///    - Smaller edges (!)
///    - Smaller nodes 
///    - Max points in island = LIMIT = 50K (needs to be measured with Dijkstra randomly)

// 3rd phase - Generate Planet ~4h (per profile)
// 3.1 Reduce memory usage TODO store network points inside NetworkRouteRegion and retrieve similar to visited points
// 3.2 Calculate points in parallel (Planet)
// 3.3 Calculate shortcuts in parallel (Planet)
// 3.4 Make process rerunnable ? 

// 4 Introduce 3/4 level (test on Europe)
// 4.1 Merge islands - Introduce 3rd level of points - FAIL
// 4.2 Implement routing algorithm 2->3->4 - ?
// 4.3 Introduce / reuse points that are center for many shortcuts to reduce edges N*N -> 2*n
// 4.4 Optimize time / space comparing to 2nd level

// 5th phase - complex routing / data
// 5.1 Implement final routing algorithm including start / end segment search 
// 5.2 Retrieve route details and turn information
// 5.3 Save data structure optimally by access time
// 5.4 Save data structure optimally by size
// 5.5 Implement border crossing issue

// 6 Future
// 6.1 Alternative routes (distribute initial points better)
// 6.2 Avoid specific road
// 6.3 Deprioritize or exclude roads
// 6.4 Live data (think about it)

public class HHRoutingGraphCreator {

	final static Log LOG = PlatformUtil.getLog(HHRoutingGraphCreator.class);

	final static int PROCESS_SET_NETWORK_POINTS = 1;
	final static int PROCESS_BUILD_NETWORK_SEGMENTS = 2;

	static boolean DEBUG_STORE_ALL_ROADS = false;
	static int DEBUG_LIMIT_START_OFFSET = 0;
	static int DEBUG_LIMIT_PROCESS = -1;
	static int DEBUG_VERBOSE_LEVEL = 0;
	static long DEBUG_START_TIME = 0;

	final static int MEMORY_RELOAD_MB = 1000; //
	final static int MEMORY_RELOAD_TIMEOUT_SECONDS = 120;
	static final int ROUTING_MEMORY_LIMIT = 1024;
	static long MEMEORY_LAST_RELOAD = System.currentTimeMillis();
	static long MEMORY_LAST_USED_MB;

	static int PROCESS = PROCESS_SET_NETWORK_POINTS;
	private static String ROUTING_PROFILE = "car";
	private static List<File> FILE_SOURCES = new ArrayList<File>();
	private static boolean CLEAN = true;

	private static int BATCH_SIZE = 500;
	private static int THREAD_POOL = 2;

	// Constants / Tests for splitting building network points {7,7,7,7} - 50 -
	protected static LatLon EX1 = new LatLon(52.3201813, 4.7644685); // 337 -> 4 (1212 -> 4)
	protected static LatLon EX2 = new LatLon(52.33265, 4.77738); // 301 -> 12 (2240 -> 8)
	protected static LatLon EX3 = new LatLon(52.2728791, 4.8064803); // 632 -> 14 (923 -> 11 )
	protected static LatLon EX4 = new LatLon(52.27757, 4.85731); // 218 -> 7 (1599 -> 5)
	protected static LatLon EX5 = new LatLon(42.78725, 18.95036); // 391 -> 8
	protected static LatLon EX6 = new LatLon(42.09664, 19.088486); //
	// TODO 0 (Lat 42.828068 Lon 19.842607): Road (389035663) bug maxflow 5 != 4 mincut 
	protected static LatLon EX = EX6; // for all - null; otherwise specific point

	// Heuristics building network points
	private static int[] MAX_VERT_DEPTH_LOOKUP = new int[] { 15, 10, 8 }; // new int[] {7,7,7,7};
	private static int MAX_NEIGHBOORS_N_POINTS = 25;
	private static float MAX_RADIUS_ISLAND = 50000; // max distance from "start point"
	private static boolean V2 = true;
	private static int BRIDGE_MAX_DEPTH = 800;
	private static int BRIDGE_MIN_DEPTH = 25;
	// this is not necessary but it gives confirmation of correctness
	boolean RECALC_CHECK_MAXFLOW = true;

	private static File sourceFile() {
		String name = "Montenegro_europe_2.road.obf";
//		name = "Netherlands_europe_2.road.obf";
//		name = "Ukraine_europe_2.road.obf";
//		name = "Germany";
		return new File(System.getProperty("maps.dir"), name);
	}

	public static void main(String[] args) throws Exception {
		File obfFile = args.length == 0 ? sourceFile() : new File(args[0]);
		for (String a : args) {
			if (a.equals("--setup-network-points")) {
				PROCESS = PROCESS_SET_NETWORK_POINTS;
				V2 = false;
			} else if (a.equals("--setup-network-points-v2")) {
				PROCESS = PROCESS_SET_NETWORK_POINTS;
				V2 = true;
			} else if (a.equals("--build-network-shortcuts")) {
				PROCESS = PROCESS_BUILD_NETWORK_SEGMENTS;
			} else if (a.startsWith("--routing_profile=")) {
				ROUTING_PROFILE = a.substring("--routing_profile=".length());
			} else if (a.startsWith("--threads=")) {
				THREAD_POOL = Integer.parseInt(a.substring("--threads=".length()));
			} else if (a.startsWith("--maxdepth=")) {
				MAX_NEIGHBOORS_N_POINTS = Integer.parseInt(a.substring("--maxdepth=".length()));
				BRIDGE_MAX_DEPTH = Integer.parseInt(a.substring("--maxdepth=".length()));
			} else if (a.equals("--clean")) {
				CLEAN = true;
			}
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getCanonicalFile().getName();

		File dbFile = new File(folder, name + HHRoutingPreparationDB.EXT);
		if (CLEAN && PROCESS == PROCESS_SET_NETWORK_POINTS && dbFile.exists()) {
			dbFile.delete();
		}
		HHRoutingPreparationDB networkDB = new HHRoutingPreparationDB(dbFile);
		if (CLEAN && PROCESS == PROCESS_BUILD_NETWORK_SEGMENTS && dbFile.exists()) {
			networkDB.recreateSegments();
		}
		if (obfFile.isDirectory()) {
			for (File f : obfFile.listFiles()) {
				if (!f.getName().endsWith(".obf")) {
					continue;
				}
				FILE_SOURCES.add(f);
			}
		} else {
			FILE_SOURCES.add(obfFile);
		}
		HHRoutingGraphCreator proc = new HHRoutingGraphCreator();
		if (PROCESS == PROCESS_SET_NETWORK_POINTS) {
			FullNetwork network = proc.collectNetworkPoints(networkDB);
			List<Entity> objects = visualizeWays(network.visualPoints(), network.visualConnections(),
					network.visitedVertices);
			saveOsmFile(objects, new File(folder, name + ".osm"));
		} else if (PROCESS == PROCESS_BUILD_NETWORK_SEGMENTS) {
			TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(true);
			networkDB.loadNetworkSegments(pnts.valueCollection());
			Collection<Entity> objects = proc.buildNetworkShortcuts(pnts, networkDB);
			saveOsmFile(objects, new File(folder, name + "-hh.osm"));
		}
		networkDB.close();
	}

	private RoutingContext gcMemoryLimitToUnloadAll(RoutingContext ctx, List<NetworkRouteRegion> subRegions,
			boolean force) throws IOException {
		long usedMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20;
		if (force || ((usedMemory - MEMORY_LAST_USED_MB) > MEMORY_RELOAD_MB
				&& (System.currentTimeMillis() - MEMEORY_LAST_RELOAD) > MEMORY_RELOAD_TIMEOUT_SECONDS * 1000)) {
			long nt = System.nanoTime();
			System.gc();
			long ntusedMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20;
			if (!force && (ntusedMemory - MEMORY_LAST_USED_MB) < MEMORY_RELOAD_MB) {
				return ctx;
			}
			Set<File> fls = null;
			if (subRegions != null) {
				fls = new LinkedHashSet<>();
				for (NetworkRouteRegion r : subRegions) {
					fls.add(r.file);
				}
			}
			ctx = prepareContext(fls, ctx);
			ctx.calculationProgress = new RouteCalculationProgress();
			System.gc();
			MEMORY_LAST_USED_MB = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20;
			MEMEORY_LAST_RELOAD = System.currentTimeMillis();
			logf("***** Reload memory used before %d MB -> GC %d MB -> reload ctx %d MB (%.1f s) *****\n", usedMemory,
					ntusedMemory, MEMORY_LAST_USED_MB, (System.nanoTime() - nt) / 1e9);
		}
		return ctx;
	}

	private static void logf(String string, Object... a) {
		if (DEBUG_START_TIME == 0) {
			DEBUG_START_TIME = System.currentTimeMillis();
		}
		String ms = String.format("%3.1fs ", (System.currentTimeMillis() - DEBUG_START_TIME) / 1000.f);
		System.out.printf(ms + string + "\n", a);

	}

	private List<BinaryMapIndexReader> initReaders(Collection<File> hints) throws IOException {
		List<BinaryMapIndexReader> readers = new ArrayList<BinaryMapIndexReader>();
		for (File source : (hints != null ? hints : FILE_SOURCES)) {
			BinaryMapIndexReader reader = new BinaryMapIndexReader(new RandomAccessFile(source, "r"), source);
			readers.add(reader);
		}
		return readers;
	}

	private RoutingContext prepareContext(Collection<File> fileSources, RoutingContext oldCtx) throws IOException {
		List<BinaryMapIndexReader> readers = initReaders(fileSources);
		if (oldCtx != null) {
			for (BinaryMapIndexReader r : oldCtx.map.keySet()) {
				r.close();
			}
		}
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.parseDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(ROUTING_MEMORY_LIMIT, ROUTING_MEMORY_LIMIT);
		Map<String, String> map = new TreeMap<String, String>();
		// map.put("avoid_ferries", "true");
		RoutingConfiguration config = builder.build(ROUTING_PROFILE, memoryLimit, map);
		config.planRoadDirection = 1;
		config.heuristicCoefficient = 0; // dijkstra
		return router.buildRoutingContext(config, null, readers.toArray(new BinaryMapIndexReader[readers.size()]),
				RouteCalculationMode.NORMAL);
	}

	class RouteSegmentConn {
		public RouteSegmentCustom s;
		public RouteSegmentCustom t;

		RouteSegmentConn(RouteSegmentCustom s, RouteSegmentCustom t) {
			this.s = s;
			this.t = t;
		}

		public int flow;
	}

	class RouteSegmentCustom extends RouteSegment {

		public List<RouteSegmentConn> connections = new ArrayList<>();
		public int cacheDepth;
		public long cacheId;
		public RouteSegmentConn flowParent;
		public RouteSegmentConn flowParentTemp;

		public RouteSegmentCustom(RouteSegment s) {
			super(s.getRoad(), s.getSegmentStart(), s.getSegmentEnd());
		}

		public RouteSegmentCustom() {
			// artificial
			super(null, 0, 0);
		}

		public void addConnection(RouteSegmentCustom pos) {
			if (pos != this && pos != null) {
				connections.add(new RouteSegmentConn(this, pos));
			}
		}

		public RouteSegmentConn getConnection(RouteSegmentCustom t) {
			for (RouteSegmentConn c : connections) {
				if (c.t == t) {
					return c;
				}
			}
			return null;
		}
	}

	//////////////////////////// BUILD NETWORK ISLANDS ////////////////////////

	class NetworkIsland {
		final NetworkIsland parent;
		final RouteSegment start;
		final PriorityQueue<RouteSegmentCustom> queue;
		int index;
		TIntIntHashMap edgeDistr = new TIntIntHashMap();
		int edges = 0;
		TLongObjectHashMap<RouteSegment> visitedVertices = new TLongObjectHashMap<>();
		TLongObjectHashMap<RouteSegment> toVisitVertices = new TLongObjectHashMap<>();
		TLongObjectHashMap<RouteSegmentCustom> allVertices = null;

		NetworkIsland(NetworkIsland parent, RouteSegment start) {
			this(parent, start, false);
		}

		NetworkIsland(NetworkIsland parent, RouteSegment start, boolean allVertices) {
			this.parent = parent;
			this.start = start;
			if (allVertices) {
				this.allVertices = new TLongObjectHashMap<>();
			}
			this.queue = parent == null ? null : new PriorityQueue<>(parent.getExpandIslandComparator());
			if (start != null) {
				addSegmentToQueue(getSegment(start, true));
			}
		}

		public RouteSegmentCustom getSegment(RouteSegment s) {
			if (s == null) {
				return null;
			}
			return allVertices.get(calculateRoutePointInternalId(s));
		}

		public RouteSegmentCustom getSegment(RouteSegment s, boolean create) {
			if (s == null) {
				return null;
			}
			long p = calculateRoutePointInternalId(s.getRoad(), Math.min(s.segStart, s.segEnd),
					Math.max(s.segStart, s.segEnd));
			RouteSegmentCustom routeSegment = allVertices != null ? allVertices.get(p) : null;
			if (routeSegment == null && create) {
				routeSegment = new RouteSegmentCustom(makePositiveDir(s));
				if (allVertices != null) {
					allVertices.put(p, routeSegment);
				}
			}
			return routeSegment;
		}

		protected Comparator<RouteSegment> getExpandIslandComparator() {
			return parent.getExpandIslandComparator();
		}

		private NetworkIsland addSegmentToQueue(RouteSegmentCustom s) {
			queue.add(s);
			toVisitVertices.put(calculateRoutePointInternalId(s), s);
			return this;
		}

		public boolean testIfNetworkPoint(long pntId) {
			if (parent != null) {
				return parent.testIfNetworkPoint(pntId);
			}
			return false;
		}

		public boolean testIfPossibleNetworkPoint(long pntId) {
			if (toVisitVertices.containsKey(pntId)) {
				return true;
			}
			if (parent != null) {
				return parent.testIfPossibleNetworkPoint(pntId);
			}
			return false;
		}

		public boolean testIfVisited(long pntId) {
			if (visitedVertices.containsKey(pntId)) {
				return true;
			}
			if (parent != null) {
				return parent.testIfVisited(pntId);
			}
			return false;
		}

		public int visitedVerticesSize() {
			if (parent instanceof FullNetwork) {
				return visitedVertices.size();
			}
			return visitedVertices.size() + (parent == null ? 0 : parent.visitedVerticesSize());
		}

		public int toVisitVerticesSize() {
			if (parent instanceof FullNetwork) {
				return toVisitVertices.size();
			}
			int pnt = (parent == null ? 0 : parent.toVisitVerticesSize());
			for (long l : visitedVertices.keys()) {
				if (parent.testIfPossibleNetworkPoint(l)) {
					// if (parent.testIfNetworkPoint(l)) assert false;
					pnt--;
				}
			}
			for (long l : toVisitVertices.keys()) {
				if (!parent.testIfPossibleNetworkPoint(l)) {
					pnt++;
				}
			}
			return pnt;
		}

		public int depth() {
			return 1 + (parent == null ? 0 : parent.depth());
		}

		public void printCurentState(String string, int lvl) {
			printCurentState(string, lvl, "");
		}

		public void printCurentState(String string, int lvl, String extra) {
			if (DEBUG_VERBOSE_LEVEL >= (parent == null ? lvl - 1 : lvl)) {
				StringBuilder tabs = new StringBuilder();
				for (int k = 0; k < depth(); k++) {
					tabs.append("   ");
				}
				System.out.println(String.format(" %s %-8s %d -> %d %s", tabs.toString(), string, visitedVerticesSize(),
						toVisitVertices.size(), extra));
			}
		}

		public RoutingContext getCtx() {
			return parent.getCtx();
		}

	}

	class FullNetwork extends NetworkIsland {

		RoutingContext ctx;
		// uses global entity by reference
		TLongObjectHashMap<Integer> networkPointsCluster = new TLongObjectHashMap<>();
		List<NetworkIsland> visualClusters = new ArrayList<>();

		FullNetwork(RoutingContext ctx) {
			super(null, null);
			this.ctx = ctx;
		}

		public RoutingContext getCtx() {
			return ctx;
		}

		protected Comparator<RouteSegment> getExpandIslandComparator() {
			return new Comparator<RouteSegment>() {

				@Override
				public int compare(RouteSegment o1, RouteSegment o2) {
					return Double.compare(o1.distanceFromStart, o2.distanceFromStart);
				}
			};
		}

		public boolean testIfNetworkPoint(long pntId) {
			if (networkPointsCluster.contains(pntId)) {
				return true;
			}
			return super.testIfNetworkPoint(pntId);
		}

		public int visitedVerticesSize() {
			// don't count top level so it's not used for cluster optimizations
			return 0;
		}

		@Override
		public int toVisitVerticesSize() {
			// don't count top level so it's not used for cluster optimizations
			return 0;
		}

		public boolean testIfPossibleNetworkPoint(long pntId) {
			return testIfNetworkPoint(pntId);
		}

		public void addCluster(NetworkIsland cluster, RouteSegmentPoint centerPoint) {
			// KISS: keep toVisitVertices - empty and use only networkPointsCluster
			if (DEBUG_STORE_ALL_ROADS) {
				visualClusters.add(cluster);
				toVisitVertices.putAll(cluster.toVisitVertices);
			}
			// no need to copy it's done before
			// networkPointsCluster.put(toVisitVertices)
			for (long key : cluster.visitedVertices.keys()) {
				if (visitedVertices.containsKey(key)) {
					throw new IllegalStateException();
				}
				visitedVertices.put(key, cluster.visitedVertices.get(key)); // reduce memory usage
			}
		}

		@Override
		public int depth() {
			return 0;
		}

		public TLongObjectHashMap<List<RouteSegment>> visualConnections() {
			TLongObjectHashMap<List<RouteSegment>> conn = new TLongObjectHashMap<>();
			for (NetworkIsland island : visualClusters) {
				List<RouteSegment> lst = new ArrayList<>();
				conn.put(calculateRoutePointInternalId(island.start), lst);
				for (RouteSegment border : island.toVisitVertices.valueCollection()) {
					lst.add(border);
				}
			}
			return conn;
		}

		public TLongObjectHashMap<RouteSegment> visualPoints() {
			TLongObjectHashMap<RouteSegment> visualPoints = new TLongObjectHashMap<>();
			visualPoints.putAll(toVisitVertices);
			for (NetworkIsland island : visualClusters) {
				visualPoints.put(calculateRoutePointInternalId(island.start), island.start);
			}
			return visualPoints;
		}

	}

	private class NetworkCollectPointCtx {
		int totalBorderPoints = 0;
		TIntIntHashMap borderPntsDistr = new TIntIntHashMap();
		TLongIntHashMap borderPntsCluster = new TLongIntHashMap();
		TIntIntHashMap pntsDistr = new TIntIntHashMap();
		TIntIntHashMap edgesDistr = new TIntIntHashMap();
		int edges;
		int isolatedIslands = 0;
		int shortcuts = 0;
		int clusterInd = 0;

		RoutingContext rctx;
		HHRoutingPreparationDB networkDB;
		List<NetworkRouteRegion> routeRegions = new ArrayList<>();
		NetworkRouteRegion currentProcessingRegion;
		TLongObjectHashMap<Integer> networkPointsCluster = new TLongObjectHashMap<>();

		public NetworkCollectPointCtx(RoutingContext rctx, HHRoutingPreparationDB networkDB) {
			this.rctx = rctx;
			this.networkDB = networkDB;
		}

		public int getTotalPoints() {
			int totalPoints = 0;
			for (NetworkRouteRegion r : routeRegions) {
				totalPoints += r.getPoints();
			}
			return totalPoints;
		}

		public int clusterSize() {
			return clusterInd;
		}

		public int borderPointsSize() {
			return networkPointsCluster.size();
		}

		public void printStatsNetworks() {
			// calculate stats
			TIntIntHashMap borderClusterDistr = new TIntIntHashMap();
			for(int a : this.borderPntsCluster.values()) {
				borderClusterDistr.increment(a);
			}
			logf("RESULT %d points (%d edges) -> %d border points, %d clusters (%d isolated), %d est shortcuts (%s edges distr)",
					getTotalPoints() + borderPointsSize(), edges, borderPointsSize(), clusterSize(), isolatedIslands,
					shortcuts, distrString(edgesDistr, ""));
			
			logf("       %.1f avg (%s) border points per cluster"
					+ "\n     %s - shared border points between clusters "
					+ "\n     %.1f avg (%s) points in cluster",
					totalBorderPoints * 1.0 / clusterSize(), distrString(borderPntsDistr, ""),
					distrString(borderClusterDistr, ""),
					getTotalPoints() * 1.0 / clusterSize(), distrString(pntsDistr, "K"));
		}

		private String distrString(TIntIntHashMap distr, String suf) {
			int[] keys = distr.keys();
			Arrays.sort(keys);
			StringBuilder b = new StringBuilder();
			for (int k = 0; k < keys.length; k++) {
				if (k > 0) {
					b.append(", ");
				}
				b.append(keys[k]).append(suf).append(" ").append(distr.get(keys[k]));
			}
			return b.toString();
		}

		public FullNetwork startRegionProcess(NetworkRouteRegion nrouteRegion) throws IOException, SQLException {
			currentProcessingRegion = nrouteRegion;

			for (NetworkRouteRegion nr : routeRegions) {
				nr.unload();
			}
			List<NetworkRouteRegion> subRegions = new ArrayList<>();
			for (NetworkRouteRegion nr : routeRegions) {
				if (nr != nrouteRegion && nr.intersects(nrouteRegion)) {
					subRegions.add(nr);
				}
			}

			subRegions.add(nrouteRegion);
			// force cause subregions could change
			rctx = gcMemoryLimitToUnloadAll(rctx, subRegions, true);

			FullNetwork network = new FullNetwork(rctx);
			for (NetworkRouteRegion nr : subRegions) {
				if (nr != nrouteRegion) {
					network.visitedVertices.putAll(nr.getVisitedVertices(networkDB));
					// 3.1 TODO store network points inside NetworkRouteRegion and retrieve similar
					// to visited points
					network.networkPointsCluster = this.networkPointsCluster; // ! use by reference
//					network.networkPointsCluster.putAll(nr.getNetworkPoints(networkDB));
				}
			}
			return network;
		}

		public void addCluster(NetworkIsland cluster, FullNetwork network, RouteSegmentPoint pntAround) {
			cluster.index = clusterInd++;
			if (currentProcessingRegion != null) {
				currentProcessingRegion.visitedVertices.putAll(cluster.visitedVertices);
			}
			for (long k : cluster.toVisitVertices.keys()) {
				borderPntsCluster.increment(k);
			}
			try {
				networkDB.insertCluster(cluster, networkPointsCluster);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			int borderPoints = cluster.toVisitVertices.size();
			if (borderPoints == 0) {
				this.isolatedIslands++;
			}
			borderPntsDistr.increment(borderPoints);
			edges += cluster.edges;
			TIntIntIterator it = cluster.edgeDistr.iterator();
			while (it.hasNext()) {
				it.advance();
				edgesDistr.adjustValue(it.key(), it.value());
			}
			pntsDistr.increment(cluster.visitedVertices.size()/1000);
			this.totalBorderPoints += borderPoints;
			this.shortcuts += borderPoints * (borderPoints - 1);
			
		}

		public void finishRegionProcess(FullNetwork network) throws SQLException {
			logf("Tiles " + rctx.calculationProgress.getInfo(null).get("tiles"));
			logf("Saving visited %,d points from %s to db...", currentProcessingRegion.getPoints(),
					currentProcessingRegion.region.getName());
			networkDB.insertVisitedVertices(currentProcessingRegion);
			currentProcessingRegion.unload();
			logf("     saved - total %,d points", getTotalPoints());

		}

	}

	private class RouteDataObjectProcessor implements ResultMatcher<RouteDataObject> {

		int indProc = 0, prevPrintInd = 0;
		private float estimatedRoads;
		private FullNetwork network;
		private NetworkCollectPointCtx ctx;

		public RouteDataObjectProcessor(FullNetwork network, NetworkCollectPointCtx ctx, float estimatedRoads) {
			this.estimatedRoads = estimatedRoads;
			this.network = network;
			this.ctx = ctx;
		}

		@Override
		public boolean publish(RouteDataObject object) {
			if (!network.ctx.getRouter().acceptLine(object)) {
				return false;
			}
			indProc++;
			if (indProc < DEBUG_LIMIT_START_OFFSET || isCancelled()) {
				System.out.println("SKIP PROCESS " + indProc);
			} else {
				RouteSegmentPoint pntAround = new RouteSegmentPoint(object, 0, 0);
				long mainPoint = calculateRoutePointInternalId(pntAround);
				if (network.visitedVertices.contains(mainPoint)
						|| network.networkPointsCluster.containsKey(mainPoint)) {
					// already existing cluster
					return false;
				}
				NetworkIsland cluster;
				if (V2) {
					cluster = buildRoadNetworkIslandV2(network, pntAround);
				} else {
					cluster = new NetworkIsland(network, pntAround);
					buildRoadNetworkIsland(cluster);
				}
				if (DEBUG_VERBOSE_LEVEL >= 1) {
					int nwPoints = cluster.toVisitVertices.size();
					logf("CLUSTER: %2d border <- %4d points (%d segments) - %s", nwPoints,
							cluster.visitedVertices.size(), nwPoints * (nwPoints - 1), pntAround);
				}
				ctx.addCluster(cluster, network, pntAround);
				network.addCluster(cluster, pntAround);
				if (DEBUG_VERBOSE_LEVEL >= 1 || indProc - prevPrintInd > 1000) {
					prevPrintInd = indProc;
					int borderPointsSize = ctx.borderPointsSize();
					logf("%,d %.2f%%: %,d points -> %,d border points, %,d clusters", indProc,
							indProc * 100.0f / estimatedRoads, ctx.getTotalPoints() + borderPointsSize,
							borderPointsSize, ctx.clusterSize());
				}

			}
			return false;
		}

		@Override
		public boolean isCancelled() {
			return DEBUG_LIMIT_PROCESS != -1 && indProc >= DEBUG_LIMIT_PROCESS;
		}
	}

	private FullNetwork collectNetworkPoints(HHRoutingPreparationDB networkDB) throws IOException, SQLException {
		RoutingContext rctx = prepareContext(null, null);
		if (EX != null) {
			DEBUG_STORE_ALL_ROADS = true;
			FullNetwork network = new FullNetwork(rctx);
			RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
			RouteSegmentPoint pnt = router.findRouteSegment(EX.getLatitude(), EX.getLongitude(), network.ctx, null);
			NetworkIsland cluster;
			if (V2) {
				cluster = buildRoadNetworkIslandV2(network, pnt);
			} else {
				cluster = new NetworkIsland(network, pnt);
				buildRoadNetworkIsland(cluster);
			}
			network.addCluster(cluster, pnt);
			networkDB.insertCluster(cluster, network.networkPointsCluster);
			return network;
		}
		NetworkCollectPointCtx ctx = new NetworkCollectPointCtx(rctx, networkDB);
		networkDB.loadNetworkPoints(ctx.networkPointsCluster);
		Set<String> routeRegionNames = new TreeSet<>();
		for (RouteRegion r : rctx.reverseMap.keySet()) {
			if (routeRegionNames.add(r.getName())) {
				ctx.routeRegions.add(new NetworkRouteRegion(r, rctx.reverseMap.get(r).getFile()));
			} else {
				logf("Ignore route region %s as duplicate", r.getName());
			}
		}
		networkDB.insertRegions(ctx.routeRegions);
		ctx.clusterInd = networkDB.getMaxClusterId();
		FullNetwork network = null;

		for (NetworkRouteRegion nrouteRegion : ctx.routeRegions) {
			System.out.println("------------------------");
			logf("Region %s %d of %d %s", nrouteRegion.region.getName(), nrouteRegion.id + 1, ctx.routeRegions.size(),
					new Date().toString());
			if (networkDB.hasVisitedPoints(nrouteRegion)) {
				System.out.println("Already processed");
				continue;
			}

			network = ctx.startRegionProcess(nrouteRegion);
			RouteRegion routeRegion = null;
			for (RouteRegion rr : network.ctx.reverseMap.keySet()) {
				if (rr.getFilePointer() == nrouteRegion.region.getFilePointer()
						&& nrouteRegion.region.getName().equals(rr.getName())) {
					routeRegion = rr;
					break;
				}
			}
			BinaryMapIndexReader reader = network.ctx.reverseMap.get(routeRegion);
			List<RouteSubregion> regions = reader.searchRouteIndexTree(
					BinaryMapIndexReader.buildSearchRequest(
							MapUtils.get31TileNumberX(nrouteRegion.region.getLeftLongitude() - 1),
							MapUtils.get31TileNumberX(nrouteRegion.region.getRightLongitude() + 1),
							MapUtils.get31TileNumberY(nrouteRegion.region.getTopLatitude() + 1),
							MapUtils.get31TileNumberY(nrouteRegion.region.getBottomLatitude() - 1), 16, null),
					routeRegion.getSubregions());

			final int estimatedRoads = 1 + routeRegion.getLength() / 150; // 5 000 / 1 MB - 1 per 200 Byte
			reader.loadRouteIndexData(regions, new RouteDataObjectProcessor(network, ctx, estimatedRoads));
			ctx.finishRegionProcess(network);
			ctx.printStatsNetworks();
		}
		return network;
	}

	private NetworkIsland buildRoadNetworkIslandV2(FullNetwork f, RouteSegmentPoint pnt) {
		NetworkIsland c = new NetworkIsland(f, pnt, true);
		c.printCurentState("START", 2);
//		double minItValue = Double.POSITIVE_INFINITY;
		while (!c.queue.isEmpty()) {
			proceed(c, c.queue.poll(), c.queue, BRIDGE_MAX_DEPTH);
		}

		for (RouteSegmentCustom r : c.allVertices.valueCollection()) {
			r.cacheId = calculateRoutePointInternalId(r);
			r.cacheDepth = r.getDepth();
		}
		List<RouteSegmentCustom> vertices = new ArrayList<>();
		List<RouteSegmentCustom> source = new ArrayList<>();
		for (RouteSegmentCustom r : c.allVertices.valueCollection()) {
			if (c.toVisitVertices.contains(r.cacheId)) {
				source.add(r);
			} else {
				c.edges += r.connections.size();
				c.edgeDistr.increment(r.connections.size());
				vertices.add(r);
				Iterator<RouteSegmentConn> it = r.connections.iterator();
				while (it.hasNext()) {
					RouteSegmentConn conn = it.next();
					if (c.toVisitVertices.contains(conn.t.cacheId)) {
						conn.t.addConnection(r);
					}
				}
			}
		}
		TLongObjectHashMap<RouteSegmentCustom> mincuts = runMaxFlow(vertices, BRIDGE_MIN_DEPTH, source, pnt.toString());


		// Debug purposes
//		recalculateToVisitPointsToDoubleCheck = false;
//		System.out.println("Max flow " + mincuts.size());
//		c.toVisitVertices.clear();
//		c.toVisitVertices.putAll(mincuts);
//		for (RouteSegmentCustom r : c.allVertices.valueCollection()) {
//			int flow = 0;
//			for (RouteSegmentConn t : r.connections) {
//				flow = Math.max(t.flow, flow);
//			}
//			if (flow <= 1) {
//				c.visitedVertices.remove(r.cacheId);
//			}
//		}

		if (RECALC_CHECK_MAXFLOW) {
			c.visitedVertices.clear();
			c.toVisitVertices.clear();
			c.allVertices.clear();
			c.queue.add(c.getSegment(pnt, true));
			while (!c.queue.isEmpty()) {
				RouteSegmentCustom ls = c.queue.poll();
				if (mincuts.containsKey(calculateRoutePointInternalId(ls))) {
					continue;
				}
				proceed(c, ls, c.queue, BRIDGE_MAX_DEPTH);
			}

//			System.out.printf("MINCUT %.2f %d -> %d \n", coeffToMinimize(c.visitedVerticesSize(), c.toVisitVerticesSize()), 
//					c.visitedVerticesSize(), c.toVisitVerticesSize());
			if (mincuts.size() != c.toVisitVerticesSize()) {
				String msg = String.format("Bug mincut %d != %d graph reached size: %s", mincuts.size(),
						c.toVisitVerticesSize(), pnt.toString());
				System.err.println(msg);
				throw new IllegalStateException(msg);
			}
		}
		
		c.printCurentState("END", 2);

		return c;
	}

	private TLongObjectHashMap<RouteSegmentCustom> runMaxFlow(List<RouteSegmentCustom> vertices, int minDepth,
			List<RouteSegmentCustom> sources, String errorDebug) {
		RouteSegmentCustom source = new RouteSegmentCustom();
		for (RouteSegmentCustom s : sources) {
			source.addConnection(s);
			s.addConnection(source);
		}
		vertices.addAll(sources);
		List<RouteSegmentCustom> sinks = new ArrayList<>();
		RouteSegmentCustom sink = null;
		do {
			for (RouteSegmentCustom rs : vertices) {
				rs.flowParentTemp = null;
			}
			sink = null;
			LinkedList<RouteSegmentCustom> queue = new LinkedList<>();
			queue.add(source);
			while (!queue.isEmpty() && sink == null) {
				RouteSegmentCustom seg = queue.poll();
				for (RouteSegmentConn conn : seg.connections) {
					if (conn.t.flowParentTemp == null && conn.flow < 1) {
						conn.t.flowParentTemp = conn;
						if (conn.t.cacheDepth <= minDepth && conn.t.cacheDepth > 0) {
							sink = conn.t;
							break;
						} else {
							queue.add(conn.t);
						}
					}
				}
			}
			if (sink != null) {
				sinks.add(sink);
				RouteSegmentCustom p = sink;
				while (true) {
					p.flowParent = p.flowParentTemp;
					p.flowParent.flow++;
					if (p.flowParent.s == source) {
						break;
					}
					p.getConnection(p.flowParent.s).flow--;
					p = p.flowParent.s;
				}
			}
		} while (sink != null);

		for (RouteSegmentCustom rs : vertices) {
			rs.flowParentTemp = null;
		}

		TLongObjectHashMap<RouteSegmentCustom> mincuts = new TLongObjectHashMap<>();
		// for debug purposes
//		for (RouteSegmentCustom s : sinks) {
//			mincuts.put(calculateRoutePointInternalId(s), s);
//		}

		LinkedList<RouteSegmentCustom> queue = new LinkedList<>();
		Set<RouteSegmentCustom> reachableSource = new HashSet<>();
		queue.add(source);
		reachableSource.add(source);
		while (!queue.isEmpty()) {
			RouteSegmentCustom ps = queue.poll();
			for (RouteSegmentConn conn : ps.connections) {
				if (conn.t.flowParentTemp == null && conn.flow < 1) {
					conn.t.flowParentTemp = conn;
					queue.add(conn.t);
					reachableSource.add(conn.t);

					conn.flow = 2;
				}
			}
		}

		queue = new LinkedList<>();
		queue.addAll(sinks);
		while (!queue.isEmpty()) {
			RouteSegmentCustom ps = queue.poll();
			boolean mincut = false;
			for (RouteSegmentConn conn : ps.connections) {
				if (reachableSource.contains(conn.t)) {
					mincut = true;
					break;
				}
			}
			if (mincut) {
				mincuts.put(calculateRoutePointInternalId(ps), ps);
			} else {
				for (RouteSegmentConn conn : ps.connections) {
					if (conn.t.flowParentTemp == null) {
						conn.t.flowParentTemp = conn;
						queue.add(conn.t);
					}
				}
			}
		}
		if (sinks.size() != mincuts.size()) {
			String msg = String.format("BUG maxflow %d != %d mincut: %s ", sinks.size(), mincuts.size(), errorDebug);
			System.err.println(msg);
//			throw new IllegalStateException(msg);
		}

		return mincuts;
	}

	private void buildRoadNetworkIsland(NetworkIsland c) {
		c.printCurentState("START", 2);
		while (c.toVisitVertices.size() < MAX_VERT_DEPTH_LOOKUP[c.depth() - 1] && !c.queue.isEmpty()) {
			if (!proceed(c, c.queue.poll(), c.queue)) {
				break;
			}
			// mergeStraights(c, queue); // potential improvement
			c.printCurentState("VISIT", 3);
		}
		c.printCurentState("INITIAL", 2);
		mergeStraights(c, null);
		if (c.depth() < MAX_VERT_DEPTH_LOOKUP.length) {
			mergeConnected(c);
			mergeStraights(c, null);
		}
		c.printCurentState("END", 2);
	}

	private void mergeConnected(NetworkIsland c) {
		List<RouteSegment> potentialNetworkPoints = new ArrayList<>(c.toVisitVertices.valueCollection());
		c.printCurentState("MERGE", 2);
		for (RouteSegment t : potentialNetworkPoints) {
			int curPoints = c.toVisitVerticesSize();
			if (!c.toVisitVertices.contains(calculateRoutePointInternalId(t))) {
				continue;
			}
			// long id = t.getRoad().getId() / 64;
			NetworkIsland bc = new NetworkIsland(c, t);
			buildRoadNetworkIsland(bc);

			// it could only increase by 1 (1->2)

			int newPoints = bc.toVisitVerticesSize();
			if (newPoints > MAX_NEIGHBOORS_N_POINTS) {
				continue;
			}
			if (((newPoints - curPoints) <= 2 || coeffToMinimize(c.visitedVerticesSize(),
					curPoints) > coeffToMinimize(bc.visitedVerticesSize(), newPoints))) {
				int sz = c.visitedVerticesSize();
				c.visitedVertices.putAll(bc.visitedVertices);
				for (long l : bc.visitedVertices.keys()) {
					c.toVisitVertices.remove(l);
				}
				for (long k : bc.toVisitVertices.keys()) {
					if (!c.visitedVertices.containsKey(k)) {
						c.toVisitVertices.put(k, bc.toVisitVertices.get(k));
					}
				}
				c.printCurentState(curPoints == newPoints ? "IGNORE" : " MERGED", 2,
						String.format(" - before %d -> %d", sz, curPoints));
			} else {
				c.printCurentState(" NOMERGE", 3, String.format(" - %d -> %d", bc.visitedVerticesSize(), newPoints));
			}
		}
	}

	private double coeffToMinimize(int internalSegments, int boundaryPoints) {
		return boundaryPoints * (boundaryPoints - 1) / 2.0 / internalSegments;
	}

	private void mergeStraights(NetworkIsland c, PriorityQueue<RouteSegmentCustom> queue) {
		boolean foundStraights = true;
		while (foundStraights && !c.toVisitVertices.isEmpty() && c.toVisitVertices.size() < MAX_NEIGHBOORS_N_POINTS) {
			foundStraights = false;
			for (RouteSegment segment : c.toVisitVertices.valueCollection()) {
				if (!c.testIfNetworkPoint(calculateRoutePointInternalId(segment))
						&& nonVisitedSegmentsLess(c, segment, 1)) {
					if (proceed(c, (RouteSegmentCustom) segment, queue)) {
						foundStraights = true;
						break;
					}
				}
			}
		}
		c.printCurentState("CONNECT", 2);
	}

	private boolean nonVisitedSegmentsLess(NetworkIsland c, RouteSegment segment, int max) {
		int cnt = countNonVisited(c, segment, segment.getSegmentEnd());
		cnt += countNonVisited(c, segment, segment.getSegmentStart());
		if (cnt <= max) {
			return true;
		}
		return false;
	}

	private int countNonVisited(NetworkIsland c, RouteSegment segment, int ind) {
		int x = segment.getRoad().getPoint31XTile(ind);
		int y = segment.getRoad().getPoint31YTile(ind);
		RouteSegment next = c.getCtx().loadRouteSegment(x, y, 0);
		int cnt = 0;
		while (next != null) {
			long pntId = calculateRoutePointInternalId(next);
			if (!c.testIfVisited(pntId) && !c.testIfPossibleNetworkPoint(pntId)) {
				cnt++;
			}
			next = next.getNext();
		}
		return cnt;
	}

	private boolean proceed(NetworkIsland c, RouteSegmentCustom segment, PriorityQueue<RouteSegmentCustom> queue) {
		return proceed(c, segment, queue, -1);
	}

	private float distSegment(RouteSegment segment) {
		if (segment == null) {
			return 0;
		}
		int prevX = segment.getRoad().getPoint31XTile(segment.getSegmentStart());
		int prevY = segment.getRoad().getPoint31YTile(segment.getSegmentStart());
		int x = segment.getRoad().getPoint31XTile(segment.getSegmentEnd());
		int y = segment.getRoad().getPoint31YTile(segment.getSegmentEnd());
		return (float) MapUtils.squareRootDist31(x, y, prevX, prevY);
	}

	private boolean proceed(NetworkIsland c, RouteSegmentCustom segment, PriorityQueue<RouteSegmentCustom> queue,
			int maxDepth) {
		if (segment.distanceFromStart > MAX_RADIUS_ISLAND) {
			return false;
		}
		if (maxDepth > 0 && segment.getDepth() >= maxDepth) {
			return false;
		}
		long pntId = calculateRoutePointInternalId(segment);
//		System.out.println(" > " + segment);
		if (c.testIfNetworkPoint(pntId)) {
			return true;
		}
		c.toVisitVertices.remove(pntId);
		if (c.testIfVisited(pntId)) {
			throw new IllegalStateException();
		}
		c.visitedVertices.put(pntId, DEBUG_STORE_ALL_ROADS ? segment : null);
		float dist = segment.distanceFromStart + distSegment(segment) / 2;

		addSegment(c, dist, segment, true, queue);
		addSegment(c, dist, segment, false, queue);
		return true;
	}

	private void addSegment(NetworkIsland c, float distFromStart, RouteSegmentCustom segment, boolean end,
			PriorityQueue<RouteSegmentCustom> queue) {
		int segmentInd = end ? segment.getSegmentEnd() : segment.getSegmentStart();
		int x = segment.getRoad().getPoint31XTile(segmentInd);
		int y = segment.getRoad().getPoint31YTile(segmentInd);
		RouteSegment next = c.getCtx().loadRouteSegment(x, y, 0);
		while (next != null) {
			// next.distanceFromStart == 0
			RouteSegmentCustom pos = c.getSegment(next, true);
			long nextPnt = calculateRoutePointInternalId(pos);
			if (!c.testIfVisited(nextPnt) && !c.toVisitVertices.containsKey(nextPnt)) {
				pos.parentRoute = segment;
				pos.distanceFromStart = distFromStart + distSegment(pos) / 2;
				queue.add(pos);
				c.toVisitVertices.put(nextPnt, pos);
			}
			segment.addConnection(pos);

			RouteSegmentCustom opp = c.getSegment(next.initRouteSegment(!next.isPositive()), true);
			long oppPnt = opp == null ? 0 : calculateRoutePointInternalId(opp);
			if (opp != null && !c.testIfVisited(oppPnt) && !c.toVisitVertices.containsKey(oppPnt)) {
				opp.parentRoute = segment;
				opp.distanceFromStart = distFromStart + distSegment(opp) / 2;
				queue.add(opp);
				c.toVisitVertices.put(oppPnt, opp);
			}
			segment.addConnection(opp);
			next = next.getNext();
		}
	}

	///////////////////////////////////////////// BUILD SHORTCUTS
	///////////////////////////////////////////// /////////////////////////////////////////////

	private static class BuildNetworkShortcutResult {
		List<NetworkDBPoint> points = new ArrayList<>();
		List<RouteCalculationProgress> progress = new ArrayList<>();
		TIntArrayList shortcuts = new TIntArrayList();
		TLongObjectHashMap<Entity> osmObjects = new TLongObjectHashMap<>();
		double totalTime;
		int taskId;
	}

	private static class BuildNetworkShortcutTask implements Callable<BuildNetworkShortcutResult> {

		private static ThreadLocal<RoutingContext> context = new ThreadLocal<>();

		private List<NetworkDBPoint> batch;
		private TLongObjectHashMap<RouteSegment> segments;
		private HHRoutingGraphCreator creator;
		private TLongObjectHashMap<NetworkDBPoint> networkPoints;
		private int taskId;

		public BuildNetworkShortcutTask(HHRoutingGraphCreator creator, List<NetworkDBPoint> batch,
				TLongObjectHashMap<RouteSegment> segments, TLongObjectHashMap<NetworkDBPoint> networkPoints,
				int taskId) {
			this.creator = creator;
			this.batch = batch;
			this.segments = segments;
			this.networkPoints = networkPoints;
			this.taskId = taskId;
		}

		@Override
		public BuildNetworkShortcutResult call() throws Exception {
			RoutingContext ctx = context.get();

			ctx = creator.gcMemoryLimitToUnloadAll(ctx, null, ctx == null);
			context.set(ctx);
			BuildNetworkShortcutResult res = new BuildNetworkShortcutResult();
			res.taskId = taskId;
			long nt = System.nanoTime();
			for (NetworkDBPoint pnt : batch) {
				if (Thread.interrupted()) {
					return res;
				}
				RouteSegment s = null;
				try {
					ctx.calculationProgress = new RouteCalculationProgress();
					long nt2 = System.nanoTime();
					s = ctx.loadRouteSegment(pnt.startX, pnt.startY, ctx.config.memoryLimitation);
					while (s != null && (s.getRoad().getId() != pnt.roadId || s.getSegmentStart() != pnt.start
							|| s.getSegmentEnd() != pnt.end)) {
						s = s.getNext();
					}
					if (s == null) {
						throw new IllegalStateException("Error on segment " + pnt.roadId / 64);
					}

					HHRoutingUtilities.addNode(res.osmObjects, pnt, getPoint(s), "highway", "stop"); // "place","city");
					List<RouteSegment> result = creator.runDijsktra(ctx, s, segments);
					for (RouteSegment t : result) {
						NetworkDBPoint end = networkPoints.get(calculateRoutePointInternalId(t.getRoad().getId(),
								Math.min(t.getSegmentStart(), t.getSegmentEnd()),
								Math.max(t.getSegmentStart(), t.getSegmentEnd())));
						NetworkDBSegment segment = new NetworkDBSegment(pnt, end, t.getDistanceFromStart(), true,
								false);
						pnt.connected.add(segment);
						while (t != null) {
							segment.geometry.add(getPoint(t));
							t = t.getParentRoute();
						}
						Collections.reverse(segment.geometry);
						if (DEBUG_STORE_ALL_ROADS) {
							addWay(res.osmObjects, segment, "highway", "secondary");
						}
						if (segment.dist < 0) {
							throw new IllegalStateException(segment + " dist < " + segment.dist);
						}
					}
					pnt.rtDistanceFromStart = (System.nanoTime() - nt2) / 1e6;
					res.points.add(pnt);
					res.progress.add(ctx.calculationProgress);
					res.shortcuts.add(result.size());
				} catch (RuntimeException e) {
					logf("Error %s while processing %d road - %s (%d)", e.getMessage(), pnt.roadId / 64, pnt.getPoint(),
							pnt.index);
					throw e;
				}

			}
			segments = null;
			res.totalTime = (System.nanoTime() - nt) / 1e9;
			return res;
		}

	}

	private Collection<Entity> buildNetworkShortcuts(TLongObjectHashMap<NetworkDBPoint> networkPoints,
			HHRoutingPreparationDB networkDB)
			throws InterruptedException, IOException, SQLException, ExecutionException {
		TLongObjectHashMap<Entity> osmObjects = new TLongObjectHashMap<>();
		double sz = networkPoints.size() / 100.0;
		int ind = 0, prevPrintInd = 0;
		// 1.3 TODO for long distance causes bugs if (pnt.index != 2005) { 2005-> 1861 }
		// - 3372.75 vs 2598
		BinaryRoutePlanner.PRECISE_DIST_MEASUREMENT = true;
		TLongObjectHashMap<RouteSegment> segments = new TLongObjectHashMap<>();
		for (NetworkDBPoint pnt : networkPoints.valueCollection()) {
			segments.put(calculateRoutePointInternalId(pnt.roadId, pnt.start, pnt.end),
					new RouteSegment(null, pnt.start, pnt.end));
			segments.put(calculateRoutePointInternalId(pnt.roadId, pnt.end, pnt.start),
					new RouteSegment(null, pnt.end, pnt.start));
			HHRoutingUtilities.addNode(osmObjects, pnt, null, "highway", "stop");
		}

		ExecutorService service = Executors.newFixedThreadPool(THREAD_POOL);
		List<Future<BuildNetworkShortcutResult>> results = new ArrayList<>();
		List<NetworkDBPoint> batch = new ArrayList<>();
		int taskId = 0;
		for (NetworkDBPoint pnt : networkPoints.valueCollection()) {
			ind++;
			if (pnt.connected.size() > 0) {
				pnt.connected.clear(); // for gc
				continue;
			}
//			if (pnt.index > 2000 || pnt.index < 1800)  continue;
			if (ind < DEBUG_LIMIT_START_OFFSET) {
				continue;
			}
			batch.add(pnt);
			if (ind > DEBUG_LIMIT_PROCESS && DEBUG_LIMIT_PROCESS != -1) {
				break;
			}
			if (batch.size() == BATCH_SIZE) {
				results.add(
						service.submit(new BuildNetworkShortcutTask(this, batch, segments, networkPoints, taskId++)));
				batch = new ArrayList<>();
			}
		}
		results.add(service.submit(new BuildNetworkShortcutTask(this, batch, segments, networkPoints, taskId++)));
		logf("Scheduled %d tasks", taskId);
		int maxDirectedPointsGraph = 0;
		int maxFinalSegmentsFound = 0;
		int totalFinalSegmentsFound = 0;
		int totalVisitedDirectSegments = 0;
		ind = 0;
		service.shutdown();
		try {
			while (!results.isEmpty()) {
				Thread.sleep(5000);
				Iterator<Future<BuildNetworkShortcutResult>> it = results.iterator();
				while (it.hasNext()) {
					Future<BuildNetworkShortcutResult> future = it.next();
					if (future.isDone()) {
						BuildNetworkShortcutResult res = future.get();
						for (int k = 0; k < res.points.size(); k++) {
							NetworkDBPoint rpnt = res.points.get(k);
							RouteCalculationProgress calculationProgress = res.progress.get(k);
							ind++;
							if (DEBUG_VERBOSE_LEVEL >= 1 || ind - prevPrintInd > 200) {
								prevPrintInd = ind;
								logf("%.2f%% Process %d (%d shortcuts) - %.1f ms", ind / sz, rpnt.roadId / 64,
										res.shortcuts.get(k), rpnt.rtDistanceFromStart);
							}
							networkDB.insertSegments(rpnt.connected);
							if (DEBUG_VERBOSE_LEVEL >= 2) {
								System.out.println(calculationProgress.getInfo(null));
							}

							maxDirectedPointsGraph = Math.max(maxDirectedPointsGraph,
									calculationProgress.visitedDirectSegments);
							totalVisitedDirectSegments += calculationProgress.visitedDirectSegments;
							maxFinalSegmentsFound = Math.max(maxFinalSegmentsFound,
									calculationProgress.finalSegmentsFound);
							totalFinalSegmentsFound += calculationProgress.finalSegmentsFound;

							// clean up for gc
							rpnt.connected.clear();
						}
						osmObjects.putAll(res.osmObjects);
						it.remove();
						logf("Task id %d executed %.1f seconds - %d (of %d) waiting completion", res.taskId,
								res.totalTime, results.size(), taskId);
					}
				}
			}
		} finally {
			List<Runnable> runnable = service.shutdownNow();
			if (!results.isEmpty()) {
				logf("!!! %d runnable were not executed: exception occurred", runnable == null ? 0 : runnable.size());
			}
			service.awaitTermination(5, TimeUnit.MINUTES);
		}

		System.out.println(String.format(
				"Total segments %d: %d total shorcuts, per border point max %d, average %d shortcuts (routing sub graph max %d, avg %d segments)",
				segments.size(), totalFinalSegmentsFound, maxFinalSegmentsFound, totalFinalSegmentsFound / ind,
				maxDirectedPointsGraph, totalVisitedDirectSegments / ind));
		return osmObjects.valueCollection();
	}

	private List<RouteSegment> runDijsktra(RoutingContext ctx, RouteSegment s, TLongObjectMap<RouteSegment> segments)
			throws InterruptedException, IOException {

		long pnt1 = calculateRoutePointInternalId(s.getRoad().getId(), s.getSegmentStart(), s.getSegmentEnd());
		long pnt2 = calculateRoutePointInternalId(s.getRoad().getId(), s.getSegmentEnd(), s.getSegmentStart());
		segments = new ExcludeTLongObjectMap<RouteSegment>(segments, pnt1, pnt2);

		List<RouteSegment> res = new ArrayList<>();

		ctx.unloadAllData(); // needed for proper multidijsktra work
		ctx.calculationProgress = new RouteCalculationProgress();
		ctx.startX = s.getRoad().getPoint31XTile(s.getSegmentStart(), s.getSegmentEnd());
		ctx.startY = s.getRoad().getPoint31YTile(s.getSegmentStart(), s.getSegmentEnd());
		RouteSegmentPoint pnt = new RouteSegmentPoint(s.getRoad(), s.getSegmentStart(), s.getSegmentEnd(), 0);
		MultiFinalRouteSegment frs = (MultiFinalRouteSegment) new BinaryRoutePlanner().searchRouteInternal(ctx, pnt,
				null, null, segments);

		if (frs != null) {
			TLongSet set = new TLongHashSet();
			for (RouteSegment o : frs.all) {
				// duplicates are possible as alternative routes
				long pntId = calculateRoutePointInternalId(o);
				if (set.add(pntId)) {
					res.add(o);
				}
			}
		}

		return res;
	}

}
