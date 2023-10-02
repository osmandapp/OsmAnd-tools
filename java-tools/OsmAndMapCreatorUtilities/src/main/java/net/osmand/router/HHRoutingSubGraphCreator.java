package net.osmand.router;

import static net.osmand.router.HHRoutingPrepareContext.logf;
import static net.osmand.router.HHRoutingUtilities.calculateRoutePointInternalId;
import static net.osmand.router.HHRoutingUtilities.distrString;
import static net.osmand.router.HHRoutingUtilities.makePositiveDir;
import static net.osmand.router.HHRoutingUtilities.saveOsmFile;
import static net.osmand.router.HHRoutingUtilities.visualizeWays;
import static net.osmand.router.HHRoutingUtilities.distrSum;
import static net.osmand.router.HHRoutingUtilities.distrCumKey;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkRouteRegion;
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
// 3.1 Calculate points in parallel (Planet)
// 3.2 Make process rerunnable ? 

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

// TODO clean up (HHRoutingPrepareContext + HHRoutingPreparationDB)?

public class HHRoutingSubGraphCreator {

	final static Log LOG = PlatformUtil.getLog(HHRoutingSubGraphCreator.class);
	static int DEBUG_STORE_ALL_ROADS = 0; // 1 - clusters, 2 - all, 0 none
	static int DEBUG_LIMIT_START_OFFSET = 0;
	static int DEBUG_LIMIT_PROCESS = -1;
	static int DEBUG_VERBOSE_LEVEL = 0;

	private static HHRoutingPrepareContext prepareContext;

	// Constants / Tests for splitting building network points {7,7,7,7} - 50 -
	protected static LatLon EX1 = new LatLon(52.3201813, 4.7644685); // 337 -> 4 (1212 -> 4)
	protected static LatLon EX2 = new LatLon(52.33265, 4.77738); // 301 -> 12 (2240 -> 8)
	protected static LatLon EX3 = new LatLon(52.2728791, 4.8064803); // 632 -> 14 (923 -> 11 )
	protected static LatLon EX4 = new LatLon(52.27757, 4.85731); // 218 -> 7 (1599 -> 5)
	protected static LatLon EX5 = new LatLon(42.78725, 18.95036); // 391 -> 8
	// test combinations
	protected static LatLon EX6 = new LatLon(42.42385, 19.261171); //
	protected static LatLon EX7 = new LatLon(42.527111, 19.43255); //

	// TODO 0 (Lat 42.828068 Lon 19.842607): Road (389035663) bug maxflow 5 != 4 mincut 50-1500;
	protected static LatLon[] EX = {
//			EX6, EX7 
	}; 

	int TOTAL_MAX_POINTS = 100000, TOTAL_MIN_POINTS = 10000;
	
	static boolean CLEAN = false;
	

	private static File testData() {
		DEBUG_VERBOSE_LEVEL = 1;
		DEBUG_STORE_ALL_ROADS = 1;
		CLEAN = true;
		
		String name = "Montenegro_europe_2.road.obf";
//		name = "Netherlands_europe_2.road.obf";
//		name = "Ukraine_europe_2.road.obf";
//		name = "Germany";
		return new File(System.getProperty("maps.dir"), name);
	}

	public static void main(String[] args) throws Exception {
		CLEAN = false;
		File obfFile = args.length == 0 ? testData() : new File(args[0]);
		String routingProfile = null;
		for (String a : args) {
			if (a.startsWith("--routing_profile=")) {
				routingProfile = a.substring("--routing_profile=".length());
			} else if (a.startsWith("--maxdepth=")) {
//				BRIDGE_MAX_DEPTH = Integer.parseInt(a.substring("--maxdepth=".length()));
			} else if (a.equals("--clean")) {
				CLEAN = true;
			}
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getCanonicalFile().getName();

		File dbFile = new File(folder, name + HHRoutingPreparationDB.EXT);
		if (CLEAN && dbFile.exists()) {
			dbFile.delete();
		}
		HHRoutingPreparationDB networkDB = new HHRoutingPreparationDB(dbFile);
		prepareContext = new HHRoutingPrepareContext(obfFile, routingProfile);
		HHRoutingSubGraphCreator proc = new HHRoutingSubGraphCreator();
		NetworkCollectPointCtx ctx = proc.collectNetworkPoints(networkDB);
		List<Entity> objects = visualizeWays(ctx.visualPoints(), ctx.visualConnections(), ctx.allVisitedVertices);
		saveOsmFile(objects, new File(folder, name + ".osm"));
		networkDB.close();
	}


	private NetworkCollectPointCtx collectNetworkPoints(HHRoutingPreparationDB networkDB) throws IOException, SQLException {
		RoutingContext rctx = prepareContext.prepareContext(null, null);
		NetworkCollectPointCtx ctx = new NetworkCollectPointCtx(rctx, networkDB);
		if (EX != null && EX.length > 0) {
			DEBUG_STORE_ALL_ROADS = 2;
			RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
			for (LatLon l : EX) {
				RouteSegmentPoint pnt = router.findRouteSegment(l.getLatitude(), l.getLongitude(), ctx.rctx, null);
				NetworkIsland cluster = buildRoadNetworkIsland(ctx, pnt);
				ctx.addCluster(cluster);
				networkDB.insertCluster(cluster, ctx.networkPointToDbInd);
			}
			return ctx;
		}
		
		networkDB.loadNetworkPoints(ctx.networkPointToDbInd);
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

		for (NetworkRouteRegion nrouteRegion : ctx.routeRegions) {
			System.out.println("------------------------");
			logf("Region %s %d of %d %s", nrouteRegion.region.getName(), nrouteRegion.id + 1, ctx.routeRegions.size(),
					new Date().toString());
			if (networkDB.hasVisitedPoints(nrouteRegion)) {
				System.out.println("Already processed");
				continue;
			}

			ctx.startRegionProcess(nrouteRegion);
			RouteRegion routeRegion = null;
			for (RouteRegion rr : ctx.rctx.reverseMap.keySet()) {
				if (rr.getFilePointer() == nrouteRegion.region.getFilePointer()
						&& nrouteRegion.region.getName().equals(rr.getName())) {
					routeRegion = rr;
					break;
				}
			}
			BinaryMapIndexReader reader = ctx.rctx.reverseMap.get(routeRegion);
			List<RouteSubregion> regions = reader.searchRouteIndexTree(
					BinaryMapIndexReader.buildSearchRequest(
							MapUtils.get31TileNumberX(nrouteRegion.region.getLeftLongitude() - 1),
							MapUtils.get31TileNumberX(nrouteRegion.region.getRightLongitude() + 1),
							MapUtils.get31TileNumberY(nrouteRegion.region.getTopLatitude() + 1),
							MapUtils.get31TileNumberY(nrouteRegion.region.getBottomLatitude() - 1), 16, null),
					routeRegion.getSubregions());

			final int estimatedRoads = 1 + routeRegion.getLength() / 150; // 5 000 / 1 MB - 1 per 200 Byte
			reader.loadRouteIndexData(regions, new RouteDataObjectProcessor(ctx, estimatedRoads));
			ctx.finishRegionProcess();
			ctx.printStatsNetworks();
		}
		return ctx;
	}



	private NetworkIsland buildRoadNetworkIsland(NetworkCollectPointCtx ctx, RouteSegmentPoint pnt) {
		NetworkIsland c = new NetworkIsland(ctx, pnt);
		
		TLongObjectHashMap<RouteSegment> existNetworkPoints = new TLongObjectHashMap<RouteSegment>();
		TIntIntHashMap depthDistr = new TIntIntHashMap();
		int STEP = 10;
		int minDepth = 1, maxDepth = STEP + minDepth;
		
		c.addSegmentToQueue(c.getVertex(pnt));
		while (distrSum(depthDistr, maxDepth) < TOTAL_MAX_POINTS && c.toVisitVertices.size() > 0) {
			maxDepth ++;
			c.initQueueFromPointsToVisit();
			while (!c.queue.isEmpty()) {
				RouteSegmentVertex seg = c.queue.poll();
				depthDistr.adjustOrPutValue(seg.getDepth(), 1, 1);
				if (c.ctx.testIfNetworkPoint(seg.getId())) {
					existNetworkPoints.put(seg.getId(), seg);
					continue;
				}
				proceed(c, seg, c.queue, maxDepth);
				
			}
		}
		c.wrapConnectionsToVisitPoints();

		// prepare max flow
		// depthDistr.get(minDepth) < TOTAL_MIN_DEPTH_POINTS
		while (minDepth < maxDepth - STEP && distrSum(depthDistr, minDepth) < TOTAL_MIN_POINTS) {
			minDepth++;
		}
		TLongObjectHashMap<RouteSegmentVertex> mincuts = findMincutUsingMaxFlow(c, minDepth, existNetworkPoints, pnt.toString());
		recalculateClusterPointsUsingMincut(c, mincuts, minDepth, maxDepth, depthDistr);
		
		for (long key : c.visitedVertices.keys()) {
			RouteSegmentVertex r = c.allVertices.get(key);
			c.edges += r.connections.size();
			c.edgeDistr.adjustOrPutValue(r.connections.size(), 1, 1);
		}
//		System.out.println(distrString(depthDistr, "", true, true, 5));
		if (DEBUG_VERBOSE_LEVEL >= 1) {
			logf("\n   Process cluster: min depth  %d (%,d) <- max depth %d (%,d) - %d flow, border points %d (%,d size ~ %d depth)", minDepth,
					distrSum(depthDistr, minDepth), maxDepth, distrSum(depthDistr, maxDepth), 
					mincuts.size(), c.toVisitVertices.size(), 
					c.visitedVertices.size(), distrCumKey(depthDistr, c.visitedVertices.size()));
		}
		return c;
	}

	private void recalculateClusterPointsUsingMincut(NetworkIsland c, TLongObjectHashMap<RouteSegmentVertex> mincuts,
				int minDepth, int maxDepth, TIntIntHashMap depthDistr) {
		// Debug purposes
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

		c.clearVisitedPoints();
		c.queue.add(c.getVertex(c.start));
		TLongObjectHashMap<RouteSegment> existNetworkPoints = new TLongObjectHashMap<RouteSegment>();
		while (!c.queue.isEmpty()) {
			RouteSegmentVertex ls = c.queue.poll();
			if (mincuts.containsKey(ls.getId())) {
				continue;
			}
			if (c.ctx.testIfNetworkPoint(ls.getId())) {
				existNetworkPoints.put(ls.getId(), ls);
				continue;
			}
			proceed(c, ls, c.queue, maxDepth);
		}
		if (mincuts.size() + existNetworkPoints.size() != c.toVisitVertices.size()) {
			String msg = String.format("BUG!! mincut %d + %d network pnts != %d graph reached size: %s", mincuts.size(),
					existNetworkPoints.size(), c.toVisitVertices.size(), c.start.toString());
			System.err.println(msg);
//				throw new IllegalStateException(msg); // TODO
		}
		c.toVisitVertices.putAll(existNetworkPoints);
		
	}


	private boolean proceed(NetworkIsland c, RouteSegmentVertex vertex, PriorityQueue<RouteSegmentVertex> queue,
			int maxDepth) {
		if (maxDepth > 0 && vertex.getDepth() >= maxDepth) {
			return false;
		}
		c.toVisitVertices.remove(vertex.getId());
		if (c.testIfVisited(vertex.getId())) {
			throw new IllegalStateException();
		}
		c.visitedVertices.put(vertex.getId(), DEBUG_STORE_ALL_ROADS > 1 ? vertex : null);
		c.loadVertexConnections(vertex);
		float distFromStart = vertex.distanceFromStart + vertex.distSegment() / 2;
		for (RouteSegmentEdge e : vertex.connections) {
			if (!c.testIfVisited(e.t.getId()) && !c.toVisitVertices.containsKey(e.t.getId())) {
				e.t.parentRoute = vertex;
				e.t.distanceFromStart = distFromStart + e.t.distSegment() / 2;
				c.addSegmentToQueue(e.t);
			}
		}
		return true;
	}


	private TLongObjectHashMap<RouteSegmentVertex> findMincutUsingMaxFlow(NetworkIsland c, int minDepth,
			TLongObjectHashMap<RouteSegment> existingVertices, String errorDebug) {
		List<RouteSegmentVertex> vertices = new ArrayList<>();
		List<RouteSegmentVertex> sources = new ArrayList<>();
		for (RouteSegmentVertex r : c.allVertices.valueCollection()) {
			if (c.toVisitVertices.contains(r.getId())) {
				if (!existingVertices.contains(r.getId())) {
					sources.add(r);
				}
			} else {
				vertices.add(r);
			}
		}

		RouteSegmentVertex source = new RouteSegmentVertex();
		for (RouteSegmentVertex s : sources) {
			source.addConnection(s);
			s.addConnection(source);
		}
		vertices.addAll(sources);
		List<RouteSegmentVertex> sinks = new ArrayList<>();
		RouteSegmentVertex sink = null;
		do {
			for (RouteSegmentVertex rs : vertices) {
				rs.flowParentTemp = null;
			}
			sink = null;
			LinkedList<RouteSegmentVertex> queue = new LinkedList<>();
			queue.add(source);
			while (!queue.isEmpty() && sink == null) {
				RouteSegmentVertex seg = queue.poll();
				for (RouteSegmentEdge conn : seg.connections) {
					if (conn.t.flowParentTemp == null && conn.flow < 1) {
						conn.t.flowParentTemp = conn;
						if (conn.t.getDepth() <= minDepth && conn.t.getDepth() > 0) {
//							|| existingVertices.contains(conn.t.getId())
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
				RouteSegmentVertex p = sink;
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
		TLongObjectHashMap<RouteSegmentVertex> mincuts = calculateMincut(vertices, source, sinks);
		if (sinks.size() != mincuts.size()) {
			String msg = String.format("BUG maxflow %d != %d mincut: %s ", sinks.size(), mincuts.size(), errorDebug);
			System.err.println(msg);
//			throw new IllegalStateException(msg); // TODO
		}
		return mincuts;
	}

	private TLongObjectHashMap<RouteSegmentVertex> calculateMincut(List<RouteSegmentVertex> vertices,
			 RouteSegmentVertex source, List<RouteSegmentVertex> sinks) {
		for (RouteSegmentVertex rs : vertices) {
			rs.flowParentTemp = null;
		}

		TLongObjectHashMap<RouteSegmentVertex> mincuts = new TLongObjectHashMap<>();
		// for debug purposes
//		for (RouteSegmentCustom s : sinks) {
//			mincuts.put(calculateRoutePointInternalId(s), s);
//		}

		LinkedList<RouteSegmentVertex> queue = new LinkedList<>();
		Set<RouteSegmentVertex> reachableSource = new HashSet<>();
		queue.add(source);
		reachableSource.add(source);
		while (!queue.isEmpty()) {
			RouteSegmentVertex ps = queue.poll();
			for (RouteSegmentEdge conn : ps.connections) {
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
			RouteSegmentVertex ps = queue.poll();
			boolean mincut = false;
			for (RouteSegmentEdge conn : ps.connections) {
				if (reachableSource.contains(conn.t)) {
					mincut = true;
					break;
				}
			}
			if (mincut) {
				mincuts.put(calculateRoutePointInternalId(ps), ps);
			} else {
				for (RouteSegmentEdge conn : ps.connections) {
					if (conn.t.flowParentTemp == null) {
						conn.t.flowParentTemp = conn;
						queue.add(conn.t);
					}
				}
			}
		}
		return mincuts;
	}


	class RouteSegmentEdge {
		RouteSegmentVertex s;
		RouteSegmentVertex t;
		int flow;

		RouteSegmentEdge(RouteSegmentVertex s, RouteSegmentVertex t) {
			this.s = s;
			this.t = t;
		}
	}

	class RouteSegmentVertex extends RouteSegment {

		public List<RouteSegmentEdge> connections = new ArrayList<>();
		public int cDepth;
		public final long cId;
		public RouteSegmentEdge flowParent;
		public RouteSegmentEdge flowParentTemp;

		public RouteSegmentVertex(RouteSegment s) {
			super(s.getRoad(), s.getSegmentStart(), s.getSegmentEnd());
			cId = calculateRoutePointInternalId(this);
		}

		public RouteSegmentVertex() {
			// artificial
			super(null, 0, 0);
			cId = 0;
		}

		public long getId() {
			return cId;
		}

		public void addConnection(RouteSegmentVertex pos) {
			if (pos != this && pos != null) {
				connections.add(new RouteSegmentEdge(this, pos));
			}
		}

		@Override
		public int getDepth() {
			if (cDepth == 0) {
				cDepth = super.getDepth();
			}
			return cDepth;
		}
		
		public float distSegment() {
			RouteSegment segment = this;
			int prevX = segment.getRoad().getPoint31XTile(segment.getSegmentStart());
			int prevY = segment.getRoad().getPoint31YTile(segment.getSegmentStart());
			int x = segment.getRoad().getPoint31XTile(segment.getSegmentEnd());
			int y = segment.getRoad().getPoint31YTile(segment.getSegmentEnd());
			return (float) MapUtils.measuredDist31(x, y, prevX, prevY);
		}

		public RouteSegmentEdge getConnection(RouteSegmentVertex t) {
			for (RouteSegmentEdge c : connections) {
				if (c.t == t) {
					return c;
				}
			}
			return null;
		}

	}

	class NetworkIsland {
		final RouteSegment start;
		final NetworkCollectPointCtx ctx;
		final PriorityQueue<RouteSegmentVertex> queue;
		
		int dbIndex;
		int edges = 0;
		TIntIntHashMap edgeDistr = new TIntIntHashMap();
		TLongObjectHashMap<RouteSegment> visitedVertices = new TLongObjectHashMap<>();
		TLongObjectHashMap<RouteSegment> toVisitVertices = new TLongObjectHashMap<>();
		TLongObjectHashMap<RouteSegmentVertex> allVertices = new TLongObjectHashMap<>();

		NetworkIsland(NetworkCollectPointCtx ctx, RouteSegment start) {
			this.ctx = ctx;
			this.start = start;
			this.queue = new PriorityQueue<>(new Comparator<RouteSegment>() {

				@Override
				public int compare(RouteSegment o1, RouteSegment o2) {
					return Double.compare(o1.distanceFromStart, o2.distanceFromStart);
				}
			});
		}

		public RouteSegmentVertex getVertex(RouteSegment s) {
			if (s == null) {
				return null;
			}
			boolean create = true;
			long p = calculateRoutePointInternalId(s.getRoad(), Math.min(s.segStart, s.segEnd),
					Math.max(s.segStart, s.segEnd));
			RouteSegmentVertex routeSegment = allVertices != null ? allVertices.get(p) : null;
			if (routeSegment == null && create) {
				routeSegment = new RouteSegmentVertex(makePositiveDir(s));
				if (allVertices != null) {
					allVertices.put(p, routeSegment);
				}
			}
			return routeSegment;
		}
		
		
		public void loadVertexConnections(RouteSegmentVertex segment) {
			if (!segment.connections.isEmpty()) {
				return;
			}
			loadVertexConnections(segment, true);
			loadVertexConnections(segment, false);
		}
		
		public void initQueueFromPointsToVisit() {
			queue.clear();
			for (RouteSegment r : toVisitVertices.valueCollection()) {
				queue.add((RouteSegmentVertex) r);
			}			
		}
		
		void loadVertexConnections(RouteSegmentVertex segment, boolean end) {
			int segmentInd = end ? segment.getSegmentEnd() : segment.getSegmentStart();
			int x = segment.getRoad().getPoint31XTile(segmentInd);
			int y = segment.getRoad().getPoint31YTile(segmentInd);
			RouteSegment next = ctx.rctx.loadRouteSegment(x, y, 0);
			while (next != null) {
				segment.addConnection(getVertex(next));
				segment.addConnection(getVertex(next.initRouteSegment(!next.isPositive())));
				next = next.getNext();
			}
		}

		private NetworkIsland addSegmentToQueue(RouteSegmentVertex s) {
			queue.add(s);
			toVisitVertices.put(s.getId(), s);
			return this;
		}


		public boolean testIfVisited(long pntId) {
			if (visitedVertices.containsKey(pntId)) {
				return true;
			}
			if (ctx.allVisitedVertices.containsKey(pntId)) {
				throw new IllegalStateException();
			}
			return false;
		}

		public void wrapConnectionsToVisitPoints() {
			for (RouteSegmentVertex r : allVertices.valueCollection()) {
				for (RouteSegmentEdge e : r.connections) {
					if (toVisitVertices.contains(e.t.getId())) {
						e.t.addConnection(r);
					}
				}
			}
		}

		public void clearVisitedPoints() {
			for(RouteSegmentVertex v : allVertices.valueCollection()) {
				v.distanceFromStart = 0;
				v.cDepth = 0;
				v.parentRoute = null;
				v.flowParent = null;
				v.flowParentTemp = null;
			}
			visitedVertices.clear();
			toVisitVertices.clear();
		}

		

	}


	private class NetworkCollectStats {
		int totalBorderPoints = 0;
		TIntIntHashMap borderPntsDistr = new TIntIntHashMap();
		TLongIntHashMap borderPntsCluster = new TLongIntHashMap();
		TIntIntHashMap pntsDistr = new TIntIntHashMap();
		TIntIntHashMap edgesDistr = new TIntIntHashMap();
		int edges;
		int isolatedIslands = 0;
		int shortcuts = 0;
		

		public void printStatsNetworks(int totalPoints, int clusterSize ) {
			int borderPointsSize = borderPntsCluster.size();
			TIntIntHashMap borderClusterDistr = new TIntIntHashMap();
			for (int a : this.borderPntsCluster.values()) {
				borderClusterDistr.adjustOrPutValue(a, 1, 1);
			}
			logf("RESULT %,d points (%,d edges) -> %d border points, %d clusters (%d isolated), %d est shortcuts (%s edges distr) \n",
					totalPoints + borderPointsSize, edges / 2, borderPointsSize, clusterSize,
					isolatedIslands, shortcuts, distrString(edgesDistr, ""));

			System.out.printf("       %.1f avg (%s) border points per cluster \n"
							+ "       %s - shared border points between clusters \n"
							+ "       %.1f avg (%s) points in cluster \n",
					totalBorderPoints * 1.0 / clusterSize, distrString(borderPntsDistr, ""),
					distrString(borderClusterDistr, ""), totalPoints * 1.0 / clusterSize,
					distrString(pntsDistr, "K"));
		}


		public void addCluster(NetworkIsland cluster) {
			for (long k : cluster.toVisitVertices.keys()) {
				borderPntsCluster.adjustOrPutValue(k, 1, 1);
			}
			int borderPoints = cluster.toVisitVertices.size();
			if (borderPoints == 0) {
				this.isolatedIslands++;
			} else {
				pntsDistr.adjustOrPutValue((cluster.visitedVertices.size() + 500) / 1000, 1, 1);
			}
			edges += cluster.edges;
			TIntIntIterator it = cluster.edgeDistr.iterator();
			while (it.hasNext()) {
				it.advance();
				edgesDistr.adjustOrPutValue(it.key(), it.value(), it.value());
			}
			borderPntsDistr.adjustOrPutValue(borderPoints, 1, 1);
			this.totalBorderPoints += borderPoints;
			this.shortcuts += borderPoints * (borderPoints - 1);			
		}

	}
	
	class NetworkCollectPointCtx {
		RoutingContext rctx;
		HHRoutingPreparationDB networkDB;
		List<NetworkRouteRegion> routeRegions = new ArrayList<>();
		
		NetworkCollectStats stats = new NetworkCollectStats();
		List<NetworkIsland> visualClusters = new ArrayList<>();
		
		int clusterInd = 0;
		NetworkRouteRegion currentProcessingRegion;
		TLongObjectHashMap<Integer> networkPointToDbInd = new TLongObjectHashMap<>();
		TLongObjectHashMap<RouteSegment> allVisitedVertices = new TLongObjectHashMap<>(); 

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
			return networkPointToDbInd.size();
		}

		public void startRegionProcess(NetworkRouteRegion nrouteRegion) throws IOException, SQLException {
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
			// force reload cause subregions could change on rerun
			rctx = prepareContext.gcMemoryLimitToUnloadAll(rctx, subRegions, true);

			// 3.1 smart 
			this.allVisitedVertices = new TLongObjectHashMap<RouteSegment>();
			for (NetworkRouteRegion nr : subRegions) {
				if (nr != nrouteRegion) {
					this.allVisitedVertices.putAll(nr.getVisitedVertices(networkDB));
				}
			}
		}

		public void addCluster(NetworkIsland cluster) {
			cluster.dbIndex = clusterInd++;
			if (currentProcessingRegion != null) {
				currentProcessingRegion.visitedVertices.putAll(cluster.visitedVertices);
			}
			try {
				networkDB.insertCluster(cluster, networkPointToDbInd);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			stats.addCluster(cluster);
			if (DEBUG_STORE_ALL_ROADS > 0) {
				visualClusters.add(cluster);
			}

			for (long key : cluster.visitedVertices.keys()) {
				if (allVisitedVertices.containsKey(key)) {
					throw new IllegalStateException("Point was already visited");
				}
				allVisitedVertices.put(key, cluster.visitedVertices.get(key)); 
			}
		}

		public void finishRegionProcess() throws SQLException {
			logf("Tiles " + rctx.calculationProgress.getInfo(null).get("tiles"));
			logf("Saving visited %,d points from %s to db...", currentProcessingRegion.getPoints(),
					currentProcessingRegion.region.getName());
			networkDB.insertVisitedVertices(currentProcessingRegion);
			currentProcessingRegion.unload();
			currentProcessingRegion = null;
			logf("     saved - total %,d points", getTotalPoints());

		}
		
		public boolean testIfNetworkPoint(long pntId) {
			if (networkPointToDbInd.contains(pntId)) {
				return true;
			}
			return false;
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
			for (NetworkIsland island : visualClusters) {
				visualPoints.putAll(island.toVisitVertices);
				visualPoints.put(calculateRoutePointInternalId(island.start), island.start);
			}
			return visualPoints;
		}

		public void printStatsNetworks() {
			stats.printStatsNetworks(getTotalPoints(), clusterSize());
		}

	}

	
	
	private class RouteDataObjectProcessor implements ResultMatcher<RouteDataObject> {

		int indProc = 0, prevPrintInd = 0;
		private float estimatedRoads;
		private NetworkCollectPointCtx ctx;

		public RouteDataObjectProcessor(NetworkCollectPointCtx ctx, float estimatedRoads) {
			this.estimatedRoads = estimatedRoads;
			this.ctx = ctx;
		}

		@Override
		public boolean publish(RouteDataObject object) {
			if (!ctx.rctx.getRouter().acceptLine(object)) {
				return false;
			}
			indProc++;
			if (indProc < DEBUG_LIMIT_START_OFFSET || isCancelled()) {
				System.out.println("SKIP PROCESS " + indProc);
			} else {
				RouteSegmentPoint pntAround = new RouteSegmentPoint(object, 0, 0);
				long mainPoint = calculateRoutePointInternalId(pntAround);
				if (ctx.allVisitedVertices.contains(mainPoint)
						|| ctx.networkPointToDbInd.containsKey(mainPoint)) {
					// already existing cluster
					return false;
				}
				NetworkIsland cluster = buildRoadNetworkIsland(ctx, pntAround);
				if (DEBUG_VERBOSE_LEVEL >= 1) {
					int nwPoints = cluster.toVisitVertices.size();
					logf("CLUSTER: %2d border <- %4d points (%d segments) - %s", nwPoints,
							cluster.visitedVertices.size(), nwPoints * (nwPoints - 1), pntAround);
				}
				ctx.addCluster(cluster);
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
}
