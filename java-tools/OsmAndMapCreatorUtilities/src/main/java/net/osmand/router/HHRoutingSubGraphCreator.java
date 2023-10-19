package net.osmand.router;

import static net.osmand.router.HHRoutingPrepareContext.logf;
import static net.osmand.router.HHRoutePlanner.calcUniDirRoutePointInternalId;
import static net.osmand.router.HHRoutePlanner.calculateRoutePointInternalId;
import static net.osmand.router.HHRoutingUtilities.distrCumKey;
import static net.osmand.router.HHRoutingUtilities.distrString;
import static net.osmand.router.HHRoutingUtilities.distrSum;
import static net.osmand.router.HHRoutingUtilities.makePositiveDir;
import static net.osmand.router.HHRoutingUtilities.saveOsmFile;
import static net.osmand.router.HHRoutingUtilities.visualizeWays;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
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
import net.osmand.router.HHRoutingPreparationDB.NetworkBorderPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkRouteRegion;
import net.osmand.util.MapUtils;


// IN PROGRESS
// 1.x Holes Bug restriction on turns and Direction shortcuts -https://www.openstreetmap.org/#map=17/50.54312/30.18480 (uturn) (!) 
// 1.0 BUG!! Germany Bicycle mincut 30 + 22 network pnts != 51 graph reached size: 41980845 0 1



// TESTING
// 1.x compact chdb even more (1)use short dist 2) use point ind in cluster) - 2 bytes per edge  - 90 MB -> 30 MB

// TODO 
// 1.0 BUG!! __europe car BUG!! mincut 5 + 9 network pnts != 13 graph reached size: 976618135 0 1
// 1.1 HHRoutingShortcutCreator BinaryRoutePlanner.DEBUG_BREAK_EACH_SEGMENT TODO test that routing time is different with on & off! should be the same
// 1.2 HHRoutingShortcutCreator BinaryRoutePlanner.DEBUG_PRECISE_DIST_MEASUREMENT for long distance causes bugs if (pnt.index != 2005) { 2005-> 1861 } - 3372.75 vs 2598 -
// 1.5 BinaryRoutePlanner TODO ?? we don't stop here in order to allow improve found *potential* final segment - test case on short route

// 1.3 HHRoutePlanner routing 1/-1/0 FIX routing time 7288 / 7088 / 7188 (43.15274, 19.55169 -> 42.955495, 19.0972263)
// 1.4 HHRoutePlanner use cache boundaries to speed up
// 1.6 HHRoutePlanner revert 2 queues to fail fast in 1 direction
// 1.7 HHRoutePlanner this is more correct to preserve startDistance

// 1.8 HHRoutePlanner encapsulate HHRoutingPreparationDB, RoutingContext -> HHRoutingContext
// 1.11 clean up (HHRoutingPrepareContext + HHRoutingPreparationDB)?
// 1.12 Make separate / lightweight for Runtime memory NetworkDBPoint / NetworkDBSegment
// 1.13 Allow private roads on server calculation (allow_private)
// 1.14 Cut start / end to projection as in detailed calculation (MapCreator)
// 1.15 HHRoutePlanner calculate start/end alternative routes

// 2nd  phase - points selection / Planet ~6-12h per profile
// 2.1 HHRoutePlanner Improve / Review A* finish condition
// 2.2 FILE: calculate different settings profile (short vs long, use elevation data)
// 2.3 TESTS: 1) Straight parallel roads -> 4 points 2) parking slots -> exit points 3) road and suburb -> exit points including road?
// 2.4 SERVER: Calculate points in parallel (Planet) - Combine 2 processes 
// 2.5 SERVER: Optimize shortcut calculation process (local to use less memory) or calculate same time as points
// 2.6 FILE: Final data structure optimal by size, access time - protobuf (2 bytes per edge!)
// 2.7 FILE: Implement border crossing issue on client
// 2.8 Implement route recalculation in case distance > original 10% ? 
// 2.9 FILE: different dates for maps!
// 2.10 Implement check that routing doesn't allow more roads (custom routing.xml) i.e. 
//       There should be maximum at preproce visited points < 50K-100K
// 2.11 EX10 - example that min depth doesn't give good approximation
// 2.13 Theoretically possible situation with u-turn on same geo point - create bug + explanation - test?
// 2.14 Some points have no segments in/out (oneway roads) - simplify?

// 3 Later implementation
// 3.1 HHRoutePlanner Alternative routes - could use distributions like 50% route (2 alt), 25%/75% route (1 alt)
// 3.2 Avoid specific road
// 3.3 Deprioritize or exclude roads (parameters)
// 3.4 Live data (think about it)
// 3.5 Merge clusters (and remove border points): 1-2 border point or (22 of 88 clusters has only 2 neighboor clusters) 

// *4* Future (if needed) - Introduce 3/4 level 
// 4.1 Implement midpoint algorithm - HARD to calculate midpoint level
// 4.2 Implement CH algorithm - HARD to wait to finish CH
// 4.3 Try different recursive algorithm for road separation - DIDN'T IMPLEMENT

public class HHRoutingSubGraphCreator {

	final static Log LOG = PlatformUtil.getLog(HHRoutingSubGraphCreator.class);
	static int DEBUG_STORE_ALL_ROADS = 0; // 1 - clusters, 2 - geometry cluster, 3 - all, 
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
	
	// Fixed issues: Maxflow 6 (actually 7 bug) != 7 mincut - TOTAL_MAX_POINTS = 10000; TOTAL_MIN_POINTS = 100;
	protected static LatLon EX8 = new LatLon(42.105892, 19.089802);
	protected static LatLon EX9 = new LatLon(42.306454, 18.804703);
	protected static LatLon EX10 = new LatLon(42.10768, 19.103357);
	protected static LatLon EX11 = new LatLon(42.31288, 19.275553);
	

	protected static LatLon[] EX = {
//			EX11
	}; 

	static int TOTAL_MAX_POINTS = 100000, TOTAL_MIN_POINTS = 10000;
	static boolean CLEAN = false;
	static String ROUTING_PROFILE = "car";
	

	private static File testData() {
		DEBUG_VERBOSE_LEVEL = 0;
//		DEBUG_STORE_ALL_ROADS = 1;
//		CLEAN = true;
		
//		TOTAL_MAX_POINTS = 10000;
//		TOTAL_MIN_POINTS = 100;
		
		String name = "Montenegro_europe_2.road.obf";
//		name = "Netherlands_europe_2.road.obf";
//		name = "Ukraine_europe_2.road.obf";
		name = "Germany";
		ROUTING_PROFILE = "bicycle";
		return new File(System.getProperty("maps.dir"), name);
	}

	public static void main(String[] args) throws Exception {
		CLEAN = false;
		File obfFile = args.length == 0 ? testData() : new File(args[0]);
		for (String a : args) {
			if (a.startsWith("--routing_profile=")) {
				ROUTING_PROFILE = a.substring("--routing_profile=".length());
			} else if (a.equals("--clean")) {
				CLEAN = true;
			}
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getCanonicalFile().getName() + "_" + ROUTING_PROFILE;

		File dbFile = new File(folder, name + HHRoutingDB.EXT);
		if (CLEAN && dbFile.exists()) {
			dbFile.delete();
		}
		HHRoutingPreparationDB networkDB = new HHRoutingPreparationDB(dbFile);
		prepareContext = new HHRoutingPrepareContext(obfFile, ROUTING_PROFILE);
		HHRoutingSubGraphCreator proc = new HHRoutingSubGraphCreator();
		NetworkCollectPointCtx ctx = proc.collectNetworkPoints(networkDB);
		List<Entity> objects = visualizeWays(ctx.visualClusters);
		saveOsmFile(objects, new File(folder, name + ".osm"));
		networkDB.close();
	}



	private NetworkCollectPointCtx collectNetworkPoints(HHRoutingPreparationDB networkDB) throws IOException, SQLException {
		RoutingContext rctx = prepareContext.prepareContext(null, null);
		NetworkCollectPointCtx ctx = new NetworkCollectPointCtx(rctx, networkDB);
		if (EX != null && EX.length > 0) {
			DEBUG_STORE_ALL_ROADS = 3;
			RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
			for (LatLon l : EX) {
				RouteSegmentPoint pnt = router.findRouteSegment(l.getLatitude(), l.getLongitude(), ctx.rctx, null);
				NetworkIsland cluster = buildRoadNetworkIsland(ctx, pnt);
				ctx.addCluster(cluster);
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
		Collections.sort(ctx.routeRegions, new Comparator<NetworkRouteRegion>() {

			@Override
			public int compare(NetworkRouteRegion o1, NetworkRouteRegion o2) {
				return Integer.compare(o1.id, o2.id);
			}
		});
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
			System.out.println("Region bbox (l,t - r,b): " + nrouteRegion.region.getLeftLongitude() + ", "
					+ nrouteRegion.region.getTopLatitude() + " x " + nrouteRegion.region.getRightLongitude() + ", "
					+ nrouteRegion.region.getBottomLatitude());
			if (nrouteRegion.region.getLeftLongitude() > nrouteRegion.region.getRightLongitude()) {
				if (routeRegion.getLength() < 1000) {
					System.out.printf("Skip region  %s - %d bytes\n", nrouteRegion.region.getName(),
							routeRegion.getLength());
					continue;
				}
				throw new IllegalStateException();
			}
			List<RouteSubregion> regions = reader.searchRouteIndexTree(
					BinaryMapIndexReader.buildSearchRequest(
							MapUtils.get31TileNumberX(nrouteRegion.region.getLeftLongitude()),
							MapUtils.get31TileNumberX(nrouteRegion.region.getRightLongitude()),
							MapUtils.get31TileNumberY(nrouteRegion.region.getTopLatitude()),
							MapUtils.get31TileNumberY(nrouteRegion.region.getBottomLatitude()), 16, null),
					routeRegion.getSubregions());

			final int estimatedRoads = 1 + routeRegion.getLength() / 150; // 5 000 / 1 MB - 1 per 200 Byte
			RouteDataObjectProcessor proc = new RouteDataObjectProcessor(ctx, estimatedRoads);
			reader.loadRouteIndexData(regions, proc);
			proc.finish();
			ctx.finishRegionProcess();
			ctx.printStatsNetworks();
		}
		return ctx;
	}



	private NetworkIsland buildRoadNetworkIsland(NetworkCollectPointCtx ctx, RouteSegmentPoint pnt) {
		NetworkIsland c = new NetworkIsland(ctx, pnt);
		
		TLongObjectHashMap<RouteSegmentVertex> existNetworkPoints = new TLongObjectHashMap<>();
		TIntIntHashMap depthDistr = new TIntIntHashMap();
		int STEP = 10;
		int minDepth = 1, maxDepth = STEP + minDepth;
		
		c.addSegmentToQueue(c.getVertex(pnt, true));
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
		for (RouteSegmentVertex r : c.toVisitVertices.valueCollection()) {
			c.loadVertexConnections(r, false);
		}
		// depthDistr.get(minDepth) < TOTAL_MIN_DEPTH_POINTS
		while (minDepth < maxDepth - STEP && distrSum(depthDistr, minDepth) < TOTAL_MIN_POINTS) {
			minDepth++;
		}
		TLongObjectHashMap<RouteSegmentBorderPoint> mincuts = findMincutUsingMaxFlow(c, minDepth, existNetworkPoints, pnt.toString());
		c.borderVertices = recalculateClusterPointsUsingMincut(c, pnt, mincuts);
		for (long key : c.visitedVertices.keys()) {
			RouteSegmentVertex v = c.allVertices.get(key);
			c.edgeDistr.adjustOrPutValue(v.connections.size(), 1, 1);
			c.edges += v.connections.size();
		}
		// debug
//		c.borderVertices.putAll(mincuts);
		if (DEBUG_VERBOSE_LEVEL >= 1) {
			logf("Cluster: borders %d (%,d size ~ %d depth). Flow %d: depth min %d (%,d) <- max %d (%,d). Start %s",
					c.toVisitVertices.size(), c.visitedVertices.size(), distrCumKey(depthDistr, c.visitedVertices.size()), 
					mincuts.size(), minDepth, distrSum(depthDistr, minDepth), maxDepth, distrSum(depthDistr, maxDepth),  
					pnt);
		}
		return c;
	}

	private List<RouteSegmentBorderPoint> recalculateClusterPointsUsingMincut(NetworkIsland c, RouteSegmentPoint pnt, 
			TLongObjectHashMap<RouteSegmentBorderPoint> mincuts) {
		c.clearVisitedPoints();
		c.queue.add(c.getVertex(pnt, false));
		List<RouteSegmentBorderPoint> borderPoints = new ArrayList<>();
		while (!c.queue.isEmpty()) {
			RouteSegmentVertex ls = c.queue.poll();
			if (mincuts.containsKey(ls.getId())) {
				continue;
			}
			if (c.ctx.testIfNetworkPoint(ls.getId())) {
				boolean pos = (ls.parentRoute.getEndPointX() == ls.getStartPointX()
						&& ls.parentRoute.getEndPointY() == ls.getStartPointY())
						|| (ls.parentRoute.getStartPointX() == ls.getStartPointX()
								&& ls.parentRoute.getStartPointY() == ls.getStartPointY());
				borderPoints.add(new RouteSegmentBorderPoint(ls, pos));
				continue;
			}
			proceed(c, ls, c.queue, -1);
		}
		// TODO BUG 1.0 BUG!! Germany Bicycle mincut 30 + 22 network pnts != 51 graph reached size: 41980845 0 1
		int diff = mincuts.size() + borderPoints.size() - c.toVisitVertices.size();
		if (diff != 0) {
			String msg = String.format("BUG!! mincut %d + %d network pnts != %d graph reached size: %s", mincuts.size(),
					borderPoints.size(), c.toVisitVertices.size(), c.startToString);
			System.err.println(msg);
			throw new IllegalStateException(msg); 
		}
		borderPoints.addAll(mincuts.valueCollection());
		return borderPoints;
		
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
		c.visitedVertices.put(vertex.getId(), DEBUG_STORE_ALL_ROADS > 2 ? vertex : null);
		c.loadVertexConnections(vertex, true);
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

	private class MaxFlowVertex {
		List<MaxFlowEdge> connections = new ArrayList<>();
		RouteSegmentVertex segment;
		boolean end;
		MaxFlowEdge flowParentTemp;
		public MaxFlowVertex(RouteSegmentVertex e, boolean end) {
			this.segment = e;
			this.end = end;
			
		}
		
		@Override
		public String toString() {
			if (segment == null) {
				return "Null vertex";
			}
			return segment.getRoad() + " [" + (end ? segment.getSegmentEnd() : segment.getSegmentStart()) + "]";
		}

	}
	
	private class MaxFlowEdge {
		RouteSegmentVertex vertex;
		MaxFlowVertex s;
		MaxFlowVertex t;
		int flow;
		
		MaxFlowEdge(RouteSegmentVertex v) {
			this.vertex = v;
		}
		
		@Override
		public String toString() {
			return s + " -> " + t;
		}

		public MaxFlowEdge reverseConnect() {
			for (MaxFlowEdge r : t.connections) {
				if (r.t == s) {
					return r;
				}
			}
			throw new IllegalArgumentException("No reverse connection: " + this);
		}
	}

	private TLongObjectHashMap<RouteSegmentBorderPoint> findMincutUsingMaxFlow(NetworkIsland c, int minDepth,
			TLongObjectHashMap<RouteSegmentVertex> existingVertices, String errorDebug) {
		List<MaxFlowVertex> sources = new ArrayList<>();
		// For maxflow / mincut algorithm RouteSegmentVertex is edge with capacity 1 connected using end boolean flag on both ends
		List<MaxFlowVertex> vertices = constructMaxFlowGraph(c, existingVertices, sources);

		MaxFlowVertex source = new MaxFlowVertex(null, false);
		for (MaxFlowVertex s : sources) {
			MaxFlowEdge edge = new MaxFlowEdge(null);
			edge.s = source;
			edge.t = s;
			source.connections.add(edge);
			
			edge = new MaxFlowEdge(null);
			edge.s = s;
			edge.t = source;
			s.connections.add(edge);
		}
		vertices.add(source);
		
		List<MaxFlowVertex> sinks = new ArrayList<>();
		MaxFlowVertex sink = null;
		do {
			for (MaxFlowVertex rs : vertices) {
				rs.flowParentTemp = null;
			}
			sink = null;
			LinkedList<MaxFlowVertex> queue = new LinkedList<>();
			queue.add(source);
			while (!queue.isEmpty() && sink == null) {
				MaxFlowVertex vert = queue.poll();
				for (MaxFlowEdge conn : vert.connections) {
					if (conn.t.flowParentTemp == null && conn.flow < 1) {
						conn.t.flowParentTemp = conn;
						if (conn.vertex != null && conn.vertex.getDepth() <= minDepth && conn.vertex.getDepth() > 0) {
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
				MaxFlowVertex p = sink;
				while (true) {
					p.flowParentTemp.flow++;
					if (p.flowParentTemp.s == source) {
						break;
					}
					p.flowParentTemp.reverseConnect().flow--;
					p = p.flowParentTemp.s;
				}
			}
			
		} while (sink != null);
		TLongObjectHashMap<RouteSegmentBorderPoint> mincuts = calculateMincut(vertices, source, sinks, c.visitedVertices);
		if (sinks.size() != mincuts.size()) {
			// debug
//			mincuts.clear();
//			for (MaxFlowVertex v : sinks) {
//				System.out.println(v);
//				mincuts.put(v.segment.cId, v.segment);
//			}
			String msg = String.format("BUG maxflow %d != %d mincut: %s ", sinks.size(), mincuts.size(), errorDebug);
			System.err.println(msg);
			throw new IllegalStateException(msg);  
		}
		return mincuts;
	}

	private List<MaxFlowVertex> constructMaxFlowGraph(NetworkIsland c,
			TLongObjectHashMap<RouteSegmentVertex> existingVertices, List<MaxFlowVertex> sources) {
		TLongObjectHashMap<MaxFlowEdge> edges = new TLongObjectHashMap<>();
		List<MaxFlowVertex> vertices = new ArrayList<>();
		for (RouteSegmentVertex r : c.allVertices.valueCollection()) {
			MaxFlowVertex s = null;
			MaxFlowVertex t = null;
			for (RouteSegmentEdge e : r.connections) {
				MaxFlowEdge ex = edges.get(e.t.cId);
				if (ex != null) {
					MaxFlowVertex conn = e.tEnd ? ex.t : ex.s;
					if (e.sEnd) {
						if (t != null && t != conn)
							throw new IllegalStateException(t + " != " + conn);
						t = conn;
					} else {
						if (s != null && s != conn)
							throw new IllegalStateException(s + " != " + conn);
						s = conn;
					}
				}
			}
			if (s == null) {
				s = new MaxFlowVertex(r, false);
				vertices.add(s);
			}
			if (t == null) {
				t = new MaxFlowVertex(r, true);
				vertices.add(t);
			}
			MaxFlowEdge newEdge = new MaxFlowEdge(r);
			newEdge.s = s;
			newEdge.t = t;
			s.connections.add(newEdge);
			edges.put(r.cId, newEdge);
			newEdge = new MaxFlowEdge(r);
			newEdge.s = t;
			newEdge.t = s;
			t.connections.add(newEdge);
			if (c.toVisitVertices.contains(r.getId())) {
				if (!existingVertices.contains(r.getId())) {
					sources.add(newEdge.s);
				}
			}
		}
		return vertices;
	}

	private TLongObjectHashMap<RouteSegmentBorderPoint> calculateMincut(List<MaxFlowVertex> vertices,
			MaxFlowVertex source, Collection<MaxFlowVertex> sinks, TLongObjectHashMap<RouteSegment> visitedVertices) {
		for (MaxFlowVertex rs : vertices) {
			rs.flowParentTemp = null;
		}
		// debug
//		 visitedVertices.clear();

		TLongObjectHashMap<RouteSegmentBorderPoint> mincuts = new TLongObjectHashMap<>();
		LinkedList<MaxFlowVertex> queue = new LinkedList<>();
		Set<MaxFlowVertex> reachableSource = new HashSet<>();
		queue.add(source);
		reachableSource.add(source);
		while (!queue.isEmpty()) {
			MaxFlowVertex ps = queue.poll();
			for (MaxFlowEdge conn : ps.connections) {
				if (conn.t.flowParentTemp == null && conn.flow < 1) {
					conn.t.flowParentTemp = conn;
					queue.add(conn.t);
					reachableSource.add(conn.t);
				}
			}
		}

		queue = new LinkedList<>();
		queue.addAll(sinks);
		while (!queue.isEmpty()) {
			MaxFlowVertex ps = queue.poll();
			for (MaxFlowEdge conn : ps.connections) {
				if (reachableSource.contains(conn.t)) {
					MaxFlowEdge c = conn.vertex == null ? conn.s.flowParentTemp : conn;
					boolean pos = c.vertex.getStartPointX() == (c.s.end ? c.s.segment.getEndPointX() : c.s.segment.getStartPointX()) &&
							c.vertex.getStartPointY() == (c.s.end ? c.s.segment.getEndPointY() : c.s.segment.getStartPointY());
					RouteSegmentBorderPoint dir = new RouteSegmentBorderPoint(c.vertex, pos);
					mincuts.put(calcUniDirRoutePointInternalId(c.vertex), dir);
//					System.out.println("? " + dir + " " + c.s + " " + c.t);
				}
				// debug
//				if (conn.vertex != null) {
//					visitedVertices.put(conn.vertex.cId, conn.vertex);
//				}
				if (conn.t.flowParentTemp == null) {
					conn.t.flowParentTemp = conn;
					queue.add(conn.t);
				}
			}
		}
		return mincuts;
	}


	class RouteSegmentEdge {
		RouteSegmentVertex s;
		boolean sEnd;
		RouteSegmentVertex t;
		boolean tEnd;

		RouteSegmentEdge(RouteSegmentVertex s, boolean sEnd, RouteSegmentVertex t, boolean tEnd) {
			this.s = s;
			this.sEnd = sEnd;
			this.t = t;
			this.tEnd = tEnd;
		}
		
		@Override
		public String toString() {
			return String.format("%s (%s) -> %s (%s)", s, sEnd, t, tEnd);
		}
	}
	
	
	class RouteSegmentBorderPoint extends RouteSegment {
		public final boolean dir;
		public final long unidirId;
		public final long uniqueId;
		// segment [inner point index -> outer point index]
		public RouteSegmentBorderPoint(RouteSegment s, boolean dir) {
			super(s.getRoad(), dir ? s.getSegmentStart() : s.getSegmentEnd(),
					dir ? s.getSegmentEnd() : s.getSegmentStart());
			unidirId = calcUniDirRoutePointInternalId(s);
			uniqueId = calculateRoutePointInternalId(getRoad().getId(), getSegmentStart(), getSegmentEnd());
			this.dir = dir;
		}
		
	}

	class RouteSegmentVertex extends RouteSegment {

		public List<RouteSegmentEdge> connections = new ArrayList<>();
		public int cDepth;
		public final long cId;

		public RouteSegmentVertex(RouteSegment s) {
			super(s.getRoad(), s.getSegmentStart(), s.getSegmentEnd());
			cId = calcUniDirRoutePointInternalId(this);
		}

		public RouteSegmentVertex() {
			// artificial
			super(null, 0, 0);
			cId = 0;
		}

		public long getId() {
			return cId;
		}

		public void removeConnection(RouteSegmentVertex pos) {
			Iterator<RouteSegmentEdge> it = connections.iterator();
			while (it.hasNext()) {
				RouteSegmentEdge e = it.next();
				if (e.t == pos) {
					it.remove();
					break;
				}
			}
		}
		
		public void addConnection(boolean sEnd, RouteSegmentVertex t) {
			if (t != this && t != null) {
				boolean tEnd = t.getEndPointX() == (sEnd ? getEndPointX() : getStartPointX())
						&& t.getEndPointY() == (sEnd ? getEndPointY() : getStartPointY());
				connections.add(new RouteSegmentEdge(this, sEnd, t, tEnd));
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

		public void clearPoint() {
			this.distanceFromStart = 0;
			this.cDepth = 0;
			this.parentRoute = null;
		}

	}

	class NetworkIsland {
		final LatLon startLatLon;
		final String startToString;
		final NetworkCollectPointCtx ctx;
		
		int dbIndex;
		int edges = 0;
		TIntIntHashMap edgeDistr = new TIntIntHashMap();
		
		PriorityQueue<RouteSegmentVertex> queue;
		TLongObjectHashMap<RouteSegment> visitedVertices = new TLongObjectHashMap<>();
		TLongObjectHashMap<RouteSegmentVertex> toVisitVertices = new TLongObjectHashMap<>();
		TLongObjectHashMap<RouteSegmentVertex> allVertices = new TLongObjectHashMap<>();
		List<RouteSegmentBorderPoint> borderVertices;
		
		TLongObjectHashMap<List<LatLon>> visualBorders = null;

		NetworkIsland(NetworkCollectPointCtx ctx, RouteSegment start) {
			this.ctx = ctx;
			this.startLatLon = HHRoutingUtilities.getPoint(start);
			this.startToString = start.getRoad().getId() / 64 + " " + start.getSegmentStart() + " "
					+ start.getSegmentEnd();
			this.queue = new PriorityQueue<>(new Comparator<RouteSegment>() {

				@Override
				public int compare(RouteSegment o1, RouteSegment o2) {
					return Double.compare(o1.distanceFromStart, o2.distanceFromStart);
				}
			});
		}

		public RouteSegmentVertex getVertex(RouteSegment s, boolean create) {
			if (s == null) {
				return null;
			}
			long p = calcUniDirRoutePointInternalId(s);
			RouteSegmentVertex routeSegment = allVertices != null ? allVertices.get(p) : null;
			if (routeSegment == null && create) {
				routeSegment = new RouteSegmentVertex(makePositiveDir(s));
				if (allVertices != null) {
					allVertices.put(p, routeSegment);
				}
			}
			return routeSegment;
		}
		
		public void loadVertexConnections(RouteSegmentVertex segment, boolean createVertices) {
			if (!segment.connections.isEmpty()) {
				return;
			}
			loadVertexConnections(segment, true, createVertices);
			loadVertexConnections(segment, false, createVertices);
		}
		
		public void initQueueFromPointsToVisit() {
			queue.clear();
			for (RouteSegment r : toVisitVertices.valueCollection()) {
				queue.add((RouteSegmentVertex) r);
			}			
		}
		
		void loadVertexConnections(RouteSegmentVertex segment, boolean end, boolean createVertices) {
			int segmentInd = end ? segment.getSegmentEnd() : segment.getSegmentStart();
			int x = segment.getRoad().getPoint31XTile(segmentInd);
			int y = segment.getRoad().getPoint31YTile(segmentInd);
			RouteSegment next = ctx.rctx.loadRouteSegment(x, y, 0);
			while (next != null) {
				segment.addConnection(end, getVertex(next, createVertices));
				segment.addConnection(end, getVertex(next.initRouteSegment(!next.isPositive()), createVertices));
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

		public void clearVisitedPoints() {
			for (RouteSegmentVertex v : allVertices.valueCollection()) {
				v.clearPoint();
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
		long edges;
		int isolatedIslands = 0;
		int toMergeIslands = 0;
		int shortcuts = 0;
		

		public void printStatsNetworks(long totalPoints, int clusterSize) {
			int borderPointsSize = borderPntsCluster.size();
			TIntIntHashMap borderClusterDistr = new TIntIntHashMap();
			for (int a : this.borderPntsCluster.values()) {
				borderClusterDistr.adjustOrPutValue(a, 1, 1);
			}
			logf("RESULT %,d points (%,d edges) -> %,d border points, %,d clusters + %,d isolated + %d to merge, %,d est shortcuts (%s edges distr) \n",
					totalPoints + borderPointsSize, edges / 2, borderPointsSize, clusterSize - isolatedIslands - toMergeIslands,
					isolatedIslands, toMergeIslands, shortcuts, distrString(edgesDistr, ""));

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
			} else if (borderPoints <= 2) {
				this.toMergeIslands++; 
			} else {
				borderPntsDistr.adjustOrPutValue(borderPoints, 1, 1);
				this.shortcuts += borderPoints * (borderPoints - 1);
				pntsDistr.adjustOrPutValue((cluster.visitedVertices.size() + 500) / 1000, 1, 1);
			}
			edges += cluster.edges;
			TIntIntIterator it = cluster.edgeDistr.iterator();
			while (it.hasNext()) {
				it.advance();
				edgesDistr.adjustOrPutValue(it.key(), it.value(), it.value());
			}
			this.totalBorderPoints += borderPoints;
						
		}

	}
	
	class NetworkCollectPointCtx {
		RoutingContext rctx;
		HHRoutingPreparationDB networkDB;
		List<NetworkRouteRegion> routeRegions = new ArrayList<>();
		
		NetworkCollectStats stats = new NetworkCollectStats();
		List<NetworkIsland> visualClusters = new ArrayList<>();
		
		int lastClusterInd = 0;
		NetworkRouteRegion currentProcessingRegion;
		TLongObjectHashMap<NetworkBorderPoint> networkPointToDbInd = new TLongObjectHashMap<>();
		TLongIntHashMap allVisitedVertices = new TLongIntHashMap(); 

		public NetworkCollectPointCtx(RoutingContext rctx, HHRoutingPreparationDB networkDB) {
			this.rctx = rctx;
			this.networkDB = networkDB;
		}

		public long getTotalPoints() {
			long totalPoints = 0;
			for (NetworkRouteRegion r : routeRegions) {
				totalPoints += r.getPoints();
			}
			return totalPoints;
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

			this.allVisitedVertices = new TLongIntHashMap();
			for (NetworkRouteRegion nr : subRegions) {
				if (nr != nrouteRegion) {
					this.allVisitedVertices.putAll(nr.getVisitedVertices(networkDB));
				}
			}
		}

		public void addCluster(NetworkIsland cluster) {
			cluster.dbIndex = networkDB.prepareBorderPointsToInsert(cluster.borderVertices, networkPointToDbInd);
			lastClusterInd = cluster.dbIndex;
			stats.addCluster(cluster);
			for (long key : cluster.visitedVertices.keys()) {
				if (allVisitedVertices.containsKey(key)) {
					throw new IllegalStateException("Point was already visited");
				}
				allVisitedVertices.put(key, cluster.dbIndex);
				if (currentProcessingRegion != null) {
					currentProcessingRegion.visitedVertices.put(key, cluster.dbIndex);
				}
			}
				
			if (DEBUG_STORE_ALL_ROADS > 0) {
				visualClusters.add(cluster);
				if (DEBUG_STORE_ALL_ROADS > 0) {
					cluster.visualBorders = new TLongObjectHashMap<List<LatLon>>();
					for (RouteSegmentVertex p : cluster.toVisitVertices.valueCollection()) {
						List<LatLon> l = new ArrayList<LatLon>();
						RouteSegment par = p;
						while (par != null) {
							l.add(HHRoutingUtilities.getPoint(par));
							par = par.parentRoute;
							if (DEBUG_STORE_ALL_ROADS == 1) {
								break;
							}
						}
						cluster.visualBorders.put(p.cId, l);
					}
				}
				if (DEBUG_STORE_ALL_ROADS < 2) {
					cluster.borderVertices = null;
					cluster.toVisitVertices = null;
					cluster.queue = null;
					cluster.allVertices = null;
				}
			}
		}

		public void finishRegionProcess() throws SQLException {
			logf("Tiles " + rctx.calculationProgress.getInfo(null).get("tiles"));
			int ins = 0, tl = 0;
			for (NetworkBorderPoint npnt : networkPointToDbInd.valueCollection()) {
				if (npnt.positiveObj != null) {
					ins++;
				}
				if (npnt.positiveDbId > 0) {
					tl++;
				}
				if (npnt.negativeObj != null) {
					ins++;
				}
				if (npnt.negativeDbId > 0) {
					tl++;
				}
			}
			logf("Saving visited %,d points (%,d border points) from %s to db...", currentProcessingRegion.getPoints(), ins,
					currentProcessingRegion.region.getName());
			networkDB.insertVisitedVerticesBorderPoints(currentProcessingRegion, networkPointToDbInd);
			currentProcessingRegion.unload();
			currentProcessingRegion = null;
			logf("     saved - total %,d points (%,d border points), ", getTotalPoints(), tl);

		}
		
		public boolean testIfNetworkPoint(long pntId) {
			if (networkPointToDbInd.contains(pntId)) {
				return true;
			}
			return false;
		}

		
		public void printStatsNetworks() {
			stats.printStatsNetworks(getTotalPoints(), lastClusterInd);
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
		
		public void finish() {
//			System.out.printf("%d. calc %.2f ms, cache %d\n", s++, ts / 1e6, MapUtils.DIST_CACHE.size());
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
				for (int pos = 0; pos < object.getPointsLength() - 1; pos++) {
					RouteSegmentPoint pntAround = new RouteSegmentPoint(object, pos, 0);
					long mainPoint = calcUniDirRoutePointInternalId(pntAround);
					if (ctx.allVisitedVertices.contains(mainPoint) || ctx.networkPointToDbInd.containsKey(mainPoint)) {
						// already existing cluster
						continue;
					}
					NetworkIsland cluster = buildRoadNetworkIsland(ctx, pntAround);
					ctx.addCluster(cluster);
					if (DEBUG_VERBOSE_LEVEL >= 1 || indProc - prevPrintInd > 1000) {
						prevPrintInd = indProc;
						int borderPointsSize = ctx.borderPointsSize();
						logf("%,d %.2f%%: %,d points -> %,d border points, %,d clusters", indProc,
								indProc * 100.0f / estimatedRoads, ctx.getTotalPoints() + borderPointsSize,
								borderPointsSize, ctx.lastClusterInd);
					}
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
