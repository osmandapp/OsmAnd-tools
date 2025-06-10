package net.osmand.router;

import static net.osmand.router.HHRoutingUtilities.logf;
import static net.osmand.router.HHRoutePlanner.calcUniDirRoutePointInternalId;
import static net.osmand.router.HHRoutePlanner.calculateRoutePointInternalId;
import static net.osmand.router.HHRoutingUtilities.distrCumKey;
import static net.osmand.router.HHRoutingUtilities.distrString;
import static net.osmand.router.HHRoutingUtilities.distrSum;
import static net.osmand.router.HHRoutingUtilities.makePositiveDir;
import static net.osmand.router.HHRoutingUtilities.saveOsmFile;
import static net.osmand.router.HHRoutingUtilities.visualizeClusters;

import java.io.File;
import java.io.IOException;
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
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;

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
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.osm.edit.Entity;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkBorderPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkLongRoad;
import net.osmand.router.HHRoutingPreparationDB.NetworkRouteRegion;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

//////     TESTING     ///////
// 2.3 ROUTING: Missing map:  Better select region (Czech vs Sacsen old files) - check start / end point / route (partial) -
// 2.7 Multithread Server routing
// 2.8.0 ROUTING: NPE  (net.osmand.router.HHRouteDataStructure$NetworkDBPointRouteInfo.setDetailedParentRt(HHRouteDataStructure.java:575)
//  http://localhost:3000/map/?start=51.825843,6.712063&finish=51.954521,8.130041&type=osmand&profile=car&params=car,prefer_unpaved#10/51.8905/7.4206
// 2.0.1 ROUTING: ! Progress bar - Cancellation
// 2.0.2 ROUTING: ! Progress bar - Progress
// 2.1 SERVER: Automate monthly procedures
// 2.8.1 ROUTING: Avoid "motorways" could be more efficient to exclude points earlier
// 2.8.3 ROUTING: Test live maps

// !! IN PROGRESS !!!
// 2.8.2 height_obstacles ROUTING: Too many recalculations with height elevation (https://test.osmand.net/map/?start=42.770191,0.620610&finish=42.980344,-0.419359&type=osmand&profile=pedestrian)

// 3. MID-TERM Speedups, small bugs and Data research
// 3.0 UI: Suggest that maps need to be updated (downloaded)
// 3.1 SERVER: Speedup points: Calculate in parallel (Planet) - Combine 2 processes ?
// 3.2 SERVER: Speedup shortcut: group by clusters to use less memory, different unload routing context
// 3.3 DATA: Merge clusters (and remove border points): 1-2 border point or (22 of 88 clusters has only 2 neighbor clusters)
// 3.4 DATA: Tests 1) Straight parallel roads -> 4 points 2) parking slots -> exit points 3) road and suburb -> exit points including road?
// 3.5 DATA: Investigate difference ALG_BY_DEPTH_REACH_POINTS = true / false (speed / network) -  TOTAL_MAX_POINTS = 99000 vs (50000), TOTAL_MIN_POINTS = 1000
// 3.6 DATA: EX10 - example that min depth doesn't give good approximation
// 3.7 BUG: 2-dir routing speed https://github.com/osmandapp/OsmAnd/issues/18566
// 3.8 SPEEDUP: HHRoutePlanner / BinaryRoutePlanner should be speed up by just clearing visited (review all unloadAllData())
// 3.9 PRIVATE: Private roads could be calculated only from start/end (to have private road in the middle, we need special index - Segment Flags)
// 3.10.1 ! ROUTING: Alternative routes doesn't look correct (!) - could use distributions like 50% route (2 alt), 25%/75% route (1 alt)?
// 3.10.2 ! ROUTING: Loop non suitable alternative routes as a main route

// *4* LATE Future (if needed) - Introduce 3/4 level
// 4.1 Implement midpoint algorithm - HARD to calculate midpoint level
// 4.2 Implement CH algorithm - HARD to wait to finish CH
// 4.3 Try different recursive algorithm for road separation - DIDN'T IMPLEMENT
// 4.4 Implement Arc flags or CH for clusters inside

public class HHRoutingSubGraphCreator {

	final static Log LOG = PlatformUtil.getLog(HHRoutingSubGraphCreator.class);
	static int DEBUG_STORE_ALL_ROADS = 0; // 1 - clusters, 2 - geometry cluster, 3 - all,
	static int DEBUG_LIMIT_START_OFFSET = 0;
	static int DEBUG_LIMIT_PROCESS = -1;
	static int DEBUG_VERBOSE_LEVEL = 0;

	static final double OVERLAP_FOR_VISITED = 0.2; // degrees to overlap to validate visited could be close to 0
	// make overlap for routing context ideally we should have full worldwide connnection but we compensate LONG_DISTANCE_SEGMENTS_SPLIT
	static final double OVERLAP_FOR_ROUTING = 2; // degrees to overlap for routing context
	static final int LONG_DISTANCE_SEGMENTS_SPLIT = 100 * 1000; //distance for ferry segments to put network point

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
	protected static LatLon EX12 = new LatLon(32.919117, -96.96761);



	protected static LatLon[] EX = {
//			EX1
	};

	static int TOTAL_MAX_POINTS = 50000; // Max points in cluster - used as source for max flow
	static int TOTAL_MIN_POINTS = 1000; // Min points in cluster - used as sink for max flow
	static boolean ALG_BY_DEPTH_REACH_POINTS = true;
	static int ALG_BY_DEPTH_MINMAX_DIFF = 10;

	static boolean CLEAN = false;
	static String ROUTING_PROFILE = "car";
	static String ROUTING_PARAMS = "allow_private";

	private static File testData() {
		DEBUG_VERBOSE_LEVEL = 1;
		DEBUG_STORE_ALL_ROADS = 1;
		CLEAN = false;

//		TOTAL_MAX_POINTS = 100000;
//		TOTAL_MIN_POINTS = 1000;

		String name = "Montenegro_europe_2.road.obf";
//		name = "Italy_test";
//		name = "Netherlands_europe_2.road.obf";
//		ROUTING_PROFILE = "bicycle";
		return new File(System.getProperty("maps.dir"), name);
	}

	public static void main(String[] args) throws Exception {
		CLEAN = false;
		boolean onlymerge = false;
		File obfFile = args.length == 0 ? testData() : new File(args[0]);
		for (String a : args) {
			if (a.startsWith("--routing_profile=")) {
				ROUTING_PROFILE = a.substring("--routing_profile=".length());
			} else if (a.startsWith("--routing_params=")) {
				ROUTING_PARAMS = a.substring("--routing_params=".length());
			} else if (a.equals("--network_by_depth")) {
				ALG_BY_DEPTH_REACH_POINTS = true;
			} else if (a.equals("--network_by_limits")) {
				ALG_BY_DEPTH_REACH_POINTS = false;
			} else if (a.equals("--clean")) {
				CLEAN = true;
			} else if (a.equals("--merge")) {
				onlymerge = true;
			} else if (a.equals("--debug")) {
				DEBUG_VERBOSE_LEVEL = 1;
				DEBUG_STORE_ALL_ROADS = 1;
			} else if (a.equals("--debug2")) {
				DEBUG_VERBOSE_LEVEL = 2;
				DEBUG_STORE_ALL_ROADS = 2;
			} else if (a.equals("--debug3")) {
				DEBUG_VERBOSE_LEVEL = 3;
				DEBUG_STORE_ALL_ROADS = 3;
			}
		}
		String name = new File(".").getCanonicalFile().getName();
		if (args.length == 0) {
			File dir = testData();
			if (!dir.isDirectory()) {
				dir = dir.getParentFile();
			}
			name = dir.getAbsolutePath() + "/" + testData().getCanonicalFile().getName();
		}
		name += "_" + ROUTING_PROFILE;
		File dbFile = new File(name + HHRoutingDB.EXT);
		if (CLEAN && dbFile.exists()) {
			dbFile.delete();
		}
		HHRoutingSubGraphCreator proc = new HHRoutingSubGraphCreator();
		HHRoutingPreparationDB networkDB = new HHRoutingPreparationDB(dbFile);
		HHRoutingPrepareContext prepareContext = new HHRoutingPrepareContext(obfFile, ROUTING_PROFILE, ROUTING_PARAMS);
		NetworkCollectPointCtx ctx = new NetworkCollectPointCtx(prepareContext, networkDB);
		try {
			ctx.networkDB.loadNetworkPoints(ctx.networkPointToDbInd);
			ctx.longRoads = ctx.networkDB.loadNetworkLongRoads();
			if (onlymerge) {
				proc.mergeConnectedPoints(ctx);
			} else {
				proc.collectNetworkPoints(ctx);
			}
			TLongObjectHashMap<NetworkDBPoint> totalPnts = networkDB.loadNetworkPoints((short) 0, NetworkDBPoint.class);
			createOSMNetworkPoints(new File(name + "-pnts.osm"), totalPnts);
			System.out.printf("Profile has %,d points\n", totalPnts.size());
		} finally {
			if (ctx.visualClusters.size() > 0) {
				saveOsmFile(visualizeClusters(ctx.visualClusters), new File(name + ".osm"));
			}


			networkDB.close();
		}
	}

	public static void createOSMNetworkPoints(File osm, TLongObjectHashMap<NetworkDBPoint> pnts) throws XMLStreamException, IOException {
		TLongObjectHashMap<Entity> osmObjects = new TLongObjectHashMap<Entity>();
		for(NetworkDBPoint p : pnts.valueCollection()) {
			HHRoutingUtilities.addNode(osmObjects, p, null, "highway", "stop");
		}
		HHRoutingUtilities.saveOsmFile(osmObjects.valueCollection(), osm);
	}

	private int compareRect(QuadRect q1, QuadRect q2) {
		int x1 = (int) MapUtils.getTileNumberX(6, q1.left);
		int y1 = (int) MapUtils.getTileNumberY(6, q1.top);
		int x2 = (int) MapUtils.getTileNumberX(6, q2.left);
		int y2 = (int) MapUtils.getTileNumberY(6, q2.top);
		// sort from left to right, top to bottom
		if (Integer.compare(x1, x2) != 0) {
			return Integer.compare(x1, x2);
		}
		if (Integer.compare(y1, y2) != 0) {
			return Integer.compare(y1, y2);
		}
		return 0;
	}


	private NetworkCollectPointCtx collectNetworkPoints(NetworkCollectPointCtx ctx) throws IOException, SQLException {
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

		Set<String> routeRegionNames = new TreeSet<>();
		for (RouteRegion r : ctx.rctx.reverseMap.keySet()) {
			if (routeRegionNames.add(r.getName())) {
				NetworkRouteRegion reg = new NetworkRouteRegion(r, ctx.rctx.reverseMap.get(r).getFile(), null);
				ctx.routeRegions.add(reg);
			} else {
				logf("Ignore route region %s as duplicate", r.getName());
			}
		}
		Collections.sort(ctx.routeRegions, new Comparator<NetworkRouteRegion>() {
			@Override
			public int compare(NetworkRouteRegion o1, NetworkRouteRegion o2) {
				int c = compareRect(o1.rect, o2.rect);
				if (c != 0) {
					return c;
				}
				return Long.compare(o1.region.getLength(), o2.region.getLength());
			}
		});
		ctx.networkDB.insertRegions(ctx.routeRegions);
		int procInd = 0;
		for (NetworkRouteRegion nrouteRegion : ctx.routeRegions) {
			System.out.println("------------------------");
			procInd++;
			logf("Region bbox %s %d of %d (l,t - r,b): %.5f, %.5f x %.5f, %.5f", nrouteRegion.region.getName(), procInd, ctx.routeRegions.size(),
					nrouteRegion.rect.left, nrouteRegion.rect.top, nrouteRegion.rect.right, nrouteRegion.rect.bottom);
			if (ctx.networkDB.hasVisitedPoints(nrouteRegion)) {
				System.out.println("Already processed");
				continue;
			}
			if (nrouteRegion.region.getLeftLongitude() > nrouteRegion.region.getRightLongitude()) {
				if (nrouteRegion.region.getLength() < 1000) {
					System.out.printf("Skip region  %s - %d bytes\n", nrouteRegion.region.getName(),
							nrouteRegion.region.getLength());
					continue;
				}
				throw new IllegalStateException();
			}

			double overlapBbox = OVERLAP_FOR_ROUTING;
			boolean notProcessed = true;
			while (notProcessed) {
				ctx.startRegionProcess(nrouteRegion, overlapBbox);
				RouteRegion routeRegion = null;
				for (RouteRegion rr : ctx.rctx.reverseMap.keySet()) {
					if (rr.getFilePointer() == nrouteRegion.region.getFilePointer()
							&& nrouteRegion.region.getName().equals(rr.getName())) {
						routeRegion = rr;
						break;
					}
				}
				BinaryMapIndexReader reader = ctx.rctx.reverseMap.get(routeRegion);
				logf("Region %s %d of %d %s", nrouteRegion.region.getName(), procInd, ctx.routeRegions.size(),
						new Date().toString());

				List<RouteSubregion> regions = reader.searchRouteIndexTree(
						BinaryMapIndexReader.buildSearchRequest(
								MapUtils.get31TileNumberX(nrouteRegion.region.getLeftLongitude()),
								MapUtils.get31TileNumberX(nrouteRegion.region.getRightLongitude()),
								MapUtils.get31TileNumberY(nrouteRegion.region.getTopLatitude()),
								MapUtils.get31TileNumberY(nrouteRegion.region.getBottomLatitude()), 16, null),
						routeRegion.getSubregions());

				final long estimatedRoads = 1 + routeRegion.getLength() / 150; // 5 000 / 1 MB - 1 per 200 Byte
				RouteDataObjectProcessor proc = new RouteDataObjectProcessor(ctx, estimatedRoads);
				reader.loadRouteIndexData(regions, proc);
				boolean ok = ctx.finishRegionProcess(overlapBbox);
				if (!ok) {
					overlapBbox *= 2;
					// clean up for reprocessing
					ctx.networkDB.cleanupRegionForReprocessing(nrouteRegion, ctx.networkPointToDbInd, ctx.longRoads);

				} else {
					notProcessed = false;
				}
				ctx.printStatsNetworks();
			}
		}
		if (ctx.longRoads.size() > 0) {
			processLongRoads(ctx);
		}
		mergeConnectedPoints(ctx);
		return ctx;
	}

	private void processLongRoads(NetworkCollectPointCtx ctx) throws IOException, SQLException {
		int size = ctx.longRoads.size();
		if (size == 0) {
			return;
		}
		ctx.checkLongRoads = false;
		for (NetworkLongRoad l : ctx.longRoads) {
			l.addConnected(ctx.longRoads);
		}
		List<LongRoadGroup> connectedGroups = new ArrayList<>();
		// group by all long roads by connected points
		while (!ctx.longRoads.isEmpty()) {
			LongRoadGroup group = new LongRoadGroup();

			NetworkLongRoad r = ctx.longRoads.get(0);
			LinkedList<NetworkLongRoad> queue = new LinkedList<>();
			queue.add(r);
			while (!queue.isEmpty()) {
				NetworkLongRoad l = queue.poll();
				boolean add = group.set.add(l);
				if (!add) {
					continue;
				}
				queue.addAll(l.connected);
			}
			Iterator<NetworkLongRoad> it = ctx.longRoads.iterator();
			while (it.hasNext()) {
				if (group.set.contains(it.next())) {
					it.remove();
				}
			}
			for (NetworkLongRoad o : group.set) {
				QuadRect t = o.getQuadRect();
				group.r = HHRoutingUtilities.expandLatLonRect(group.r, t.left, t.top, t.right, t.bottom);
			}
			connectedGroups.add(group);
		}

		Collections.sort(connectedGroups, new Comparator<LongRoadGroup>() {
			@Override
			public int compare(LongRoadGroup o1, LongRoadGroup o2) {
				int c = compareRect(o1.r, o2.r);
				if (c != 0) {
					return c;
				}
				return Integer.compare(o1.set.size(), o2.set.size());
			}
		});

		logf("Process long roads %d and %d connected groups...", size, connectedGroups.size());
		int id = -2;
		for (LongRoadGroup group  : connectedGroups) {
			NetworkRouteRegion reg = new NetworkRouteRegion(null, null, group.r);
			reg.id = id--;
			if (ctx.networkDB.hasVisitedPoints(reg)) {
				continue;
			}
			logf("Group %d [%s] - %s ", reg.id, group.r, group);
			ctx.startRegionProcess(reg, OVERLAP_FOR_ROUTING);
			RouteDataObjectProcessor proc = new RouteDataObjectProcessor(ctx, ctx.longRoads.size());
			for (NetworkLongRoad o : group.set) {
				RouteSegment s = ctx.rctx.loadRouteSegment(o.pointsX[o.startIndex], o.pointsY[o.startIndex], 0);
				RouteDataObject obj = null;
				while (s != null) {
					if (s.getRoad().getId() == o.roadId) {
						obj = s.getRoad();
						break;
					}
					s = s.getNext();
				}
				if (obj == null) {
					throw new IllegalStateException(String.format("Not found long road %d (osm %d)", o.roadId, o.roadId / 64));
				}
				proc.publish(obj);
			}
			ctx.finishRegionProcess(OVERLAP_FOR_ROUTING);
		}
		ctx.printStatsNetworks();
	}


	private boolean checkLongRoads(NetworkIsland c, RouteSegmentVertex seg) {
		if (c.ctx.checkLongRoads && MapUtils.squareRootDist31(seg.getStartPointX(), seg.getStartPointY(), seg.getEndPointX(),
				seg.getEndPointY()) > LONG_DISTANCE_SEGMENTS_SPLIT) {
			return true;
		}

		return false;
	}

	private NetworkIsland buildRoadNetworkIsland(NetworkCollectPointCtx ctx, RouteSegmentPoint pnt) {
		NetworkIsland c = new NetworkIsland(ctx, pnt);

		TLongObjectHashMap<RouteSegmentVertex> existNetworkPoints = new TLongObjectHashMap<>();
		TIntIntHashMap depthDistr = new TIntIntHashMap();
		int minDepth = 0, maxDepth = ALG_BY_DEPTH_MINMAX_DIFF + 2;
		if (DEBUG_VERBOSE_LEVEL >= 1) {
			logf("Cluster %d. %s", ctx.lastClusterInd + 1, pnt );
		}
		c.addSegmentToQueue(c.getVertex(pnt));
		int maxPoints = 0;
		if (ALG_BY_DEPTH_REACH_POINTS) {
			while (distrSum(depthDistr, maxDepth) < TOTAL_MAX_POINTS && c.toVisitVertices.size() > 0) {
				maxDepth++;
				c.queue.clear();
				for (RouteSegment r : c.toVisitVertices.valueCollection()) {
					c.queue.add((RouteSegmentVertex) r);
				}
				maxPoints = reachAllPoints(c, existNetworkPoints, depthDistr, maxDepth, maxPoints);
			}
			while (minDepth < maxDepth - ALG_BY_DEPTH_MINMAX_DIFF && distrSum(depthDistr, minDepth) < TOTAL_MIN_POINTS) {
				minDepth++;
			}
		} else {
			maxPoints = reachAllPoints(c, existNetworkPoints, depthDistr, maxDepth, maxPoints);
		}

		for (RouteSegmentVertex seg : c.toVisitVertices.valueCollection()) {
			if (existNetworkPoints.contains(seg.getId())) {
				throw new IllegalStateException();
			}
			c.loadVertexConnections(seg);
		}
		TLongObjectHashMap<MaxFlowEdge> edges = new TLongObjectHashMap<>();
		List<MaxFlowVertex> vertices = constructMaxFlowGraph(c.visitedVertices, existNetworkPoints, edges);
		List<MaxFlowVertex> sources = constructMaxFlowGraph(c.toVisitVertices, existNetworkPoints, edges);
		TLongObjectHashMap<RouteSegmentBorderPoint> mincuts = findMincutUsingMaxFlow(c, minDepth, sources, vertices,
				pnt.toString());

		c.clearVisitedPoints(existNetworkPoints);
		c.borderVertices = recalculateClusterPointsUsingMincut(c, pnt, mincuts);
		for (RouteSegmentVertex v : c.visitedVertices.valueCollection()) {
			c.edgeDistr.adjustOrPutValue(v.connections.size(), 1, 1);
			c.edges += v.connections.size();
		}
//
		// debug
//		c.borderVertices.putAll(mincuts);
		if (DEBUG_VERBOSE_LEVEL >= 1) {
			logf("   Borders %d (%,d size ~ %d depth). Flow %d: depth min %d (%,d) <- max %d (%,d).",
					c.toVisitVertices.size(), c.visitedVertices.size(),
					distrCumKey(depthDistr, c.visitedVertices.size()), mincuts.size(), minDepth,
					distrSum(depthDistr, minDepth), maxDepth, distrSum(depthDistr, maxDepth));
		}
		c.clearVisitedPoints(existNetworkPoints);
		return c;
	}

	private int reachAllPoints(NetworkIsland c, TLongObjectHashMap<RouteSegmentVertex> existNetworkPoints,
			TIntIntHashMap depthDistr, int maxDepth, int order) {
		while (!c.queue.isEmpty()) {
			RouteSegmentVertex seg = c.queue.poll();
			seg.order = order++;
			depthDistr.adjustOrPutValue(seg.getDepth(), 1, 1);
			if (c.ctx.testIfNetworkPoint(seg.getId()) || checkLongRoads(c, seg)) {
				c.toVisitVertices.remove(seg.getId());
				existNetworkPoints.put(seg.getId(), seg);
				continue;
			}
			if (ALG_BY_DEPTH_REACH_POINTS) {
				if (seg.getDepth() > maxDepth) {
					continue;
				}
			} else {
				if (order > TOTAL_MAX_POINTS) {
					continue;
				}
			}
			proceed(c, seg, existNetworkPoints, c.queue);
		}
		return order;
	}

	private List<RouteSegmentBorderPoint> recalculateClusterPointsUsingMincut(NetworkIsland c, RouteSegmentPoint pnt,
			TLongObjectHashMap<RouteSegmentBorderPoint> mincuts) {
		c.clearQueues();
		c.queue.add(c.getVertex(pnt));
		List<RouteSegmentBorderPoint> borderPoints = new ArrayList<>();
		List<RouteSegmentBorderPoint> exPoints = new ArrayList<>();
		int longPoints = 0;
		while (!c.queue.isEmpty()) {
			RouteSegmentVertex ls = c.queue.poll();
			if (mincuts.containsKey(ls.getId())) {
				borderPoints.add(mincuts.get(ls.getId()));
			} else if (c.ctx.testIfNetworkPoint(ls.getId())) {
				exPoints.add(RouteSegmentBorderPoint.fromParent(ls));
			} else if (checkLongRoads(c, ls)) {
				longPoints++;
//				if (DEBUG_VERBOSE_LEVEL > 0) {
					System.out.println(" Add long point segment " + ls);
//				}
				borderPoints.add(RouteSegmentBorderPoint.fromParent(ls));
			} else {
				proceed(c, ls, null, c.queue);
			}
		}

		if (borderPoints.size() + exPoints.size() != c.toVisitVertices.size() ||
				(mincuts.size() + longPoints) != borderPoints.size()) {
			List<RouteSegmentBorderPoint> toVisit = new ArrayList<>();
			for (RouteSegmentVertex b : c.toVisitVertices.valueCollection()) {
				toVisit.add(RouteSegmentBorderPoint.fromParent(b));
			}
			print(sortPoints(exPoints), "Existing");
			print(sortPoints(borderPoints), "Mincut reached");
			print(sortPoints(new ArrayList<>(mincuts.valueCollection())), "Mincut");
			print(sortPoints(toVisit), "To Visit");
			String msg = String.format("BUG !! mincut border %d  ( = %d mincut + %d long ) + %d existing pnts != %d graph reached size: %s",
					borderPoints.size(), mincuts.size(), longPoints, exPoints.size(), c.toVisitVertices.size(), c.startToString);
			if ((mincuts.size() + longPoints) != borderPoints.size()) {
				System.err.println(msg);
				throw new IllegalStateException(msg);
			} else {
				throw new IllegalStateException(msg);
			}
		}
		borderPoints.addAll(exPoints);
		return borderPoints;

	}


	private void print(List<RouteSegmentBorderPoint> borderPoints, String prefix) {
		int ind = 0;
		for (RouteSegmentBorderPoint p : borderPoints) {
			System.out.println(prefix + " " + (ind++) + " " + p);
		}
	}

	private List<RouteSegmentBorderPoint> sortPoints(List<RouteSegmentBorderPoint> borderPoints) {
		borderPoints.sort(new Comparator<RouteSegmentBorderPoint>() {

			@Override
			public int compare(RouteSegmentBorderPoint o1, RouteSegmentBorderPoint o2) {
				return Long.compare(o1.roadId, o2.roadId);
			}
		});
		return borderPoints;
	}

	private boolean proceed(NetworkIsland c, RouteSegmentVertex vertex, TLongObjectHashMap<RouteSegmentVertex> existNetworkPoints, PriorityQueue<RouteSegmentVertex> queue) {
		c.toVisitVertices.remove(vertex.getId());
		if (c.testIfVisited(vertex.getId())) {
			throw new IllegalStateException(String.format("%d %s was already locally visited", vertex.getId(), vertex.toString()));
		}
		if (c.ctx.testGlobalVisited(vertex.getId())) {
			throw new IllegalStateException(String.format("%d %s was already globally visited: %s",
					vertex.getId(), vertex.toString(), c.ctx.globalVisitedMessage(vertex.getId())));
		}
		c.visitedVertices.put(vertex.getId(), vertex);
		c.loadVertexConnections(vertex);
		float distFromStart = vertex.distanceFromStart + vertex.distSegment() / 2;
		for (RouteSegmentEdge e : vertex.connections) {
			long id = e.t.getId();
			if (!c.testIfVisited(id) && !c.toVisitVertices.containsKey(id)) {
				if (existNetworkPoints != null && !existNetworkPoints.contains(id) && e.t.parentRoute != null) {
					// assert node is visited once
					throw new IllegalArgumentException(e.t + " parent " + e.t.parentRoute);
				}
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
			 List<MaxFlowVertex> sources, List<MaxFlowVertex> vertices, String errorDebug) {
		// For maxflow / mincut algorithm RouteSegmentVertex is edge with capacity 1 connected using end boolean flag on both ends
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
			vertices.add(s);
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
					int maxFlow = conn.vertex == null ? Integer.MAX_VALUE : 1;
					if (conn.t.flowParentTemp == null && conn.flow < maxFlow) {
						conn.t.flowParentTemp = conn;
						boolean isSink;
						if (ALG_BY_DEPTH_REACH_POINTS) {
							isSink = conn.vertex != null && conn.vertex.getDepth() < minDepth;
						} else {
							isSink = conn.vertex != null && conn.vertex.order <= TOTAL_MIN_POINTS && conn.vertex.order > 0;
						}
						if (isSink) {
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
//				System.out.println("Sink " + sink);
				while (true) {
//					System.out.println(p.flowParentTemp);
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

	private List<MaxFlowVertex> constructMaxFlowGraph(TLongObjectHashMap<RouteSegmentVertex> values,
			TLongObjectHashMap<RouteSegmentVertex> existingVertices, TLongObjectHashMap<MaxFlowEdge> edges) {
		List<MaxFlowVertex> vertices = new ArrayList<>();
		for (RouteSegmentVertex r : values.valueCollection()) {
			MaxFlowVertex s = null;
			MaxFlowVertex t = null;
			for (RouteSegmentEdge e : r.connections) {
				MaxFlowEdge ex = edges.get(e.t.cId);
				if (existingVertices.contains(e.t.getId()) || existingVertices.contains(e.s.getId())) {
					continue;
				}
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

		}
		return vertices;
	}

	private TLongObjectHashMap<RouteSegmentBorderPoint> calculateMincut(List<MaxFlowVertex> vertices,
			MaxFlowVertex source, Collection<MaxFlowVertex> sinks, TLongObjectHashMap<RouteSegmentVertex> visitedVertices) {
		for (MaxFlowVertex rs : vertices) {
			rs.flowParentTemp = null;
		}
		// debug
//		 visitedVertices.clear();

		TLongObjectHashMap<RouteSegmentBorderPoint> mincuts = new TLongObjectHashMap<>();
		LinkedList<MaxFlowVertex> queue = new LinkedList<>();
		Set<MaxFlowVertex> reachableSource = new HashSet<>();
		queue.add(source);
		if (DEBUG_VERBOSE_LEVEL > 1) {
			for (MaxFlowEdge t : source.connections) {
				MaxFlowVertex sourceL = t.t;
				int flow = 0;
				for (MaxFlowEdge tc : sourceL.connections) {
					flow += tc.flow;
				}
				if (flow > 0) {
					System.out.printf("-> Source: %s depth %d, flow %d\n", sourceL, sourceL.segment.getDepth(), flow) ;
				}
			}
			for (MaxFlowVertex s : sinks) {
				System.out.printf("<- Sink: %s depth %d\n", s, s.segment.getDepth()) ;
			}
		}
		reachableSource.add(source);
		while (!queue.isEmpty()) {
			MaxFlowVertex ps = queue.poll();
			for (MaxFlowEdge conn : ps.connections) {
				int maxFlow = conn.vertex == null ? Integer.MAX_VALUE : 1;
				if (conn.t.flowParentTemp == null && conn.flow < maxFlow) {
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
					MaxFlowEdge c = conn;
					boolean posDir = c.vertex.getStartPointX() == (c.s.end ? c.s.segment.getEndPointX() : c.s.segment.getStartPointX()) &&
							c.vertex.getStartPointY() == (c.s.end ? c.s.segment.getEndPointY() : c.s.segment.getStartPointY());
					RouteSegmentBorderPoint borderPnt = new RouteSegmentBorderPoint(c.vertex, posDir);
					mincuts.put(calcUniDirRoutePointInternalId(c.vertex), borderPnt);
					if (DEBUG_VERBOSE_LEVEL > 1) {
						System.out.println("? Mincut " + c.s + " -> " + borderPnt);
					}
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


	static class RouteSegmentBorderPoint {
		public final long unidirId;
		public final long uniqueId;
		public final int segmentStart;
		public final int segmentEnd;
		public final long roadId;
		public final int sx, sy, ex, ey;
		// segment [inner point index -> outer point index]
		public int pointDbId;
		public int clusterDbId;
		public int fileDbId;
		public boolean inserted;
		public String[] tagValues;


		public RouteSegmentBorderPoint(RouteSegment s, boolean dir) {
			segmentStart = dir ? s.getSegmentStart() : s.getSegmentEnd();
			segmentEnd = dir ? s.getSegmentEnd() : s.getSegmentStart();
			roadId = s.getRoad().getId();
			sx = dir ? s.getStartPointX() : s.getEndPointX();
			sy = dir ? s.getStartPointY() : s.getEndPointY();
			ex = !dir ? s.getStartPointX() : s.getEndPointX();
			ey = !dir ? s.getStartPointY() : s.getEndPointY();
			unidirId = uniId();
			uniqueId = uniqueId();
			int[] tps = s.getRoad().getTypes();
			if (tps != null) {
				this.tagValues = new String[tps.length * 2];
				for (int i = 0; i < tps.length; i++) {
					RouteTypeRule rtr = s.getRoad().region.quickGetEncodingRule(tps[i]);
					this.tagValues[2 * i] = rtr.getTag();
					this.tagValues[2 * i + 1] = rtr.getValue();
				}
			}
		}

		public RouteSegmentBorderPoint(long roadId, int st, int end, int sx, int sy, int ex, int ey, String[] tagValues) {
			this.roadId = roadId;
			this.segmentStart = st;
			this.segmentEnd = end;
			this.sx = sx;
			this.sy = sy;
			this.ex = ex;
			this.ey = ey;
			unidirId = uniId();
			uniqueId = uniqueId();
			this.tagValues = tagValues;
			inserted = true;
		}

		private long uniqueId() {
			return calculateRoutePointInternalId(roadId, segmentStart, segmentEnd);
		}

		private long uniId() {
			return calculateRoutePointInternalId(roadId, Math.min(segmentStart, segmentEnd),Math.max(segmentStart, segmentEnd));
		}

		public static RouteSegmentBorderPoint fromParent(RouteSegment ls) {
			boolean pos = (ls.parentRoute.getEndPointX() == ls.getStartPointX()
					&& ls.parentRoute.getEndPointY() == ls.getStartPointY())
					|| (ls.parentRoute.getStartPointX() == ls.getStartPointX()
							&& ls.parentRoute.getStartPointY() == ls.getStartPointY());
			return new RouteSegmentBorderPoint(ls, pos);
		}

		public boolean isPositive() {
			return segmentStart < segmentEnd;
		}

		@Override
		public String toString() {
			return String.format("Border point db %d (cluster %d), road %d [%d - %d]", pointDbId, clusterDbId,
					roadId / 64, segmentStart, segmentEnd);
		}
	}


	class LongRoadGroup {
		QuadRect r;
		Set<NetworkLongRoad> set = new LinkedHashSet<>();

		@Override
		public String toString() {
			return set.toString();
		}
	}

	class RouteSegmentVertex extends RouteSegment {

		public List<RouteSegmentEdge> connections = new ArrayList<>();
		public int cDepth;
		public final long cId;
		public int order = 0;

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
			return (float) MapUtils.squareRootDist31(x, y, prevX, prevY);
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
		TLongObjectHashMap<RouteSegmentVertex> visitedVertices = new TLongObjectHashMap<>();
		TLongObjectHashMap<RouteSegmentVertex> toVisitVertices = new TLongObjectHashMap<>();

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

		public RouteSegmentVertex getVertex(RouteSegment s) {
			if (s == null) {
				return null;
			}
			long p = calcUniDirRoutePointInternalId(s);
			RouteSegmentVertex routeSegment = ctx.allVerticesCache.get(p);
			if (routeSegment == null) {
				routeSegment = new RouteSegmentVertex(makePositiveDir(s));
				ctx.allVerticesCache.put(p, routeSegment);
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


		void loadVertexConnections(RouteSegmentVertex segment, boolean end) {
			int segmentInd = end ? segment.getSegmentEnd() : segment.getSegmentStart();
			int x = segment.getRoad().getPoint31XTile(segmentInd);
			int y = segment.getRoad().getPoint31YTile(segmentInd);
			RouteSegment next = ctx.rctx.loadRouteSegment(x, y, 0);
			while (next != null) {
				segment.addConnection(end, getVertex(next));
				segment.addConnection(end, getVertex(next.initRouteSegment(!next.isPositive())));
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

			return false;
		}

		public void clearVisitedPoints(TLongObjectHashMap<RouteSegmentVertex> existNetworkPoints) {
//			for (RouteSegmentVertex v : ctx.allVerticesCache.valueCollection()) {
//				v.clearPoint();
//			}
			for (RouteSegmentVertex v : visitedVertices.valueCollection()) {
				v.clearPoint();
			}
			for (RouteSegmentVertex v : toVisitVertices.valueCollection()) {
				v.clearPoint();
			}
			for (RouteSegmentVertex v : existNetworkPoints.valueCollection()) {
				v.clearPoint();
			}
		}

		public void clearQueues() {
			visitedVertices.clear();
			toVisitVertices.clear();
			queue.clear();
		}

	}


	private static class NetworkCollectStats {
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

	private static class NetworkCollectPointCtx {
		HHRoutingPrepareContext prepareContext;
		HHRoutingPreparationDB networkDB;
		NetworkCollectStats stats = new NetworkCollectStats();

		int lastClusterInd = -1;
		RoutingContext rctx;
		List<NetworkRouteRegion> routeRegions = new ArrayList<>();
		List<NetworkIsland> visualClusters = new ArrayList<>();

		NetworkRouteRegion currentProcessingRegion;
		TLongObjectHashMap<RouteSegmentVertex> allVerticesCache = new TLongObjectHashMap<>();
		boolean checkLongRoads = true;

		List<NetworkLongRoad> longRoads = new ArrayList<>();
		TLongObjectHashMap<NetworkBorderPoint> networkPointToDbInd = new TLongObjectHashMap<>();
		List<NetworkRouteRegion> validateIntersectionRegions = new ArrayList<>();



		public NetworkCollectPointCtx(HHRoutingPrepareContext prepareContext, HHRoutingPreparationDB networkDB) throws IOException {
			this.prepareContext = prepareContext;
			this.rctx = prepareContext.prepareContext(null, null);
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

		public String globalVisitedMessage(long k) {
			NetworkRouteRegion r = null;
			if (currentProcessingRegion != null && currentProcessingRegion.visitedVertices.containsKey(k)) {
				r = currentProcessingRegion;
			} else {
				for (NetworkRouteRegion n : validateIntersectionRegions) {
					if (n.visitedVertices.containsKey(k)) {
						r = n;
						break;
					}
				}
			}
			if (r != null) {
				return String.format("%s region %d cluster", r.getName(), r.visitedVertices.get(k));
			}
			return "";
		}

		public boolean testGlobalVisited(long k) {
			if (currentProcessingRegion != null && currentProcessingRegion.visitedVertices.containsKey(k)) {
				return true;
			}
			for (NetworkRouteRegion n : validateIntersectionRegions) {
				if (n.visitedVertices.containsKey(k)) {
					return true;
				}
			}
			return false;
		}

		public void startRegionProcess(NetworkRouteRegion nrouteRegion, double overlapBbox) throws IOException, SQLException {
			currentProcessingRegion = nrouteRegion;
			currentProcessingRegion.visitedVertices = new TLongIntHashMap();
			currentProcessingRegion.calcRect = null;
			currentProcessingRegion.points = -1;
			validateIntersectionRegions = new ArrayList<>();
			allVerticesCache = new TLongObjectHashMap<>();
			List<NetworkRouteRegion> regionsForRouting = new ArrayList<>();
			for (NetworkRouteRegion nr : routeRegions) {
				if (nr == nrouteRegion) {
					continue;
				}
				if (nr.intersects(nrouteRegion, OVERLAP_FOR_VISITED)) {
					logf("Intersects with %s %s.", nr.getName(), nr.rect.toString());
					nr.loadVisitedVertices(networkDB);
					validateIntersectionRegions.add(nr);
					regionsForRouting.add(nr);
				} else if (nr.intersects(nrouteRegion, overlapBbox)) {
					regionsForRouting.add(nr);
				} else {
					nr.unload();
				}
			}
			if (nrouteRegion.file != null) {
				regionsForRouting.add(nrouteRegion);
			}
			// force reload cause subregions could change on rerun
			rctx = prepareContext.gcMemoryLimitToUnloadAll(rctx, regionsForRouting, true);

		}

		public void addCluster(NetworkIsland cluster) {
			cluster.dbIndex = networkDB.prepareBorderPointsToInsert(currentProcessingRegion == null ? 0 : currentProcessingRegion.id,
					cluster.borderVertices, networkPointToDbInd);
			lastClusterInd = cluster.dbIndex;
			stats.addCluster(cluster);
			if (cluster.visitedVertices.size() > TOTAL_MAX_POINTS * 1.5) {
				throw new IllegalStateException(
						"Cluster " + cluster.dbIndex + " has too many points: " + cluster.visitedVertices.size());
			}
			for (RouteSegmentVertex v : cluster.visitedVertices.valueCollection()) {
				if (testGlobalVisited(v.getId())) {
					throw new IllegalStateException(String.format("Point was already visited %s: %s", v, globalVisitedMessage(v.getId())));
				}
				if (currentProcessingRegion != null) {
					currentProcessingRegion.visitedVertices.put(v.getId(), cluster.dbIndex);
					currentProcessingRegion.updateBbox(v.getEndPointX() / 2 + v.getStartPointX() / 2,
							v.getEndPointY() / 2 + v.getStartPointY() / 2);
				}
			}

			if (DEBUG_STORE_ALL_ROADS > 0) {
				visualClusters.add(cluster);
				// preserve cluster.borderVertices
				if (DEBUG_STORE_ALL_ROADS <= 2) {
					long[] keys = cluster.visitedVertices.keys();
					for(long k : keys) {
						cluster.visitedVertices.put(k, null);
					}
				}
				if (DEBUG_STORE_ALL_ROADS > 1) {
					cluster.visualBorders = new TLongObjectHashMap<List<LatLon>>();
					for (RouteSegmentVertex p : cluster.toVisitVertices.valueCollection()) {
						List<LatLon> l = new ArrayList<LatLon>();
						RouteSegment par = p;
						while (par != null) {
							l.add(HHRoutingUtilities.getPoint(par));
							par = par.parentRoute;
						}
						cluster.visualBorders.put(p.cId, l);
					}
				}
				cluster.toVisitVertices = null;
				cluster.queue = null;

			}
		}

		public boolean finishRegionProcess(double overlapBbox) throws SQLException {
			logf("Tiles " + rctx.calculationProgress.getInfo(null).get("tiles"));
			QuadRect c = currentProcessingRegion.getCalcBbox();
			QuadRect r = currentProcessingRegion.rect;
			if(c.left < r.left || c.top > r.top || c.bottom < r.bottom || c.right > r.right) {
				QuadRect n = new QuadRect(Math.min(c.left, r.left), Math.max(c.top, r.top),
						Math.max(c.right, r.right), Math.min(c.bottom, r.bottom));
				System.out.printf("Updating bbox (L T R B) for %s from [%.3f, %.3f] x [%.3f, %.3f] add  "
						+ " [%.3f, %.3f] x [%.3f, %.3f] to [%.3f, %.3f] x [%.3f, %.3f]\n",
						currentProcessingRegion.getName(), r.left, r.top, r.right, r.bottom,
						c.left, c.top, c.right, c.bottom, n.left, n.top, n.right, n.bottom);
				if (Math.abs(n.left - r.left) > overlapBbox || Math.abs(n.right - r.right) > overlapBbox
						|| Math.abs(n.top - r.top) > overlapBbox
						|| Math.abs(n.bottom - r.bottom) > overlapBbox) {
					System.err.println("BBOX is out of range for routing");
					return false;
				}
				currentProcessingRegion.rect = n;
			}
			int ins = 0, tl = 0;
			for (NetworkBorderPoint npnt : networkPointToDbInd.valueCollection()) {
				if (npnt.positiveObj != null) {
					if (!npnt.positiveObj.inserted) {
						ins++;
					}
					tl++;
				}
				if (npnt.negativeObj != null) {
					if (!npnt.negativeObj.inserted) {
						ins++;
					}
					tl++;
				}
			}
			logf("Saving visited %,d points (%,d border points) from %s to db...", currentProcessingRegion.getPoints(), ins,
					currentProcessingRegion.getName());
			networkDB.insertProcessedRegion(currentProcessingRegion, networkPointToDbInd, longRoads);
			logf("     saved - total %,d points (%,d border points), ", getTotalPoints(), tl);

			currentProcessingRegion.unload();
			currentProcessingRegion = null;
			return true;

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

		@Override
		public boolean publish(RouteDataObject object) {
			if (!ctx.rctx.getRouter().acceptLine(object)) {
				return false;
			}
			indProc++;
			if (indProc < DEBUG_LIMIT_START_OFFSET || isCancelled()) {
				System.out.println("SKIP PROCESS " + indProc);
			} else {
				// check long roads max segment
				for (int pos = 0; pos < object.getPointsLength() - 1 && ctx.checkLongRoads; pos++) {
					double dst = segmentDist(object, pos, pos+1);
					if (dst > LONG_DISTANCE_SEGMENTS_SPLIT) {
						String msg = String.format("Skip long road to process later %s (length %d) %d <-> %d - %.1f",
								object, object.getPointsLength(), pos, pos + 1, dst / 1000);
						System.out.println(msg);
						ctx.longRoads.add(new NetworkLongRoad(object.getId(), pos, object.pointsX, object.pointsY));
						return false;
					}
				}
				for (int pos = 0; pos < object.getPointsLength() - 1; pos++) {
					RouteSegmentPoint pntAround = new RouteSegmentPoint(object, pos, 0);
					long mainPoint = calcUniDirRoutePointInternalId(pntAround);
					if (ctx.testGlobalVisited(mainPoint) || ctx.networkPointToDbInd.containsKey(mainPoint)) {
						// already existing cluster
						continue;
					}
					NetworkIsland cluster = buildRoadNetworkIsland(ctx, pntAround);
					ctx.addCluster(cluster);
					if (DEBUG_VERBOSE_LEVEL >= 1 || indProc - prevPrintInd > 1000) {
						prevPrintInd = indProc;
						int borderPointsSize = ctx.borderPointsSize();
						logf("Progress %.2f%%: all %,d points -> %,d border points, %,d clusters",
								indProc * 100.0f / estimatedRoads, ctx.getTotalPoints() + borderPointsSize,
								borderPointsSize, ctx.lastClusterInd);
					}
				}
			}
			return false;
		}

		private double segmentDist(RouteDataObject object, int pos, int next) {
			return MapUtils.squareRootDist31(object.getPoint31XTile(pos), object.getPoint31YTile(pos),
					object.getPoint31XTile(next), object.getPoint31YTile(next));
		}

		@Override
		public boolean isCancelled() {
			return DEBUG_LIMIT_PROCESS != -1 && indProc >= DEBUG_LIMIT_PROCESS;
		}
	}

	public void mergeConnectedPoints(NetworkCollectPointCtx ctx) {
		// Connected points needs to be merged to create continuous network
		// As single point will be ignored as end of the roads that might be a problem for continuity
		// Points to merge happen in both cases: processing long roads and having a limit TOTAL_MAX_POINTS
		TLongObjectHashMap<List<RouteSegmentBorderPoint>> mp = new TLongObjectHashMap<>();
		List<NetworkBorderPoint> lst = new ArrayList<>(ctx.networkPointToDbInd.valueCollection());
		for (NetworkBorderPoint p : lst) {
			if (p.positiveObj != null && p.negativeObj == null) {
				add(p.positiveObj, mp);
			}
			if (p.negativeObj != null && p.positiveObj == null) {
				add(p.negativeObj, mp);
			}
		}
		System.out.printf("Check to merge %d points... ", mp.size());
		for (List<RouteSegmentBorderPoint> lstMerge : mp.valueCollection()) {
			if (lstMerge.size() <= 1) {
				// nothing to merge
			} else {
				RouteSegmentBorderPoint f = lstMerge.get(0);
				RouteSegmentBorderPoint s = lstMerge.get(1);
				// && f.roadId == s.roadId && f.isPositive() == !s.isPositive() && f.segmentEnd == s.segmentEnd - no need to check same road id - 2 points is a simple merge
				if (lstMerge.size() == 2) {
					// simple merge scenario
					simpleMerge(ctx, f, s.clusterDbId, s);
					continue;
				}
				TIntIntHashMap clusters = new TIntIntHashMap();
				for (RouteSegmentBorderPoint p : lstMerge) {
					clusters.adjustOrPutValue(p.clusterDbId, 1, 1);
				}
				// Potentially creates points without dual point that actually is a problem later (if it crashes we can revert it)
//				if (clusters.size() == 1) {
//					// ignoring points that belong to same cluster is ok cause it doesn't change connectivity between clusters
//					System.err.println("Ignore (same cluster points to merge): " + lstMerge);
//					continue;
//				}
				List<RouteSegmentBorderPoint> multiplePointClusters = new ArrayList<>();
				int multiClusterId = -1;
				for (RouteSegmentBorderPoint p : lstMerge) {
					if (clusters.get(p.clusterDbId) > 1) {
						multiplePointClusters.add(p);
						if(multiClusterId == -1) {
							multiClusterId = p.clusterDbId;
						} else if(multiClusterId != p.clusterDbId){
							// there are multiple clusters with multiple points
							multiClusterId = -1;
							break;
						}
					}
				}
				if (multiClusterId != -1) {
					lstMerge.removeAll(multiplePointClusters);
					RouteSegmentBorderPoint[] arr = multiplePointClusters.toArray(new RouteSegmentBorderPoint[multiplePointClusters.size()]);
					for (RouteSegmentBorderPoint singlePointCluster : lstMerge) {
						System.out.printf(
								"Complex scenario with [%d] clusters (%s): main point (of %d) (%s) merging with lstMerge [%d] (%s)\n",
								clusters.size(), clusters, lstMerge.size(), singlePointCluster, lstMerge.size(), lstMerge);
						simpleMerge(ctx, singlePointCluster, lstMerge.get(0).clusterDbId, arr);
					}
					continue;
				}
				String msg = String.format("Can't merge points %s", lstMerge);
				throw new IllegalArgumentException(msg);
			}
		}
	}

	private void add(RouteSegmentBorderPoint po, TLongObjectHashMap<List<RouteSegmentBorderPoint>> mp) {
		long epnt = Algorithms.combine2Points(po.ex, po.ey);
		if (!mp.containsKey(epnt)) {
			mp.put(epnt, new ArrayList<>());
		}
		mp.get(epnt).add(po);
	}

	private void simpleMerge(NetworkCollectPointCtx ctx, RouteSegmentBorderPoint main, int clusterOppId, RouteSegmentBorderPoint... toMerges) {
		logf("MERGE route road %s with %s", main, Arrays.toString(toMerges));
		RouteSegmentBorderPoint newOpp = new RouteSegmentBorderPoint(main.roadId, main.segmentEnd, main.segmentStart,
				main.ex, main.ey, main.sx, main.sy, main.tagValues);
		if (main.isPositive()) {
			ctx.networkPointToDbInd.get(main.unidirId).negativeObj = newOpp;
		} else {
			ctx.networkPointToDbInd.get(main.unidirId).positiveObj = newOpp;
 	 	}
		newOpp.clusterDbId = clusterOppId;
		newOpp.inserted = main.inserted;
		newOpp.fileDbId = main.fileDbId;
		ctx.networkDB.mergePoints(main, newOpp);

		for (RouteSegmentBorderPoint toMerge : toMerges) {
			ctx.networkDB.deleteMergePoints(toMerge);
			if (ctx.currentProcessingRegion != null) {
				ctx.currentProcessingRegion.visitedVertices.put(toMerge.unidirId, toMerge.clusterDbId);
			}
			ctx.networkPointToDbInd.remove(toMerge.unidirId);
		}

	}


}
