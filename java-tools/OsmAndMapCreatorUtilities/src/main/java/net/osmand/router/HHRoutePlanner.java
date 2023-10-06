package net.osmand.router;

import static net.osmand.router.HHRoutingUtilities.calculateRoutePointInternalId;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;

import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.router.BinaryRoutePlanner.FinalRouteSegment;
import net.osmand.router.BinaryRoutePlanner.MultiFinalRouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBSegment;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.util.MapUtils;

public class HHRoutePlanner {
	static String ROUTING_PROFILE = "car";
	static int DEBUG_VERBOSE_LEVEL = 0;
	static boolean USE_LAST_MILE_ROUTING = true;
	static boolean CALCULATE_GEOMETRY = true;
	
	static LatLon PROCESS_START = null;
	static LatLon PROCESS_END = null;
	
	static boolean PRELOAD_SEGMENTS = false;

	static final int PROC_ROUTING = 0;
	static int PROCESS = PROC_ROUTING;

	
	private RoutingContext ctx;
	private HHRoutingPreparationDB networkDB;
	private TLongObjectHashMap<NetworkDBPoint> cachePoints;
	private TLongObjectHashMap<NetworkDBPoint> cachePointsByGeo;
	private TLongObjectHashMap<RouteSegment> cacheBoundaries; 
	
	public HHRoutePlanner(RoutingContext ctx, HHRoutingPreparationDB networkDB) {
		this.ctx = ctx;
		this.networkDB = networkDB;
	}
	
	public static class DijkstraConfig {
		float HEURISTIC_COEFFICIENT = 0; // A* - 1, Dijkstra - 0
		float DIJKSTRA_DIRECTION = 0; // 0 - 2 directions, 1 - positive, -1 - reverse
		
		double MAX_COST;
		int MAX_DEPTH = -1; // max depth to go to
		int MAX_SETTLE_POINTS = -1; // max points to settle
		
		boolean USE_CH;
		boolean USE_CH_SHORTCUTS;

		boolean USE_MIDPOINT;
		int MIDPOINT_ERROR = 3;
		int MIDPOINT_MAX_DEPTH = 20 + MIDPOINT_ERROR;
		
		public List<NetworkDBPoint> visited = new ArrayList<>();
		public List<NetworkDBPoint> visitedRev = new ArrayList<>();
		
		public static DijkstraConfig dijkstra(int direction) {
			DijkstraConfig df = new DijkstraConfig();
			df.HEURISTIC_COEFFICIENT = 0;
			df.DIJKSTRA_DIRECTION = direction;
			return df;
		}
		
		public static DijkstraConfig astar(int direction) {
			DijkstraConfig df = new DijkstraConfig();
			df.HEURISTIC_COEFFICIENT = 1;
			df.DIJKSTRA_DIRECTION = direction;
			return df;
		}
		
		public static DijkstraConfig ch() {
			DijkstraConfig df = new DijkstraConfig();
			df.HEURISTIC_COEFFICIENT = 0;
			df.USE_CH = true;
			df.USE_CH_SHORTCUTS = true;
			df.DIJKSTRA_DIRECTION = 0;
			return df;
		}
		
		public static DijkstraConfig midPoints(boolean astar, int dir) {
			DijkstraConfig df = new DijkstraConfig();
			df.HEURISTIC_COEFFICIENT = astar ? 1 : 0;
			df.USE_MIDPOINT = true;
			df.DIJKSTRA_DIRECTION = dir;
			return df;
		}
		
		public DijkstraConfig useShortcuts() {
			USE_CH_SHORTCUTS = true;
			return this;
		}
		
		public DijkstraConfig maxCost(double cost) {
			MAX_COST = cost;
			return this;
		}
		
		public DijkstraConfig maxDepth(int depth) {
			MAX_DEPTH = depth;
			return this;
		}
		
		public DijkstraConfig maxSettlePoints(int maxPoints) {
			MAX_SETTLE_POINTS = maxPoints;
			return this;
		}

		@Override
		public String toString() {
			return toString(null, null);
		}
		
		public String toString(LatLon start, LatLon end) {
			return String.format("Routing %s -> %s (HC %d, dir %d) : midpoint %s, error %d, max %d", 
					start == null ? "?" : start.toString() , end == null ? "?" :  end.toString(),
					(int) HEURISTIC_COEFFICIENT, (int) DIJKSTRA_DIRECTION,
					USE_MIDPOINT +"", MIDPOINT_ERROR, MIDPOINT_MAX_DEPTH);
		}
	}
	
	static class RoutingStats {
		int visitedVertices = 0;
		int uniqueVisitedVertices = 0;
		int addedVertices = 0;

		double loadPointsTime = 0;
		int loadEdgesCnt;
		double loadEdgesTime = 0;
		double routingTime = 0;
		double searchPointsTime = 0;
		double addQueueTime = 0;
		double pollQueueTime = 0;
		double prepTime = 0;
	}
	
	private static File testData() {
		String name = "Montenegro_europe_2.road.obf";
		name = "Netherlands_europe";
//		name = "Ukraine_europe";
//		name = "Germany";
		
		// "Netherlands_europe_2.road.obf"
		PROCESS_START = new LatLon(52.34800, 4.86206); // AMS
////		PROCESS_END = new LatLon(51.57803, 4.79922); // Breda
		PROCESS_END = new LatLon(51.35076, 5.45141); // ~Eindhoven
		
		// "Montenegro_europe_2.road.obf"
//		PROCESS_START = new LatLon(43.15274, 19.55169); 
//		PROCESS_END= new LatLon(42.45166, 18.54425);
		
		// Ukraine
//		PROCESS_START = new LatLon(50.43539, 30.48234); // Kyiv
//		PROCESS_START = new LatLon(50.01689, 36.23278); // Kharkiv
//		PROCESS_END = new LatLon(46.45597, 30.75604);   // Odessa
//		PROCESS_END = new LatLon(48.43824, 22.705723); // Mukachevo
//		
		
//		PROCESS_START = new LatLon(50.30487, 31.29761); // 
//		PROCESS_END = new LatLon(50.30573, 28.51402); //
		
		// Germany
//		PROCESS_START = new LatLon(53.06264, 8.79675); // Bremen 
//		PROCESS_END = new LatLon(48.08556, 11.50811); // Munich
		
		return new File(System.getProperty("maps.dir"), name);
	}
	
	public static void main(String[] args) throws Exception {
		File obfFile = args.length == 0 ? testData() : new File(args[0]);
		DijkstraConfig c = null;
		for (String a : args) {
			if (a.startsWith("--start=")) {
				String[] latLons = a.substring("--start=".length()).split(",");
				PROCESS_START = new LatLon(Double.parseDouble(latLons[0]), Double.parseDouble(latLons[1]));
			} else if (a.startsWith("--end=")) {
				String[] latLons = a.substring("--end=".length()).split(",");
				PROCESS_END = new LatLon(Double.parseDouble(latLons[0]), Double.parseDouble(latLons[1]));
			} else if (a.startsWith("--heuristic=")) {
				if (c == null) {
					c = new DijkstraConfig();
				}
				c.HEURISTIC_COEFFICIENT = (float) Double.parseDouble(a.substring("--heuristic=".length()));
			} else if (a.startsWith("--direction=")) {
				if (c == null) {
					c = new DijkstraConfig();
				}
				c.DIJKSTRA_DIRECTION = (float) Double.parseDouble(a.substring("--direction=".length()));
			} else if (a.startsWith("--ch")) {
				c = DijkstraConfig.ch();
			} else if (a.startsWith("--preload")) {
				PRELOAD_SEGMENTS = true;
			} else if (a.startsWith("--midpoint=")) {
				String[] s = a.substring("--midpoint=".length()).split(":");
				if (c == null) {
					c = new DijkstraConfig();
				}
				c.MIDPOINT_MAX_DEPTH = Integer.parseInt(s[0]);
				c.MIDPOINT_ERROR = Integer.parseInt(s[1]);
			}
		}
		if (PROCESS_START == null || PROCESS_END == null) {
			System.err.println("Start / end point is not specified");
			return;
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getCanonicalFile().getName();
		HHRoutePlanner planner = new HHRoutePlanner(prepareContext(ROUTING_PROFILE), 
				new HHRoutingPreparationDB(new File(folder, name + HHRoutingPreparationDB.EXT)));
		if (PROCESS == PROC_ROUTING) {
			HHNetworkRouteRes route = planner.runRouting(PROCESS_START, PROCESS_END, c);
			TLongObjectHashMap<Entity> entities = new TLongObjectHashMap<Entity>();
			for (HHNetworkSegmentRes r : route.segments) {
				if (r.list != null) {
					for (RouteSegment rs : r.list) {
						HHRoutingUtilities.addWay(entities, rs, "highway", "secondary");
					}
				} else if (r.segment != null) {
					HHRoutingUtilities.addWay(entities, r.segment, "highway", "primary");
				}
			}
			HHRoutingUtilities.saveOsmFile(entities.valueCollection(), new File(folder, name + "-rt.osm"));
		}
		planner.networkDB.close();
	}



	public static RoutingContext prepareContext(String routingProfile) {
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.getDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3,
				RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		Map<String, String> map = new TreeMap<String, String>();
		RoutingConfiguration config = builder.build(routingProfile, memoryLimit, map);
		config.planRoadDirection = 1;
		config.heuristicCoefficient = 0; // dijkstra
		return router.buildRoutingContext(config, null, new BinaryMapIndexReader[0], RouteCalculationMode.NORMAL);
	}

	public HHNetworkRouteRes runRouting(LatLon start, LatLon end, DijkstraConfig c) throws SQLException, IOException, InterruptedException {
		RoutingStats stats = new RoutingStats();
		long time = System.nanoTime(), startTime = System.nanoTime();
		if (c == null) {
			c = new DijkstraConfig();
			// test data for debug swap
//			c = DijkstraConfig.dijkstra(0);
			c = DijkstraConfig.astar(1);
//			c = DijkstraConfig.ch();
			PRELOAD_SEGMENTS = false;
			USE_LAST_MILE_ROUTING = true;
			DEBUG_VERBOSE_LEVEL = 0;
		}
		System.out.println(c.toString(start, end));
		System.out.print("Loading points... ");
		if (cachePoints == null) {
			cachePoints = networkDB.getNetworkPoints(false);
			cacheBoundaries = new TLongObjectHashMap<RouteSegment>();
			cachePointsByGeo = new TLongObjectHashMap<NetworkDBPoint>();
			stats.loadPointsTime = (System.nanoTime() - time) / 1e6;
			System.out.printf(" %,d - %.2fms\n", cachePoints.size(), stats.loadPointsTime);
			if (PRELOAD_SEGMENTS) {
				time = System.nanoTime();
				System.out.printf("Loading segments...");
				int cntEdges = networkDB.loadNetworkSegments(cachePoints.valueCollection());
				stats.loadEdgesTime = (System.nanoTime() - time) / 1e6;
				System.out.printf(" %,d - %.2fms\n", cntEdges, stats.loadEdgesTime);
				stats.loadEdgesCnt = cntEdges;
			} else {
				for (NetworkDBPoint p : cachePoints.valueCollection()) {
					p.markSegmentsNotLoaded();
				}
			}
			for (NetworkDBPoint pnt : cachePoints.valueCollection()) {
				long pos = calculateRoutePointInternalId(pnt.roadId, pnt.start, pnt.end);
				long neg = calculateRoutePointInternalId(pnt.roadId, pnt.end, pnt.start);
				if (pos != pnt.pntGeoId && neg != pnt.pntGeoId) {
					throw new IllegalStateException();
				}
				cacheBoundaries.put(pos, null);
				cacheBoundaries.put(neg, null);
				cachePointsByGeo.put(pos, pnt);
				cachePointsByGeo.put(neg, pnt);
			}
		}
		for (NetworkDBPoint pnt : cachePoints.valueCollection()) {
			pnt.clearRouting();
		}
		List<NetworkDBPoint> stPoints = initStart(c, start, end,!USE_LAST_MILE_ROUTING, false);
		List<NetworkDBPoint> endPoints = initStart(c, end, start, !USE_LAST_MILE_ROUTING, true);

		stats.searchPointsTime = (System.nanoTime() - time) / 1e6;
		time = System.nanoTime();
		System.out.printf("Looking for route %s -> %s \n", start, end);
		System.out.printf("Routing...\n");
		NetworkDBPoint pnt = runDijkstraNetworkRouting(stPoints, endPoints, start, end, c, stats);
		stats.routingTime = (System.nanoTime() - time) / 1e6;
		
		time = System.nanoTime();
		HHNetworkRouteRes route = prepareRoutingResults(networkDB, pnt, stats);
		stats.prepTime = (System.nanoTime() - time) / 1e6;
		System.out.println(c.toString(start, end));
		System.out.printf("Routing finished all %.1f ms: last mile %.1f ms, load data %.1f ms (%,d edges), routing %.1f ms (queue  - %.1f add ms + %.1f poll ms), prep result %.1f ms\n",
				(System.nanoTime() - startTime) / 1e6, stats.searchPointsTime,
				stats.loadEdgesTime + stats.loadPointsTime, stats.loadEdgesCnt, stats.routingTime, stats.addQueueTime,
				stats.pollQueueTime, stats.prepTime);
		HHRoutingUtilities.printGCInformation();
		return route;
	}

	private List<NetworkDBPoint> initStart(DijkstraConfig c, LatLon p, LatLon e, boolean simple, boolean reverse)
			throws IOException, InterruptedException {
		if (simple) {
			NetworkDBPoint st = null;
			for (NetworkDBPoint pnt : cachePoints.valueCollection()) {
				if (st == null) {
					st = pnt;
				}
				if (MapUtils.getDistance(p, pnt.getPoint()) < MapUtils.getDistance(p, st.getPoint())) {
					st = pnt;
				}
			}
			if (st == null) {
				Collections.emptyList();
			}
			return Collections.singletonList(st);
		}
		RoutePlannerFrontEnd planner = new RoutePlannerFrontEnd();
		RouteSegmentPoint s = planner.findRouteSegment(p.getLatitude(), p.getLongitude(), ctx, null);
		if (s == null) {
			return Collections.emptyList();
		}
		ctx.config.planRoadDirection = reverse ? -1 : 1;
		ctx.config.heuristicCoefficient = 0; // dijkstra
		ctx.unloadAllData(); // needed for proper multidijsktra work
		ctx.calculationProgress = new RouteCalculationProgress();
		if (reverse) {
			ctx.targetX = s.getRoad().getPoint31XTile(s.getSegmentStart(), s.getSegmentEnd());
			ctx.targetY = s.getRoad().getPoint31XTile(s.getSegmentStart(), s.getSegmentEnd());
		} else {
			ctx.startX = s.getRoad().getPoint31XTile(s.getSegmentStart(), s.getSegmentEnd());
			ctx.startY = s.getRoad().getPoint31YTile(s.getSegmentStart(), s.getSegmentEnd());
		}
		
		MultiFinalRouteSegment frs = (MultiFinalRouteSegment) new BinaryRoutePlanner().searchRouteInternal(ctx,
				reverse ? null : s, reverse ? s : null, null, cacheBoundaries);
		List<NetworkDBPoint> pnts = new ArrayList<>();
		if (frs != null) {
			TLongSet set = new TLongHashSet();
			for (RouteSegment o : frs.all) {
				// duplicates are possible as alternative routes
				long pntId = calculateRoutePointInternalId(o);
				if (set.add(pntId)) {
					NetworkDBPoint pnt = cachePointsByGeo.get(pntId);
					pnt.setCostParentRt(reverse, o.getDistanceFromStart() + distanceToEnd(c, reverse, pnt, e), null,
							o.getDistanceFromStart());
					pnt.setCostDetailedParentRt(reverse, o);
					pnts.add(pnt);
				}
			}
		}
		return pnts;
	}

	
	static class NetworkDBPointCost {
		NetworkDBPoint point;
		double cost;
		boolean rev;
		
		NetworkDBPointCost(NetworkDBPoint p, double cost, boolean rev) {
			point = p;
			this.cost = cost;
			this.rev = rev;
		}
	}
	
	protected NetworkDBPoint runDijkstraNetworkRouting(NetworkDBPoint start, NetworkDBPoint end, DijkstraConfig c,
			RoutingStats stats) throws SQLException {
		return runDijkstraNetworkRouting(start == null ? Collections.emptyList() : Collections.singletonList(start),
				end == null ? Collections.emptyList() : Collections.singletonList(end),
				start == null ? null : start.getPoint(), end == null ? null : end.getPoint(), c, stats);
	}
	
	protected NetworkDBPoint runDijkstraNetworkRouting(Collection<NetworkDBPoint> starts, Collection<NetworkDBPoint> ends,
			LatLon startLatLon, LatLon endLatLon, DijkstraConfig c,
			RoutingStats stats) throws SQLException {
		Queue<NetworkDBPointCost> queue = new PriorityQueue<>(new Comparator<NetworkDBPointCost>() {
			@Override
			public int compare(NetworkDBPointCost o1, NetworkDBPointCost o2) {
				return Double.compare(o1.cost, o2.cost);
			}
		});
		// TODO revert 2 queues to fail fast in 1 direction
		for (NetworkDBPoint start : starts) {
			if (start.rtCost(false) <= 0) {
				start.setCostParentRt(false, distanceToEnd(c, false, start, endLatLon), null, 0);
			}
			queue.add(new NetworkDBPointCost(start, start.rtCost(false), false));
		}
		for (NetworkDBPoint end : ends) {
			if (end.rtCost(false) <= 0) {
				end.setCostParentRt(true, distanceToEnd(c, true, end, startLatLon), null, 0);
			}
			queue.add(new NetworkDBPointCost(end, end.rtCost(true), true));
		}

		while (!queue.isEmpty()) {
			long tm = System.nanoTime();
			NetworkDBPointCost pointCost = queue.poll();
			NetworkDBPoint point = pointCost.point;
			boolean rev = pointCost.rev;
			stats.pollQueueTime += (System.nanoTime() - tm) / 1e6;
			stats.visitedVertices++;
			if (point.visited(!rev)) {
				if (c.HEURISTIC_COEFFICIENT == 1 && c.DIJKSTRA_DIRECTION == 0) {
					// TODO could be improved while adding vertices ? too slow
					double rcost = point.rtDistanceFromStart + point.rtDistanceFromStartRev;
					if (rcost <= pointCost.cost) {
						return point;
					} else {
						queue.add(new NetworkDBPointCost(point, rcost, rev));
						point.markVisited(rev);
						continue;
					}
				} else {
					NetworkDBPoint finalPoint = point;
					if (c.DIJKSTRA_DIRECTION == 0) {
						finalPoint = scanFinalPoint(finalPoint, c.visited);
						finalPoint = scanFinalPoint(finalPoint, c.visitedRev);
					}
					return finalPoint;
				}
			}
			if (point.visited(rev)) {
				continue;
			}
			stats.uniqueVisitedVertices++;
			point.markVisited(rev);
			(rev ? c.visitedRev : c.visited).add(point);
			printPoint(point, rev);
			if (c.MAX_COST > 0 && pointCost.cost > c.MAX_COST) {
				break;
			}
			if (c.MAX_SETTLE_POINTS > 0 && (rev ? c.visitedRev : c.visited).size() > c.MAX_SETTLE_POINTS) {
				break;
			}
			boolean directionAllowed = (c.DIJKSTRA_DIRECTION <= 0 && rev) || (c.DIJKSTRA_DIRECTION >= 0 && !rev);
			if (directionAllowed) {
				addToQueue(queue, point, rev ? startLatLon : endLatLon, rev, c, stats);
			}
		}			
		return null;
		
	}

	private NetworkDBPoint scanFinalPoint(NetworkDBPoint finalPoint, List<NetworkDBPoint> lt) {
		for (NetworkDBPoint p : lt) {
			if (p.rtDistanceFromStart == 0 || p.rtDistanceFromStartRev == 0) {
				continue;
			}
			if (p.rtDistanceFromStart + p.rtDistanceFromStartRev < finalPoint.rtDistanceFromStart + finalPoint.rtDistanceFromStartRev) {
				finalPoint = p;
			}
		}
		return finalPoint;
	}
	
	private void addToQueue(Queue<NetworkDBPointCost> queue, NetworkDBPoint point, LatLon target, boolean reverse, 
			DijkstraConfig c, RoutingStats stats) throws SQLException {
		int depth = c.USE_MIDPOINT || c.MAX_DEPTH > 0 ? point.getDepth(!reverse) : 0;
		if (c.MAX_DEPTH > 0 && depth >= c.MAX_DEPTH) {
			return;
		}
		long tm = System.nanoTime();
		int cnt = networkDB.loadNetworkSegmentPoint(cachePoints, point, reverse);
		stats.loadEdgesCnt += cnt;
		stats.loadEdgesTime += (System.nanoTime() - tm) / 1e6;
		for (NetworkDBSegment connected : point.connected(reverse)) {
			NetworkDBPoint nextPoint = reverse ? connected.start : connected.end;
			if (!c.USE_CH && !c.USE_CH_SHORTCUTS && connected.shortcut) {
				continue;
			}
			if (nextPoint.rtExclude) {
				continue;
			}
			// modify CH to not compute all top points
			if (c.USE_CH && (nextPoint.chInd > 0 && nextPoint.chInd < point.chInd)) {
				continue;
			}
			if (c.USE_MIDPOINT && Math.min(depth, c.MIDPOINT_MAX_DEPTH) > nextPoint.rtCnt + c.MIDPOINT_ERROR) {
				continue;
			}
			double cost = point.distanceFromStart(reverse) + connected.dist + distanceToEnd(c, reverse, nextPoint, target);
			double exCost = nextPoint.rtCost(reverse);
			if ((exCost == 0 && !nextPoint.visited(reverse)) || cost < exCost) {
				if (nextPoint.visited(reverse)) {
					throw new IllegalStateException(String.format("%s visited - cost %.2f > prev cost %.2f", nextPoint, cost, exCost));
				}
				nextPoint.setCostParentRt(reverse, cost, point, connected.dist);
				tm = System.nanoTime();
				queue.add(new NetworkDBPointCost(nextPoint, cost, reverse)); // we need to add new object to not  remove / rebalance priority queue
				stats.addQueueTime += (System.nanoTime() - tm) / 1e6;
				if (DEBUG_VERBOSE_LEVEL > 2) {
					System.out.printf("Add  %s to visit - cost %.2f > prev cost %.2f \n", nextPoint, cost, exCost);
				}
				stats.addedVertices++;
			}
		}
	}

	private double distanceToEnd(DijkstraConfig c, boolean reverse,  NetworkDBPoint nextPoint, LatLon target) {
		if (c.HEURISTIC_COEFFICIENT > 0) {
			double distanceToEnd = nextPoint.distanceToEnd(reverse);
			if (distanceToEnd == 0) {
				distanceToEnd = c.HEURISTIC_COEFFICIENT * 
						MapUtils.getDistance(target, nextPoint.getPoint()) / ctx.getRouter().getMaxSpeed();
				nextPoint.setDistanceToEnd(reverse, distanceToEnd);
			}
			return distanceToEnd;
		}
		return 0;
	}
		
	private void printPoint(NetworkDBPoint p, boolean rev) {
		if (DEBUG_VERBOSE_LEVEL > 1) {
			int pind = 0; long pchInd = 0;
			if (rev && p.rtRouteToPointRev != null) {
				pind = p.rtRouteToPointRev.index;
				pchInd = p.rtRouteToPointRev.chInd;
			}
			if (!rev && p.rtRouteToPoint != null) {
				pind = p.rtRouteToPoint.index;
				pchInd = p.rtRouteToPoint.chInd;
			}
			String symbol = String.format("%s %d [%d] (from %d [%d])", rev ? "<-" : "->", p.index, p.chInd, pind, pchInd);
			System.out.printf("Visit Point %s (cost %.1f s) %.5f/%.5f - %d\n", symbol, p.rtCost(rev),
					MapUtils.get31LatitudeY(p.startY), MapUtils.get31LongitudeX(p.startX), p.roadId / 64);
		}
	}
	
	public static class HHNetworkSegmentRes {
		public NetworkDBSegment segment;
		public List<RouteSegment> list = null;
		public HHNetworkSegmentRes(NetworkDBSegment s) {
			segment = s;
		}
	}
	
	public static class HHNetworkRouteRes {
		public List<HHNetworkSegmentRes> segments = new ArrayList<>();
		public List<RouteSegment> detailed = new ArrayList<>();
	}

	
	private HHNetworkSegmentRes runDetailedRouting(HHNetworkSegmentRes res) throws InterruptedException, IOException {
		BinaryRoutePlanner planner = new BinaryRoutePlanner();
		NetworkDBSegment segment = res.segment;
		ctx.config.planRoadDirection = 0; // A* bidirectional
		ctx.config.heuristicCoefficient = 1; 
		ctx.unloadAllData(); // needed for proper multidijsktra work
		RouteSegmentPoint start = HHRoutingUtilities.loadPoint(ctx, segment.start);
		RouteSegmentPoint end = HHRoutingUtilities.loadPoint(ctx, segment.end);
		ctx.startX = start.getRoad().getPoint31XTile(start.getSegmentStart(), start.getSegmentEnd());
		ctx.startY = start.getRoad().getPoint31YTile(start.getSegmentStart(), start.getSegmentEnd());
		ctx.targetX = end.getRoad().getPoint31XTile(end.getSegmentStart(), end.getSegmentEnd());
		ctx.targetY = end.getRoad().getPoint31YTile(end.getSegmentStart(), end.getSegmentEnd());
		// TODO use cache boundaries to speed up
		FinalRouteSegment f = planner.searchRouteInternal(ctx, start, end, null, null);
		res.list = new ArrayList<>();
		RouteSegment p = f.opposite;
		while (p != null && p.getRoad() != null) {
			res.list.add(p);
			p = p.parentRoute;
		}
		Collections.reverse(res.list);

		p = f;
		while (p != null && p.getRoad() != null) {
			res.list.add(p);
			p = p.parentRoute;
		}
		Collections.reverse(res.list);
		return res;
	}
	
	


	private HHNetworkRouteRes prepareRoutingResults(HHRoutingPreparationDB networkDB, NetworkDBPoint pnt, RoutingStats stats) throws SQLException, InterruptedException, IOException {
		HHNetworkRouteRes route = new HHNetworkRouteRes();
		if (pnt != null) {
			NetworkDBPoint itPnt = pnt;
			if (itPnt.rtDetailedRouteRev != null) {
				HHNetworkSegmentRes res = new HHNetworkSegmentRes(null);
				res.list = new ArrayList<>();
				RouteSegment p = itPnt.rtDetailedRouteRev;
				while (p != null && p.getRoad() != null) {
					res.list.add(p);
					p = p.parentRoute;
				}
				route.segments.add(res);
			}
			while (itPnt.rtRouteToPointRev != null) {
				NetworkDBPoint nextPnt = itPnt.rtRouteToPointRev;
				NetworkDBSegment segment = nextPnt.getSegment(itPnt, false);
				networkDB.loadGeometry(segment, false);
				HHNetworkSegmentRes res = new HHNetworkSegmentRes(segment);
				route.segments.add(res);
				if (CALCULATE_GEOMETRY && segment.geometry.size() <= 2) {
					runDetailedRouting(res);
				}
				itPnt = nextPnt;
			}
			Collections.reverse(route.segments);
			itPnt = pnt;
			while (itPnt.rtRouteToPoint != null) {
				NetworkDBPoint nextPnt = itPnt.rtRouteToPoint;
				NetworkDBSegment segment = nextPnt.getSegment(itPnt, true);
				networkDB.loadGeometry(segment, false);
				HHNetworkSegmentRes res = new HHNetworkSegmentRes(segment);
				route.segments.add(res);
				if (CALCULATE_GEOMETRY && segment.geometry.size() <= 2) {
					runDetailedRouting(res);
				}
				itPnt = nextPnt;
			}
			if (itPnt.rtDetailedRoute != null) {
				HHNetworkSegmentRes res = new HHNetworkSegmentRes(null);
				res.list = new ArrayList<>();
				RouteSegment p = itPnt.rtDetailedRoute;
				while (p != null && p.getRoad() != null) {
					res.list.add(p);
					p = p.parentRoute;
				}
				route.segments.add(res);
				Collections.reverse(res.list);
			}
			Collections.reverse(route.segments);
		}
		
		double sumDist = 0;
		RouteSegment last = null;
		
		// TODO holes
		for (HHNetworkSegmentRes res : route.segments) {
			NetworkDBSegment s = res.segment;
			if (res.list != null) {
				for (RouteSegment r : res.list) {
					if (last == null) {
						last = r;
					} else {
						if (last.getRoad().getId() == r.getRoad().getId()) {
							if(last.getSegmentEnd() == r.getSegmentStart()) {
								last = new RouteSegment(last.getRoad(), last.getSegmentStart(), r.getSegmentEnd());
							} else if(last.getSegmentEnd() == r.getSegmentEnd()) {
								last = new RouteSegment(last.getRoad(), last.getSegmentStart(), r.getSegmentStart());
							} else {
								int maxLast = Math.max(last.getSegmentEnd(), last.getSegmentStart());
								int maxR = Math.max(r.getSegmentEnd(), r.getSegmentStart());
								int minLast = Math.min(last.getSegmentEnd(), last.getSegmentStart());
								int minR = Math.min(r.getSegmentEnd(), r.getSegmentStart());
								boolean pos = maxLast < maxR;
								if (pos && minR - maxLast <= 1) {
									last = new RouteSegment(last.getRoad(), minLast, maxR );
								} else if (!pos && minLast - maxR <= 1) {
									last = new RouteSegment(last.getRoad(), maxLast, minR);
								} else {
									if (last.getRoad().getPoint31XTile(last.getSegmentEnd()) == r.getRoad().getPoint31XTile(r.getSegmentStart())
											&& last.getRoad().getPoint31YTile(last.getSegmentEnd()) == r.getRoad().getPoint31YTile(r.getSegmentStart())) {
										route.detailed.add(last);
										last = r;
									} else {
										throw new IllegalStateException(
												String.format("Problematic merge %s -> %s", last, r));
									}
								}
							}
						} else {
							route.detailed.add(last);
							last = r;
						}
					}
				}
			}
			if (s == null) {
				System.out.printf("First / last segment \n");
				continue;
			}
			sumDist += s.dist;
			System.out.printf("Route %d [%d] -> %d [%d] %s ( %.5f/%.5f - %d - %.2f s) \n", 
					s.start.index, s.start.chInd, s.end.index,s.end.chInd, s.shortcut ? "sh" : "bs",
					MapUtils.get31LatitudeY(s.end.startY), MapUtils.get31LongitudeX(s.end.startX), s.end.roadId / 64,
					sumDist);
		}
		if (last != null) {
			route.detailed.add(last);
		}
		System.out.println(String.format("Found final route - cost %.2f, %d depth ( visited %,d (%,d unique) of %,d added vertices )", 
				sumDist, route.segments.size(), stats.visitedVertices, stats.uniqueVisitedVertices, stats.addedVertices));
		return route;
	}




}
