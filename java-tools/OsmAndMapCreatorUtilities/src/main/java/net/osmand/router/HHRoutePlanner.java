package net.osmand.router;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBSegment;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.util.MapUtils;

public class HHRoutePlanner {
	static String ROUTING_PROFILE = "car";
	static int DEBUG_VERBOSE_LEVEL = 0;
	
	static LatLon PROCESS_START = null;
	static LatLon PROCESS_END = null;
	
	static boolean PRELOAD_SEGMENTS = true;

	static final int PROC_ROUTING = 0;
	static int PROCESS = PROC_ROUTING;

	
	private RoutingContext ctx;
	private HHRoutingPreparationDB networkDB;
	private TLongObjectHashMap<NetworkDBPoint> cachePoints; 
	
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
			} else if (a.startsWith("--midpoint=")) {
				String[] s = a.substring("--midpoint=".length()).split(":");
				if (c == null) {
					c = new DijkstraConfig();
				}
				c.USE_MIDPOINT = Boolean.parseBoolean(s[0]);
				c.MIDPOINT_ERROR = Integer.parseInt(s[1]);
				c.MIDPOINT_MAX_DEPTH = Integer.parseInt(s[2]);
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
			Collection<Entity> objects = planner.runRouting(PROCESS_START, PROCESS_END, c);
			HHRoutingUtilities.saveOsmFile(objects, new File(folder, name + "-rt.osm"));
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

	public Collection<Entity> runRouting(LatLon start, LatLon end, DijkstraConfig c) throws SQLException, IOException {
		RoutingStats stats = new RoutingStats();
		long time = System.nanoTime(), startTime = System.nanoTime();
		if (c == null) {
			c = new DijkstraConfig();
			// test data for debug swap
//			c = DijkstraConfig.dijkstra(0);
//			c = DijkstraConfig.astar(0);
			c = DijkstraConfig.ch();
			PRELOAD_SEGMENTS = false;
			DEBUG_VERBOSE_LEVEL = 0;
		}
		System.out.println(c.toString(start, end));
		System.out.print("Loading points... ");
		if (cachePoints == null) {
			cachePoints = networkDB.getNetworkPoints(false);
			networkDB.loadMidPointsIndex(cachePoints, cachePoints.valueCollection(), false);
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
		}
		
		NetworkDBPoint startPnt = null;
		NetworkDBPoint endPnt = null;
		for (NetworkDBPoint pnt : cachePoints.valueCollection()) {
			pnt.clearRouting();
			if (startPnt == null) {
				startPnt = endPnt = pnt;
			}
			if (MapUtils.getDistance(start, pnt.getPoint()) < MapUtils.getDistance(start, startPnt.getPoint())) {
				startPnt = pnt;
			}
			if (MapUtils.getDistance(end, pnt.getPoint()) < MapUtils.getDistance(end, endPnt.getPoint())) {
				endPnt = pnt;
			}
		}
		
		time = System.nanoTime();
		System.out.printf("Looking for route %s -> %s \n", startPnt, endPnt);
		System.out.printf("Routing...\n");
		NetworkDBPoint pnt = runDijkstraNetworkRouting(startPnt, endPnt, c, stats);
		stats.routingTime = (System.nanoTime() - time) / 1e6;
		
		time = System.nanoTime();
		Collection<Entity> objects = prepareRoutingResults(networkDB, pnt, new TLongObjectHashMap<>(), stats);
		stats.prepTime = (System.nanoTime() - time) / 1e6;
		System.out.println(c.toString(start, end));
		System.out.printf("Routing finished %.1f ms: load data %.1f ms (%,d edges), routing %.1f ms (%.1f poll ms + %.1f queue ms), prep result %.1f ms\n",
				(System.nanoTime() - startTime) /1e6, stats.loadEdgesTime + stats.loadPointsTime, stats.loadEdgesCnt, stats.routingTime,
				stats.addQueueTime, stats.pollQueueTime, stats.prepTime);
		return objects;
	}

	private double distanceToEnd(NetworkDBPoint s, NetworkDBPoint end) {
		if(end == null || s == null) {
			return 0;
		}
		LatLon p1 = end.getPoint();
		LatLon p2 = s.getPoint();
		return MapUtils.getDistance(p1, p2) / ctx.getRouter().getMaxSpeed();
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
		Queue<NetworkDBPointCost> queue = new PriorityQueue<>(new Comparator<NetworkDBPointCost>() {
			@Override
			public int compare(NetworkDBPointCost o1, NetworkDBPointCost o2) {
				return Double.compare(o1.cost, o2.cost);
			}
		});
		// TODO better PriorityQueue could be used 4ary-heaps ? 
//		queue = new FourAryHeap<>(cmp);
		if (start != null) {
			start.setCostParentRt(false, 0.00001, null, 0);
			start.rtVisited = true;
			c.visited.add(start);
			if (c.DIJKSTRA_DIRECTION >= 0) {
				addToQueue(queue, start, end, false, c, stats);
			} 
		}
		if (end != null) {
			end.setCostParentRt(true, 0.00001, null, 0);
			end.rtVisitedRev = true;
			c.visitedRev.add(end);
			if (c.DIJKSTRA_DIRECTION <= 0) {
				addToQueue(queue, end, start, true, c, stats);
			}
		}
		
		while (!(queue.isEmpty())) {
			long tm = System.nanoTime();
			NetworkDBPointCost pointCost = queue.poll();
			NetworkDBPoint point = pointCost.point;
			boolean rev = pointCost.rev;
			stats.pollQueueTime += (System.nanoTime() - tm) / 1e6;
			stats.visitedVertices++;
			if (point.visited(rev)) {
				continue;
			}
			stats.uniqueVisitedVertices++;
			point.markVisited(rev);
			if (point.visited(!rev)) {
				NetworkDBPoint finalPoint = point;
				if (c.DIJKSTRA_DIRECTION == 0) {
					// TODO check if it's correct for A*
					finalPoint = scanFinalPoint(finalPoint, c.visited);
					finalPoint = scanFinalPoint(finalPoint, c.visitedRev);
				}
				return finalPoint;
			}
			(rev ? c.visitedRev : c.visited).add(point);
			printPoint(point, rev);
			if (c.MAX_COST > 0 && pointCost.cost > c.MAX_COST) {
				break;
			}
			if (c.MAX_SETTLE_POINTS > 0 && (rev ? c.visitedRev : c.visited).size() > c.MAX_SETTLE_POINTS) {
				break;
			}
			addToQueue(queue, point, rev ? start : end, rev, c, stats);
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
	
	private void addToQueue(Queue<NetworkDBPointCost> queue, NetworkDBPoint point, NetworkDBPoint target, boolean reverse, 
			DijkstraConfig c, RoutingStats stats) throws SQLException {
		int depth = c.USE_MIDPOINT || c.MAX_DEPTH > 0? point.getDepth(!reverse) : 0;
		if (c.MAX_DEPTH > 0 && depth >= c.MAX_DEPTH) {
			return;
		}
		long tm = System.nanoTime();
		int cnt = networkDB.loadNetworkSegmentPoint(cachePoints, point, reverse);
		if (cnt > 0) {
			stats.loadEdgesCnt += cnt;
			stats.loadEdgesTime += (System.nanoTime() - tm) / 1e6;
		}
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
			double cost = point.distanceFromStart(reverse) + connected.dist;
			if (c.HEURISTIC_COEFFICIENT > 0) {
				cost += c.HEURISTIC_COEFFICIENT * distanceToEnd(nextPoint, target);
			}
			double exCost = nextPoint.rtCost(reverse);
			if (exCost == 0 || cost < exCost) {
				if (nextPoint.visited(reverse)) {
					throw new IllegalStateException(String.format("%s visited - cost %.2f > prev cost %.2f", nextPoint, cost, exCost));
				}
				nextPoint.setCostParentRt(reverse, cost, point, connected.dist);
				tm = System.nanoTime();
				queue.add(new NetworkDBPointCost(nextPoint, cost, reverse)); // we need to add new object to not  remove / rebalance priority queue
				if (DEBUG_VERBOSE_LEVEL > 2) {
					System.out.printf("Add  %s to visit - cost %.2f > prev cost %.2f \n", nextPoint, cost, exCost);
				}
				stats.addedVertices++;
				stats.addQueueTime += (System.nanoTime() - tm) / 1e6;
			}
		}
	}
		
	private void printPoint(NetworkDBPoint p, boolean rev) {
		if (DEBUG_VERBOSE_LEVEL > 1) {
			String symbol = String.format("%s %d [%d] (from %d [%d])", rev ? "<-" : "->", p.index, p.chInd,
					rev ? p.rtRouteToPointRev.index : p.rtRouteToPoint.index,
					rev ? p.rtRouteToPointRev.chInd : p.rtRouteToPoint.chInd);
			System.out.printf("Visit Point %s (cost %.1f s) %.5f/%.5f - %d\n", symbol, p.rtCost(rev),
					MapUtils.get31LatitudeY(p.startY), MapUtils.get31LongitudeX(p.startX), p.roadId / 64);
		}
	}



	private Collection<Entity> prepareRoutingResults(HHRoutingPreparationDB networkDB, NetworkDBPoint pnt,
			TLongObjectHashMap<Entity> entities, RoutingStats stats) throws SQLException {
		System.out.println("-----");
		LinkedList<NetworkDBSegment> segments = new LinkedList<>();
		if (pnt != null) {
			NetworkDBPoint itPnt = pnt;
			while (itPnt.rtRouteToPointRev != null) {
				NetworkDBPoint nextPnt = itPnt.rtRouteToPointRev;
				NetworkDBSegment segment = nextPnt.getSegment(itPnt, false);
				networkDB.loadGeometry(segment, false);
				segments.add(segment);
				HHRoutingUtilities.addWay(entities, segment, "highway", "secondary");
				itPnt = nextPnt;
			}
			itPnt = pnt;
			while (itPnt.rtRouteToPoint != null) {
				NetworkDBPoint nextPnt = itPnt.rtRouteToPoint;
				NetworkDBSegment segment = nextPnt.getSegment(itPnt, true);
				networkDB.loadGeometry(segment, false);
				segments.addFirst(segment);
				HHRoutingUtilities.addWay(entities, segment, "highway", "secondary");
				itPnt = nextPnt;
			}
		}
		
		double sumDist = 0;
		for (NetworkDBSegment s : segments) {
			sumDist += s.dist;
			System.out.printf("Route %d [%d] -> %d [%d] %s ( %.5f/%.5f - %d - %.2f s) \n", 
					s.start.index, s.start.chInd, s.end.index,s.end.chInd, s.shortcut ? "sh" : "bs",
					MapUtils.get31LatitudeY(s.end.startY), MapUtils.get31LongitudeX(s.end.startX), s.end.roadId / 64,
					sumDist);
		}
		System.out.println(String.format("Found final route - cost %.2f, %d depth ( visited %,d (%,d unique) of %,d added vertices )", 
				sumDist, entities.size(), stats.visitedVertices, stats.uniqueVisitedVertices, stats.addedVertices));
		return entities.valueCollection();
	}




}
