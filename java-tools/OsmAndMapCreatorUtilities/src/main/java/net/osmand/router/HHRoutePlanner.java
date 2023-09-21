package net.osmand.router;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
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
		public float HEURISTIC_COEFFICIENT = 1; // A* - 1, Dijkstra - 0
		public float DIJKSTRA_DIRECTION = 0; // 0 - 2 directions, 1 - positive, -1 - reverse
		public int MAX_DEPTH = -1; // max depth to go to
		public int MAX_POINTS = -1; // max points to settle
		public boolean USE_CH = false;
		public boolean USE_CH_SHORTCUTS = false;
		public boolean USE_MIDPOINT = false;
		public int MIDPOINT_ERROR = 3;
		public int MIDPOINT_MAX_DEPTH = 20 + MIDPOINT_ERROR;
		
		public double MAX_COST;
		public List<NetworkDBPoint> visited = null;
		public List<NetworkDBPoint> visitedRev = null;

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
		int visitedEdges = 0;
		int addedEdges = 0;
		
		
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
			c.HEURISTIC_COEFFICIENT = 1;
			c.DIJKSTRA_DIRECTION = 1;
			c.USE_CH = false;
			c.USE_CH_SHORTCUTS = true;
			c.USE_MIDPOINT = false;
//			PRELOAD_SEGMENTS = false;
			DEBUG_VERBOSE_LEVEL = 0;
			c.MIDPOINT_ERROR = 3;
			c.MIDPOINT_MAX_DEPTH = 20;
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
		System.out.printf("Looking for route %s -> %s \n", startPnt, endPnt);
		time = System.nanoTime();

		// Routing
		System.out.printf("Routing...\n");
		NetworkDBPoint pnt = runDijkstraNetworkRouting(startPnt, endPnt, c, stats);
		stats.routingTime = (System.nanoTime() - time) / 1e6;
		time = System.nanoTime();
		Collection<Entity> objects = prepareRoutingResults(networkDB, pnt, new TLongObjectHashMap<>(), stats);
		stats.prepTime = (System.nanoTime() - time) / 1e6;
		time = System.nanoTime();
		
		System.out.printf("Routing finished %.1f ms: load data %.1f ms (%,d edges), routing %.1f ms (%.1f poll ms + %.1f queue ms), prep result %.1f ms\n",
				(time - startTime) /1e6, stats.loadEdgesTime + stats.loadPointsTime, stats.loadEdgesCnt, stats.routingTime,
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
	
	// delete rtDistanceToEnd as it's not used (carefully)
	// TODO speedup cost could be updated once reverse search covers the road
	// TODO better PriorityQueue could be used 4ary-heaps 
	protected NetworkDBPoint runDijkstraNetworkRouting(NetworkDBPoint start, NetworkDBPoint end, DijkstraConfig c,
			RoutingStats stats) throws SQLException {
		Comparator<NetworkDBSegment> cmp = new Comparator<NetworkDBSegment>() {

			@Override
			public int compare(NetworkDBSegment o1, NetworkDBSegment o2) {
				return Double.compare(o1.rtCost, o2.rtCost);
			}
		};
		Queue<NetworkDBSegment> queue = new PriorityQueue<>(cmp);
//		queue = new FourAryHeap<>(cmp);
		if (start != null) {
			start.rtDistanceFromStart = 0.0001; // visited
			if (c.visited != null) {
				c.visited.add(start);
			}
		}
		if (end != null) {
			end.rtDistanceFromStartRev = 0.0001; // visited
			if (c.visitedRev != null) {
				c.visitedRev.add(end);
			}
		}
		if (c.DIJKSTRA_DIRECTION >= 0) {
			addToQueue(queue, start, end, false, c, stats);
		} 
		if (c.DIJKSTRA_DIRECTION <= 0) {
			addToQueue(queue, end, start, true, c, stats);
		}
		
		while (!queue.isEmpty()) {
			stats.visitedEdges++;
			
			long tm = System.nanoTime();
			NetworkDBSegment segment = queue.poll();
			stats.pollQueueTime += (System.nanoTime() - tm) / 1e6;
			if (c.MAX_COST > 0 && segment.rtCost > c.MAX_COST) {
				break;
			}
			boolean dir = segment.direction;
			if (dir) {
				if (segment.end.rtDistanceFromStart > 0) { // or segment.end.rtRouteToPoint != null
					if (segment.start.rtDistanceFromStart + segment.dist < segment.end.rtDistanceFromStart
							&& c.HEURISTIC_COEFFICIENT > 1) {
						throw new IllegalStateException();
					}
					continue; // already visited
				}
				printPoint(segment, false);
				segment.end.rtRouteToPoint = segment;
				segment.end.rtDistanceFromStart = segment.start.rtDistanceFromStart + segment.dist + (segment.shortcut ? 0 : 1);
//				if (segment.end == end) { // TODO not optimal 
				if (segment.end.rtDistanceFromStartRev > 0) { // wrong
					return segment.end;
				}
				addToQueue(queue, segment.end, end, false, c, stats);
				if (c.visited != null) {
					c.visited.add(segment.end);
				}
				if (c.MAX_POINTS > 0 && c.visited.size() > c.MAX_POINTS) {
					break;
				}
			} else {
				if (segment.start.rtDistanceFromStartRev > 0) { // or segment.end.rtRouteToPoint != null
					if (segment.end.rtDistanceFromStartRev + segment.dist < segment.start.rtDistanceFromStartRev
							&& c.HEURISTIC_COEFFICIENT > 1) {
						throw new IllegalStateException();
					}
					continue; // already visited
				}
				printPoint(segment, true);
				segment.start.rtRouteToPointRev = segment;
				segment.start.rtDistanceFromStartRev = segment.end.rtDistanceFromStartRev + segment.dist;
//				if (segment.start == start) { // not optimal 
				if (segment.start.rtDistanceFromStart > 0) { // wrong
					return segment.start;
				}
				addToQueue(queue, segment.start, start, true, c, stats);
				if (c.visitedRev != null) {
					c.visitedRev.add(segment.start);
				}
				if (c.MAX_POINTS > 0 && c.visitedRev.size() > c.MAX_POINTS) {
					break;
				}
			}
			stats.visitedVertices++;
			
		}
		return null;
		
	}
		
	private void printPoint(NetworkDBSegment segment, boolean reverse) {
		if (DEBUG_VERBOSE_LEVEL > 1) {
			String symbol;
			if (!reverse) {
				symbol = String.format("-> %d [%d] (from %d [%d])", segment.end.index, segment.end.chInd,
						segment.start.index, segment.start.chInd);
			} else {
				symbol = String.format("<- %d [%d] (from %d [%d])", segment.start.index, segment.start.chInd,
						segment.end.index, segment.end.chInd);
			}
			double cost = (reverse ? segment.end.rtDistanceFromStartRev : segment.start.rtDistanceFromStart) + segment.dist;
			NetworkDBPoint pnt = reverse ? segment.start : segment.end;
			System.out.printf("Visit Point %s (%.1f s from start) %.5f/%.5f - %d\n", 
					symbol, cost, MapUtils.get31LatitudeY(pnt.startY), MapUtils.get31LongitudeX(pnt.startX),
					pnt.roadId / 64);
		}
	}



	private Collection<Entity> prepareRoutingResults(HHRoutingPreparationDB networkDB, NetworkDBPoint pnt,
			TLongObjectHashMap<Entity> entities, RoutingStats stats) throws SQLException {
		System.out.println("-----");
		LinkedList<NetworkDBSegment> segments = new LinkedList<>();
		if (pnt != null && pnt.rtRouteToPointRev != null) {
			NetworkDBSegment parent = pnt.rtRouteToPointRev;
			while (parent != null) {
				networkDB.loadGeometry(parent, false);
				segments.add(parent);
				HHRoutingUtilities.addWay(entities, parent, "highway", "secondary");
				parent = parent.end.rtRouteToPointRev;
			}
		}
		if (pnt != null && pnt.rtRouteToPoint != null) {
			NetworkDBSegment parent = pnt.rtRouteToPoint;
			while (parent != null) {
				networkDB.loadGeometry(parent, false);
				segments.addFirst(parent);
				HHRoutingUtilities.addWay(entities, parent, "highway", "secondary");
				parent = parent.start.rtRouteToPoint;
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
		System.out.println(String.format("Found final route - cost %.2f, %d depth ( visited %,d vertices, %,d (of %,d) edges )", 
				sumDist, entities.size(), stats.visitedVertices, stats.visitedEdges, stats.addedEdges));
		return entities.valueCollection();
	}



	private void addToQueue(Queue<NetworkDBSegment> queue, NetworkDBPoint start, NetworkDBPoint finish, boolean reverse, 
			DijkstraConfig c, RoutingStats stats) throws SQLException {
		if (start == null) {
			return;
		}
		int depth = c.USE_MIDPOINT || c.MAX_DEPTH > 0? start.getDepth(!reverse) : 0;
		int cnt;
		if (c.MAX_DEPTH > 0 && depth >= c.MAX_DEPTH) {
			return;
		}
		long tm = System.nanoTime();
		if (reverse) {
			cnt = networkDB.loadNetworkSegmentStart(cachePoints, start);
		} else {
			cnt = networkDB.loadNetworkSegmentEnd(cachePoints, start);
		}
		if (cnt > 0) {
			stats.loadEdgesCnt += cnt;
			stats.loadEdgesTime += (System.nanoTime() - tm) / 1e6;
		}
		
		for (NetworkDBSegment connected : (reverse ? start.connectedReverse : start.connected) ) {
			NetworkDBPoint nextPoint = reverse ? connected.start : connected.end;
			if (!c.USE_CH && !c.USE_CH_SHORTCUTS && connected.shortcut) {
				continue;
			}
			if (nextPoint.rtExclude) {
				continue;
			}
			// modify CH to not compute all top points
			if (c.USE_CH && (nextPoint.chInd > 0 && nextPoint.chInd < start.chInd)) {
				continue;
			}
			if (c.USE_MIDPOINT && Math.min(depth, c.MIDPOINT_MAX_DEPTH) > nextPoint.rtCnt + c.MIDPOINT_ERROR) {
				continue;
			}
			double rtDistanceToEnd = distanceToEnd(nextPoint, finish);
			if (reverse) {
				// TODO here rtDistanceFromStart not needed
				connected.rtCost = start.rtDistanceFromStartRev + connected.dist + 
						c.HEURISTIC_COEFFICIENT * (connected.start.rtDistanceFromStart > 0 ? connected.start.rtDistanceFromStart :  rtDistanceToEnd);
			} else {
				connected.rtCost =  start.rtDistanceFromStart + connected.dist +
						c.HEURISTIC_COEFFICIENT * (connected.end.rtDistanceFromStartRev > 0 ? connected.end.rtDistanceFromStartRev :  rtDistanceToEnd); 
			}
			connected.rtCost += (connected.shortcut ? 0 : 1);
			tm = System.nanoTime();			
			queue.add(connected);
			stats.addedEdges++;
			stats.addQueueTime += (System.nanoTime() - tm) / 1e6;
		}
	}

}
