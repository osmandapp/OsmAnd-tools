package net.osmand.router;

import java.io.File;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
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

public class HHRoutingBasePlanner {
	private static String ROUTING_PROFILE = "car";
	static int DEBUG_VERBOSE_LEVEL = 0;
	
	static LatLon PROCESS_START = null;
	static LatLon PROCESS_END = null;
	static float HEURISTIC_COEFFICIENT = 1; // A* - 1, Dijkstra - 0
	static float DIJKSTRA_DIRECTION = -1; // 0 - 2 directions, 1 - positive, -1 - reverse 


	private RoutingContext ctx;
	private HHRoutingBasePlanner(RoutingContext ctx) {
		this.ctx = ctx;
	}
	
	static class RoutingStats {
		int visitedVertices = 0;
		int checkedVertices = 0;
		int addedEdges = 0;
	}
	
	private static File sourceFile() {
		String name = "Montenegro_europe_2.road.obf";
		name = "Netherlands_europe_2.road.obf";
		name = "Ukraine_europe_2.road.obf";
//		name = "Germany";
		return new File(System.getProperty("maps.dir"), name);
	}
	
	private static RoutingContext prepareContext() {
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.getDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3,
				RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		Map<String, String> map = new TreeMap<String, String>();
		RoutingConfiguration config = builder.build(ROUTING_PROFILE, memoryLimit, map);
		config.planRoadDirection = 1;
		config.heuristicCoefficient = 0; // dijkstra
		return router.buildRoutingContext(config, null, new BinaryMapIndexReader[0], RouteCalculationMode.NORMAL);
	}

	private static void sourceLatLons() {
		// "Netherlands_europe_2.road.obf"
		PROCESS_START = new LatLon(52.34800, 4.86206); // AMS
//		PROCESS_END = new LatLon(51.57803, 4.79922); // Breda
		PROCESS_END = new LatLon(51.35076, 5.45141); // ~Eindhoven
		
		// "Montenegro_europe_2.road.obf"
//		PROCESS_START = new LatLon(43.15274, 19.55169); 
//		PROCESS_END= new LatLon(42.45166, 18.54425);
		
		// Ukraine
		PROCESS_START = new LatLon(50.43539, 30.48234); // Kyiv
		PROCESS_START = new LatLon(50.01689, 36.23278); // Kharkiv
		PROCESS_END = new LatLon(46.45597, 30.75604);   // Odessa
		PROCESS_END = new LatLon(48.43824, 22.705723); // Mukachevo
	}
	
	public static void main(String[] args) throws Exception {
		sourceLatLons();
		File obfFile = args.length == 0 ? sourceFile() : new File(args[0]);
		for (String a : args) {
			if (a.startsWith("--start=")) {
				String[] latLons = a.substring("--start=".length()).split(",");
				PROCESS_START = new LatLon(Double.parseDouble(latLons[0]), Double.parseDouble(latLons[1]));
			} else if (a.startsWith("--end=")) {
				String[] latLons = a.substring("--end=".length()).split(",");
				PROCESS_START = new LatLon(Double.parseDouble(latLons[0]), Double.parseDouble(latLons[1]));
			}
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getName();
		HHRoutingPreparationDB networkDB = new HHRoutingPreparationDB(new File(folder, name + ".db"),
				HHRoutingPreparationDB.READ);
		HHRoutingBasePlanner planner = new HHRoutingBasePlanner(prepareContext());
		long startTime = System.nanoTime();
		System.out.print("Loading points... ");
		TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(false);
		NetworkDBPoint startPnt = null;
		NetworkDBPoint endPnt = null;
		for (NetworkDBPoint pnt : pnts.valueCollection()) {
			if (startPnt == null) {
				startPnt = endPnt = pnt;
			}
			if (MapUtils.getDistance(PROCESS_START, pnt.getPoint()) < MapUtils.getDistance(PROCESS_START,
					startPnt.getPoint())) {
				startPnt = pnt;
			}
			if (MapUtils.getDistance(PROCESS_END, pnt.getPoint()) < MapUtils.getDistance(PROCESS_END,
					endPnt.getPoint())) {
				endPnt = pnt;
			}
		}
		long loadTime = System.nanoTime();
		System.out.printf(" %.2fms\nLoading segments...", (loadTime - startTime) / 1e6);
		networkDB.loadNetworkSegments(pnts);
		loadTime = System.nanoTime();

		// Routing
		System.out.printf(" %.2fms\nRouting...\n", (loadTime - startTime) / 1e6);
		RoutingStats stats = new RoutingStats();
		NetworkDBPoint pnt = planner.runDijkstraNetworkRouting(networkDB, pnts, startPnt, endPnt, stats);
		long routingTime = System.nanoTime();
		Collection<Entity> objects = planner.prepareRoutingResults(networkDB, pnt, new TLongObjectHashMap<>(), stats);
		long prepTime = System.nanoTime();
		HHRoutingUtilities.saveOsmFile(objects, new File(folder, name + "-rt.osm"));
		System.out.printf("Routing finished %.2f ms: load data %.2f ms, route %.2f ms, prep result %.2f ms\n",
				(prepTime - startTime) / 1e6, (loadTime - startTime) / 1e6, (routingTime - loadTime) / 1e6,
				(prepTime - routingTime) / 1e6);

		networkDB.close();
	}



	private double distanceToEnd(NetworkDBPoint s, NetworkDBPoint end) {
		LatLon p1 = end.getPoint();
		LatLon p2 = s.getPoint();
		return MapUtils.getDistance(p1, p2) / ctx.getRouter().getMaxSpeed();
	}
	
	private double rtSegmentCost(NetworkDBSegment o1) {
		return o1.start.rtDistanceFromStart + o1.dist +
				HEURISTIC_COEFFICIENT * (o1.end.rtDistanceFromStartRev > 0 ? o1.end.rtDistanceFromStartRev :  o1.rtDistanceToEnd);
	}
	
	private double rtSegmentCostRev(NetworkDBSegment o1) {
		return o1.end.rtDistanceFromStartRev + o1.dist + 
				HEURISTIC_COEFFICIENT * (o1.start.rtDistanceFromStart > 0 ? o1.start.rtDistanceFromStart :  o1.rtDistanceToEnd);
	}
	
	private NetworkDBPoint runDijkstraNetworkRouting(HHRoutingPreparationDB networkDB, TLongObjectHashMap<NetworkDBPoint> pnts, NetworkDBPoint start,
			NetworkDBPoint end, RoutingStats stats) throws SQLException {
		PriorityQueue<NetworkDBSegment> queue = new PriorityQueue<>(new Comparator<NetworkDBSegment>() {

			@Override
			public int compare(NetworkDBSegment o1, NetworkDBSegment o2) {
				return Double.compare(rtSegmentCost(o1),rtSegmentCost(o2));
			}
		});
		PriorityQueue<NetworkDBSegment> queueReverse = new PriorityQueue<>(new Comparator<NetworkDBSegment>() {

			@Override
			public int compare(NetworkDBSegment o1, NetworkDBSegment o2) {
				return Double.compare(rtSegmentCostRev(o1), rtSegmentCostRev(o2));
			}
		});
		start.rtDistanceFromStart = 0.1; // visited
		end.rtDistanceFromStartRev = 0.1; // visited
		if (DIJKSTRA_DIRECTION >= 0) {
			addToQueue(queue, start, end, false, stats);
		} 
		if (DIJKSTRA_DIRECTION <= 0) {
			addToQueue(queueReverse, end, start, true, stats);
		}

		while (!queue.isEmpty() || !queueReverse.isEmpty()) {
			stats.checkedVertices++;
			boolean dir = !queue.isEmpty();
			if(!queue.isEmpty() && !queueReverse.isEmpty()) {
				dir = rtSegmentCost(queue.peek()) <= rtSegmentCostRev(queueReverse.peek());  
			}
			NetworkDBSegment segment = dir ? queue.poll() : queueReverse.poll();
			if (dir) {
				if (segment.end.rtDistanceFromStart > 0) { // segment.end.rtRouteToPoint != null
					continue;
				}
				printPoint(segment, false);
				segment.end.rtRouteToPoint = segment;
				segment.end.rtDistanceFromStart = segment.start.rtDistanceFromStart + segment.dist;
				if (segment.end.rtDistanceFromStartRev > 0 && segment.end.rtDistanceFromStart > 0) { 
					return segment.end;
				}
				addToQueue(queue, segment.end, end, false, stats);
			} else {
				if (segment.start.rtDistanceFromStartRev > 0) { // segment.end.rtRouteToPoint != null
					continue;
				}
				printPoint(segment, true);
				segment.start.rtRouteToPointRev = segment;
				segment.start.rtDistanceFromStartRev = segment.end.rtDistanceFromStartRev + segment.dist;
				if (segment.start.rtDistanceFromStartRev > 0 && segment.start.rtDistanceFromStart > 0) { 
					return segment.start;
				}
				addToQueue(queueReverse, segment.start, start, true, stats);
			}
			
			stats.visitedVertices++;
		}
		return null;
		
	}



	private void printPoint(NetworkDBSegment segment, boolean reverse) {
		if (DEBUG_VERBOSE_LEVEL > 1) {
			String symbol;
			if (!reverse) {
				symbol = "-> " + segment.end.index + " (from " + segment.start.index + ")";
			} else {
				symbol = "<- " + segment.start.index + " (from " + segment.end.index + ")";
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
				networkDB.loadGeometry(parent);
				segments.add(parent);
				HHRoutingUtilities.addWay(entities, parent, "highway", "secondary");
				parent = parent.end.rtRouteToPointRev;
			}
		}
		if (pnt != null && pnt.rtRouteToPoint != null) {
			NetworkDBSegment parent = pnt.rtRouteToPoint;
			while (parent != null) {
				networkDB.loadGeometry(parent);
				segments.addFirst(parent);
				HHRoutingUtilities.addWay(entities, parent, "highway", "secondary");
				parent = parent.start.rtRouteToPoint;
			}
		}
		
		double sumDist = 0;
		for (NetworkDBSegment s : segments) {
			sumDist += s.dist;
			System.out.printf("Route %d -> %d ( %.5f/%.5f - %d - %.2f s) \n", s.start.index, s.end.index,
					MapUtils.get31LatitudeY(s.end.startY), MapUtils.get31LongitudeX(s.end.startX), s.end.roadId / 64,
					sumDist);
		}
		System.out.println(String.format("Found final route - cost %.2f, %d depth ( visited %d (%d) vertices, %d edges )", 
				sumDist, entities.size(), stats.visitedVertices, stats.checkedVertices, stats.addedEdges));
		return entities.valueCollection();
	}




	private void addToQueue(PriorityQueue<NetworkDBSegment> queue, NetworkDBPoint start, NetworkDBPoint finish, boolean reverse, RoutingStats stats) {
		for (NetworkDBSegment connected : (reverse ? start.connectedReverse : start.connected) ) {
			connected.rtDistanceToEnd = distanceToEnd(reverse ? connected.start : connected.end, finish);
			queue.add(connected);
			stats.addedEdges++;
		}
	}

}
