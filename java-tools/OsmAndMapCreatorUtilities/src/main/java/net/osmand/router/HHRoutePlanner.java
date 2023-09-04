package net.osmand.router;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
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
	private static String ROUTING_PROFILE = "car";
	static int DEBUG_VERBOSE_LEVEL = 0;
	
	static LatLon PROCESS_START = null;
	static LatLon PROCESS_END = null;
	static float HEURISTIC_COEFFICIENT = 0; // A* - 1, Dijkstra - 0
	static float DIJKSTRA_DIRECTION = 1; // 0 - 2 directions, 1 - positive, -1 - reverse 


	static int MONTE_CARLO_BATCH = 300;
	
	private RoutingContext ctx;
	private HHRoutingPreparationDB networkDB;
	
	private HHRoutePlanner(RoutingContext ctx, HHRoutingPreparationDB networkDB) {
		this.ctx = ctx;
		this.networkDB = networkDB;
	}
	
	static class RoutingStats {
		int visitedVertices = 0;
		int visitedEdges = 0;
		int addedEdges = 0;
		
		double loadPointsTime = 0;
		double loadEdgesTime = 0;
		double routingTime = 0;
		double addQueueTime = 0;
		double prepTime = 0;
	}
	
	private static File sourceFile() {
		String name = "Montenegro_europe_2.road.obf";
		name = "Netherlands_europe";
//		name = "Ukraine_europe";
//		name = "Germany";
		return new File(System.getProperty("maps.dir"), name);
	}
	
	private static void testLatLons() {
		// "Netherlands_europe_2.road.obf"
//		PROCESS_START = new LatLon(52.34800, 4.86206); // AMS
////		PROCESS_END = new LatLon(51.57803, 4.79922); // Breda
//		PROCESS_END = new LatLon(51.35076, 5.45141); // ~Eindhoven
		
		// "Montenegro_europe_2.road.obf"
//		PROCESS_START = new LatLon(43.15274, 19.55169); 
//		PROCESS_END= new LatLon(42.45166, 18.54425);
		
		// Ukraine
		PROCESS_START = new LatLon(50.43539, 30.48234); // Kyiv
		PROCESS_START = new LatLon(50.01689, 36.23278); // Kharkiv
		PROCESS_END = new LatLon(46.45597, 30.75604);   // Odessa
		PROCESS_END = new LatLon(48.43824, 22.705723); // Mukachevo
//		
		
//		PROCESS_START = new LatLon(50.30487, 31.29761); // 
//		PROCESS_END = new LatLon(50.30573, 28.51402); //
		
		// Germany
//		PROCESS_START = new LatLon(53.06264, 8.79675); // Bremen 
//		PROCESS_END = new LatLon(48.08556, 11.50811); // Munich
	}
	
	public static void main(String[] args) throws Exception {
		testLatLons();
		File obfFile = args.length == 0 ? sourceFile() : new File(args[0]);
		for (String a : args) {
			if (a.startsWith("--start=")) {
				String[] latLons = a.substring("--start=".length()).split(",");
				PROCESS_START = new LatLon(Double.parseDouble(latLons[0]), Double.parseDouble(latLons[1]));
			} else if (a.startsWith("--end=")) {
				String[] latLons = a.substring("--end=".length()).split(",");
				PROCESS_END = new LatLon(Double.parseDouble(latLons[0]), Double.parseDouble(latLons[1]));
			}
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getName();
		HHRoutePlanner planner = new HHRoutePlanner(prepareContext(), 
				new HHRoutingPreparationDB(new File(folder, name + HHRoutingPreparationDB.EXT), HHRoutingPreparationDB.READ));
		if (false) {
			Collection<Entity> objects = planner.runRouting(PROCESS_START, PROCESS_END);
			HHRoutingUtilities.saveOsmFile(objects, new File(folder, name + "-rt.osm"));
		} else {
			planner.runMonteCarloRouting();
		}
		planner.networkDB.close();
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

	private Collection<Entity> runRouting(LatLon start, LatLon end) throws SQLException, IOException {
		RoutingStats stats = new RoutingStats();
		long time = System.nanoTime(), startTime = System.nanoTime();
		System.out.print("Loading points... ");
		TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(false);
		NetworkDBPoint startPnt = null;
		NetworkDBPoint endPnt = null;
		for (NetworkDBPoint pnt : pnts.valueCollection()) {
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
		stats.loadPointsTime = (System.nanoTime() - time) / 1e6;
		time = System.nanoTime();
		System.out.printf(" %,d - %.2fms\nLoading segments...", pnts.size(), stats.loadPointsTime);
		int cntEdges = networkDB.loadNetworkSegments(pnts);
		stats.loadEdgesTime = (System.nanoTime() - time) / 1e6;
		time = System.nanoTime();

		// Routing
		System.out.printf(" %,d - %.2fms\nRouting...\n", cntEdges, stats.loadEdgesTime);
		NetworkDBPoint pnt = runDijkstraNetworkRouting(networkDB, pnts, startPnt, endPnt, stats);
		stats.routingTime = (System.nanoTime() - time) / 1e6;
		time = System.nanoTime();
		Collection<Entity> objects = prepareRoutingResults(networkDB, pnt, new TLongObjectHashMap<>(), stats);
		stats.prepTime = (System.nanoTime() - time) / 1e6;
		time = System.nanoTime();
		
		System.out.printf("Routing finished %.2f ms: load data %.2f ms, routing %.2f ms (%.2f queue ms), prep result %.2f ms\n",
				(time - startTime) /1e6, stats.loadEdgesTime + stats.loadPointsTime, stats.routingTime,
				stats.addQueueTime, stats.prepTime);
		return objects;
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
//				return Double.compare(rtSegmentCost(o1),rtSegmentCost(o2));
				return Double.compare(o1.rtCost, o2.rtCost);
			}
		});
		PriorityQueue<NetworkDBSegment> queueReverse = new PriorityQueue<>(new Comparator<NetworkDBSegment>() {

			@Override
			public int compare(NetworkDBSegment o1, NetworkDBSegment o2) {
//				return Double.compare(rtSegmentCostRev(o1), rtSegmentCostRev(o2));
				return Double.compare(o1.rtCost, o2.rtCost);
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
			stats.visitedEdges++;
			boolean dir = !queue.isEmpty();
			if (!queue.isEmpty() && !queueReverse.isEmpty()) {
				dir = rtSegmentCost(queue.peek()) <= rtSegmentCostRev(queueReverse.peek());
			}
			long tm = System.nanoTime();
			NetworkDBSegment segment = dir ? queue.poll() : queueReverse.poll();
			stats.addQueueTime += (System.nanoTime() - tm) / 1e6;
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

	private void runMonteCarloRouting() throws SQLException {
		RoutingStats stats = new RoutingStats();
		long time = System.nanoTime(), startTime = System.nanoTime();
		System.out.print("Loading points... ");
		TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(false);
		
		stats.loadPointsTime = (System.nanoTime() - time) / 1e6;
		time = System.nanoTime();
		System.out.printf(" %,d - %.2fms\nLoading segments...", pnts.size(), stats.loadPointsTime);
		int cntEdges = networkDB.loadNetworkSegments(pnts);
		stats.loadEdgesTime = (System.nanoTime() - time) / 1e6;

		// Routing
		System.out.printf(" %,d - %.2fms\nRouting...\n", cntEdges, stats.loadEdgesTime);
		List<NetworkDBPoint> pntsList = new ArrayList<>(pnts.valueCollection());
		Random rm = new Random();
		for (int i = 0; i < MONTE_CARLO_BATCH; i++) {
			for(NetworkDBPoint p : pntsList) {
				p.clearRouting();
			}
			if ((i + 1) % 10 == 0) {
				System.out.printf("Routing %d ...\n", i + 1);
			}
			time = System.nanoTime();
			NetworkDBPoint startPnt = pntsList.get(rm.nextInt(pntsList.size() - 1));
			NetworkDBPoint endPnt = pntsList.get(rm.nextInt(pntsList.size() - 1));
			NetworkDBPoint pnt = runDijkstraNetworkRouting(networkDB, pnts, startPnt, endPnt, stats);
			stats.routingTime += (System.nanoTime() - time) / 1e6;
			if (pnt == null) {
				continue;
			}
			
			time = System.nanoTime();
			pnt.rtCnt++;
			if (pnt != null && pnt.rtRouteToPointRev != null) {
				NetworkDBSegment parent = pnt.rtRouteToPointRev;
				while (parent != null) {
					parent.end.rtCnt++;
					parent = parent.end.rtRouteToPointRev;
				}
			}
			if (pnt != null && pnt.rtRouteToPoint != null) {
				NetworkDBSegment parent = pnt.rtRouteToPoint;
				while (parent != null) {
					parent.start.rtCnt++;
					parent = parent.start.rtRouteToPoint;
				}
			}
			stats.prepTime += (System.nanoTime() - time) / 1e6;
		}
		
		Collections.sort(pntsList, new Comparator<NetworkDBPoint>() {

			@Override
			public int compare(NetworkDBPoint o1, NetworkDBPoint o2) {
				return -Integer.compare(o1.rtCnt,o2.rtCnt);
			}
		});
		for (int k = 0; k < 100 && k < pntsList.size(); k++) {
			NetworkDBPoint pnt = pntsList.get(k);
			if (pnt.rtCnt == 0) {
				break;
			}
			System.out.printf("%d %.4f, %.4f - %s\n", pnt.rtCnt, pnt.getPoint().getLatitude(),
					pnt.getPoint().getLongitude(), pnt);
		}
		time = System.nanoTime();
		System.out.printf("Routing finished %.2f ms: load data %.2f ms, routing %.2f ms (%.2f queue ms), prep result %.2f ms\n",
				(time - startTime) /1e6, stats.loadEdgesTime + stats.loadPointsTime, stats.routingTime,
				stats.addQueueTime, stats.prepTime);
		
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
		System.out.println(String.format("Found final route - cost %.2f, %d depth ( visited %,d vertices, %,d (of %,d) edges )", 
				sumDist, entities.size(), stats.visitedVertices, stats.visitedEdges, stats.addedEdges));
		return entities.valueCollection();
	}



	private void addToQueue(PriorityQueue<NetworkDBSegment> queue, NetworkDBPoint start, NetworkDBPoint finish, boolean reverse, RoutingStats stats) {
		long tm = System.nanoTime();
		for (NetworkDBSegment connected : (reverse ? start.connectedReverse : start.connected) ) {
			connected.rtDistanceToEnd = distanceToEnd(reverse ? connected.start : connected.end, finish);
			connected.rtCost = reverse ? rtSegmentCostRev(connected) : rtSegmentCost(connected); 
			queue.add(connected);
			stats.addedEdges++;
		}
		stats.addQueueTime += (System.nanoTime() - tm) / 1e6;
	}

}
