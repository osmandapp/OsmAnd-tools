package net.osmand.router;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.osm.edit.Entity;
import net.osmand.router.HHRoutePlanner.RoutingStats;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBSegment;

public class HHRoutingTopGraphCreator {
	static int DEBUG_VERBOSE_LEVEL = 0;

	static final int PROC_MONTECARLO = 1;
	static final int PROC_MIDPOINTS = 2;
	static final int PROC_2ND_LEVEL = 3;

	static int MAX_ITERATIONS = 100;
	static int MAX_DEPTH = 40;
	static int SAVE_ITERATIONS = 20;
	static int LOG_STAT_THRESHOLD = 10;
	static int LOG_STAT_MAX_DEPTH = 30;
	static int PROCESS = PROC_MIDPOINTS;

	static long DEBUG_START_TIME = 0;
	
	private HHRoutingPreparationDB networkDB;
	private HHRoutePlanner routePlanner;
	
	public HHRoutingTopGraphCreator(HHRoutePlanner routePlanner, HHRoutingPreparationDB networkDB) {
		this.routePlanner = routePlanner;
		this.networkDB = networkDB;
	}
	
	
	private static File testData() {
		String name = "Montenegro_europe_2.road.obf";
		name = "Netherlands_europe";
		name = "Ukraine_europe";
//		name = "Germany";
		
		return new File(System.getProperty("maps.dir"), name);
	}
	
	private static void logf(String string, Object... a) {
		if (DEBUG_START_TIME == 0) {
			DEBUG_START_TIME = System.currentTimeMillis();
		}
		String ms = String.format("%3.1fs ", (System.currentTimeMillis() - DEBUG_START_TIME) / 1000.f);
		System.out.printf(ms + string, a);

	}
	
	public static void main(String[] args) throws Exception {
		File obfFile = args.length == 0 ? testData() : new File(args[0]);
		for (String a : args) {
			if (a.equals("--setup-midpoints")) {
				PROCESS = PROC_MIDPOINTS;
			} else if (a.equals("--proc-montecarlo")) {
				PROCESS = PROC_MONTECARLO;
			} else if (a.equals("--proc-2nd-level")) {
				PROCESS = PROC_2ND_LEVEL;
			} else if (a.startsWith("--iterations=")) {
				MAX_ITERATIONS = Integer.parseInt(a.substring("--iterations=".length()));
			} else if (a.startsWith("--maxdepth=")) {
				MAX_DEPTH = Integer.parseInt(a.substring("--maxdepth=".length()));
			}
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getCanonicalFile().getName();
		
		HHRoutingPreparationDB networkDB = 
				new HHRoutingPreparationDB(new File(folder, name + HHRoutingPreparationDB.EXT));
		HHRoutePlanner routePlanner = new HHRoutePlanner(HHRoutePlanner.prepareContext(
				HHRoutePlanner.ROUTING_PROFILE), networkDB);
		HHRoutingTopGraphCreator planner = new HHRoutingTopGraphCreator(routePlanner, networkDB);
		if (PROCESS == PROC_MIDPOINTS) {
			planner.calculateMidPoints();
//			planner.run2ndLevelRouting();
		} else if (PROCESS == PROC_MONTECARLO) {
			Collection<Entity> objects = planner.runMonteCarloRouting();
			HHRoutingUtilities.saveOsmFile(objects, new File(folder, name + "-mc.osm"));
		}
		planner.networkDB.close();
	}


	private class NetworkHHCluster {
		private int clusterId;
		private List<NetworkDBPoint> points = new ArrayList<>();
		private List<NetworkDBPoint> expoints = new ArrayList<>();
		private Set<NetworkHHCluster> neighbors = new HashSet<>();
		
		private TLongHashSet routeMidPoints = null;
		// 
		private NetworkHHCluster mergedTo = null;
		private List<NetworkHHCluster> merged = new ArrayList<>();
		
		public NetworkHHCluster(int clusterId) {
			this.clusterId = clusterId;
		}
		@Override
		public String toString() {
			return String.format("C-%d [%d, %d]", clusterId, points.size(), neighbors.size());
		}
		
		public void clearRouting() {
			for (NetworkDBPoint p : points) {
				p.clearRouting();
			}
			for (NetworkDBPoint p : expoints) {
				p.clearRouting();
			}
		}
		
		public void adoptMerge(NetworkHHCluster c) {
			if (c == this && neighbors.size() != 0 ) {
				throw new IllegalStateException();
			} else if (mergedTo != null) {
				throw new IllegalStateException();
			} else if (c.mergedTo != null) {
				throw new IllegalStateException();
			} else {
				c.mergedTo = this;
				if (c != this) {
					for (NetworkHHCluster p : c.merged) {
						p.mergedTo = this;
						merged.add(p);
					}
					c.merged.clear();
					merged.add(c);
				}
			}
		}
		
		public NetworkHHCluster getMergeCluster() {
			return mergedTo == null || mergedTo == this ? this : mergedTo.getMergeCluster() ;
		}
		
		public int neighborsSize() {
			return neighbors.size();
		}
	}
	
	private void calculateMidPoints() throws SQLException {
		RoutingStats stats = new RoutingStats();
		long time = System.nanoTime(), startTime = System.nanoTime();
		System.out.print("Loading points... ");
		TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(false);
		List<NetworkDBPoint> pointsList = new ArrayList<>(pnts.valueCollection());
		networkDB.loadMidPointsIndex(pnts, pointsList, false);
		stats.loadPointsTime = (System.nanoTime() - time) / 1e6;
		
//		Map<Integer, NetworkHHCluster> clusters = restoreClusters(pnts);
		
		time = System.nanoTime();
		System.out.printf(" %,d - %.2fms\nLoading segments...", pnts.size(), stats.loadPointsTime);
		int cntEdges = networkDB.loadNetworkSegments(pointsList);
		stats.loadEdgesTime = (System.nanoTime() - time) / 1e6;

		// Routing
		System.out.printf(" %,d - %.2fms\nRouting...\n", cntEdges, stats.loadEdgesTime);
		
		int iteration = Math.min(MAX_ITERATIONS, pnts.size() / 2);
		Random random = new Random();
		
		while (iteration > 0) {
			NetworkDBPoint startPnt = pointsList.get(random.nextInt(pointsList.size()));
			if (startPnt.rtIndex > 0) {
				continue;
			}
			logf("%d. Routing %s - ", iteration, startPnt);
			iteration--;
			startPnt.rtIndex = 1;
			HHRoutePlanner.HEURISTIC_COEFFICIENT = 0;
			HHRoutePlanner.USE_MIDPOINT = false;
			HHRoutePlanner.MAX_DEPTH = MAX_DEPTH;
			stats = new RoutingStats();
			routePlanner.runDijkstraNetworkRouting(startPnt, null, stats);
			for (NetworkDBPoint pnt : pointsList) {
				pnt.rtLevel = 0;
				if (pnt.rtRouteToPoint != null) {
					pnt.rtLevel++;
					NetworkDBPoint p = pnt;
					while ((p = p.rtRouteToPoint.start) != startPnt) {
						pnt.rtLevel++;
					}
				}
			}
			for (NetworkDBPoint pnt : pointsList) {
				if (pnt.rtRouteToPoint != null) {
					int k = 0;
					NetworkDBPoint p = pnt;
					while ((p = p.rtRouteToPoint.start) != startPnt) {
						k++;
						p.rtCnt = Math.max(p.rtCnt, Math.min(k, p.rtLevel));
					}
				}
			}
			int maxInc = 0, countInc = 0, maxTop = 0;
			for (NetworkDBPoint pnt : pointsList) {
				// calculate max increase
				if (pnt.rtCnt - pnt.rtPrevCnt > 0 && pnt.rtPrevCnt < LOG_STAT_THRESHOLD) {
					countInc++;
					maxInc = Math.max(pnt.rtCnt - pnt.rtPrevCnt, maxInc);
					maxTop = Math.max(pnt.rtCnt, maxTop);
				}
				pnt.rtPrevCnt = pnt.rtCnt;
				pnt.clearRouting();
			}
			System.out.printf("increased %d points - max diff %d, max top %d (%d vertices %d / %dedges, %.2f queue ms) \n", 
					countInc, maxInc, maxTop, stats.visitedVertices, stats.visitedEdges, stats.addedEdges, stats.addQueueTime);
			if (iteration % SAVE_ITERATIONS == 0) {
				saveAndPrintPoints(pointsList, pnts, LOG_STAT_MAX_DEPTH);
			}
		}
		saveAndPrintPoints(pointsList, pnts, LOG_STAT_MAX_DEPTH);
		
		
		time = System.nanoTime();
		System.out.printf("Routing finished %.2f ms: load data %.2f ms, routing %.2f ms (%.2f queue ms), prep result %.2f ms\n",
				(time - startTime) /1e6, stats.loadEdgesTime + stats.loadPointsTime, stats.routingTime,
				stats.addQueueTime, stats.prepTime);
		System.out.println(String.format("Found final route - cost %.2f, %d depth ( visited %,d vertices, %,d (of %,d) edges )", 
				0.0, 0, stats.visitedVertices, stats.visitedEdges, stats.addedEdges));
	}


	private void saveAndPrintPoints(List<NetworkDBPoint> pointsList, TLongObjectHashMap<NetworkDBPoint> pnts, int max) throws SQLException {
		long now = System.currentTimeMillis();
		
		Collections.sort(pointsList, new Comparator<NetworkDBPoint>() {

			@Override
			public int compare(NetworkDBPoint o1, NetworkDBPoint o2) {
				return -Integer.compare(o1.rtCnt, o2.rtCnt);
			}
		});
		int prev = 0;
		for (int k = 0; k < pointsList.size(); k++) {
			NetworkDBPoint p = pointsList.get(k);
			if (k > 0 && Math.min(pointsList.get(k - 1).rtCnt, max) == Math.min(p.rtCnt, max)) {
				prev++;
				continue;
			}
			System.out.printf("\n (^%d, %.1f%%) %d depth: %s", prev, prev * 100.0 / pointsList.size(), p.rtCnt, p.toString());
			//prev = 0;
		}
		System.out.printf("\n (^%d, %.1f%%) - saving %.2f s \n", prev, prev * 100.0 / pointsList.size(), (System.currentTimeMillis() - now) / 1000.0);
		networkDB.loadMidPointsIndex(pnts, pointsList, true);
		for (NetworkDBPoint pnt : pointsList) {
			pnt.rtPrevCnt = pnt.rtCnt;
		}
	}


	

	private void run2ndLevelRouting() throws SQLException {
		RoutingStats stats = new RoutingStats();
		long time = System.nanoTime(), startTime = System.nanoTime();
		System.out.print("Loading points... ");
		TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(false);
		stats.loadPointsTime = (System.nanoTime() - time) / 1e6;
		
		Map<Integer, NetworkHHCluster> clusters = restoreClusters(pnts);
		
		time = System.nanoTime();
		System.out.printf(" %,d - %.2fms\nLoading segments...", pnts.size(), stats.loadPointsTime);
		int cntEdges = networkDB.loadNetworkSegments(pnts.valueCollection());
		stats.loadEdgesTime = (System.nanoTime() - time) / 1e6;

		// Routing
		System.out.printf(" %,d - %.2fms\nRouting...\n", cntEdges, stats.loadEdgesTime);
		List<NetworkDBPoint> pntsList = new ArrayList<>(pnts.valueCollection());
		
		
		TLongObjectHashMap<NetworkDBPoint> toExclude = new TLongObjectHashMap<>();
		boolean random = false;
		if (random) {
			for (int i = 0; i < pntsList.size(); i++) {
				NetworkDBPoint pnt = pntsList.get(i);
				if (i % 100 == 0) {
					continue;
				}
				toExclude.put(pnt.index, pnt);
			}
		} else {
			int origPnts = printStats(clusters);
			for (int k = 0; k < 4; k++) {
				mergeClustersHalf(clusters);
				recalculatePointsNeighboors(clusters, pntsList, toExclude);
				int pntst = printStats(clusters);
				if (pntst * 500 < origPnts) {
					break;
				}
			}
		}
		
		calculateEdgeSize(clusters, pntsList, toExclude);
		calculateNewGraphSize(cntEdges, pntsList, toExclude);
		time = System.nanoTime();
		System.out.printf("Routing finished %.2f ms: load data %.2f ms, routing %.2f ms (%.2f queue ms), prep result %.2f ms\n",
				(time - startTime) /1e6, stats.loadEdgesTime + stats.loadPointsTime, stats.routingTime,
				stats.addQueueTime, stats.prepTime);
		
	}


	private void calculateEdgeSize(Map<Integer, NetworkHHCluster> clusters, List<NetworkDBPoint> pntsList,
			TLongObjectHashMap<NetworkDBPoint> toExclude) throws SQLException {
		int p = 0;
		int clusteInd = 0;
		for (NetworkHHCluster c : clusters.values()) {
			if (c.mergedTo == null) {
				clusteInd++;
				p += calculateEdgesForCluster(clusteInd, c);
				
			}
		}
		System.out.println(p + " --- ");
	}
	
	private long link(NetworkDBPoint pnt1, NetworkDBPoint pnt2) {
		return (((long)pnt1.index) << 32) + pnt2.index;
	}


	private int calculateEdgesForCluster(int clusteInd, NetworkHHCluster c) throws SQLException {
		RoutingStats stats = new RoutingStats();
		int total = 0;
		Map<NetworkDBPoint, List<NetworkDBPoint>> totPoints = new HashMap<>();
		TLongHashSet shortcuts = new TLongHashSet();
		TLongHashSet shortcutMids = new TLongHashSet();
		TLongHashSet existing = new TLongHashSet();
		for (NetworkDBPoint pnt1 : c.points) {
			if (totPoints.containsKey(pnt1)) {
				// TODO ? this wrong situation how merge clusters (easier make hex grid?)
//				System.out.println(c.points);
//				System.out.println(pnt1 + " " + totPoints.get(pnt1));
//				throw new IllegalStateException();
			}
			for (NetworkDBPoint pnt2 : c.points) {
				if (pnt1 == pnt2) {
					continue;
				}
				total++;
				if (checkConnected(pnt1, pnt2)) {
					existing.add(link(pnt1, pnt2));
					continue;
				}
				c.clearRouting();

				NetworkDBPoint res = routePlanner.runDijkstraNetworkRouting(pnt1, pnt2, stats);
				if (res != null) {
					shortcuts.add(link(pnt1, pnt2));
					NetworkDBSegment parent = res.rtRouteToPointRev;
					while (parent != null) {
						addPnt(totPoints, parent.end, pnt1, pnt2);
						parent = parent.end.rtRouteToPointRev;
					}
					parent = res.rtRouteToPoint;
					while (parent != null) {
						addPnt(totPoints, parent.start, pnt1, pnt2);
						parent = parent.start.rtRouteToPoint;
					}
				}
			}
		}
		List<NetworkDBPoint> centerPoints = new ArrayList<>(totPoints.keySet());
		Collections.sort(centerPoints, new Comparator<NetworkDBPoint>() {

			@Override
			public int compare(NetworkDBPoint o1, NetworkDBPoint o2) {
				return -Integer.compare(totPoints.get(o1).size(), totPoints.get(o2).size());
			}
		});
		for (NetworkDBPoint p : centerPoints) {
			List<NetworkDBPoint> startEnds = totPoints.get(p);
			if (c.points.contains(p)) {
				for (int i = 0; i < startEnds.size(); i += 2) {
					if (shortcuts.remove(link(startEnds.get(0), startEnds.get(1)))) {
						existing.add(link(startEnds.get(0), startEnds.get(1)));
					}
				}
				continue;
			}
			if (startEnds.size() < 10) {
				continue;
			}
			for (int i = 0; i < startEnds.size(); i += 2) {
				if (shortcuts.remove(link(startEnds.get(0), startEnds.get(1)))) {
					shortcutMids.add(link(startEnds.get(0), p));
					shortcutMids.add(link(p, startEnds.get(1)));
				}
			}
		}
		System.out.println(String.format("Cluster %d: new shortcuts %d (total %d, existing %d) final routes ( visited %,d vertices, %,d (of %,d) edges )",
				clusteInd, shortcuts.size() + shortcutMids.size(), total, existing.size(), stats.visitedVertices, stats.visitedEdges, stats.addedEdges));
		return shortcuts.size() + shortcutMids.size();
	}


	


	private void addPnt(Map<NetworkDBPoint, List<NetworkDBPoint>> totPoints, NetworkDBPoint inter, NetworkDBPoint pnt1,
			NetworkDBPoint pnt2) {
		if (inter == pnt1 || inter == pnt2) {
			return;
		}
		List<NetworkDBPoint> lst = totPoints.get(inter);
		if (lst == null) {
			lst = new ArrayList<>();
			totPoints.put(inter, lst);
		}
		lst.add(pnt1);
		lst.add(pnt2);
	}


	private void recalculatePointsNeighboors(Map<Integer, NetworkHHCluster> clusters, List<NetworkDBPoint> pntsList,
			TLongObjectHashMap<NetworkDBPoint> toExclude) {
		Set<NetworkHHCluster> set = new HashSet<>();
		for (NetworkHHCluster c : clusters.values()) {
			c.neighbors.clear();
			c.points.clear();
			c.expoints.clear();
		}
		
		for (NetworkDBPoint pnt : pntsList) {
			if (toExclude.contains(pnt.index)) {
				continue;
			}
			set.clear();
			TIntIterator it = pnt.clusters.iterator();
			while (it.hasNext()) {
				set.add(clusters.get(it.next()).getMergeCluster());
			}
			if (set.size() <= 1) {
				set.iterator().next().expoints.add(pnt);
				toExclude.put(pnt.index, pnt);
			} else {
				for (NetworkHHCluster cl : set) {
					cl.points.add(pnt);
					for (NetworkHHCluster cl2 : set) {
						if (cl != cl2) {
							cl.neighbors.add(cl2);
						}
					}
				}
			}
		}
	}


	private void mergeClustersHalf(Map<Integer, NetworkHHCluster> clustersM) {
		List<NetworkHHCluster> clusters = new ArrayList<NetworkHHCluster>(clustersM.values());
		Collections.sort(clusters, new Comparator<NetworkHHCluster>() {

			@Override
			public int compare(NetworkHHCluster o1, NetworkHHCluster o2) {
				return Integer.compare(o1.neighborsSize(), o2.neighborsSize());
			}
		});
		int totPnts = 0, totI = 0;
		for (NetworkHHCluster c : clusters) {
			if (c.mergedTo != null) {
				continue;
			}
			totPnts += c.points.size();
			totI ++;
		}
		int pnts = 0, isl = 0;
		for (NetworkHHCluster c : clusters) {
			if (c.mergedTo != null) {
				continue;
			}
			pnts += c.points.size();
			isl++;
			if(pnts >= totPnts / 2 || isl >= totI / 2) {
				break;
			}
			if (c.neighbors.size() == 0) {
				// merge itself
				c.adoptMerge(c);
			} else {
				for (NetworkHHCluster nm : c.neighbors) {
					if (nm.getMergeCluster() != c) {
						nm.getMergeCluster().adoptMerge(c);
						break;
					}
				}
			}
		}
	}
	

	private void calculateNewGraphSize(int cntEdges, List<NetworkDBPoint> pntsList,
			TLongObjectHashMap<NetworkDBPoint> toExclude) {
		// calculate +- edges / vertexes
		int edgesPlus = 0, edgesMinus = 0;
		List<NetworkDBPoint> queue = new ArrayList<>();
		TLongObjectHashMap<NetworkDBPoint> visited = new TLongObjectHashMap<>();
		for (int i = 0; i < pntsList.size(); i++) {
			NetworkDBPoint point = pntsList.get(i);
			if (i % 5000 == 0) {
				System.out.println(i);
			}
			if (toExclude.contains(i)) {
				edgesMinus += point.connected.size();
			} else {
				queue.clear();
				visited.clear();
				for (NetworkDBSegment s : point.connected) {
					if (toExclude.contains(s.end.index)) {
						queue.add(s.end);
						edgesMinus++;
					}
				}
				while (!queue.isEmpty()) {
					NetworkDBPoint last = queue.remove(queue.size() - 1);
					if (visited.put(last.index, last) != null) {
						continue;
					}
					for (NetworkDBSegment s : last.connected) {
						if (toExclude.contains(s.end.index)) {
							queue.add(s.end);
						} else if (visited.put(s.end.index, s.end) == null && !checkConnected(point, s.end)) {
							edgesPlus++;
						}
					}
				}
				
			}
		}
		System.out.printf("Points %,d - %,d = %,d, shortcuts %,d + %,d - %,d = %,d \n", pntsList.size(), toExclude.size(), pntsList.size() - toExclude.size(),  
				cntEdges, edgesPlus, edgesMinus, cntEdges + edgesPlus - edgesMinus);
	}
	
	private int printStats(Map<Integer, NetworkHHCluster> clustersM) {
		int islands = 0, cl = 0, pnts = 0, totIslands = 0, totPnts = 0;
		List<NetworkHHCluster> clusters = new ArrayList<NetworkHHCluster>(clustersM.values());
		Collections.sort(clusters, new Comparator<NetworkHHCluster>() {

			@Override
			public int compare(NetworkHHCluster o1, NetworkHHCluster o2) {
				return Integer.compare(o1.neighborsSize(), o2.neighborsSize());
			}
		});
		for (NetworkHHCluster c : clusters) {
			if (c.mergedTo != null) {
				continue;
			}
			if(cl != c.neighborsSize()) {
				System.out.printf("Neighbors %d - %d islands (%,d points) \n", cl, islands, pnts);
				islands = 0;
				cl = c.neighborsSize();
				pnts = 0;
			}
			pnts += c.points.size();
			totPnts += c.points.size();
			islands++;
			totIslands++;
		}
		System.out.printf("Neighbors %d - %d islands (%,d points) \n", cl, islands, pnts);
		System.out.printf("Total %,d - %,d points\n---------------------\n", totIslands, totPnts);
		return totPnts;
		
	}

	private Map<Integer, NetworkHHCluster> restoreClusters(TLongObjectHashMap<NetworkDBPoint> pnts) throws SQLException {
		Map<Integer, NetworkHHCluster> clustersMap = new HashMap<>();
		List<NetworkHHCluster> tmpList = new ArrayList<>();
		networkDB.loadClusterData(pnts, false);
		for (NetworkDBPoint p : pnts.valueCollection()) {
			tmpList.clear();
			for (int clusterId : p.clusters.toArray()) {
				if (!clustersMap.containsKey(clusterId)) {
					clustersMap.put(clusterId, new NetworkHHCluster(clusterId));
				}
				NetworkHHCluster c = clustersMap.get(clusterId);
				tmpList.add(c);
				c.points.add(p);
			}
			for (NetworkHHCluster c1 : tmpList) {
				for (NetworkHHCluster c2 : tmpList) {
					if (c1 != c2) {
						c1.neighbors.add(c2);
					}
				}
			}
		}

		return clustersMap;
	}

	private boolean checkConnected(NetworkDBPoint start, NetworkDBPoint end) {
		for (NetworkDBSegment s : start.connected) {
			if (s.end == end) {
				return true;
			}
		}
		return false;
	}
	
	private Collection<Entity> runMonteCarloRouting() throws SQLException {
		RoutingStats stats = new RoutingStats();
		long time = System.nanoTime(), startTime = System.nanoTime();
		System.out.print("Loading points... ");
		TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(false);
		
		stats.loadPointsTime = (System.nanoTime() - time) / 1e6;
		time = System.nanoTime();
		System.out.printf(" %,d - %.2fms\nLoading segments...", pnts.size(), stats.loadPointsTime);
		int cntEdges = networkDB.loadNetworkSegments(pnts.valueCollection());
		stats.loadEdgesTime = (System.nanoTime() - time) / 1e6;

		// Routing
		System.out.printf(" %,d - %.2fms\nRouting...\n", cntEdges, stats.loadEdgesTime);
		List<NetworkDBPoint> pntsList = new ArrayList<>(pnts.valueCollection());
		Random rm = new Random();
		for (int i = 0; i < MAX_ITERATIONS; i++) {
			for(NetworkDBPoint p : pntsList) {
				p.clearRouting();
			}
			if ((i + 1) % 10 == 0) {
				System.out.printf("Routing %d ...\n", i + 1);
			}
			time = System.nanoTime();
			NetworkDBPoint startPnt = pntsList.get(rm.nextInt(pntsList.size() - 1));
			NetworkDBPoint endPnt = pntsList.get(rm.nextInt(pntsList.size() - 1));
			NetworkDBPoint pnt = routePlanner.runDijkstraNetworkRouting(startPnt, endPnt, stats);
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
		TLongObjectHashMap<Entity> entities = new TLongObjectHashMap<Entity>();
		for (int k = 0; k < pntsList.size(); k++) {
			NetworkDBPoint pnt = pntsList.get(k);
			if (pnt.rtCnt > 0.01 * MAX_ITERATIONS) {
				HHRoutingUtilities.addNode(entities, pnt, null, "highway", "stop");
				System.out.printf("%d %.4f, %.4f - %s\n", pnt.rtCnt, pnt.getPoint().getLatitude(),
						pnt.getPoint().getLongitude(), pnt);

			}
		}
		time = System.nanoTime();
		System.out.printf("Routing finished %.2f ms: load data %.2f ms, routing %.2f ms (%.2f queue ms), prep result %.2f ms\n",
				(time - startTime) /1e6, stats.loadEdgesTime + stats.loadPointsTime, stats.routingTime,
				stats.addQueueTime, stats.prepTime);
		return entities.valueCollection();
	}
}
