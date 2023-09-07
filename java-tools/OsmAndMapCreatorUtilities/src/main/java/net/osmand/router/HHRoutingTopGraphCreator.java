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

import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.osm.edit.Entity;
import net.osmand.router.HHRoutePlanner.RoutingStats;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBSegment;

public class HHRoutingTopGraphCreator {
	static int DEBUG_VERBOSE_LEVEL = 0;

	static final int PROC_MONTECARLO = 1;
	static final int PROC_NEXT_LEVEL = 2;
	static int PROCESS = PROC_NEXT_LEVEL;

	static int MONTE_CARLO_BATCH = 300;
	private HHRoutingPreparationDB networkDB;
	private HHRoutePlanner routePlanner;
	
	public HHRoutingTopGraphCreator(HHRoutePlanner routePlanner, HHRoutingPreparationDB networkDB) {
		this.routePlanner = routePlanner;
		this.networkDB = networkDB;
	}
	
	
	private static File testData() {
		String name = "Montenegro_europe_2.road.obf";
		name = "Netherlands_europe";
//		name = "Ukraine_europe";
//		name = "Germany";
		
		return new File(System.getProperty("maps.dir"), name);
	}
	
	public static void main(String[] args) throws Exception {
		File obfFile = args.length == 0 ? testData() : new File(args[0]);
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getName();
		HHRoutingPreparationDB networkDB = 
				new HHRoutingPreparationDB(new File(folder, name + HHRoutingPreparationDB.EXT), HHRoutingPreparationDB.READ);
		HHRoutePlanner routePlanner = new HHRoutePlanner(HHRoutePlanner.prepareContext(
				HHRoutePlanner.ROUTING_PROFILE), networkDB);
		HHRoutingTopGraphCreator planner = new HHRoutingTopGraphCreator(routePlanner, networkDB);
		if (PROCESS == PROC_NEXT_LEVEL) {
			planner.run2ndLevelRouting();
		} else if (PROCESS == PROC_MONTECARLO) {
			Collection<Entity> objects = planner.runMonteCarloRouting();
			HHRoutingUtilities.saveOsmFile(objects, new File(folder, name + "-mc.osm"));
		}
		planner.networkDB.close();
	}


	private class NetworkHHCluster {
		private int clusterId;
		private List<NetworkDBPoint> points = new ArrayList<>();
		private Set<NetworkHHCluster> neighbors = new HashSet<>();
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

	
	private void run2ndLevelRouting() throws SQLException {
		RoutingStats stats = new RoutingStats();
		long time = System.nanoTime(), startTime = System.nanoTime();
		System.out.print("Loading points... ");
		TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(false);
		stats.loadPointsTime = (System.nanoTime() - time) / 1e6;
		
		Map<Integer, NetworkHHCluster> clusters = restoreClusters(pnts);
		
		time = System.nanoTime();
		System.out.printf(" %,d - %.2fms\nLoading segments...", pnts.size(), stats.loadPointsTime);
		int cntEdges = networkDB.loadNetworkSegments(pnts);
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
			for (int k = 0; k < 6; k++) {
//				mergeClusters(clusters, 9);
				mergeClustersHalf(clusters);
				recalculatePointsNeighboors(clusters, pntsList, toExclude);
				int pntst = printStats(clusters);
				if (pntst * 500 < origPnts) {
					break;
				}
			}
		}
		
		calculateNewGraphSize(cntEdges, pntsList, toExclude);
		time = System.nanoTime();
		System.out.printf("Routing finished %.2f ms: load data %.2f ms, routing %.2f ms (%.2f queue ms), prep result %.2f ms\n",
				(time - startTime) /1e6, stats.loadEdgesTime + stats.loadPointsTime, stats.routingTime,
				stats.addQueueTime, stats.prepTime);
		
	}


	private void recalculatePointsNeighboors(Map<Integer, NetworkHHCluster> clusters, List<NetworkDBPoint> pntsList,
			TLongObjectHashMap<NetworkDBPoint> toExclude) {
		Set<NetworkHHCluster> set = new HashSet<>();
		for (NetworkHHCluster c : clusters.values()) {
			c.neighbors.clear();
			c.points.clear();
		}
		
		for (NetworkDBPoint pnt : pntsList) {
			if (toExclude.contains(pnt.index)) {
				continue;
			}
			set.clear();
			for (int ind : pnt.clusters) {
				set.add(clusters.get(ind).getMergeCluster());
			}
			if (set.size() <= 1) {
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
	
	private void mergeClusters(Map<Integer, NetworkHHCluster> clusters, int max) {
		for (NetworkHHCluster c : clusters.values()) {
			if (c.mergedTo != null) {
				continue;
			}
			if (c.neighbors.size() == 0) {
				// merge itself
				c.adoptMerge(c);
			} else if (c.neighbors.size() <= max) {
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

	private Map<Integer, NetworkHHCluster> restoreClusters(TLongObjectHashMap<NetworkDBPoint> pnts) {
		Map<Integer, NetworkHHCluster> clustersMap = new HashMap<>();
		List<NetworkHHCluster> tmpList = new ArrayList<>();
		for (NetworkDBPoint p : pnts.valueCollection()) {
			tmpList.clear();
			for (int clusterId : p.clusters) {
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
			NetworkDBPoint pnt = routePlanner.runDijkstraNetworkRouting(networkDB, pnts, startPnt, endPnt, stats);
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
			if (pnt.rtCnt > 0.01 * MONTE_CARLO_BATCH) {
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
