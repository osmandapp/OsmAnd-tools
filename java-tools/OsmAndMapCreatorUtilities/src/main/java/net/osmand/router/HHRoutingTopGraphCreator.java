package net.osmand.router;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;

import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.PlatformUtil;
import net.osmand.router.HHRouteDataStructure.HHRoutingConfig;
import net.osmand.router.HHRouteDataStructure.HHRoutingContext;
import net.osmand.router.HHRouteDataStructure.RoutingStats;
import net.osmand.router.HHRoutingDB.NetworkDBPoint;
import net.osmand.router.HHRoutingDB.NetworkDBSegment;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBPointPrep;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBSegmentPrep;

public class HHRoutingTopGraphCreator {
	static int DEBUG_VERBOSE_LEVEL = 0;

	final static Log LOG = PlatformUtil.getLog(HHRoutingTopGraphCreator.class);
	
	static final int PROC_MONTECARLO = 1;
	static final int PROC_MIDPOINTS = 2;
	static final int PROC_2ND_LEVEL = 3;
	static final int PROC_CH = 4;

	static int SAVE_ITERATIONS = 20;
	static int LOG_STAT_THRESHOLD = 10;
	static int LOG_STAT_MAX_DEPTH = 30;
	static int PROCESS = PROC_CH;

	static long DEBUG_START_TIME = 0;
	
	private HHRoutingPreparationDB networkDB;
	private HHRoutePlanner<NetworkDBPointPrep> routePlanner;
//	private RoutingStats routingStats;
//	private HHRoutingContext hctx;
	
	public HHRoutingTopGraphCreator(HHRoutePlanner<NetworkDBPointPrep> routePlanner, HHRoutingPreparationDB networkDB) throws SQLException {
		this.routePlanner = routePlanner;
		this.networkDB = networkDB;
	}
	
	
	private static File testData() {
		String name = "Montenegro";
		name = "Netherlands_europe";
//		name = "Ukraine_europe";
//		name = "Germany";
		
		return new File(System.getProperty("maps.dir"), name);
	}
	
	private static void logf(String string, Object... a) {
		if (DEBUG_START_TIME == 0) {
			DEBUG_START_TIME = System.currentTimeMillis();
		}
		String ms = String.format("%3.1fs ", (System.currentTimeMillis() - DEBUG_START_TIME) / 1000.f);
		System.out.printf(ms + string + "\n", a);

	}
	
	public static void main(String[] args) throws Exception {
		File obfFile = args.length == 0 ? testData() : new File(args[0]);
		int MAX_ITERATIONS = 100;
		int MAX_DEPTH = 15;
		int PERCENT_CH = 80;
		String ROUTING_PROFILE = "car";
		for (String a : args) {
			if (a.equals("--setup-midpoints")) {
				PROCESS = PROC_MIDPOINTS;
			} else if (a.equals("--proc-montecarlo")) {
				PROCESS = PROC_MONTECARLO;
			} else if (a.equals("--proc-ch")) {
				PROCESS = PROC_CH;
			} else if (a.equals("--proc-2nd-level")) {
				PROCESS = PROC_2ND_LEVEL;
			} else if (a.startsWith("--iterations=")) {
				MAX_ITERATIONS = Integer.parseInt(a.substring("--iterations=".length()));
			} else if (a.startsWith("--percent=")) {
				PERCENT_CH = Integer.parseInt(a.substring("--percent=".length()));
			} else if (a.startsWith("--maxdepth=")) {
				MAX_DEPTH = Integer.parseInt(a.substring("--maxdepth=".length()));
			}
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getCanonicalFile().getName() + "_" + ROUTING_PROFILE;
		
		HHRoutingPreparationDB networkDB = new HHRoutingPreparationDB(new File(folder, name + HHRoutingDB.EXT));
		HHRoutePlanner<NetworkDBPointPrep> routePlanner = new HHRoutePlanner<NetworkDBPointPrep>(HHRoutePlanner.prepareContext(ROUTING_PROFILE), 
				networkDB, NetworkDBPointPrep.class);
		HHRoutingTopGraphCreator planner = new HHRoutingTopGraphCreator(routePlanner, networkDB);
		
		if (PROCESS == PROC_MIDPOINTS) {
			planner.calculateMidPoints(MAX_DEPTH, MAX_ITERATIONS);
		} else if (PROCESS == PROC_CH) { 
			planner.runContractionHierarchy(MAX_DEPTH, PERCENT_CH / 100.0, 0);
		}
		planner.networkDB.close();
	}


	


	class NetworkHHCluster {
		private int clusterId;
		private List<NetworkDBPoint> points = new ArrayList<>();
		private List<NetworkDBPoint> expoints = new ArrayList<>();
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
	
	private void calculateMidPoints(int MAX_DEPTH, int MAX_ITERATIONS) throws SQLException, IOException {
		// rtCnt -> midMaxDepth
		// rtIndex -> midProc
		HHRoutingConfig config = HHRoutingConfig.dijkstra(0).preloadSegments();
		HHRoutingContext<NetworkDBPointPrep> hctx = routePlanner.initHCtx(config, null, null);
		long time = System.nanoTime(), startTime = System.nanoTime();
		TLongObjectHashMap<NetworkDBPointPrep> pnts = hctx.pointsById;
		List<NetworkDBPointPrep> pointsList = new ArrayList<>(pnts.valueCollection());
		networkDB.loadMidPointsIndex(pnts, pointsList, false);
		hctx.stats.loadPointsTime += (System.nanoTime() - time) / 1e6;
		
		// Routing
		System.out.printf(" %,d - %.2fms\nRouting...\n", hctx.stats.loadEdgesCnt, hctx.stats.loadEdgesTime);
		
		int iteration = Math.min(MAX_ITERATIONS, pnts.size() / 2);
		Random random = new Random();
		HHRoutingConfig c = new HHRoutingConfig();
		c.HEURISTIC_COEFFICIENT = 0;
		c.USE_MIDPOINT = false;
		c.MAX_DEPTH = MAX_DEPTH;
		while (iteration > 0) {
			NetworkDBPointPrep startPnt = pointsList.get(random.nextInt(pointsList.size()));
			if (startPnt.midProc > 0) {
				continue;
			}
			logf("%d. Routing %s - ", iteration, startPnt);
			iteration--;
			startPnt.midProc = 1;
			
			hctx.stats = new RoutingStats();
			routePlanner.runRoutingPointToPoint(hctx, startPnt, null);
			for (NetworkDBPointPrep pnt : pointsList) {
				pnt.midDepth = 0;
				if (pnt.rt(false).rtRouteToPoint != null) {
					pnt.midDepth++;
					NetworkDBPoint p = pnt;
					while ((p = p.rt(false).rtRouteToPoint) != startPnt) {
						pnt.midDepth++;
					}
				}
			}
			for (NetworkDBPointPrep pnt : pointsList) {
				if (pnt.rt(false).rtRouteToPoint != null) {
					int k = 0;
					NetworkDBPointPrep p = pnt;
					while ((p = (NetworkDBPointPrep) p.rt(false).rtRouteToPoint) != startPnt) {
						k++;
						p.midMaxDepth = Math.max(p.midMaxDepth, Math.min(k, p.midDepth));
					}
				}
			}
			int maxInc = 0, countInc = 0, maxTop = 0;
			for (NetworkDBPointPrep pnt : pointsList) {
				// calculate max increase
				if (pnt.midMaxDepth - pnt.midPrevMaxDepth > 0 && pnt.midPrevMaxDepth < LOG_STAT_THRESHOLD) {
					countInc++;
					maxInc = Math.max(pnt.midMaxDepth - pnt.midPrevMaxDepth, maxInc);
					maxTop = Math.max(pnt.midMaxDepth, maxTop);
				}
				pnt.midPrevMaxDepth = pnt.midMaxDepth;
				pnt.clearRouting();
			}
			System.out.printf("increased %d points - max diff %d, max top %d (%,d (%,d unique) visited / %,d added vertices, %.2f queue ms) \n", 
					countInc, maxInc, maxTop, hctx.stats.visitedVertices, hctx.stats.uniqueVisitedVertices, 
					hctx.stats.addedVertices, hctx.stats.addQueueTime);
			if (iteration % SAVE_ITERATIONS == 0) {
				saveAndPrintMidPoints(pointsList, pnts, LOG_STAT_MAX_DEPTH);
			}
		}
		saveAndPrintMidPoints(pointsList, pnts, LOG_STAT_MAX_DEPTH);
		
		
		time = System.nanoTime();
		System.out.printf("Routing finished %.2f ms: load data %.2f ms, routing %.2f ms (%.2f queue ms), prep result %.2f ms\n",
				(time - startTime) /1e6, hctx.stats.loadEdgesTime + hctx.stats.loadPointsTime, hctx.stats.routingTime,
				hctx.stats.addQueueTime, hctx.stats.prepTime);
		System.out.println(String.format("Found final route - cost %.2f, %d depth ( %,d (%,d unique) visited / %,d added vertices )", 
				0.0, 0, hctx.stats.visitedVertices, hctx.stats.uniqueVisitedVertices, hctx.stats.visitedVertices));
	}


	private void saveAndPrintMidPoints(List<NetworkDBPointPrep> pointsList, TLongObjectHashMap<NetworkDBPointPrep> pnts, int max) throws SQLException {
		long now = System.currentTimeMillis();
		
		Collections.sort(pointsList, new Comparator<NetworkDBPointPrep>() {

			@Override
			public int compare(NetworkDBPointPrep o1, NetworkDBPointPrep o2) {
				return -Integer.compare(o1.midMaxDepth, o2.midMaxDepth);
			}
		});
		int prev = 0;
		for (int k = 0; k < pointsList.size(); k++) {
			NetworkDBPointPrep p = pointsList.get(k);
			if (k > 0 && Math.min(pointsList.get(k - 1).midMaxDepth, max) == Math.min(p.midMaxDepth, max)) {
				prev++;
				continue;
			}
			System.out.printf("\n (^%d, %.1f%%) %d depth: %s", prev, prev * 100.0 / pointsList.size(), p.midMaxDepth, p.toString());
			//prev = 0;
		}
		System.out.printf("\n (^%d, %.1f%%) - saving %.2f s \n", prev, prev * 100.0 / pointsList.size(), (System.currentTimeMillis() - now) / 1000.0);
		networkDB.loadMidPointsIndex(pnts, pointsList, true);
		for (NetworkDBPointPrep pnt : pointsList) {
			pnt.midPrevMaxDepth = pnt.midMaxDepth;
		}
	}


	private void runContractionHierarchy(int maxPoints, double percent, int routingProfile) throws SQLException, IOException {
		HHRoutingConfig config = HHRoutingConfig.dijkstra(1).maxSettlePoints(maxPoints).preloadSegments();
		HHRoutingContext<NetworkDBPointPrep> hctx = routePlanner.initHCtx(config, null, null);
		routePlanner.setRoutingProfile(routingProfile);
		long time = System.nanoTime(), startTime = System.nanoTime();
		TLongObjectHashMap<NetworkDBPointPrep> pnts = hctx.pointsById;
		List<NetworkDBPointPrep> list = new ArrayList<>(pnts.valueCollection());
//		Map<Integer, NetworkHHCluster> clusters = restoreClusters(pnts);
		time = System.nanoTime();
				
		System.out.printf(" %,d - %.2fms\nContracting nodes..\n", hctx.stats.loadEdgesCnt, hctx.stats.loadEdgesTime);
		
		calculateAndPrintVertexDegree(list);
		
		time = System.nanoTime();

		TIntIntHashMap edgeDiffMap = new TIntIntHashMap();
		PriorityQueue<NetworkDBPointPrep> pq = new PriorityQueue<>(new Comparator<NetworkDBPointPrep>() {

			@Override
			public int compare(NetworkDBPointPrep o1, NetworkDBPointPrep o2) {
				return Integer.compare(o1.chIndexEdgeDiff, o2.chIndexEdgeDiff);
			}
		});
		int prog = 0;
		for (NetworkDBPointPrep p : list) {
			if (++prog % 1000 == 0) {
				logf("Preparing %d...", prog);
			}
			calculateCHEdgeDiff(hctx, p, null);
			pq.add(p);
			if (!edgeDiffMap.containsKey(p.chIndexEdgeDiff)) {
				edgeDiffMap.put(p.chIndexEdgeDiff, 0);
			}
			edgeDiffMap.put(p.chIndexEdgeDiff, edgeDiffMap.get(p.chIndexEdgeDiff) + 1);
		}
		prog = 0;
		int reindex = 0;
		List<NetworkDBSegment> allShortcuts = new ArrayList<>();
		List<NetworkDBSegment> shortcuts = new ArrayList<>();
		int contracted = 0;
		long timeC = System.nanoTime();
		double toContract = list.size() * percent;
		while (!pq.isEmpty()) {
			if (contracted > toContract) {
				break;
			}
			if (++prog % 1000 == 0) {
				logf("Contracting %d %.1f%% (reindexing %d, shortcuts %d)...", contracted, contracted / toContract * 100.0, reindex, allShortcuts.size());
				printStat("Contraction stat ", hctx.stats, timeC, 1000);
				if (prog % 10000 == 0) {
					calculateAndPrintVertexDegree(list);
				}
				hctx.stats = new RoutingStats();
				timeC = System.nanoTime();
			}
			
			NetworkDBPointPrep pnt = pq.poll();
			int oldIndex = pnt.chIndexEdgeDiff;
			shortcuts.clear();
			calculateCHEdgeDiff(hctx, pnt, shortcuts);
			if (oldIndex < pnt.chIndexEdgeDiff) {
				pq.add(pnt);
				reindex++;
				continue;
			}
			for (NetworkDBSegment sh : shortcuts) {
				NetworkDBSegment dup = sh.start.getSegment(sh.end, true);
				if (dup != null) {
					if (dup.dist < sh.dist) {
						// skip shortcut (not needed) - not enough depth for Dijkstra
						continue;
					} else {
						if (!dup.shortcut) {
							// possible situation due triangle inequality not guaranteed  
						} else {
							allShortcuts.remove(dup);
							sh.start.connected.remove(dup);
							sh.end.connectedReverse.remove(sh.end.getSegment(sh.start, false));
						}
					}
				}
				allShortcuts.add(sh);
				sh.start.connected.add(sh);
				NetworkDBSegment rev = new NetworkDBSegment(sh.start, sh.end, sh.dist, !sh.direction, sh.shortcut);
				rev.getGeometry().addAll(sh.getGeometry());
				sh.end.connectedReverse.add(rev);
			}
			pnt.chFinalInd = contracted++;
			pnt.rtExclude = true;
		}
		networkDB.updatePointsCHInd(list);
		networkDB.deleteShortcuts();
		networkDB.insertSegments(allShortcuts, routingProfile);
		
		System.out.printf("Added %d shortcuts, reindexed %d \n", allShortcuts.size(), reindex);
		
		printStat("Contraction ", hctx.stats, time, list.size());
		time = System.nanoTime();
		System.out.printf("Routing finished %.2f ms: load data %.2f ms, routing %.2f ms (%.2f queue ms), prep result %.2f ms\n",
				(time - startTime) / 1e6, hctx.stats.loadEdgesTime + hctx.stats.loadPointsTime, hctx.stats.routingTime,
				hctx.stats.addQueueTime, hctx.stats.prepTime);
	}


	private void calculateAndPrintVertexDegree(List<NetworkDBPointPrep> list) {
		TIntIntHashMap degreeIn = new TIntIntHashMap();
		TIntIntHashMap degreeOut = new TIntIntHashMap();
		int cnt = 0;
		for (NetworkDBPoint p : list) {
			if (p.rtExclude) {
				continue;
			}
			cnt++;
			degreeIn.adjustOrPutValue(p.connectedReverse.size(), 1, 1);
			degreeOut.adjustOrPutValue(p.connected.size(), 1, 1);
		}
		if (cnt > 0) {
			System.out.println("Vertex degree in - " + formatVertexDegree(degreeIn, cnt).toString());
			System.out.println("Vertex degree out - " + formatVertexDegree(degreeOut, cnt).toString());
		}
	}


	private StringBuilder formatVertexDegree(TIntIntHashMap degreeIn, int cnt) {
		int[] keys = degreeIn.keys();
		Arrays.sort(keys);
		StringBuilder s = new StringBuilder();
		int k = -1, st = 0, v = 0;
		for (int l = 0; l <= keys.length; l++) {
			if (l < keys.length) {
				k = keys[l];
				v += degreeIn.get(k);
			}
			if (v * 100.0 / cnt > 8 || l == keys.length) {
				s.append(String.format("%s - %,d (%.1f%%), ", (st != k ? st + "-" : "") + k, v, v * 100.0 / cnt));
				st = k + 1;
				v = 0;
			}
		}
		return s;
	}


	private void printStat(String name, RoutingStats stats, long time, int size) {
		double contractionTime = (System.nanoTime() - time) / 1e6;
		System.out.println(
				String.format(name + " for %d - %.2f ms (%.2f mcs per node), visited %,d (%,d unique) of %,d added vertices",
						size, contractionTime, contractionTime * 1e3 / size,
				stats.visitedVertices, stats.uniqueVisitedVertices, stats.addedVertices));
	}


	private void calculateCHEdgeDiff(HHRoutingContext<NetworkDBPointPrep> hctx, NetworkDBPointPrep p, List<NetworkDBSegment> shortcuts) throws SQLException, IOException {
		hctx.config.MAX_COST = 0;
		for (NetworkDBSegment out : p.connected) {
			hctx.config.MAX_COST = Math.max(out.dist, hctx.config.MAX_COST);
		}
		// shortcuts
		p.chIndexCnt = 0;
		p.rtExclude = true;
		for (NetworkDBSegment in : p.connectedReverse) {
			if (in.start.rtExclude) {
				continue;
			}
			routePlanner.runRoutingPointToPoint(hctx, (NetworkDBPointPrep) in.start, null);
			for (NetworkDBSegment out : p.connected) {
				if (out.end.rtExclude) {
					continue;
				}
				if (out.end.rt(false).rtDistanceFromStart == 0 || out.end.rt(false).rtDistanceFromStart > out.dist) {
					p.chIndexCnt++;
					if (shortcuts != null) {
						if (DEBUG_VERBOSE_LEVEL >= 1) {
							System.out.printf("Shortcut %d -> %d via %d %.2f cost \n ", in.start.index, out.end.index,
									in.end.index, in.dist + out.dist);
						}
						NetworkDBSegmentPrep sh = new NetworkDBSegmentPrep(in.start, out.end,
								in.dist + out.dist, true, true);
						if (in.shortcut) {
							sh.segmentsStartEnd.addAll(((NetworkDBSegmentPrep)in).segmentsStartEnd);
						} else {
							sh.segmentsStartEnd.add(in.start.index);
							sh.segmentsStartEnd.add(in.end.index);
						}
						if (out.shortcut) {
							sh.segmentsStartEnd.addAll(((NetworkDBSegmentPrep)out).segmentsStartEnd);
						} else {
							sh.segmentsStartEnd.add(out.start.index);
							sh.segmentsStartEnd.add(out.end.index);
						}
						shortcuts.add(sh);
					}
				}
			}
			hctx.clearVisited();
		}
		// edge diff
		p.chIndexEdgeDiff = p.chIndexCnt - p.connected.size() - p.connectedReverse.size();
		p.rtExclude = false;
	}
	

	
	
//	
}
