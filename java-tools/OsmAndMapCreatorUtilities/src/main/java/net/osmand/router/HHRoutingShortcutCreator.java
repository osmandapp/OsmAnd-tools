package net.osmand.router;

import static net.osmand.router.HHRoutePlanner.calculateRoutePointInternalId;
import static net.osmand.router.HHRoutingUtilities.addWay;
import static net.osmand.router.HHRoutingUtilities.getPoint;
import static net.osmand.router.HHRoutingUtilities.logf;
import static net.osmand.router.HHRoutingUtilities.saveOsmFile;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.logging.Log;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.osm.edit.Entity;
import net.osmand.router.BinaryRoutePlanner.MultiFinalRouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.HHRoutingDB.NetworkDBPoint;
import net.osmand.router.HHRoutingDB.NetworkDBSegment;
import net.osmand.util.MapUtils;

public class HHRoutingShortcutCreator {

	final static Log LOG = PlatformUtil.getLog(HHRoutingShortcutCreator.class);

	final static int PROCESS_SET_NETWORK_POINTS = 1;
	final static int PROCESS_BUILD_NETWORK_SEGMENTS = 2;

	static boolean DEBUG_STORE_ALL_ROADS = false;
	static int DEBUG_LIMIT_START_OFFSET = 0;
	static int DEBUG_LIMIT_PROCESS = -1;
	static int DEBUG_VERBOSE_LEVEL = 0;
	
	private static HHRoutingPrepareContext prepareContext;
	
	private static boolean CLEAN;
	private static int BATCH_SIZE = 1000;
	private static int THREAD_POOL = 2;
	
	private static String ROUTING_PROFILE = "car";
	private static String[] ROUTING_PARAMS = new String[] { "" };

	private static File sourceFile() {
		CLEAN = true;
		DEBUG_VERBOSE_LEVEL = 0;
//		DEBUG_LIMIT_START_OFFSET = 1150;
//		DEBUG_LIMIT_PROCESS = 1200;
		THREAD_POOL = 1;
		String name = "Montenegro_europe_2.road.obf";
//		name = "Italy_test";
//		name = "Netherlands_europe_2.road.obf";
//		name = "Ukraine_europe_2.road.obf";
//		name = "Germany";
		
		return new File(System.getProperty("maps.dir"), name);
	}
	
	static void testCompact() throws SQLException, IOException {
		String nameFile = System.getProperty("maps.dir");
//		nameFile += "Germany";
		nameFile += "Montenegro_europe_2.road.obf_car";
		File source = new File(nameFile + HHRoutingDB.EXT);
		File target = new File(nameFile + HHRoutingDB.CEXT);
		compact(source, target);
	}
	
	public static void main(String[] args) throws Exception {
//		if (args.length == 0) testCompact(); return;
		File obfFile = args.length == 0 ? sourceFile() : new File(args[0]);
		 
		boolean onlyCompact = false;
		for (String a : args) {
			if (a.startsWith("--routing_profile=")) {
				ROUTING_PROFILE = a.substring("--routing_profile=".length());
			} else if (a.startsWith("--routing_params=")) {
				ROUTING_PARAMS = a.substring("--routing_params=".length()).trim().split("---");
			} else if (a.startsWith("--threads=")) {
				THREAD_POOL = Integer.parseInt(a.substring("--threads=".length()));
			} else if (a.equals("--clean")) {
				CLEAN = true;
			} else if (a.equals("--onlycompact")) {
				onlyCompact = true;
			}
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getCanonicalFile().getName() + "_" + ROUTING_PROFILE;
		File dbFile = new File(folder, name + HHRoutingDB.EXT);
		if (onlyCompact) {
			compact(dbFile, new File(folder, name + HHRoutingDB.CEXT));
			return;
		}
		HHRoutingPreparationDB networkDB = new HHRoutingPreparationDB(dbFile);
		if (CLEAN && dbFile.exists()) {
			networkDB.recreateSegments();
		}
		TLongObjectHashMap<NetworkDBPoint> totalPnts = networkDB.loadNetworkPoints(NetworkDBPoint.class);
		createOSMNetworkPoints(new File(folder, name + "-pnts.osm"), totalPnts);
		System.out.printf("Loaded %,d points\n", totalPnts.size());
		
		for (String routingParam : ROUTING_PARAMS) {
			prepareContext = new HHRoutingPrepareContext(obfFile, ROUTING_PROFILE, routingParam.split(","));
			int routingProfile = networkDB.insertRoutingProfile(routingParam);
			HHRoutingShortcutCreator proc = new HHRoutingShortcutCreator();
			networkDB.selectRoutingProfile(routingProfile);
			// reload points to avoid cache
			TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.loadNetworkPoints(NetworkDBPoint.class);
			int segments = networkDB.loadNetworkSegments(pnts.valueCollection());
			System.out.printf("Calculating segments for routing (%s) - existing segments %,d \n", routingParam, segments);	
			Collection<Entity> objects = proc.buildNetworkShortcuts(pnts, networkDB);
			saveOsmFile(objects, new File(folder, name + "-hh.osm"));
		}
		networkDB.close();
		compact(dbFile, new File(folder, name + HHRoutingDB.CEXT));
	}
	
	
	public static void createOSMNetworkPoints(File osm, TLongObjectHashMap<NetworkDBPoint> pnts) throws XMLStreamException, IOException {
		TLongObjectHashMap<Entity> osmObjects = new TLongObjectHashMap<Entity>();
		for(NetworkDBPoint p : pnts.valueCollection()) {
			HHRoutingUtilities.addNode(osmObjects, p, null, "highway", "stop");
		}
		HHRoutingUtilities.saveOsmFile(osmObjects.valueCollection(), osm);
	}
	public static void compact(File source, File target) throws SQLException, IOException {
		System.out.printf("Compacting %s -> %s...\n", source.getName(), target.getName());
		target.delete();
		HHRoutingPreparationDB.compact(DBDialect.SQLITE.getDatabaseConnection(source.getAbsolutePath(), LOG),
				DBDialect.SQLITE.getDatabaseConnection(target.getAbsolutePath(), LOG));
	}

	
	private static class BuildNetworkShortcutResult {
		List<NetworkDBPoint> points = new ArrayList<>();
		List<RouteCalculationProgress> progress = new ArrayList<>();
		TIntArrayList shortcuts = new TIntArrayList();
		TLongObjectHashMap<Entity> osmObjects = new TLongObjectHashMap<>();
		double totalTime;
		int taskId;
	}

	private static class BuildNetworkShortcutTask implements Callable<BuildNetworkShortcutResult> {

		private static ThreadLocal<RoutingContext> context = new ThreadLocal<>();

		private List<NetworkDBPoint> batch;
		private TLongObjectHashMap<RouteSegment> segments;
		private HHRoutingShortcutCreator creator;
		private TLongObjectHashMap<NetworkDBPoint> networkPointsByGeoId;
		private int taskId;

		public BuildNetworkShortcutTask(HHRoutingShortcutCreator creator, List<NetworkDBPoint> batch,
				TLongObjectHashMap<RouteSegment> segments, TLongObjectHashMap<NetworkDBPoint> networkPointsByGeoId,
				int taskId) {
			this.creator = creator;
			this.batch = batch;
			this.segments = segments;
			this.networkPointsByGeoId = networkPointsByGeoId;
			this.taskId = taskId;
		}

		@Override
		public BuildNetworkShortcutResult call() throws Exception {
			RoutingContext ctx = context.get();
			ctx = prepareContext.gcMemoryLimitToUnloadAll(ctx, null, ctx == null);
			context.set(ctx);
			BuildNetworkShortcutResult res = new BuildNetworkShortcutResult();
			res.taskId = taskId;
			long nt = System.nanoTime();
			for (NetworkDBPoint pnt : batch) {
				if (Thread.interrupted()) {
					return res;
				}
				ctx.calculationProgress = new RouteCalculationProgress();
				long nt2 = System.nanoTime();
				RouteSegmentPoint s = HHRoutePlanner.loadPoint(ctx, pnt);
				if (s == null) {
					// routing params 
					System.out.printf("Skip segment %d as not accessible (%s) \n", pnt.roadId / 64, pnt);
					continue;
				}
				HHRoutingUtilities.addNode(res.osmObjects, pnt, getPoint(s), "highway", "stop"); // "place","city");
				try {
					BinaryRoutePlanner routePlanner = new BinaryRoutePlanner();
					List<RouteSegment> result = creator.runDijsktra(ctx, routePlanner, s, segments);
					boolean errorFound = false;
					for (RouteSegment t : result) {
						NetworkDBPoint end = networkPointsByGeoId.get(calculateRoutePointInternalId(t.getRoad().getId(),
								t.getSegmentStart(), t.getSegmentEnd()));
						double h = MapUtils.squareRootDist31(pnt.midX(), pnt.midY(), end.midX(), end.midY())
								/ ctx.getRouter().getMaxSpeed();
						float routeTime = t.getDistanceFromStart() + routePlanner.calcRoutingSegmentTimeOnlyDist(ctx.getRouter(), t) / 2;
						if (h > routeTime) {
							routeTime += 1; // happens in extra rare cases straight line / maxspeed and float roundings
							if (h > routeTime) {
								throw new IllegalStateException(String.format("%s %s - %.2f > %.2f", pnt, end, h, routeTime));
							}
						}
						final NetworkDBSegment segment = new NetworkDBSegment(pnt, end, routeTime, true, false);
						final List<LatLon> geometry = segment.getGeometry();
						while (t != null) {
							geometry.add(getPoint(t));
							t = t.getParentRoute();
						}
						Collections.reverse(geometry);
						if (pnt.dualPoint.clusterId != end.clusterId) {
							if (errorFound) {
								continue;
							}
							errorFound = true;
							StringBuilder b = new StringBuilder();
							for (RouteSegment test : result) {
								NetworkDBPoint other = networkPointsByGeoId.get(calculateRoutePointInternalId(
										test.getRoad().getId(), test.getSegmentStart(), test.getSegmentEnd()));
								b.append(other).append(" (").append(other.clusterId).append("), ");
							}
							String msg = String.format("%s can lead only to dual cluster %d - found %s (cluster %d): %s",
									pnt, pnt.dualPoint.clusterId, end, end.clusterId, b.toString());
							System.err.println(HHRoutingUtilities.testGetGeometry(geometry));
							System.err.println("BUG needs to be fixed " + msg);
							System.err.println(msg);
							throw new IllegalStateException(msg);
						}
						if (segment.dist < 0) {
							throw new IllegalStateException(segment + " dist < " + segment.dist);
						}
						pnt.connected.add(segment);
						if (DEBUG_STORE_ALL_ROADS) {
							addWay(res.osmObjects, segment, "highway", "secondary");
						}
						
					}
					if (DEBUG_VERBOSE_LEVEL >= 2) {
						logf("Run dijkstra %d point - %d shortcuts: %.2f ms (calc %.2f ms, load %.2f ms) - %s", pnt.index, result.size(), 
								(System.nanoTime() - nt2) / 1e6, ctx.calculationProgress.timeToCalculate / 1e6,
								ctx.calculationProgress.timeToLoad / 1e6, s.toString());
					}
					// use to store time
					pnt.rt(false).rtDistanceFromStart = (System.nanoTime() - nt2) / 1e6;
					res.points.add(pnt);
					res.progress.add(ctx.calculationProgress);
					res.shortcuts.add(result.size());
				} catch (RuntimeException e) {
					logf("Error %s while processing %d road - %s (%d)", e.getMessage(), pnt.roadId / 64, pnt.getPoint(),
							pnt.index);
					throw e;
				}

			}
			segments = null;
			res.totalTime = (System.nanoTime() - nt) / 1e9;
			return res;
		}


	}

	private Collection<Entity> buildNetworkShortcuts(TLongObjectHashMap<NetworkDBPoint> pnts,
			HHRoutingPreparationDB networkDB)
			throws InterruptedException, IOException, SQLException, ExecutionException {
		TLongObjectHashMap<Entity> osmObjects = new TLongObjectHashMap<>();
		double sz = pnts.size() / 100.0;
		int ind = 0, prevPrintInd = 0;
		TLongObjectHashMap<RouteSegment> segments = new TLongObjectHashMap<>();
		for (NetworkDBPoint pnt : pnts.valueCollection()) {
			RouteSegment s = new RouteSegment(null, pnt.start, pnt.end);
			segments.put(calculateRoutePointInternalId(pnt.roadId, pnt.start, pnt.end), s);
			HHRoutingUtilities.addNode(osmObjects, pnt, null, "highway", "stop");
		}

		ExecutorService service = Executors.newFixedThreadPool(THREAD_POOL);
		List<Future<BuildNetworkShortcutResult>> results = new ArrayList<>();
		List<NetworkDBPoint> batch = new ArrayList<>();
		int taskId = 0;
		int total = 0;
		int batchSize = BATCH_SIZE;
		if (pnts.size() / THREAD_POOL < batchSize) {
			batchSize = pnts.size() / THREAD_POOL + 1;
		}
		
		TLongObjectHashMap<NetworkDBPoint> networkPointsByGeoId = new TLongObjectHashMap<>();
		for (NetworkDBPoint pnt : pnts.valueCollection()) {
			networkPointsByGeoId.put(pnt.pntGeoId, pnt);
		}
		for (NetworkDBPoint pnt : pnts.valueCollection()) {
			ind++;
			if (pnt.connected.size() > 0) {
				pnt.connected.clear(); // for gc
				continue;
			}
			if (ind < DEBUG_LIMIT_START_OFFSET) {
				continue;
			}
			batch.add(pnt);
			if (ind > DEBUG_LIMIT_PROCESS && DEBUG_LIMIT_PROCESS != -1) {
				break;
			}
			if (batch.size() == batchSize) {
				results.add(
						service.submit(new BuildNetworkShortcutTask(this, batch, segments, networkPointsByGeoId, taskId++)));
				total += batch.size();
				batch = new ArrayList<>();
			}
		}
		total += batch.size();
		results.add(service.submit(new BuildNetworkShortcutTask(this, batch, segments, networkPointsByGeoId, taskId++)));
		logf("Scheduled %d tasks, %d total points", taskId, total);
		int maxDirectedPointsGraph = 0;
		int maxFinalSegmentsFound = 0;
		int totalFinalSegmentsFound = 0;
		int totalVisitedDirectSegments = 0;
		ind = 1;
		service.shutdown();
		try {
			while (!results.isEmpty()) {
				Thread.sleep(5000);
				Iterator<Future<BuildNetworkShortcutResult>> it = results.iterator();
				while (it.hasNext()) {
					Future<BuildNetworkShortcutResult> future = it.next();
					if (future.isDone()) {
						BuildNetworkShortcutResult res = future.get();
						for (int k = 0; k < res.points.size(); k++) {
							NetworkDBPoint rpnt = res.points.get(k);
							RouteCalculationProgress calculationProgress = res.progress.get(k);
							ind++;
							if (DEBUG_VERBOSE_LEVEL >= 1 || ind - prevPrintInd > 200) {
								prevPrintInd = ind;
								logf("%.2f%% Process %d (%d shortcuts) - %.1f ms", ind / sz, rpnt.roadId / 64,
										res.shortcuts.get(k), rpnt.rt(false).rtDistanceFromStart);
							}
							networkDB.insertSegments(rpnt.connected);
							if (DEBUG_VERBOSE_LEVEL >= 2) {
								System.out.println(calculationProgress.getInfo(null));
							}

							maxDirectedPointsGraph = Math.max(maxDirectedPointsGraph,
									calculationProgress.visitedDirectSegments);
							totalVisitedDirectSegments += calculationProgress.visitedDirectSegments;
							maxFinalSegmentsFound = Math.max(maxFinalSegmentsFound,
									calculationProgress.finalSegmentsFound);
							totalFinalSegmentsFound += calculationProgress.finalSegmentsFound;

							// clean up for gc
							rpnt.connected.clear();
						}
						osmObjects.putAll(res.osmObjects);
						it.remove();
						logf("Task id %d executed %.1f seconds - %d (of %d) waiting completion", res.taskId,
								res.totalTime, results.size(), taskId);
					}
				}
			}
		} finally {
			List<Runnable> runnable = service.shutdownNow();
			if (!results.isEmpty()) {
				logf("!!! %d runnable were not executed: exception occurred", runnable == null ? 0 : runnable.size());
			}
			service.awaitTermination(5, TimeUnit.MINUTES);
		}

		System.out.println(String.format(
				"Total segments %d: %d total shorcuts, per border point max %d, average %d shortcuts (routing sub graph max %d, avg %d segments)",
				segments.size(), totalFinalSegmentsFound, maxFinalSegmentsFound, totalFinalSegmentsFound / ind,
				maxDirectedPointsGraph, totalVisitedDirectSegments / ind));
		return osmObjects.valueCollection();
	}

	private List<RouteSegment> runDijsktra(RoutingContext ctx, BinaryRoutePlanner routePlanner, RouteSegmentPoint s, TLongObjectMap<RouteSegment> segments)
			throws InterruptedException, IOException {
		long pnt = calculateRoutePointInternalId(s.getRoad().getId(), s.getSegmentEnd(), s.getSegmentStart());
		segments = new ExcludeTLongObjectMap<RouteSegment>(segments, pnt);
		TLongObjectHashMap<RouteSegment> resUnique = new TLongObjectHashMap<>();
		// REMOVE TEST BLOCK ONCE NOT USED ///// 
//		BinaryRoutePlanner.TRACE_ROUTING = s.getRoad().getId() / 64 == 451406223; //233801367l;
		boolean testBUG = true;
		TLongObjectHashMap<RouteSegment> testIteration = null;
		for (int iteration = 0; iteration < (testBUG ? 2 : 1); iteration++) {
			testIteration = resUnique; 
			if (testBUG) {
				ctx.config.planRoadDirection = iteration;
//				BinaryRoutePlanner.DEBUG_BREAK_EACH_SEGMENT = iteration != 0;
//				BinaryRoutePlanner.DEBUG_PRECISE_DIST_MEASUREMENT = iteration != 0;
			}
			///// TEST BLOCK ////
			
			resUnique = new TLongObjectHashMap<>();
			ctx.unloadAllData(); // needed for proper multidijsktra work
			ctx.calculationProgress = new RouteCalculationProgress();
			ctx.config.PENALTY_FOR_REVERSE_DIRECTION = -1;
			MultiFinalRouteSegment frs = (MultiFinalRouteSegment) routePlanner.searchRouteInternal(ctx, s,
					null, segments);
			if (frs != null) {
				for (RouteSegment o : frs.all) {
					// duplicates are possible as alternative routes
					long pntId = calculateRoutePointInternalId(o.getRoad().getId(), o.getSegmentStart(), o.getSegmentEnd());
					if (resUnique.containsKey(pntId)) {
						if (resUnique.get(pntId).getDistanceFromStart() > o.getDistanceFromStart()) {
							throw new IllegalStateException(resUnique.get(pntId) + " > " + o + " - " + s);
						} else {
							continue;
						}
					} 
					resUnique.put(pntId, o);
				}
			}
			//// TEST BLOCK
		}
		long[] testKeys = testIteration == null ? new long[0] : testIteration.keys();
		for (long pntId : testKeys) {
			RouteSegment prev = testIteration.get(pntId);
			RouteSegment o = resUnique.get(pntId);
			if (Math.abs(1 - prev.distanceFromStart / o.distanceFromStart) * 100 > 0.1) {
				double d1 = HHRoutingUtilities.testGetDist(prev, false);
				double d2 = HHRoutingUtilities.testGetDist(o, true);
				System.out.printf("1 = false (2 = true): %.1f%% (%.1f%%). %.2f s (%.2f m) != %.2f  s (%.2fm) - %s %s - %.5f, %.5f -> %.5f, %.5f \n",
						Math.abs(1 - prev.distanceFromStart / o.distanceFromStart) * 100, Math.abs(1 - d1 / d2) * 100,
						prev.distanceFromStart, HHRoutingUtilities.testGetDist(prev, false), o.distanceFromStart,
						HHRoutingUtilities.testGetDist(o, true), s, o,
						MapUtils.get31LatitudeY(s.getStartPointY()), MapUtils.get31LongitudeX(s.getStartPointX()), 
						MapUtils.get31LatitudeY(o.getStartPointY()), MapUtils.get31LongitudeX(o.getStartPointX()));
				List<LatLon> lprev = HHRoutingUtilities.testGeometry(prev);
				List<LatLon> lcur = HHRoutingUtilities.testGeometry(o);
				boolean diff = false;
				if (lprev.size() != lcur.size()) {
					diff = true;
				}
				for (int k = 0; !diff && k < lprev.size(); k++) {
					if (MapUtils.getDistance(lprev.get(k), lcur.get(k)) > 5) {
						diff = true;
					}
				}
				if (diff) {
					System.out.println(HHRoutingUtilities.testGetGeometry(lprev));
					System.out.println(HHRoutingUtilities.testGetGeometry(lcur));
				}
			}
		}
		return new ArrayList<>(resUnique.valueCollection());
	}
	
}
