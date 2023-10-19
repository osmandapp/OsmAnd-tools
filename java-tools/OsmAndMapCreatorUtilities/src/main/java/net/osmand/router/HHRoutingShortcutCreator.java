package net.osmand.router;

import static net.osmand.router.HHRoutePlanner.calculateRoutePointInternalId;
import static net.osmand.router.HHRoutingPrepareContext.logf;
import static net.osmand.router.HHRoutingUtilities.addWay;
import static net.osmand.router.HHRoutingUtilities.getPoint;
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

import org.apache.commons.logging.Log;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.PlatformUtil;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.osm.edit.Entity;
import net.osmand.router.BinaryRoutePlanner.MultiFinalRouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.HHRoutingDB.NetworkDBPoint;
import net.osmand.router.HHRoutingDB.NetworkDBSegment;

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

	private static File sourceFile() {
		CLEAN = true;
		DEBUG_VERBOSE_LEVEL = 1;
		THREAD_POOL = 1;
		String name = "Montenegro_europe_2.road.obf";
//		name = "Netherlands_europe_2.road.obf";
//		name = "Ukraine_europe_2.road.obf";
//		name = "Germany";
		
		return new File(System.getProperty("maps.dir"), name);
	}
	
	static void testCompact() throws SQLException {
		String nameFile = System.getProperty("maps.dir");
//		nameFile += "Germany";
		nameFile += "Montenegro_europe_2.road.obf_car";
		File source = new File(nameFile + HHRoutingDB.EXT);
		File target = new File(nameFile + HHRoutingDB.CEXT);
		compact(source, target);
	}
	
	public static void main(String[] args) throws Exception {
		testCompact(); if(true) return;
		File obfFile = args.length == 0 ? sourceFile() : new File(args[0]);
		String routingProfile = "car"; 
		for (String a : args) {
			if (a.startsWith("--routing_profile=")) {
				routingProfile = a.substring("--routing_profile=".length());
			} else if (a.startsWith("--threads=")) {
				THREAD_POOL = Integer.parseInt(a.substring("--threads=".length()));
			} else if (a.equals("--clean")) {
				CLEAN = true;
			}
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getCanonicalFile().getName() + "_" + routingProfile;
		File dbFile = new File(folder, name + HHRoutingDB.EXT);
		HHRoutingPreparationDB networkDB = new HHRoutingPreparationDB(dbFile);
		if (CLEAN && dbFile.exists()) {
			networkDB.recreateSegments();
		}
		prepareContext = new HHRoutingPrepareContext(obfFile, routingProfile);
		HHRoutingShortcutCreator proc = new HHRoutingShortcutCreator();
		TLongObjectHashMap<NetworkDBPoint> pntsByGeoId = networkDB.loadNetworkPointsByGeoId();
		int segments = networkDB.loadNetworkSegments(pntsByGeoId.valueCollection());
		System.out.printf("Loaded %,d points, existing shortcuts %,d \n", pntsByGeoId.size(), segments);
		Collection<Entity> objects = proc.buildNetworkShortcuts(pntsByGeoId, networkDB);
		saveOsmFile(objects, new File(folder, name + "-hh.osm"));
		networkDB.close();
		
		compact(new File(folder, name + HHRoutingDB.EXT),
				new File(folder, name + HHRoutingDB.CEXT));
	}
	
	public static void compact(File source, File target) throws SQLException {
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
				RouteSegmentPoint s = null;
				try {
					ctx.calculationProgress = new RouteCalculationProgress();
					long nt2 = System.nanoTime();
					s = HHRoutingUtilities.loadPoint(ctx, pnt);
					HHRoutingUtilities.addNode(res.osmObjects, pnt, getPoint(s), "highway", "stop"); // "place","city");
					List<RouteSegment> result = creator.runDijsktra(ctx, s, segments);
					for (RouteSegment t : result) {
						NetworkDBPoint end = networkPointsByGeoId.get(calculateRoutePointInternalId(t.getRoad().getId(), t.getSegmentStart(), t.getSegmentEnd()));
						if (pnt.dualPoint.clusterId != end.clusterId) {
							throw new IllegalStateException("Point can lead only to 1 dual cluster " + pnt + " " + end);
						}
						NetworkDBSegment segment = new NetworkDBSegment(pnt, end, t.getDistanceFromStart(), true, false);
						pnt.connected.add(segment);
						while (t != null) {
							segment.geometry.add(getPoint(t));
							t = t.getParentRoute();
						}
						Collections.reverse(segment.geometry);
						if (DEBUG_STORE_ALL_ROADS) {
							addWay(res.osmObjects, segment, "highway", "secondary");
						}
						if (segment.dist < 0) {
							throw new IllegalStateException(segment + " dist < " + segment.dist);
						}
					}
					if (DEBUG_VERBOSE_LEVEL >= 2) {
						logf("Run dijkstra %d point - %d shortcuts: %.2f ms (calc %.2f ms, load %.2f ms) - %s", pnt.index, result.size(), 
								(System.nanoTime() - nt2) / 1e6, ctx.calculationProgress.timeToCalculate / 1e6,
								ctx.calculationProgress.timeToLoad / 1e6, s.toString());
					}
					pnt.rtDistanceFromStart = (System.nanoTime() - nt2) / 1e6;
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

	private Collection<Entity> buildNetworkShortcuts(TLongObjectHashMap<NetworkDBPoint> networkPointsByGeoId,
			HHRoutingPreparationDB networkDB)
			throws InterruptedException, IOException, SQLException, ExecutionException {
		TLongObjectHashMap<Entity> osmObjects = new TLongObjectHashMap<>();
		double sz = networkPointsByGeoId.size() / 100.0;
		int ind = 0, prevPrintInd = 0;
		TLongObjectHashMap<RouteSegment> segments = new TLongObjectHashMap<>();
		for (NetworkDBPoint pnt : networkPointsByGeoId.valueCollection()) {
			segments.put(calculateRoutePointInternalId(pnt.roadId, pnt.start, pnt.end),
					new RouteSegment(null, pnt.start, pnt.end));
			HHRoutingUtilities.addNode(osmObjects, pnt, null, "highway", "stop");
		}

		ExecutorService service = Executors.newFixedThreadPool(THREAD_POOL);
		List<Future<BuildNetworkShortcutResult>> results = new ArrayList<>();
		List<NetworkDBPoint> batch = new ArrayList<>();
		int taskId = 0;
		int total = 0;
		int batchSize = BATCH_SIZE;
		if (networkPointsByGeoId.size() / THREAD_POOL < batchSize) {
			batchSize = networkPointsByGeoId.size() / THREAD_POOL + 1;
		}
		for (NetworkDBPoint pnt : networkPointsByGeoId.valueCollection()) {
			ind++;
			if (pnt.connected.size() > 0) {
				pnt.connected.clear(); // for gc
				continue;
			}
//			if (pnt.index > 2000 || pnt.index < 1800)  continue;
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
										res.shortcuts.get(k), rpnt.rtDistanceFromStart);
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

	private List<RouteSegment> runDijsktra(RoutingContext ctx, RouteSegmentPoint s, TLongObjectMap<RouteSegment> segments)
			throws InterruptedException, IOException {
		long pnt1 = calculateRoutePointInternalId(s.getRoad().getId(), s.getSegmentStart(), s.getSegmentEnd());
		long pnt2 = calculateRoutePointInternalId(s.getRoad().getId(), s.getSegmentEnd(), s.getSegmentStart());
		segments = new ExcludeTLongObjectMap<RouteSegment>(segments, pnt2);
		TLongObjectHashMap<Double> allDistances = new TLongObjectHashMap<Double>();
		List<RouteSegment> res = new ArrayList<>();
		// TODO 1.1 HHRoutingShortcutCreator BinaryRoutePlanner.DEBUG_BREAK_EACH_SEGMENT TODO test that routing time is different with on & off! should be the same
		// TODO 1.2 HHRoutingShortcutCreator BinaryRoutePlanner.DEBUG_PRECISE_DIST_MEASUREMENT for long distance causes bugs if (pnt.index != 2005) { 2005-> 1861 } - 3372.75 vs 2598 -
//		BinaryRoutePlanner.DEBUG_PRECISE_DIST_MEASUREMENT = false;
		
		for (int iteration = 0; iteration < 1; iteration++) {
			res.clear();
//			BinaryRoutePlanner.DEBUG_BREAK_EACH_SEGMENT = iteration != 0;
//			BinaryRoutePlanner.DEBUG_PRECISE_DIST_MEASUREMENT = iteration != 0;
			ctx.unloadAllData(); // needed for proper multidijsktra work
			ctx.calculationProgress = new RouteCalculationProgress();
			ctx.config.PENALTY_FOR_REVERSE_DIRECTION = -1;
//			BinaryRoutePlanner.TRACE_ROUTING = true;
			MultiFinalRouteSegment frs = (MultiFinalRouteSegment) new BinaryRoutePlanner().searchRouteInternal(ctx, s,
					null, segments);
			if (frs != null) {
				TLongSet set = new TLongHashSet();
				for (RouteSegment o : frs.all) {
					// duplicates are possible as alternative routes
					long pntId = calculateRoutePointInternalId(o.getRoad().getId(), o.getSegmentStart(), o.getSegmentEnd());
					if (set.add(pntId)) {
						res.add(o);
						if(iteration == 0) {
							allDistances.put(pntId, (double) o.distanceFromStart);
						} else {
							Double d1 = allDistances.get(pntId);
							if (Math.abs(1 - d1 / o.distanceFromStart)*100 > 0.1) {
								System.out.printf("%.2f %% err, %.2f != %.2f \n", Math.abs(1 - d1 / o.distanceFromStart)*100, d1, o.distanceFromStart);
							}
						}
					}
				}
			}
		}

		return res;
	}

}
