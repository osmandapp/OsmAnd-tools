package net.osmand.router;

import static net.osmand.router.HHRoutingPrepareContext.logf;
import static net.osmand.router.HHRoutingUtilities.addWay;
import static net.osmand.router.HHRoutingUtilities.calculateRoutePointInternalId;
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
//		CLEAN = true;
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
		nameFile += "Germany";
		File source = new File(nameFile + HHRoutingDB.EXT);
		File target = new File(nameFile + HHRoutingDB.CEXT);
		compact(source, target);
	}
	
	public static void main(String[] args) throws Exception {
		File obfFile = args.length == 0 ? sourceFile() : new File(args[0]);
		String routingProfile = null; 
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
		HHRoutingPreparationDB networkDB = new HHRoutingPreparationDB(DBDialect.SQLITE.getDatabaseConnection(dbFile.getAbsolutePath(), LOG));
		if (CLEAN && dbFile.exists()) {
			networkDB.recreateSegments();
		}
		prepareContext = new HHRoutingPrepareContext(obfFile, routingProfile);
		HHRoutingShortcutCreator proc = new HHRoutingShortcutCreator();
		TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(true);
		int segments = networkDB.loadNetworkSegments(pnts.valueCollection());
		System.out.printf("Loaded %,d points, existing shortcuts %,d \n", pnts.size(), segments);
		Collection<Entity> objects = proc.buildNetworkShortcuts(pnts, networkDB);
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
		private TLongObjectHashMap<NetworkDBPoint> networkPoints;
		private int taskId;

		public BuildNetworkShortcutTask(HHRoutingShortcutCreator creator, List<NetworkDBPoint> batch,
				TLongObjectHashMap<RouteSegment> segments, TLongObjectHashMap<NetworkDBPoint> networkPoints,
				int taskId) {
			this.creator = creator;
			this.batch = batch;
			this.segments = segments;
			this.networkPoints = networkPoints;
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
						NetworkDBPoint end = networkPoints.get(calculateRoutePointInternalId(t.getRoad().getId(),
								Math.min(t.getSegmentStart(), t.getSegmentEnd()),
								Math.max(t.getSegmentStart(), t.getSegmentEnd())));
						NetworkDBSegment segment = new NetworkDBSegment(pnt, end, t.getDistanceFromStart(), true,
								false);
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
						logf("Run dijkstra %s: %.2f ms (calc %.2f ms, load %.2f ms)", s.toString(),
								(System.nanoTime() - nt2) / 1e6,
								ctx.calculationProgress.timeToCalculate / 1e6,
								ctx.calculationProgress.timeToLoad / 1e6);
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

	private Collection<Entity> buildNetworkShortcuts(TLongObjectHashMap<NetworkDBPoint> networkPoints,
			HHRoutingPreparationDB networkDB)
			throws InterruptedException, IOException, SQLException, ExecutionException {
		TLongObjectHashMap<Entity> osmObjects = new TLongObjectHashMap<>();
		double sz = networkPoints.size() / 100.0;
		int ind = 0, prevPrintInd = 0;
		// 1.3 TODO for long distance causes bugs if (pnt.index != 2005) { 2005-> 1861 }
		// - 3372.75 vs 2598
		BinaryRoutePlanner.PRECISE_DIST_MEASUREMENT = true;
		TLongObjectHashMap<RouteSegment> segments = new TLongObjectHashMap<>();
		for (NetworkDBPoint pnt : networkPoints.valueCollection()) {
			segments.put(calculateRoutePointInternalId(pnt.roadId, pnt.start, pnt.end),
					new RouteSegment(null, pnt.start, pnt.end));
			segments.put(calculateRoutePointInternalId(pnt.roadId, pnt.end, pnt.start),
					new RouteSegment(null, pnt.end, pnt.start));
			HHRoutingUtilities.addNode(osmObjects, pnt, null, "highway", "stop");
		}

		ExecutorService service = Executors.newFixedThreadPool(THREAD_POOL);
		List<Future<BuildNetworkShortcutResult>> results = new ArrayList<>();
		List<NetworkDBPoint> batch = new ArrayList<>();
		int taskId = 0;
		int total = 0;
		int batchSize = BATCH_SIZE;
		if (networkPoints.size() / THREAD_POOL < batchSize) {
			batchSize = networkPoints.size() / THREAD_POOL + 1;
		}
		for (NetworkDBPoint pnt : networkPoints.valueCollection()) {
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
						service.submit(new BuildNetworkShortcutTask(this, batch, segments, networkPoints, taskId++)));
				total += batch.size();
				batch = new ArrayList<>();
			}
		}
		total += batch.size();
		results.add(service.submit(new BuildNetworkShortcutTask(this, batch, segments, networkPoints, taskId++)));
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
		segments = new ExcludeTLongObjectMap<RouteSegment>(segments, pnt1, pnt2);

		List<RouteSegment> res = new ArrayList<>();

		ctx.unloadAllData(); // needed for proper multidijsktra work
		ctx.calculationProgress = new RouteCalculationProgress();
		ctx.startX = s.getRoad().getPoint31XTile(s.getSegmentStart(), s.getSegmentEnd());
		ctx.startY = s.getRoad().getPoint31YTile(s.getSegmentStart(), s.getSegmentEnd());
		MultiFinalRouteSegment frs = (MultiFinalRouteSegment) new BinaryRoutePlanner().searchRouteInternal(ctx, s,
				null, null, segments);

		if (frs != null) {
			TLongSet set = new TLongHashSet();
			for (RouteSegment o : frs.all) {
				// duplicates are possible as alternative routes
				long pntId = calculateRoutePointInternalId(o);
				if (set.add(pntId)) {
					res.add(o);
				}
			}
		}

		return res;
	}

}
