package net.osmand.router.tester;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import net.osmand.router.*;
import net.osmand.NativeLibrary;
import org.apache.commons.logging.Log;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.PlatformUtil;

class RandomRouteTester {
	class GeneratorConfig {
		final String[] PREDEFINED_TESTS = { // optional predefined routes in "url" format (imply ITERATIONS=0)
//				"https://test.osmand.net/map/?start=48.211348,24.478998&finish=48.172382,24.421492&type=osmand&profile=bicycle&params=bicycle,height_obstacles#14/48.1852/24.4208",
//				"https://osmand.net/map/?start=50.450128,30.535611&finish=50.460479,30.589365&via=50.452647,30.588330&type=osmand&profile=car#14/50.4505/30.5511",
//				"start=48.211348,24.478998&finish=48.172382,24.421492&type=osmand&profile=bicycle&params=bicycle,height_obstacles",
//				"start=50.450128,30.535611&finish=50.460479,30.589365&via=50.452647,30.588330&profile=car",
//				"start=50.450128,30.535611&finish=50.460479,30.589365&via=1,2;3,4;5,6&profile=car",
//				"start=L,L&finish=L,L&via=L,L;L,L&profile=pedestrian&params=height_obstacles" // example
		};

		// random tests settings
		final int ITERATIONS = 1; // number of random routes
		final int MAX_INTER_POINTS = 2; // 0-2 intermediate points // (2)
		final int MIN_DISTANCE_KM = 10; // min distance between start and finish (50)
		final int MAX_DISTANCE_KM = 20; // max distance between start and finish (100)
		final int MAX_SHIFT_ALL_POINTS_M = 500; // shift LatLon of all points by 0-500 meters (500)
		final String[] RANDOM_PROFILES = { // randomly selected profiles[,params] for each iteration
				"car",
				"bicycle",
				"bicycle,height_obstacles",
//				"bicycle,driving_style_prefer_unpaved,driving_style_balance:false,height_obstacles",
//				"bicycle,driving_style_prefer_unpaved,driving_style_balance=false,height_obstacles",
		};

		// cost/distance deviation limits
		final double DEVIATION_RED = 1.0F; // > 1% - mark as failed
		final double DEVIATION_YELLOW = 0.1F; // > 0.1% - mark as acceptable
	}

	public static void main(String[] args) throws Exception {
		// TODO parse args --obf-storage --obf-prefix --iterations --min-dist --max-dist --output-html --native-lib etc
		File obfDirectory = new File(args.length == 0 ? "." : args[0]); // args[0] is a path to *.obf and hh-files

		RandomRouteTester test = new RandomRouteTester(obfDirectory);

		test.reportHtmlWriter = new FileWriter("report.html"); // TODO args optional

//		test.initHHsqliteConnections();
		test.loadNativeLibrary();
		test.initObfReaders();
		test.generateRoutes();
		test.collectRoutes();
		test.reportResult();
	}

	private long started;
	private File obfDirectory;
	FileWriter reportHtmlWriter;
	NativeLibrary nativeLibrary = null;
	private List<BinaryMapIndexReader> obfReaders = new ArrayList<>();
	private HashMap<String, File> hhFiles = new HashMap<>(); // [Profile]
	private HashMap<String, Connection> hhConnections = new HashMap<>(); // [Profile]

	private RandomRouteGenerator generator;
	private GeneratorConfig config = new GeneratorConfig();
	private List<RandomRouteEntry> testList = new ArrayList<>();

	private final Log LOG = PlatformUtil.getLog(RandomRouteTester.class);

	private RandomRouteTester(File obfDirectory) {
		this.started = System.currentTimeMillis();
		this.generator = new RandomRouteGenerator(config);
		this.obfDirectory = obfDirectory;
	}

	private void reportResult() throws IOException {
		RandomRouteReport report = new RandomRouteReport(
				started, obfReaders.size(), testList.size(),
				config.DEVIATION_RED, config.DEVIATION_YELLOW);

		for (int i = 0; i < testList.size(); i++) {
			report.entryOpen(i + 1);
			RandomRouteEntry entry = testList.get(i);
			for (int j = 0; j < entry.results.size(); j++) {
				RandomRouteResult ideal = entry.results.get(0);
				RandomRouteResult result = entry.results.get(j);
				if (j == 0) {
					report.resultIdeal(i + 1, ideal);
				} else {
					report.resultCompare(i + 1, result, ideal);
				}
			}
			report.entryClose();
		}

		report.flush(reportHtmlWriter);
	}

	private void initObfReaders() throws IOException {
		List<File> obfFiles = new ArrayList<>();

		if (obfDirectory.isDirectory()) {
			for (File f : obfDirectory.listFiles()) {
				if (f.isFile() && f.getName().endsWith(".obf")) {
					obfFiles.add(f);
				}
			}
		} else {
			obfFiles.add(obfDirectory);
		}

		// sort files by name to improve pseudo-random reproducibility
		obfFiles.sort((f1, f2) -> f1.getName().compareTo(f2.getName()));

		for (File source : obfFiles) {
			System.out.printf("Use OBF %s...\n", source.getName());
			Objects.requireNonNull(nativeLibrary).initMapFile(source.getAbsolutePath(), true);
			obfReaders.add(new BinaryMapIndexReader(new RandomAccessFile(source, "r"), source));
		}

		if (obfReaders.size() == 0) {
			throw new IllegalStateException("empty obfReaders");
		}
	}

	private void initHHsqliteConnections() throws SQLException {
		List<File> sqliteFiles = new ArrayList<>();

		if (obfDirectory.isDirectory()) {
			for (File f : obfDirectory.listFiles()) {
				if (f.isFile() && f.getName().endsWith(HHRoutingDB.EXT)) {
					sqliteFiles.add(f);
				}
			}
		}

		// sort files by name to improve pseudo-random reproducibility
		sqliteFiles.sort((f1, f2) -> f1.getName().compareTo(f2.getName()));

		for (File source : sqliteFiles) {
			String[] parts = source.getName().split("[_.]"); // Maps_PROFILE.hhdb
			if (parts.length > 2) {
				String profile = parts[parts.length - 2];
				System.out.printf("Use HH (%s) %s...\n", profile, source.getName());
				hhConnections.put(profile, DBDialect.SQLITE.getDatabaseConnection(source.getAbsolutePath(), LOG));
				hhFiles.put(profile, source);
			}
		}

		if (hhConnections.size() == 0) {
			throw new IllegalStateException("empty hhConnections");
		}
	}

	private void generateRoutes() {
		testList = generator.generateTestList(obfReaders);
	}

	private void collectRoutes() {
		class Counter {
			private int value;
		}
		Counter counter = new Counter(); // TODO remove

		testList.forEach(entry -> {
			System.err.printf("\n\n%d / %d ...\n\n", ++counter.value, testList.size());
			try {
				runBinaryRoutePlannerJava(entry);
				runBinaryRoutePlannerCpp(entry);
//				runHHRoutePlannerJava(entry);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
//			} catch (SQLException e) {
//				throw new RuntimeException(e);
			}
		});
	}

	private void runBinaryRoutePlannerJava(RandomRouteEntry entry) throws IOException, InterruptedException {
		runBinaryRoutePlanner(entry, false);
	}

	private void runBinaryRoutePlannerCpp(RandomRouteEntry entry) throws IOException, InterruptedException {
		runBinaryRoutePlanner(entry, true);
	}

	private void runBinaryRoutePlanner(RandomRouteEntry entry, boolean useNative) throws IOException, InterruptedException {
		long started = System.currentTimeMillis();
		final int MEM_LIMIT = RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT * 8; // ~ 2 GB from OsmAndMapsService

		RoutePlannerFrontEnd.USE_HH_ROUTING = false;
		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();

		RoutingConfiguration.Builder builder = RoutingConfiguration.getDefault();

		RoutingConfiguration.RoutingMemoryLimits memoryLimits =
				new RoutingConfiguration.RoutingMemoryLimits(MEM_LIMIT, MEM_LIMIT);

		RoutingConfiguration config = builder.build(entry.profile, memoryLimits, entry.mapParams());

		RoutingContext ctx = fe.buildRoutingContext(
				config,
				useNative ? nativeLibrary : null,
				obfReaders.toArray(new BinaryMapIndexReader[0]),
				RoutePlannerFrontEnd.RouteCalculationMode.NORMAL
		);

//		ctx.dijkstraMode = 0; // 0 for bidirectional, +1 for direct search, -1 for reverse search
//		ctx.config.heuristicCoefficient = 1; // h() *= 1 for A*, 0 for Dijkstra
//		BinaryRoutePlanner.DEBUG_PRECISE_DIST_MEASUREMENT = false; // debug
//		BinaryRoutePlanner.DEBUG_BREAK_EACH_SEGMENT = false; // debug
//		BinaryRoutePlanner.TRACE_ROUTING = false; // make it public

		List<RouteSegmentResult> routeSegments = fe.searchRoute(ctx, entry.start, entry.finish, entry.via, null);

		long runTime = System.currentTimeMillis() - started;

		RandomRouteResult result = new RandomRouteResult(
				useNative ? "cpp" : "java", entry, runTime, ctx, routeSegments);

		entry.results.add(result);
	}

	private void runHHRoutePlannerJava(RandomRouteEntry entry) throws SQLException, IOException, InterruptedException {
//		long started = System.currentTimeMillis();
////		RoutingContext hhContext = prepareContext.gcMemoryLimitToUnloadAll(hhContext, null, hhContext == null);
//
////		// ready to use HHRoutePlanner class
////		hhPlanner = HHRoutePlanner.create(hhContext, new HHRoutingDB(conn));
////
////		HHRouteDataStructure.HHRoutingConfig hhConfig = new HHRouteDataStructure.HHRoutingConfig().astar(0);
//////		HHRouteDataStructure.HHRoutingConfig hhConfig = new HHRouteDataStructure.HHRoutingConfig().dijkstra(0);
////		// run test HH-routing
////		HHRouteDataStructure.HHNetworkRouteRes hh = hhPlanner.runRouting(START, FINISH, hhConfig);
//
////		RoutePlannerFrontEnd.USE_HH_ROUTING = true; // really doesn't matter for direct hhPlanner.runRouting call
//		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();
//
//		RoutingConfiguration.Builder builder = RoutingConfiguration.getDefault();
//
//		RoutingConfiguration.RoutingMemoryLimits memoryLimits = new RoutingConfiguration.RoutingMemoryLimits(
//				RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 10,
//				RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
//
//		RoutingConfiguration config = builder.build(entry.profile, memoryLimits, entry.mapParams());
//
//		RoutingContext ctx = fe.buildRoutingContext(
//				config,
//				null,
//				obfReaders.toArray(new BinaryMapIndexReader[0]),
//				RoutePlannerFrontEnd.RouteCalculationMode.NORMAL
//		);
//
//		HHRouteDataStructure.HHRoutingConfig hhConfig =
//				new HHRouteDataStructure.HHRoutingConfig()
//						.astar(0)
//						.calcDetailed(2);
//
//		HHRoutingDB db = new HHRoutingDB(hhFiles.get(entry.profile), hhConnections.get(entry.profile));
//		HHRoutePlanner<HHRouteDataStructure.NetworkDBPoint> hhPlanner = HHRoutePlanner.create(ctx, db);
//
////		ctx.config.heuristicCoefficient = 1; // h() *= 1 for A*, 0 for Dijkstra
////		ctx.config.planRoadDirection = 0; // 0 for bidirectional, +1 for direct search, -1 for reverse search
//
//		HHRouteDataStructure.HHNetworkRouteRes res = hhPlanner.runRouting(entry.start, entry.finish, hhConfig);
//		// TODO check HH for params (height_obstacles)
	}

	private void loadNativeLibrary() {
		String nativePath = getNativeLibPath();
		if (nativePath != null) {
			if (NativeLibrary.loadOldLib(nativePath)) {
				nativeLibrary = new NativeLibrary();
				return; // success
			}
		}
		throw new IllegalStateException("native library not loaded");
	}

	private String getNativeLibPath() { // taken from RouterUtilTest and modified
		Path path = FileSystems.getDefault().getPath("../../core-legacy/binaries");
		if (!Files.exists(path)) {
			path = FileSystems.getDefault().getPath("../core-legacy/binaries");
		}
		if (Files.exists(path)) {
			File nativeLibPath = path.normalize().toAbsolutePath().toFile();
			for (final File f1 : Objects.requireNonNull(nativeLibPath.listFiles())) {
				if (f1.isDirectory()) {
					for (final File f2 : Objects.requireNonNull(f1.listFiles())) {
						if (f2.isDirectory()) {
							File libDir = getLatestLib(f2.listFiles());
							return libDir == null ? f2.getAbsolutePath() : libDir.getAbsolutePath();
						}
					}
				}
			}
		}
		return null;
	}

	private File getLatestLib(File[] f3) { // taken from RouterUtilTest
		File libDir = null;
		for (File f4 : Objects.requireNonNull(f3)) {
			if (f4.isDirectory() && (f4.getName().equals("Release") || f4.getName().equals("Debug"))) {
				if (libDir == null) {
					libDir = f4.getAbsoluteFile();
				} else {
					if (libDir.lastModified() < f4.lastModified()) {
						libDir = f4;
					}
				}
			}
		}
		return libDir;
	}
}

// TODO RR-1 Test height_obstacles uphill (Petros) for the up-vs-down bug
// TODO RR-2 Equalise Binary Native lib call (interpoints==0 vs interpoints>0)
// TODO RR-3 MapCreator - parse start/finish from url, share route url, route hotkeys (Ctrl + 1/2/3/4/5)
// TODO RR-4 fix start segment calc: https://osmand.net/map/?start=50.450128,30.535611&finish=50.460479,30.589365&via=50.452647,30.588330&type=osmand&profile=car#14/50.4505/30.5511
// TODO RR-5 turn back and test recently added "ignored" route test, then remove ignore=true there

// BinaryRoutePlanner.TRACE_ROUTING = s.getRoad().getId() / 64 == 451406223; // 233801367L;