package net.osmand.router.tester;

import java.io.File;
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
		final int ITERATIONS = 10; // number of random routes
		final int MAX_INTER_POINTS = 0; // 0-2 intermediate points // (2) TODO
		final int MIN_DISTANCE_KM = 0; // min distance between start and finish (50) TODO
		final int MAX_DISTANCE_KM = 5; // max distance between start and finish (100) TODO
		final int MAX_SHIFT_ALL_POINTS_M = 500; // shift LatLon of all points by 0-500 meters (500)
		final String[] RANDOM_PROFILES = { // randomly selected profiles[,params] for each iteration
//				"car",
//				"bicycle",
				"bicycle,height_obstacles",
//				"bicycle,driving_style_prefer_unpaved,driving_style_balance:false,height_obstacles",
//				"bicycle,driving_style_prefer_unpaved,driving_style_balance=false,height_obstacles",
		};
	}

	public static void main(String[] args) throws Exception {
		// TODO parse args --obf-storage --obf-prefix --iterations --min-dist --max-dist --output-html --native-lib etc
		File obfDirectory = new File(args.length == 0 ? "." : args[0]); // args[0] is a path to *.obf and hh-files

		RandomRouteTester test = new RandomRouteTester(obfDirectory);

		test.initHHsqliteConnections();
		test.loadNativeLibrary();
		test.initObfReaders();
		test.generateRoutes();
		test.collectRoutes();
//		test.reportResult();
	}

	private File obfDirectory;
	NativeLibrary nativeLibrary = null;
	private List<BinaryMapIndexReader> obfReaders = new ArrayList<>();
	private HashMap<String, Connection> hhConnections = new HashMap<>(); // [Profile]

	private RandomRouteGenerator generator;
	private GeneratorConfig config = new GeneratorConfig();
	private List<RandomRouteEntry> testList = new ArrayList<>();

	private final Log LOG = PlatformUtil.getLog(RandomRouteTester.class);

	private RandomRouteTester(File obfDirectory) {
		this.generator = new RandomRouteGenerator(config);
		this.obfDirectory = obfDirectory;
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
		testList.forEach(entry -> {
			try {
				runBinaryRoutePlannerJava(entry);
				runBinaryRoutePlannerCpp(entry);
//				runHHRoutePlannerJava(entry);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
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

		RoutePlannerFrontEnd.USE_HH_ROUTING = false;
		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();

		RoutingConfiguration.Builder builder = RoutingConfiguration.getDefault();

		RoutingConfiguration.RoutingMemoryLimits memoryLimits = new RoutingConfiguration.RoutingMemoryLimits(
				RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 10,
				RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);

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
				useNative ? "osmand-native" : "osmand-java", entry, runTime, ctx, routeSegments);

		entry.results.add(result);
	}

	private void runHHRoutePlannerJava(RandomRouteEntry entry) {
		long started = System.currentTimeMillis();

		RoutePlannerFrontEnd.USE_HH_ROUTING = true; // TODO ?
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

// BinaryRoutePlanner.TRACE_ROUTING = s.getRoad().getId() / 64 == 451406223; // 233801367L;
//	private static RoutingContext hhContext;
//	private static HHRoutePlanner hhPlanner;
//	private static BinaryRoutePlanner brPlanner;

//		// use HHRoutingPrepareContext to list *.obf and parse profile/params
//		TestPrepareContext prepareContext = new TestPrepareContext(obfDirectory, ROUTING_PROFILE, ROUTING_PARAMS[0].split(","));
//
//		// run garbage collector, return ctx TODO does it need to use force = true every cycle?
//		hhContext = prepareContext.gcMemoryLimitToUnloadAll(hhContext, null, hhContext == null);
//
//		// hhFile as SQLITE database now, but will be changed to obf-data later
//		Connection conn = DBDialect.SQLITE.getDatabaseConnection(hhFile.getAbsolutePath(), LOG);
//		// ready to use HHRoutePlanner class
//		hhPlanner = HHRoutePlanner.create(hhContext, new HHRoutingDB(conn));
//
//		HHRouteDataStructure.HHRoutingConfig hhConfig = new HHRouteDataStructure.HHRoutingConfig().astar(0);
////		HHRouteDataStructure.HHRoutingConfig hhConfig = new HHRouteDataStructure.HHRoutingConfig().dijkstra(0);
//		// run test HH-routing
//		HHRouteDataStructure.HHNetworkRouteRes hh = hhPlanner.runRouting(START, FINISH, hhConfig);

////////////// TODO need fresh RoutingContext for next use! How to reset it??? //////////////////
//		hhContext = hhPrepareContext.gcMemoryLimitToUnloadAll(hhContext, null, true);
//		hhContext.routingTime = 0;
//
//		// use BinaryRoutePlanner as default route frontend
//		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
//		// run test BinaryRoutePlanner TODO is it correct to use hhContext here?
//		List<RouteSegmentResult> routeSegments = router.searchRoute(hhContext, START, FINISH, null);

//	private static class TestPrepareContext extends HHRoutingPrepareContext {
//		public TestPrepareContext(File obfFile, String routingProfile, String... profileSettings) {
//			super(obfFile, routingProfile, profileSettings);
//		}
//
//		@Override
//		public RoutingConfiguration getRoutingConfig() {
//			RoutingConfiguration config = super.getRoutingConfig();
//			config.heuristicCoefficient = 1; // Binary A*
////			config.planRoadDirection = 1;
//			return config;
//		}
//	}
