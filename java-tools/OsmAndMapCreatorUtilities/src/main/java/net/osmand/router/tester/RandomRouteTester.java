package net.osmand.router.tester;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import net.osmand.router.*;
import net.osmand.NativeLibrary;
import org.apache.commons.logging.Log;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.PlatformUtil;

public class RandomRouteTester {
	class GeneratorConfig {
		String[] PREDEFINED_TESTS = { // optional predefined routes in "url" format (imply ITERATIONS=0)
//				"https://test.osmand.net/map/?start=48.211348,24.478998&finish=48.172382,24.421492&type=osmand&profile=bicycle&params=bicycle,height_obstacles#14/48.1852/24.4208",
//				"https://osmand.net/map/?start=50.450128,30.535611&finish=50.460479,30.589365&via=50.452647,30.588330&type=osmand&profile=car#14/50.4505/30.5511",
//				"start=48.211348,24.478998&finish=48.172382,24.421492&type=osmand&profile=bicycle&params=bicycle,height_obstacles",
//				"start=50.450128,30.535611&finish=50.460479,30.589365&via=50.452647,30.588330&profile=car",
//				"start=50.450128,30.535611&finish=50.460479,30.589365&via=1,2;3,4;5,6&profile=car",
//				"start=L,L&finish=L,L&via=L,L;L,L&profile=pedestrian&params=height_obstacles" // example
		};

		// random tests settings
		int ITERATIONS = 10; // number of random routes
		int MAX_INTER_POINTS = 2; // 0-2 intermediate points // (2)
		int MIN_DISTANCE_KM = 10; // min distance between start and finish (50)
		int MAX_DISTANCE_KM = 20; // max distance between start and finish (100)
		int MAX_SHIFT_ALL_POINTS_M = 500; // shift LatLon of all points by 0-500 meters (500)
		String[] RANDOM_PROFILES = { // randomly selected profiles[,params] for each iteration
				"car",
				"bicycle",
//				"bicycle,height_obstacles",
//				"bicycle,driving_style_prefer_unpaved,driving_style_balance:false,height_obstacles",
//				"bicycle,driving_style_prefer_unpaved,driving_style_balance=false,height_obstacles",
		};

		// cost/distance deviation limits
		double DEVIATION_RED = 1.0F; // > 1% - mark as failed
		double DEVIATION_YELLOW = 0.1F; // > 0.1% - mark as acceptable
	}

	class CommandLineOpts {
		public String getOpt(String key) {
			return opts.get(key);
		}

		public List<String> getStrings() {
			return strings;
		}

		public CommandLineOpts(String[] args) {
			for (String a : args) {
				if (a.startsWith("--")) {
					if (a.contains("=")) {
						String[] keyval = a.split("=");
						opts.put(keyval[0], keyval[1]); // --opt=value
					} else {
						opts.put(a, "true"); // --opt
					}
				} else {
					strings.add(a);
				}
			}
		}

		private HashMap<String, String> opts = new HashMap<>(); // --opt=value --opt[=true]
		private List<String> strings = new ArrayList<>(); // other args not parsed as opts
	}

	public static void main(String[] args) throws Exception {
		RandomRouteTester test = new RandomRouteTester(args);

		test.applyCommandLineOpts();
		test.loadNativeLibrary();
		test.initObfReaders();
		test.generateRoutes();
		test.collectRoutes();
		test.reportResult();
	}

	CommandLineOpts opts;
	private String optMapsDir;
	private String optLibsDir;
	private String optObfPrefix;
	private String optHtmlReport;

	private long started;
	private File obfDirectory;
	NativeLibrary nativeLibrary = null;
	private List<BinaryMapIndexReader> obfReaders = new ArrayList<>();
	private HashMap<String, File> hhFiles = new HashMap<>(); // [Profile]
//	private HashMap<String, Connection> hhConnections = new HashMap<>(); // [Profile]

	private RandomRouteGenerator generator;
	private GeneratorConfig config = new GeneratorConfig();
	private List<RandomRouteEntry> testList = new ArrayList<>();

	private final Log LOG = PlatformUtil.getLog(RandomRouteTester.class);

	private RandomRouteTester(String[] args) {
		this.opts = new CommandLineOpts(args);
		this.started = System.currentTimeMillis();
		this.generator = new RandomRouteGenerator(config);
	}

	private Boolean isFileWriteable(String name) {
		Path path = Paths.get(name);
		if (Files.exists(path) && Files.isWritable(path)) {
			return true;
		}
		Path parent = path.normalize().toAbsolutePath().getParent();
		if (Files.exists(parent) && Files.isWritable(parent)) {
			return true;
		}
		return false;
	}

	private void applyCommandLineOpts() {
		// apply system options
		optMapsDir = Objects.requireNonNullElse(opts.getOpt("--maps-dir"), "./");
		optObfPrefix = Objects.requireNonNullElse(opts.getOpt("--obf-prefix"), "");
		optHtmlReport = Objects.requireNonNullElse(opts.getOpt("--html-report"), "rr-report.html");
		optLibsDir = Objects.requireNonNullElse(
				opts.getOpt("--libs-dir"), optMapsDir + "/../core-legacy/binaries");

		// validate
		if (false == isFileWriteable(optHtmlReport)) {
			throw new IllegalStateException(optHtmlReport + " (html-report) file is not writable");
		}

		// apply generator options
		config.ITERATIONS = Integer.parseInt(Objects.requireNonNullElse(
				opts.getOpt("--iterations"), String.valueOf(config.ITERATIONS)));
		config.MIN_DISTANCE_KM = Integer.parseInt(Objects.requireNonNullElse(
				opts.getOpt("--min-dist"), String.valueOf(config.MIN_DISTANCE_KM)));
		config.MAX_DISTANCE_KM = Integer.parseInt(Objects.requireNonNullElse(
				opts.getOpt("--max-dist"), String.valueOf(config.MAX_DISTANCE_KM)));
		config.MAX_INTER_POINTS = Integer.parseInt(Objects.requireNonNullElse(
				opts.getOpt("--max-inter"), String.valueOf(config.MAX_INTER_POINTS)));
		config.MAX_SHIFT_ALL_POINTS_M = Integer.parseInt(Objects.requireNonNullElse(
				opts.getOpt("--max-shift"), String.valueOf(config.MAX_SHIFT_ALL_POINTS_M)));
		config.DEVIATION_RED = Double.parseDouble(Objects.requireNonNullElse(
				opts.getOpt("--red"), String.valueOf(config.DEVIATION_RED)));
		config.DEVIATION_YELLOW = Double.parseDouble(Objects.requireNonNullElse(
				opts.getOpt("--yellow"), String.valueOf(config.DEVIATION_YELLOW)));
		if (opts.getOpt("--profile") != null) {
			config.RANDOM_PROFILES = new String[] {
					opts.getOpt("--profile")
			};
		}

		// apply predefined tests from command line
		if (opts.getStrings().size() > 0) {
			config.PREDEFINED_TESTS = opts.getStrings().toArray(new String[0]);
		}

		/**
		 *  Usage: random-route-tester [--options] [TEST_ROUTE(s)]
		 *
		 *  --maps-dir=/path/to/directory/with/*.obf (default ./)
		 *  --obf-prefix=prefix to filter obf files (default all)
		 *  --html-report=/path/to/report.html (rr-report.html)
		 *  --libs-dir=/path/to/native/libs/dir (default auto)
		 *
		 *  --iterations=N
		 *  --min-dist=N km
		 *  --max-dist=N km
		 *  --max-shift=N meters
		 *  --max-inter=N number
		 *  --profile=profile,settings,key:value force one profile
		 *
		 *  --ideal= TYPE (java, cpp, hh)
		 *  --run-java
		 *  --run-cpp
		 *  --run-hh
		 *
		 *  --red= % red-limit
		 *  --yellow= % yellow-limit
		 *
		 *  [TEST_ROUTE] run specific test (url or query_string format)
		 */
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

		report.flush(optHtmlReport);
	}

	private void initObfReaders() throws IOException {
		List<File> obfFiles = new ArrayList<>();

		obfDirectory = new File(optMapsDir);

		if (obfDirectory.isDirectory()) {
			for (File f : obfDirectory.listFiles()) {
				if (f.isFile() && f.getName().endsWith(".obf") && f.getName().startsWith(optObfPrefix)) {
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

//	private void initHHsqliteConnections() throws SQLException {
//		List<File> sqliteFiles = new ArrayList<>();
//
//		if (obfDirectory.isDirectory()) {
//			for (File f : obfDirectory.listFiles()) {
//				if (f.isFile() && f.getName().endsWith(HHRoutingDB.EXT)) {
//					sqliteFiles.add(f);
//				}
//			}
//		}
//
//		// sort files by name to improve pseudo-random reproducibility
//		sqliteFiles.sort((f1, f2) -> f1.getName().compareTo(f2.getName()));
//
//		for (File source : sqliteFiles) {
//			String[] parts = source.getName().split("[_.]"); // Maps_PROFILE.hhdb
//			if (parts.length > 2) {
//				String profile = parts[parts.length - 2];
//				System.out.printf("Use HH (%s) %s...\n", profile, source.getName());
//				hhConnections.put(profile, DBDialect.SQLITE.getDatabaseConnection(source.getAbsolutePath(), LOG));
//				hhFiles.put(profile, source);
//			}
//		}
//
//		if (hhConnections.size() == 0) {
//			throw new IllegalStateException("empty hhConnections");
//		}
//	}

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
		Path path = FileSystems.getDefault().getPath(optLibsDir);
		if (!Files.exists(path)) {
			// for default path, try ../core-legacy/binaries together with ../../core-legacy/binaries
			path = FileSystems.getDefault().getPath(optLibsDir.replaceFirst("../", "../../"));
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
			if (Files.isDirectory(path)) {
				return nativeLibPath.getAbsolutePath(); // fallback when final libs directory was specified
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