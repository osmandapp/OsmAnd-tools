package net.osmand.router.tester;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

import net.osmand.MainUtilities.CommandLineOpts;
import net.osmand.router.*;
import net.osmand.NativeLibrary;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.PlatformUtil;

public class RandomRouteTester {
	class GeneratorConfig {
		String[] PREDEFINED_TESTS = { // optional predefined routes in "url" format (imply ITERATIONS=0)
//				"https://test.osmand.net/map/?start=48.913403,11.872949&finish=49.079640,11.752095&type=osmand&profile=car#10/48.996521/11.812522"
//				"start=48.211348,24.478998&finish=48.172382,24.421492&type=osmand&profile=bicycle&params=bicycle,height_obstacles",
//	/*example*/ "start=L,L&finish=L,L&via=L,L;L,L&profile=pedestrian&params=height_obstacles"
		};

		// random tests settings
		int ITERATIONS = 10; // number of random routes
		int MAX_INTER_POINTS = 0; // 0-2 intermediate points // (0)
		int MIN_DISTANCE_KM = 50; // min distance between start and finish (50)
		int MAX_DISTANCE_KM = 100; // max distance between start and finish (100)
		int MAX_SHIFT_ALL_POINTS_M = 500; // shift LatLon of all points by 0-500 meters (500)
		int OPTIONAL_SLOW_DOWN_THREADS = 0; // "endless" threads to slow down routing (emulate device speed) (0-100)

		String[] RANDOM_PROFILES = { // randomly selected profiles[,params] for each iteration
				"car",
				"bicycle",
//				"pedestrian",

//				"car,short_way",
//				"bicycle,short_way",
//				"pedestrian,short_way",

//				"bicycle,height_obstacles",
//				"pedestrian,height_obstacles",

//				"car,prefer_unpaved",
//				"car,allow_private",
//				"car,avoid_unpaved",
//				"car,avoid_motorway",
//				"car,weight:3.49",
//				"car,short_way",

//				"bicycle,driving_style_prefer_unpaved,driving_style_balance:false",
//				"bicycle,avoid_unpaved",
//				"bicycle,avoid_footways",
//				"bicycle,allow_motorway",
//				"bicycle,allow_private",

//				"pedestrian,avoid_unpaved",
//				"pedestrian,allow_private",
//				"pedestrian,prefer_hiking_routes",
//				"pedestrian,avoid_stairs",
//				"pedestrian,avoid_motorway",
		};

		// cost/distance deviation limits
		double DEVIATION_RED = 1.0F; // > 1% - mark as failed
		double DEVIATION_YELLOW = 0.1F; // > 0.1% - mark as acceptable

		// enable Android mode for BRP
		boolean CAR_2PHASE_MODE = false;
		long USE_TIME_CONDITIONAL_ROUTING = 1; // 0 disable, 1 auto, >1 exact time in milliseconds
//		long USE_TIME_CONDITIONAL_ROUTING = 1727816400000L; // date -d "2024-10-01 23:00:00" "+%s000L"
//		long USE_TIME_CONDITIONAL_ROUTING = 1730498400000L; // date -d "2024-11-01 23:00:00" "+%s000L"

		final static Map <String, String> ambiguousConditionalTags = null;
//		final static Map <String, String> ambiguousConditionalTags = HHRoutingPrepareContext.ambiguousConditionalTags;
	}

	public static void main(String[] args) throws Exception {
		RandomRouteTester test = new RandomRouteTester(args);

		test.applyCommandLineOpts();
		test.loadNativeLibrary();
		test.initObfReaders();
		test.generateRoutes();
		test.startSlowDown();
		test.collectRoutes();
		test.stopSlowDown();

		int exitCode = test.reportResult();
		System.exit(exitCode);
	}

	private CommandLineOpts opts;
	private String optMapsDir;
	private String optLibsDir;
	private String optObfPrefix;
	private String optHtmlReport;
	private String optHtmlDomain;
	private PrimaryRouting optPrimaryRouting;

	private enum PrimaryRouting {
		BRP_JAVA,
		BRP_CPP,
		HH_JAVA,
		HH_CPP,
	}

	private long started;
	private File obfDirectory;
	private NativeLibrary nativeLibrary = null;
	private List<BinaryMapIndexReader> obfReaders = new ArrayList<>();
	private HashMap<String, File> hhFiles = new HashMap<>(); // [Profile]

	private RandomRouteGenerator generator;
	private GeneratorConfig config = new GeneratorConfig();
	private List<RandomRouteEntry> testList = new ArrayList<>();

	private final Log LOG = PlatformUtil.getLog(RandomRouteTester.class);

	private final int EXIT_SUCCESS = 0;
	private final int EXIT_TEST_FAILED = 1;
	private final int EXIT_RED_LIMIT_REACHED = 2;

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
		optHtmlDomain = Objects.requireNonNullElse(opts.getOpt("--html-domain"), "test.osmand.net");
		optLibsDir = Objects.requireNonNullElse(
				opts.getOpt("--libs-dir"), optMapsDir + "/../core-legacy/binaries");

		// validate
		if (!isFileWriteable(optHtmlReport)) {
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
			config.RANDOM_PROFILES = new String[]{
					opts.getOpt("--profile")
			};
		}

		// apply predefined tests from command line
		if (opts.getStrings().size() > 0) {
			config.PREDEFINED_TESTS = opts.getStrings().toArray(new String[0]);
		}

		// --avoid-java --avoid-cpp --avoid-hh additionally processed by collectRoutes()
		if (opts.getOpt("--primary") != null) {
			if ("brp-java".equals(opts.getOpt("--primary"))) {
				optPrimaryRouting = PrimaryRouting.BRP_JAVA;
			} else if ("brp-cpp".equals(opts.getOpt("--primary"))) {
				optPrimaryRouting = PrimaryRouting.BRP_CPP;
			} else if ("hh-java".equals(opts.getOpt("--primary"))) {
				optPrimaryRouting = PrimaryRouting.HH_JAVA;
			} else if ("hh-cpp".equals(opts.getOpt("--primary"))) {
				optPrimaryRouting = PrimaryRouting.HH_CPP;
			}
		}

		if (opts.getOpt("--car-2phase") != null) {
			config.CAR_2PHASE_MODE = true;
		}

		if (opts.getOpt("--no-conditionals") != null) {
			config.USE_TIME_CONDITIONAL_ROUTING = 0;
		}

		if (opts.getOpt("--help") != null) {
			System.err.printf("%s\n", String.join("\n",
					"",
					"Usage: random-route-tester [--options] [TEST_ROUTE(s)]",
					"",
					"[TEST_ROUTE] run specific test (url or query_string format)",
					"",
					"--maps-dir=/path/to/directory/with/*.obf (default ./)",
					"--obf-prefix=prefix to filter obf files (default all)",
					"--html-report=/path/to/report.html (rr-report.html)",
					"--html-domain=test.osmand.net (used in html-report)",
					"--libs-dir=/path/to/native/libs/dir (default auto)",
					"",
					"--iterations=N",
					"--min-dist=N km",
					"--max-dist=N km",
					"--max-shift=N meters",
					"--max-inter=N number",
					"--profile=profile,settings,key:value force one profile",
					"",
					"--primary=(brp-java|brp-cpp|hh-java|hh-cpp) compare others against this",
					"--avoid-brp-java avoid BinaryRoutePlanner (java)",
					"--avoid-brp-cpp avoid BinaryRoutePlanner (cpp)",
					"--avoid-hh-java avoid HHRoutePlanner (java)",
					"--avoid-hh-cpp avoid HHRoutePlanner (cpp)",
					"",
					"--car-2phase use COMPLEX mode for car (Android default)",
					"--no-conditionals disable *:conditional restrictions",
					"",
					"--yellow=N % yellow-color limit",
					"--red=N % red-color limit (affects exit code)",
					"",
					"--help show help",
					""
			));
			System.exit(EXIT_SUCCESS);
		}
	}

	private int reportResult() throws IOException {
		long runTime = System.currentTimeMillis() - started;

		RandomRouteReport report = new RandomRouteReport(runTime, obfReaders.size(), testList.size(),
				config.DEVIATION_RED, config.DEVIATION_YELLOW, optHtmlDomain, config.CAR_2PHASE_MODE);

		for (int i = 0; i < testList.size(); i++) {
			report.entryOpen(i + 1);
			RandomRouteEntry entry = testList.get(i);
			for (int j = 0; j < entry.results.size(); j++) {
				RandomRouteResult primary = entry.results.get(0);
				RandomRouteResult result = entry.results.get(j);
				if (j == 0) {
					report.resultPrimary(i + 1, primary);
				} else {
					report.resultCompare(i + 1, result, primary);
				}
			}
			report.entryClose();
		}

		report.flush(optHtmlReport);

		if (report.isFailed()) {
			return EXIT_TEST_FAILED;
		} else if (report.isDeviated()) {
			return EXIT_RED_LIMIT_REACHED;
		} else {
			return EXIT_SUCCESS;
		}
	}

	private void initObfReaders() throws IOException {
		List<File> obfFiles = new ArrayList<>();

		obfDirectory = new File(optMapsDir);

		if (obfDirectory.isDirectory()) {
			for (File f : Algorithms.getSortedFilesVersions(obfDirectory)) {
				if (f.isFile() && f.getName().endsWith(".obf") && f.getName().startsWith(optObfPrefix)) {
					obfFiles.add(f);
				}
			}
		} else {
			obfFiles.add(obfDirectory);
		}

		for (File source : obfFiles) {
			System.out.printf("Use OBF %s...\n", source.getName());
			Objects.requireNonNull(nativeLibrary).initMapFile(source.getAbsolutePath(), true);
			obfReaders.add(new BinaryMapIndexReader(new RandomAccessFile(source, "r"), source));
		}

		if (obfReaders.size() == 0) {
			throw new IllegalStateException("OBF files not initialized");
		}
	}

	private void generateRoutes() {
		testList = generator.generateTestList(obfReaders);
	}

	private void startSlowDown() {
		Runnable endless = () -> {
			while (config.OPTIONAL_SLOW_DOWN_THREADS > 0) {
				for (long i = 0; i < 1_000_000_000L; i++) {
					// 1MM long-counter makes strong load
				}
				try {
					// refresh state
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		};
		for (int i = 0; i < config.OPTIONAL_SLOW_DOWN_THREADS; i++) {
			new Thread(endless).start();
		}
	}

	private void stopSlowDown() {
		config.OPTIONAL_SLOW_DOWN_THREADS = 0;
	}

	private void collectRoutes() {
		for (int i = 0; i < testList.size(); i++) {
			RandomRouteEntry entry = testList.get(i);
			try {
				// Note: this block runs a sequence of routing calls.
				// Result of 1st call is treated as a primary route.
				// Other results will be compared to the 1st.

				// if --primary option is used, --avoid-xxx is set here to avoid double-calls
				if (optPrimaryRouting != null) {
					switch (optPrimaryRouting) {
						case BRP_JAVA:
							opts.setOpt("--avoid-brp-java", "true");
							entry.results.add(runBinaryRoutePlannerJava(entry));
							break;
						case BRP_CPP:
							opts.setOpt("--avoid-brp-cpp", "true");
							entry.results.add(runBinaryRoutePlannerCpp(entry));
							break;
						case HH_JAVA:
							opts.setOpt("--avoid-hh-java", "true");
							entry.results.add(runHHRoutePlannerJava(entry));
							break;
						case HH_CPP:
							opts.setOpt("--avoid-hh-cpp", "true");
							entry.results.add(runHHRoutePlannerCpp(entry));
							break;
						default:
							throw new RuntimeException("Wrong primary routing defined");
					}
				}

				// process --avoid-xxx options
				if (opts.getOpt("--avoid-brp-java") == null) {
					entry.results.add(runBinaryRoutePlannerJava(entry));
				}
				if (opts.getOpt("--avoid-brp-cpp") == null) {
					entry.results.add(runBinaryRoutePlannerCpp(entry));
				}
				if (opts.getOpt("--avoid-hh-java") == null) {
					entry.results.add(runHHRoutePlannerJava(entry));
				}
				if (opts.getOpt("--avoid-hh-cpp") == null) {
					entry.results.add(runHHRoutePlannerCpp(entry));
				}
			} catch (IOException | InterruptedException | SQLException e) {
				throw new RuntimeException(e);
			} finally {
				System.err.println("---------------------------------------------------------------------------------");
				// verbose all route results after each cycle
				for (int j = 0; j < entry.results.size(); j++) {
					System.err.println(RandomRouteReport.resultPrimaryText(i + 1, entry.results.get(j)));
				}
				System.err.println("---------------------------------------------------------------------------------");
			}
		}
	}

	private RandomRouteResult runBinaryRoutePlannerJava(RandomRouteEntry entry) throws IOException, InterruptedException {
		return runBinaryRoutePlanner(entry, false);
	}

	private RandomRouteResult runBinaryRoutePlannerCpp(RandomRouteEntry entry) throws IOException, InterruptedException {
		return runBinaryRoutePlanner(entry, true);
	}

	private RandomRouteResult runBinaryRoutePlanner(RandomRouteEntry entry, boolean useNative) throws IOException, InterruptedException {
		long started = System.currentTimeMillis();
		final int MEM_LIMIT = RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT * 8 * 2; // ~ 4 GB

		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();
		fe.setHHRoutingConfig(null); // hhoff=true
		fe.CALCULATE_MISSING_MAPS = false;

		RoutingConfiguration.Builder builder = RoutingConfiguration.getDefault();

		RoutingConfiguration.RoutingMemoryLimits memoryLimits =
				new RoutingConfiguration.RoutingMemoryLimits(MEM_LIMIT, MEM_LIMIT);

		RoutingConfiguration config = builder.build(entry.profile, memoryLimits, entry.mapParams());

		RoutePlannerFrontEnd.RouteCalculationMode mode =
				(this.config.CAR_2PHASE_MODE && "car".equals(entry.profile)) ?
						RoutePlannerFrontEnd.RouteCalculationMode.COMPLEX :
						RoutePlannerFrontEnd.RouteCalculationMode.NORMAL;

		if (this.config.USE_TIME_CONDITIONAL_ROUTING == 1) {
			config.routeCalculationTime = System.currentTimeMillis();
		} else if (this.config.USE_TIME_CONDITIONAL_ROUTING != 0) {
			config.routeCalculationTime = this.config.USE_TIME_CONDITIONAL_ROUTING;
		}

		if (this.config.ambiguousConditionalTags != null) {
			config.ambiguousConditionalTags = this.config.ambiguousConditionalTags;
		}

		RoutingContext ctx = fe.buildRoutingContext(
				config,
				useNative ? nativeLibrary : null,
				obfReaders.toArray(new BinaryMapIndexReader[0]),
				mode
		);

//		ctx.dijkstraMode = 0; // 0 for bidirectional, +1 for direct search, -1 for reverse search
//		ctx.config.heuristicCoefficient = 1; // h() *= 1 for A*, 0 for Dijkstra
//		BinaryRoutePlanner.DEBUG_PRECISE_DIST_MEASUREMENT = false; // debug
//		BinaryRoutePlanner.DEBUG_BREAK_EACH_SEGMENT = false; // debug
//		BinaryRoutePlanner.TRACE_ROUTING = false; // make it public

		RouteResultPreparation.RouteCalcResult res = fe.searchRoute(ctx, entry.start, entry.finish, entry.via, null);
		List<RouteSegmentResult> routeSegments = res != null ? res.getList() : new ArrayList<>();
		long runTime = System.currentTimeMillis() - started;

		return new RandomRouteResult(useNative ? "brp-cpp" : "brp-java", entry, runTime, ctx, routeSegments);
	}

	private RandomRouteResult runHHRoutePlannerJava(RandomRouteEntry entry) throws SQLException, IOException, InterruptedException {
		return runHHRoutePlanner(entry, false);
	}

	private RandomRouteResult runHHRoutePlannerCpp(RandomRouteEntry entry) throws SQLException, IOException, InterruptedException {
		return runHHRoutePlanner(entry, true);
	}

	private RandomRouteResult runHHRoutePlanner(RandomRouteEntry entry, boolean useNative) throws SQLException, IOException, InterruptedException {
		long started = System.currentTimeMillis();
		final int MEM_LIMIT = RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT * 8 * 2; // ~ 4 GB

		HHRoutePlanner.DEBUG_VERBOSE_LEVEL = 1;
		HHRouteDataStructure.HHRoutingConfig.STATS_VERBOSE_LEVEL = 1;
		RouteResultPreparation.PRINT_TO_CONSOLE_ROUTE_INFORMATION = true;

		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();
		fe.CALCULATE_MISSING_MAPS = false;
		fe.setDefaultHHRoutingConfig();
		fe.setUseOnlyHHRouting(true);

		RoutingConfiguration.Builder builder = RoutingConfiguration.getDefault();

		RoutingConfiguration.RoutingMemoryLimits memoryLimits =
				new RoutingConfiguration.RoutingMemoryLimits(MEM_LIMIT, MEM_LIMIT);

		RoutingConfiguration config = builder.build(entry.profile, memoryLimits, entry.mapParams());

		if (this.config.USE_TIME_CONDITIONAL_ROUTING == 1) {
			config.routeCalculationTime = System.currentTimeMillis();
		} else if (this.config.USE_TIME_CONDITIONAL_ROUTING != 0) {
			config.routeCalculationTime = this.config.USE_TIME_CONDITIONAL_ROUTING;
		}

		if (this.config.ambiguousConditionalTags != null) {
			config.ambiguousConditionalTags = this.config.ambiguousConditionalTags;
		}

		RoutingContext ctx = fe.buildRoutingContext(
				config,
				useNative ? nativeLibrary : null,
				obfReaders.toArray(new BinaryMapIndexReader[0]),
				RoutePlannerFrontEnd.RouteCalculationMode.NORMAL
		);

		fe.setHHRouteCpp(useNative);

//		RoutePlannerFrontEnd.HH_ROUTING_CONFIG = null; // way to set config for hh

		RouteResultPreparation.RouteCalcResult res = fe.searchRoute(ctx, entry.start, entry.finish, entry.via, null);
		List<RouteSegmentResult> routeSegments = res != null ? res.getList() : new ArrayList<>();
		long runTime = System.currentTimeMillis() - started;

		return new RandomRouteResult(useNative ? "hh-cpp" : "hh-java", entry, runTime, ctx, routeSegments);
	}

	private void loadNativeLibrary() {
		String nativePath = getNativeLibPath();
		if (NativeLibrary.loadOldLib(nativePath)) {
			nativeLibrary = new NativeLibrary();
			return; // success
		}
		throw new IllegalStateException("Native library not loaded");
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
