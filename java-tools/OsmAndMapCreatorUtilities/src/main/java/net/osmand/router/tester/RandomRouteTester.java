package net.osmand.router.tester;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import net.osmand.MainUtilities.CommandLineOpts;
import net.osmand.PlatformUtil;
import net.osmand.NativeLibrary;
import net.osmand.router.FastRoutingState;
import net.osmand.router.GeneralRouter;
import net.osmand.router.HHRouteDataStructure;
import net.osmand.router.HHRoutePlanner;
import net.osmand.router.NativeTransportRoutingResult;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RouteResultPreparation;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingContext;
import net.osmand.router.TransportRoutePlanner;
import net.osmand.router.TransportRouteResult;
import net.osmand.router.TransportRoutingConfiguration;
import net.osmand.router.TransportRoutingContext;
import net.osmand.util.Algorithms;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.util.MapUtils;

public class RandomRouteTester {
	static final String PUBLIC_TRANSPORT_PROFILE = "public_transport";
	static final String PUBLIC_TRANSPORT_PROFILE_UNLIMITED = PUBLIC_TRANSPORT_PROFILE + ",pt_limit=0";

	static final int MEM_LIMIT = RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT * 8 * 2; // ~ 4 GB

	static class GeneratorConfig {
		String[] PREDEFINED_TESTS = { // optional predefined routes in "url" format (imply ITERATIONS=0)
//				"https://test.osmand.net/map/?start=48.913403,11.872949&finish=49.079640,11.752095&type=osmand&profile=car#10/48.996521/11.812522"
//				"start=48.211348,24.478998&finish=48.172382,24.421492&type=osmand&profile=bicycle&params=bicycle,height_obstacles",
//	/*example*/ "start=L,L&finish=L,L&via=L,L;L,L&profile=pedestrian&params=height_obstacles"
		};

		// random tests settings
		int ITERATIONS = 10; // number of random routes
		int MAX_INTER_POINTS = 0; // 0-2 intermediate points // (0)
		int MIN_DISTANCE_KM = 10; // min distance between start and finish (10)
		int MAX_DISTANCE_KM = 20; // max distance between start and finish (20)
		int MAX_SHIFT_ALL_POINTS_M = 500; // shift LatLon of all points by 0-500 meters (500)
		int OPTIONAL_SLOW_DOWN_THREADS = 0; // "endless" threads to slow down routing (emulate device speed) (0-100)

		String[] RANDOM_PROFILES = { // randomly selected profiles[,params] for each iteration
//				"car",
//				"bicycle",
//				"car,short_way",
//				"bicycle,short_way",
//				"pedestrian,short_way",
				PUBLIC_TRANSPORT_PROFILE_UNLIMITED, // disable other profiles to test PT routes
		};

		// cost/distance deviation limits
		double DEVIATION_RED = 5.0F; // > 5% - mark as failed
		double DEVIATION_YELLOW = 1.0F; // > 1% - mark as acceptable

		// enable Android mode for BRP
		boolean CAR_2PHASE_MODE = false;
		long USE_TIME_CONDITIONAL_ROUTING = 1; // 0 disable, 1 auto, >1 exact time in milliseconds
//		long USE_TIME_CONDITIONAL_ROUTING = 1727816400000L; // date -d "2024-10-01 23:00:00" "+%s000L"
//		long USE_TIME_CONDITIONAL_ROUTING = 1730498400000L; // date -d "2024-11-01 23:00:00" "+%s000L"

		final static Map<String, String> ambiguousConditionalTags = null;
//		final static Map <String, String> ambiguousConditionalTags = HHRoutingPrepareContext.ambiguousConditionalTags;

		RandomPointsSource optRandomPointsSource = RandomPointsSource.ROUTE_SECTION_POINTS;
	}

	public static void main(String[] args) throws Exception {
		System.exit(run(args));
	}

	public static int run(String[] args) throws Exception {
		RandomRouteTester test = new RandomRouteTester(args);

		test.applyCommandLineOpts();
		test.loadNativeLibrary();
		test.initObfReaders();
		test.generateRoutes();
		test.startSlowDown();
		test.collectRoutes();
		test.stopSlowDown();

		return test.reportResult();
	}

	private final CommandLineOpts opts;
	private String optMapsDir;
	private String optLibsDir;
	private String optObfPrefix;
	private String optHtmlReport;
	private String optHtmlDomain;
	private boolean optNoHtmlReport;
	private boolean optNoNativeLibrary;
	private boolean optStopAtFirstRoute;
	private PrimaryRouting optPrimaryRouting;

	private enum PrimaryRouting {
		BRP_JAVA,
		BRP_CPP,
		BRP_SHARED,
		HH_JAVA,
		HH_CPP,
		HH_SHARED,
	}

	public enum RandomPointsSource {
		ROUTE_SECTION_POINTS,
		HH_SECTION_POINTS,
	}

	private final long started;
	private NativeLibrary nativeLibrary = null;
	private final List<BinaryMapIndexReader> obfReaders = new ArrayList<>();
	private final List<File> obfFiles = new ArrayList<>();
	private List<net.osmand.shared.binary.BinaryMapIndexReader> sharedReaders = null;

	private final RandomRouteGenerator generator;
	private final GeneratorConfig config = new GeneratorConfig();
	private List<RandomRouteEntry> testList = new ArrayList<>();

//	private final Log LOG = PlatformUtil.getLog(RandomRouteTester.class);

	public static final int EXIT_SUCCESS = 0;
	public static final int EXIT_TEST_FAILED = 1;
	public static final int EXIT_RED_LIMIT_REACHED = 2;

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
		return Files.exists(parent) && Files.isWritable(parent);
	}

	private void applyCommandLineOpts() {
		// apply system options
		optMapsDir = opts.getOrDefault("--maps-dir", "./");
		optObfPrefix = opts.getOrDefault("--obf-prefix", "");
		optHtmlReport = opts.getOrDefault("--html-report", "rr-report.html");
		optHtmlDomain = opts.getOrDefault("--html-domain", "test.osmand.net");
		optLibsDir = opts.getOrDefault("--libs-dir", optMapsDir + "/../core-legacy/binaries");

		optNoHtmlReport = opts.getBoolean("--no-html-report");
		optNoNativeLibrary = opts.getBoolean("--no-native-library");
		optStopAtFirstRoute = opts.getBoolean("--stop-at-first-route");

		if (opts.getBoolean("--use-hh-points")) {
			config.optRandomPointsSource = RandomPointsSource.HH_SECTION_POINTS;
		}

		// validate
		if (!isFileWriteable(optHtmlReport)) {
			throw new IllegalStateException(optHtmlReport + " (html-report) file is not writable");
		}

		// apply generator options
		config.ITERATIONS = opts.getIntOrDefault("--iterations", config.ITERATIONS);
		config.MIN_DISTANCE_KM = opts.getIntOrDefault("--min-dist", config.MIN_DISTANCE_KM);
		config.MAX_DISTANCE_KM = opts.getIntOrDefault("--max-dist", config.MAX_DISTANCE_KM);
		config.MAX_INTER_POINTS = opts.getIntOrDefault("--max-inter", config.MAX_INTER_POINTS);
		config.MAX_SHIFT_ALL_POINTS_M = opts.getIntOrDefault("--max-shift", config.MAX_SHIFT_ALL_POINTS_M);

		config.DEVIATION_RED = Algorithms.parseDoubleSilently(opts.getOpt("--red"), config.DEVIATION_RED);
		config.DEVIATION_YELLOW = Algorithms.parseDoubleSilently(opts.getOpt("--yellow"), config.DEVIATION_YELLOW);

		if (opts.getOpt("--profile") != null) {
			config.RANDOM_PROFILES = new String[]{
					opts.getOpt("--profile")
			};
		}

		// apply predefined tests from command line
		if (!opts.getStrings().isEmpty()) {
			config.PREDEFINED_TESTS = opts.getStrings().toArray(new String[0]);
		}

		// --avoid-java --avoid-cpp --avoid-hh additionally processed by collectRoutes()
		if (opts.getOpt("--primary") != null) {
			if ("brp-java".equals(opts.getOpt("--primary"))) {
				optPrimaryRouting = PrimaryRouting.BRP_JAVA;
			} else if ("brp-cpp".equals(opts.getOpt("--primary"))) {
				optPrimaryRouting = PrimaryRouting.BRP_CPP;
			} else if ("brp-shared".equals(opts.getOpt("--primary"))) {
				optPrimaryRouting = PrimaryRouting.BRP_SHARED;
			} else if ("hh-shared".equals(opts.getOpt("--primary"))) {
				optPrimaryRouting = PrimaryRouting.HH_SHARED;
			} else if ("hh-java".equals(opts.getOpt("--primary"))) {
				optPrimaryRouting = PrimaryRouting.HH_JAVA;
			} else if ("hh-cpp".equals(opts.getOpt("--primary"))) {
				optPrimaryRouting = PrimaryRouting.HH_CPP;
			}
		}

		if (opts.getBoolean("--car-2phase")) {
			config.CAR_2PHASE_MODE = true;
		}

		if (opts.getBoolean("--no-conditionals")) {
			config.USE_TIME_CONDITIONAL_ROUTING = 0;
		}

		if (opts.getBoolean("--help")) {
			System.err.printf("%s\n", String.join("\n",
					"",
					"Usage: random-route-tester [--options] [TEST_ROUTE(s)]",
					"",
					"[TEST_ROUTE] run specific test (url or query_string format)",
					"",
					"--maps-dir=/path/to/directory/with/*.obf (default ./)",
					"--obf-prefix=prefix to filter obf files (default all)",
					"",
					"--no-html-report do not create html report",
					"--html-report=/path/to/report.html (rr-report.html)",
					"--html-domain=test.osmand.net (route-href in html-report)",
					"",
					"--no-native-library useful for Java-only tests",
					"--libs-dir=/path/to/native/libs/dir (default auto)",
					"",
					"--iterations=N",
					"--min-dist=N km",
					"--max-dist=N km",
					"--max-shift=N meters",
					"--max-inter=N number",
					"--profile=profile,settings,key:value force one profile",
					"--profile=public_transport,settings,key:value run PT test",
					"--stop-at-first-route stop iterations and return 1st calculated route",
					"",
					"--primary=(brp-java|brp-cpp|brp-shared|hh-java|hh-cpp|hh-shared) compare others against this",
					"--avoid-brp-java avoid BinaryRoutePlanner (java)",
					"--avoid-brp-cpp avoid BinaryRoutePlanner (cpp)",
					"--avoid-brp-shared avoid BinaryRoutePlanner (OsmAnd-shared)",
					"--avoid-hh-java avoid HHRoutePlanner (java)",
					"--avoid-hh-cpp avoid HHRoutePlanner (cpp)",
					"--avoid-hh-shared avoid HHRoutePlanner (OsmAnd-shared)",
					"",
					"--use-hh-points use random HH-points instead of highway-points",
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
			for (int j = 0; j < entry.routeResults.size(); j++) {
				RandomRouteResult primary = entry.routeResults.get(0);
				RandomRouteResult result = entry.routeResults.get(j);
				if (j == 0) {
					report.resultPrimary(i + 1, primary);
				} else {
					report.resultCompare(i + 1, result, primary);
				}
			}
			for (int j = 0; j < entry.transportResults.size(); j++) {
				RandomRouteResult primary = entry.transportResults.get(0);
				RandomRouteResult result = entry.transportResults.get(j);
				if (j == 0) {
					report.resultPrimary(i + 1, primary);
				} else {
					report.resultCompare(i + 1, result, primary);
				}
			}
			report.entryClose();
		}

		if (!optNoHtmlReport) {
			report.flush(optHtmlReport);
		}

		if (report.isFailed()) {
			return EXIT_TEST_FAILED;
		} else if (report.isDeviated()) {
			return EXIT_RED_LIMIT_REACHED;
		} else {
			return EXIT_SUCCESS;
		}
	}

	private void initObfReaders() throws IOException {
		File obfDirectory = new File(optMapsDir);

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
			System.out.printf("Use OBF %s ...\n", source.getName());
			if (nativeLibrary != null) {
				nativeLibrary.initMapFile(source.getAbsolutePath(), true);
			}
			obfReaders.add(new BinaryMapIndexReader(new RandomAccessFile(source, "r"), source));
		}

		if (obfReaders.isEmpty()) {
			throw new IllegalStateException("OBF files not initialized");
		}
	}

	private void generateRoutes() {
		testList = generator.generateTestList(obfReaders);
	}

	private void startSlowDown() {
		Runnable endless = () -> {
			while (config.OPTIONAL_SLOW_DOWN_THREADS > 0) {
				// noinspection StatementWithEmptyBody
				for (long i = 0; i < 1_000_000_000L; i++) {
					// 1MM long-counter makes strong load
				}
				try {
					// noinspection BusyWait (refresh state)
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

	private void addResult(RandomRouteEntry entry, RandomRouteResult result) {
		if (result != null) {
			entry.routeResults.add(result);
		}
	}

	private void collectRoutes() {
		for (int i = 0; i < testList.size(); i++) {
			RandomRouteEntry entry = testList.get(i);

			if (entry.isPublicTransport()) {
				opts.setOpt("--avoid-hh-java", "true");
				opts.setOpt("--avoid-hh-cpp", "true");
				opts.setOpt("--avoid-brp-shared", "true"); // public transport is not in the shared copy
				opts.setOpt("--avoid-hh-shared", "true");
			}

			try {
				// Note: this block runs a sequence of routing calls.
				// Result of 1st call is treated as a primary route.
				// Other results will be compared to the 1st.

				// if --primary option is used, --avoid-xxx is set here to avoid double-calls
				if (optPrimaryRouting != null) {
					switch (optPrimaryRouting) {
						case BRP_JAVA:
							opts.setOpt("--avoid-brp-java", "true");
							if (entry.isPublicTransport()) {
								entry.transportResults.add(runTransportRoutePlannerJava(entry));
							} else {
								entry.routeResults.add(runBinaryRoutePlannerJava(entry));
							}
							break;
						case BRP_CPP:
							opts.setOpt("--avoid-brp-cpp", "true");
							if (entry.isPublicTransport()) {
								entry.transportResults.add(runTransportRoutePlannerCpp(entry));
							} else {
								entry.routeResults.add(runBinaryRoutePlannerCpp(entry));
							}
							break;
						case BRP_SHARED:
							opts.setOpt("--avoid-brp-shared", "true");
							entry.routeResults.add(runBinaryRoutePlannerShared(entry));
							break;
						case HH_SHARED:
							opts.setOpt("--avoid-hh-shared", "true");
							addResult(entry, runHHRoutePlannerShared(entry));
							break;
						case HH_JAVA:
							opts.setOpt("--avoid-hh-java", "true");
							addResult(entry, runHHRoutePlannerJava(entry));
							break;
						case HH_CPP:
							opts.setOpt("--avoid-hh-cpp", "true");
							addResult(entry, runHHRoutePlannerCpp(entry));
							break;
						default:
							throw new RuntimeException("Wrong primary routing defined");
					}
				}

				// process --avoid-xxx options
				if (!opts.getBoolean("--avoid-brp-java")) {
					if (entry.isPublicTransport()) {
						entry.transportResults.add(runTransportRoutePlannerJava(entry));
					} else {
						entry.routeResults.add(runBinaryRoutePlannerJava(entry));
					}
				}
				if (!opts.getBoolean("--avoid-brp-cpp")) {
					if (entry.isPublicTransport()) {
						entry.transportResults.add(runTransportRoutePlannerCpp(entry));
					} else {
						entry.routeResults.add(runBinaryRoutePlannerCpp(entry));
					}
				}
				if (!opts.getBoolean("--avoid-brp-shared")) {
					entry.routeResults.add(runBinaryRoutePlannerShared(entry));
				}
				if (!opts.getBoolean("--avoid-hh-java")) {
					addResult(entry, runHHRoutePlannerJava(entry));
				}
				if (!opts.getBoolean("--avoid-hh-cpp")) {
					addResult(entry, runHHRoutePlannerCpp(entry));
				}
				if (!opts.getBoolean("--avoid-hh-shared")) {
					addResult(entry, runHHRoutePlannerShared(entry));
				}
			} catch (IOException | InterruptedException | SQLException e) {
				throw new RuntimeException(e);
			} finally {
				if (!entry.routeResults.isEmpty() || !entry.transportResults.isEmpty()) {
					String sep = "---------------------------------------------------------------------------------";
					System.err.println(sep);
					// verbose all route results after each cycle
					for (int j = 0; j < entry.routeResults.size(); j++) {
						System.err.println(RandomRouteReport.resultPrimaryText(i + 1, entry.routeResults.get(j)));
					}
					for (int j = 0; j < entry.transportResults.size(); j++) {
						System.err.println(RandomRouteReport.resultPrimaryText(i + 1, entry.transportResults.get(j)));
					}
					System.err.println(sep);
				}
			}
			if (optStopAtFirstRoute && !entry.routeResults.isEmpty() && entry.routeResults.get(0).distance > 0) {
				testList = new ArrayList<>(List.of(entry));
				break;
			}
		}
	}

	private RandomRouteResult runTransportRoutePlannerJava(RandomRouteEntry entry) throws IOException, InterruptedException {
		return runTransportRoutePlanner(entry, false);
	}

	private RandomRouteResult runTransportRoutePlannerCpp(RandomRouteEntry entry) throws IOException, InterruptedException {
		return runTransportRoutePlanner(entry, true);
	}

	private RandomRouteResult runBinaryRoutePlannerJava(RandomRouteEntry entry) throws IOException, InterruptedException {
		return runBinaryRoutePlanner(entry, false);
	}

	private RandomRouteResult runBinaryRoutePlannerCpp(RandomRouteEntry entry) throws IOException, InterruptedException {
		return runBinaryRoutePlanner(entry, true);
	}

	private RandomRouteResult runTransportRoutePlanner(RandomRouteEntry entry, boolean useNative) throws IOException, InterruptedException {
		long started = System.currentTimeMillis();

		RoutingConfiguration.Builder builder = RoutingConfiguration.getDefault();

		GeneralRouter ptRouter = builder.getRouter(PUBLIC_TRANSPORT_PROFILE);
		Map<String, String> routeParameters = getDefaultParameters(ptRouter);
		routeParameters.putAll(entry.mapParams());

		TransportRoutingConfiguration cfg = new TransportRoutingConfiguration(ptRouter, routeParameters);

		TransportRoutingContext ctx = new TransportRoutingContext(cfg, nativeLibrary,
				obfReaders.toArray(new BinaryMapIndexReader[0]));
		TransportRoutePlanner planner = new TransportRoutePlanner();

		List<TransportRouteResult> results;
		if (useNative) {
			NativeTransportRoutingResult[] nativeRes = ctx.library.runNativePTRouting(
					MapUtils.get31TileNumberX(entry.start.getLongitude()),
					MapUtils.get31TileNumberY(entry.start.getLatitude()),
					MapUtils.get31TileNumberX(entry.finish.getLongitude()),
					MapUtils.get31TileNumberY(entry.finish.getLatitude()),
					cfg, null);
			results = TransportRoutePlanner.convertToTransportRoutingResult(nativeRes, cfg);
		} else {
			results = planner.buildRoute(ctx, entry.start, entry.finish);
		}

		long runTime = System.currentTimeMillis() - started;
		return new RandomRouteResult(useNative ? "transport-cpp" : "transport-java", entry, runTime, results);
	}

	private Map<String, String> getDefaultParameters(GeneralRouter router) {
		Map<String, String> params = new LinkedHashMap<>();
		for (Map.Entry<String, GeneralRouter.RoutingParameter> entry : router.getParameters().entrySet()) {
			String key = entry.getKey();
			GeneralRouter.RoutingParameter rp = entry.getValue();
			if (rp.getType() == GeneralRouter.RoutingParameterType.BOOLEAN) {
				if (rp.getDefaultBoolean()) {
					params.put(key, "true");
				}
			} else if (rp.getDefaultNumeric() > 0) {
				params.put(key, rp.getDefaultString());
			}
		}
		return params;
	}

	private void applyImpassableRoads(RoutingContext ctx, RandomRouteEntry entry) {
		if (entry.avoidRoads.isEmpty() || !(ctx.getRouter() instanceof GeneralRouter router)) {
			return;
		}
		Set<Long> impassableRoads = new HashSet<>(entry.avoidRoads);
		router.setImpassableRoads(impassableRoads);
	}

	private RoutingConfiguration buildRoutingConfiguration(RoutingConfiguration.Builder builder,
			RandomRouteEntry entry, RoutingConfiguration.RoutingMemoryLimits memoryLimits) {
		GeneralRouter router = builder.getRouter(entry.profile);
		Map<String, String> routeParameters = getDefaultParameters(router);
		for (Map.Entry<String, String> e : entry.mapParams().entrySet()) {
			if ("false".equalsIgnoreCase(e.getValue())) {
				routeParameters.remove(e.getKey());
			} else {
				routeParameters.put(e.getKey(), e.getValue());
			}
		}
		return builder.build(entry.profile, memoryLimits, routeParameters);
	}

	private RandomRouteResult runBinaryRoutePlanner(RandomRouteEntry entry, boolean useNative) throws IOException, InterruptedException {
		long started = System.currentTimeMillis();

		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();
		RoutePlannerFrontEnd.CALCULATE_MISSING_MAPS = false;
		fe.setHHRoutingConfig(null); // hhoff=true

		RoutingConfiguration.Builder builder = RoutingConfiguration.getDefault();

		RoutingConfiguration.RoutingMemoryLimits memoryLimits =
				new RoutingConfiguration.RoutingMemoryLimits(MEM_LIMIT, MEM_LIMIT);

		RoutingConfiguration config = buildRoutingConfiguration(builder, entry, memoryLimits);

		RoutePlannerFrontEnd.RouteCalculationMode mode =
				(this.config.CAR_2PHASE_MODE && "car".equals(entry.profile)) ?
						RoutePlannerFrontEnd.RouteCalculationMode.COMPLEX :
						RoutePlannerFrontEnd.RouteCalculationMode.NORMAL;

		if (this.config.USE_TIME_CONDITIONAL_ROUTING == 1) {
			config.routeCalculationTime = System.currentTimeMillis();
		} else if (this.config.USE_TIME_CONDITIONAL_ROUTING != 0) {
			config.routeCalculationTime = this.config.USE_TIME_CONDITIONAL_ROUTING;
		}

		// noinspection ConstantConditions
		if (GeneratorConfig.ambiguousConditionalTags != null) {
			config.ambiguousConditionalTags = GeneratorConfig.ambiguousConditionalTags;
		}

		RoutingContext ctx = fe.buildRoutingContext(
				config,
				useNative ? nativeLibrary : null,
				obfReaders.toArray(new BinaryMapIndexReader[0]),
				mode
		);
		applyImpassableRoads(ctx, entry);

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

	private RandomRouteResult runBinaryRoutePlannerShared(RandomRouteEntry entry) throws IOException {
		return runSharedRoutePlanner(entry, false);
	}

	private RandomRouteResult runHHRoutePlannerShared(RandomRouteEntry entry) throws IOException {
		return runSharedRoutePlanner(entry, true);
	}

	/** The readers of the shared copy over the same obf files; it opens its own and keeps them. */
	private List<net.osmand.shared.binary.BinaryMapIndexReader> sharedReaders() throws IOException {
		if (sharedReaders == null) {
			sharedReaders = new ArrayList<>();
			for (File source : obfFiles) {
				sharedReaders.add(new net.osmand.shared.binary.BinaryMapIndexReader(source.getAbsolutePath()));
			}
		}
		return sharedReaders;
	}

	/**
	 * The route of the OsmAnd-shared copy of the planner - the one iOS runs - over its own readers
	 * of the same maps, by the A* search or over the hub graph as the java and C++ runs above.
	 */
	private RandomRouteResult runSharedRoutePlanner(RandomRouteEntry entry, boolean hh) throws IOException {
		long started = System.currentTimeMillis();

		net.osmand.shared.routing.HHRoutePlanner.DEBUG_VERBOSE_LEVEL = hh ? 1 : 0;
		net.osmand.shared.routing.HHRoutingConfig.STATS_VERBOSE_LEVEL = hh ? 1 : 0;

		net.osmand.shared.routing.RoutePlannerFrontEnd fe = new net.osmand.shared.routing.RoutePlannerFrontEnd();
		net.osmand.shared.routing.RoutePlannerFrontEnd.CALCULATE_MISSING_MAPS = false;
		if (hh) {
			fe.setDefaultHHRoutingConfig();
			fe.setUseOnlyHHRouting(true);
		} else {
			fe.setHHRoutingConfig(null);
		}

		net.osmand.shared.routing.RoutingConfiguration.Builder builder =
				net.osmand.shared.routing.RoutingConfiguration.getDefault();

		net.osmand.shared.routing.RoutingConfiguration.RoutingMemoryLimits memoryLimits =
				new net.osmand.shared.routing.RoutingConfiguration.RoutingMemoryLimits(MEM_LIMIT, MEM_LIMIT);

		net.osmand.shared.routing.RoutingConfiguration config =
				buildSharedRoutingConfiguration(builder, entry, memoryLimits);

		net.osmand.shared.routing.RouteCalculationMode mode =
				(!hh && this.config.CAR_2PHASE_MODE && "car".equals(entry.profile)) ?
						net.osmand.shared.routing.RouteCalculationMode.COMPLEX :
						net.osmand.shared.routing.RouteCalculationMode.NORMAL;

		if (this.config.USE_TIME_CONDITIONAL_ROUTING == 1) {
			config.routeCalculationTime = System.currentTimeMillis();
		} else if (this.config.USE_TIME_CONDITIONAL_ROUTING != 0) {
			config.routeCalculationTime = this.config.USE_TIME_CONDITIONAL_ROUTING;
		}

		// noinspection ConstantConditions
		if (GeneratorConfig.ambiguousConditionalTags != null) {
			config.ambiguousConditionalTags = new LinkedHashMap<>(GeneratorConfig.ambiguousConditionalTags);
		}

		net.osmand.shared.routing.RoutingContext ctx = fe.buildRoutingContext(config, sharedReaders(), mode);
		applySharedImpassableRoads(ctx, entry);

		net.osmand.shared.routing.RouteCalcResult res = fe.searchRoute(ctx,
				sharedLatLon(entry.start), sharedLatLon(entry.finish), sharedLatLons(entry.via));
		List<net.osmand.shared.routing.RouteSegmentResult> routeSegments =
				res != null ? res.getList() : new ArrayList<>();

		String type = hh ? "hh-shared" : "brp-shared";
		if (hh && ctx.calculationProgress != null
				&& noHHRoutingData(ctx.calculationProgress.getFastRoutingStatus().name(), type, entry)) {
			return null;
		}

		long runTime = System.currentTimeMillis() - started;
		return new RandomRouteResult(type, entry, runTime, ctx, routeSegments);
	}

	private net.osmand.shared.data.KLatLon sharedLatLon(net.osmand.data.LatLon l) {
		return l == null ? null : new net.osmand.shared.data.KLatLon(l.getLatitude(), l.getLongitude());
	}

	private List<net.osmand.shared.data.KLatLon> sharedLatLons(List<net.osmand.data.LatLon> list) {
		if (list == null) {
			return null;
		}
		List<net.osmand.shared.data.KLatLon> res = new ArrayList<>();
		for (net.osmand.data.LatLon l : list) {
			res.add(sharedLatLon(l));
		}
		return res;
	}

	private void applySharedImpassableRoads(net.osmand.shared.routing.RoutingContext ctx, RandomRouteEntry entry) {
		if (entry.avoidRoads.isEmpty()
				|| !(ctx.getRouter() instanceof net.osmand.shared.routing.GeneralRouter router)) {
			return;
		}
		router.setImpassableRoads(new HashSet<>(entry.avoidRoads));
	}

	private net.osmand.shared.routing.RoutingConfiguration buildSharedRoutingConfiguration(
			net.osmand.shared.routing.RoutingConfiguration.Builder builder, RandomRouteEntry entry,
			net.osmand.shared.routing.RoutingConfiguration.RoutingMemoryLimits memoryLimits) {
		net.osmand.shared.routing.GeneralRouter router = builder.getRouter(entry.profile);
		Map<String, String> routeParameters = getSharedDefaultParameters(router);
		for (Map.Entry<String, String> e : entry.mapParams().entrySet()) {
			if ("false".equalsIgnoreCase(e.getValue())) {
				routeParameters.remove(e.getKey());
			} else {
				routeParameters.put(e.getKey(), e.getValue());
			}
		}
		return builder.build(entry.profile, memoryLimits, new LinkedHashMap<>(routeParameters));
	}

	private Map<String, String> getSharedDefaultParameters(net.osmand.shared.routing.GeneralRouter router) {
		Map<String, String> params = new LinkedHashMap<>();
		for (Map.Entry<String, net.osmand.shared.routing.GeneralRouter.RoutingParameter> entry
				: router.getParameters().entrySet()) {
			String key = entry.getKey();
			net.osmand.shared.routing.GeneralRouter.RoutingParameter rp = entry.getValue();
			if (rp.getType() == net.osmand.shared.routing.GeneralRouter.RoutingParameterType.BOOLEAN) {
				if (rp.getDefaultBoolean()) {
					params.put(key, "true");
				}
			} else if (rp.getDefaultNumeric() > 0) {
				params.put(key, rp.getDefaultString());
			}
		}
		return params;
	}

	private RandomRouteResult runHHRoutePlannerJava(RandomRouteEntry entry) throws SQLException, IOException, InterruptedException {
		return runHHRoutePlanner(entry, false);
	}

	private RandomRouteResult runHHRoutePlannerCpp(RandomRouteEntry entry) throws SQLException, IOException, InterruptedException {
		return runHHRoutePlanner(entry, true);
	}

	private RandomRouteResult runHHRoutePlanner(RandomRouteEntry entry, boolean useNative) throws IOException, InterruptedException {
		long started = System.currentTimeMillis();

		HHRoutePlanner.DEBUG_VERBOSE_LEVEL = 1;
		HHRouteDataStructure.HHRoutingConfig.STATS_VERBOSE_LEVEL = 1;
		RouteResultPreparation.PRINT_TO_CONSOLE_ROUTE_INFORMATION = true;

		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();
		RoutePlannerFrontEnd.CALCULATE_MISSING_MAPS = false;
		fe.setDefaultHHRoutingConfig();
		fe.setUseOnlyHHRouting(true);

		RoutingConfiguration.Builder builder = RoutingConfiguration.getDefault();

		RoutingConfiguration.RoutingMemoryLimits memoryLimits =
				new RoutingConfiguration.RoutingMemoryLimits(MEM_LIMIT, MEM_LIMIT);

		RoutingConfiguration config = buildRoutingConfiguration(builder, entry, memoryLimits);

		if (this.config.USE_TIME_CONDITIONAL_ROUTING == 1) {
			config.routeCalculationTime = System.currentTimeMillis();
		} else if (this.config.USE_TIME_CONDITIONAL_ROUTING != 0) {
			config.routeCalculationTime = this.config.USE_TIME_CONDITIONAL_ROUTING;
		}

		// noinspection ConstantConditions
		if (GeneratorConfig.ambiguousConditionalTags != null) {
			config.ambiguousConditionalTags = GeneratorConfig.ambiguousConditionalTags;
		}

		RoutingContext ctx = fe.buildRoutingContext(
				config,
				useNative ? nativeLibrary : null,
				obfReaders.toArray(new BinaryMapIndexReader[0]),
				RoutePlannerFrontEnd.RouteCalculationMode.NORMAL
		);
		applyImpassableRoads(ctx, entry);

		fe.setHHRouteCpp(useNative);

//		RoutePlannerFrontEnd.HH_ROUTING_CONFIG = null; // way to set config for hh

		RouteResultPreparation.RouteCalcResult res = fe.searchRoute(ctx, entry.start, entry.finish, entry.via, null);
		List<RouteSegmentResult> routeSegments = res != null ? res.getList() : new ArrayList<>();

		String type = useNative ? "hh-cpp" : "hh-java";
		if (noHHRoutingData(ctx.calculationProgress.getFastRoutingStatus().name(), type, entry)) {
			return null;
		}

		long runTime = System.currentTimeMillis() - started;
		return new RandomRouteResult(type, entry, runTime, ctx, routeSegments);
	}

	/**
	 * Whether the maps have no hub graph for this profile - a pedestrian one, or maps older than the
	 * HH sections. There is no route to compare then, and the run is left out of the results.
	 */
	private boolean noHHRoutingData(String status, String type, RandomRouteEntry entry) {
		if (!FastRoutingState.Status.FAILED_NO_HH_ROUTING_DATA.name().equals(status)) {
			return false;
		}
		System.err.printf("%s: no HH routing data for %s, skipped\n", type, entry.profile);
		return true;
	}

	private void loadNativeLibrary() {
		if (optNoNativeLibrary) {
			return;
		}
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
