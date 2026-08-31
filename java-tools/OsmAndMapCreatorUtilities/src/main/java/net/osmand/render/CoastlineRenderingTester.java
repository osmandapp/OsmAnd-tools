package net.osmand.render;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.imageio.ImageIO;

import com.google.gson.Gson;

import net.osmand.NativeJavaRendering;
import net.osmand.NativeJavaRendering.RenderingImageContext;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.CachedOsmandIndexes;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.osmand.util.MapsCollection;

/**
 * Renders map tiles with the native (legacy) renderer and compares their <b>water mask</b> with the
 * water mask of the reference raster tiles of {@code https://tile.osmand.net/hd/{z}/{x}/{y}.png},
 * to reproduce the coastline problems of
 * <a href="https://github.com/osmandapp/OsmAnd-Issues/issues/3291">Epic - Coastline issues</a>.
 *
 * <p>Two independent numbers are produced per tile:
 * <ul>
 * <li><i>extra water</i> &mdash; pixels rendered as water while the reference has land
 * (water flooding the land);</li>
 * <li><i>missing water</i> &mdash; pixels rendered as land while the reference has water
 * (land or grey squares flooding the sea).</li>
 * </ul>
 * The masks are compared with a tolerance of a few pixels (erode/dilate), so different line widths,
 * thin rivers and small ponds of the two different styles do not produce false positives - only
 * large solid areas are reported.
 *
 * <h3>Running</h3>
 * It is a utility of OsmAndMapCreator, so it is started through {@code utilities.sh}:
 * <pre>
 * # the known problems of the epic, from coastline-tests.json
 * OsmAndMapCreator/utilities.sh test-coastline-rendering -maps.dir=/var/maps
 * # every tile of the world between two zooms, with every map of the maps folder
 * OsmAndMapCreator/utilities.sh test-coastline-rendering -scan -minzoom=1 -maxzoom=10 -maps.dir=/var/maps
 * </pre>
 * Exit code: <b>0</b> - everything matches the reference, <b>2</b> - problems were reproduced,
 * <b>1</b> - the tester could not run (native library could not be loaded, no maps, broken json).
 *
 * <p>Every option can be given either as an argument ({@code -maps.dir=...}) or as a system
 * property ({@code -Dmaps.dir=...}):
 * <ul>
 * <li>{@code maps.dir} - folder with the *.obf maps, default {@code ~/osmand/maps}. Missing maps of
 * a case are downloaded from <a href="https://download.osmand.net/list">download.osmand.net</a> and
 * unpacked into it;</li>
 * <li>{@code load} - {@code all} (default) initializes every map of {@code maps.dir},
 * {@code case} initializes only the maps a case declares;</li>
 * <li>{@code basemap} - the basemap loaded in the {@code load=case} mode, default
 * {@code World_basemap_2.obf};</li>
 * <li>{@code exclude} - comma separated name parts that {@code load=all} skips, default
 * {@code World_seamarks,basemap_mini} - an overlay and a second basemap would distort the
 * rendering; pass {@code -exclude=} to load literally everything;</li>
 * <li>{@code cases} - path to the json with the cases, default the bundled
 * {@code coastline-tests.json};</li>
 * <li>{@code issue} - run only the cases of one issue, e.g. {@code -issue=25618};</li>
 * <li>{@code randomTilesK} - size of the random part of a run in thousands of tiles, default
 * {@value #DEFAULT_RANDOM_TILES_K}; {@code -randomTilesK=0} runs the json cases only. The tiles are
 * spread evenly over {@code minzoom}..{@code maxzoom} (1..17 by default), {@value #SHARE_COASTAL}%
 * of them with a coastline in them and the rest split between open ocean and inland. {@code seed}
 * defaults to the calendar month, so the same tiles are checked all month long. {@code -random}
 * runs that part alone;</li>
 * <li>{@code scan}, {@code minzoom}, {@code maxzoom}, {@code bbox} - scan every tile of a zoom
 * range instead of the cases; {@code bbox} is {@code leftLon,bottomLat,rightLon,topLat} and
 * defaults to the whole world;</li>
 * <li>{@code out} - output folder, default {@code build/coastline-tiles};</li>
 * <li>{@code save} - {@code failed} (default) writes the png tiles of the failed tiles only,
 * {@code all} writes everything, {@code none} keeps statistics only;</li>
 * <li>{@code native} - path to {@code libosmand.dylib/so/dll}; by default the library bundled into
 * OsmAndMapCreator is used, a local repository checkout picks it up from {@code core-legacy};</li>
 * <li>{@code fonts}, {@code style} - renderer setup, autodetected;</li>
 * <li>{@code threads} - parallel reference tile downloads, default 16. Rendering itself is single
 * threaded, this only controls how many reference tiles are fetched at once - raise it if the
 * progress line reports a high "waiting for references" share;</li>
 * <li>{@code download} - {@code false} to never download a missing map;</li>
 * <li>{@code referenceDir} - where the downloaded reference tiles are kept, by default
 * {@code coastline-reference} in the current folder. It is reused by every run, so a rerun only
 * downloads the tiles it has not seen yet - delete the folder to force a refetch;</li>
 * <li>{@code referenceCache} - {@code false} to delete a reference tile once it was compared;</li>
 * <li>{@code tileSize}, {@code tolerance} - size of the compared tile and the mask tolerance.</li>
 * </ul>
 */
public class CoastlineRenderingTester {

	// ----------------------------------------------------------------- water detection

	/** Water color of default.render.xml (day mode). */
	private static final int[] OSMAND_WATER_COLORS = { 0x5cc3e5 };

	/** Water color of the reference tiles (tile.osmand.net renders openstreetmap-carto). */
	private static final int[] REFERENCE_WATER_COLORS = { 0xaad3df };

	/**
	 * Dashes of the {@code wetland_saltern} shader. default.render.xml paints
	 * {@code landuse=salt_pond} with {@code $waterColor} plus this shader on purpose, while
	 * openstreetmap-carto paints the same ponds as a wetland. That is a style difference and not a
	 * broken coastline, so the areas covered by the shader are excluded from the extra water mask
	 * (they are still counted and reported separately).
	 */
	private static final int[] SHADED_WATER_COLORS = { 0x2992ef };

	private static final int SHADED_WATER_SPREAD_PX = 16;

	/**
	 * Ice: {@code natural=glacier} is {@code #E4FDFF} in default.render.xml and {@code #ddecec} in
	 * openstreetmap-carto. Antarctica and the Arctic are drawn from it, the two styles disagree
	 * about how ice relates to water, and none of that is a coastline problem - so pixels that are
	 * ice on either side are ignored on both.
	 */
	private static final int[] OSMAND_ICE_COLORS = { 0xE4FDFF };
	private static final int[] REFERENCE_ICE_COLORS = { 0xddecec };

	/** Max per channel difference to still treat a pixel as water. */
	private static final int COLOR_TOLERANCE = 10;

	/** Server the report links to, so that a failed tile can be opened on the live map. */
	private static final String MAP_SERVER = "https://test.osmand.net";

	/** Default size of the random part of a run, in thousands of tiles - see {@code randomTilesK}. */
	private static final int DEFAULT_RANDOM_TILES_K = 10;

	/** How the random tiles are split: coastal, open ocean, inland. */
	private static final int SHARE_COASTAL = 80, SHARE_OCEAN = 10;

	static final String GROUP_FIXED = "Fixed cases of coastline-tests.json";
	static final String GROUP_RANDOM = "Random tiles";
	static final String GROUP_SCAN = "Full scan";

	private static final String BUNDLED_CASES = "/net/osmand/render/coastline-tests.json";
	private static final String CHECK_SEAMARKS_INLAND = "seamarksInland";

	/** Tiles are downloaded and compared in chunks of that size. */
	private static final int CHUNK = 256;

	/** How many failed tiles at most are kept for the html report. */
	private static final int MAX_REPORTED_TILES = 3000;

	/**
	 * Maps that must not be loaded together with the normal ones: the seamarks overlay and the
	 * cut down basemap, which would be a second basemap next to World_basemap.
	 */
	private static final String DEFAULT_EXCLUDED_MAPS = "World_seamarks,basemap_mini";

	/** Without it the ocean is not rendered at all outside of the detailed maps. */
	private static final String DEFAULT_BASEMAP = "World_basemap_2.obf";

	// ----------------------------------------------------------------- json model

	/** Content of coastline-tests.json. */
	public static class CasesFile {
		public String referenceUrl = "https://tile.osmand.net/hd/{z}/{x}/{y}.png";
		public String downloadUrl = "https://download.osmand.net/download?standard=yes&file={name}.zip";
		public List<CaseDef> cases = new ArrayList<>();
	}

	/** One reproducible location, or a zoom range scan. */
	public static class CaseDef {
		public int issue;
		public String title;
		public String url;
		public double lat;
		public double lon;
		/** leftLon, bottomLat, rightLon, topLat - an alternative to lat/lon + radius */
		public double[] bbox;
		public int[] zooms;
		public int minzoom = -1;
		public int maxzoom = -1;
		/** tiles around the central tile: 0 -> 1 tile, 1 -> 3x3 tiles */
		public int radius = 1;
		public String[] maps = new String[0];
		public String check = "water";
		/** explicit {zoom, x, y} tiles - used by the random mode instead of bbox/radius */
		public transient List<int[]> tiles;
		/** {@link #GROUP_FIXED}, {@link #GROUP_RANDOM} or {@link #GROUP_SCAN} */
		public transient String group = GROUP_FIXED;
		/** max share of a tile that may be rendered as water while the reference is land */
		public double maxExtraWater = 0.02;
		/** max share of a tile that may be rendered as land while the reference is water */
		public double maxMissingWater = 0.02;
		/** max share of an inland tile that the maps of a {@code seamarksInland} case may draw */
		public double maxDrawn = 0.001;

		boolean isSeamarksCheck() {
			return CHECK_SEAMARKS_INLAND.equals(check);
		}

		int[] zoomList() {
			if (zooms != null && zooms.length > 0) {
				return zooms;
			}
			if (minzoom < 0 || maxzoom < minzoom) {
				throw new IllegalArgumentException("Case " + this + " has neither zooms nor minzoom/maxzoom");
			}
			int[] res = new int[maxzoom - minzoom + 1];
			for (int i = 0; i < res.length; i++) {
				res[i] = minzoom + i;
			}
			return res;
		}

		String key() {
			return issue + " " + title;
		}

		@Override
		public String toString() {
			return "#" + issue + " " + title;
		}
	}

	// ----------------------------------------------------------------- results

	/** One compared tile that is kept for the report. */
	private static class TileResult {
		final CaseDef def;
		final int zoom, x, y;
		final Map<String, String> images = new LinkedHashMap<>();
		final Map<String, String> metrics = new LinkedHashMap<>();
		final List<String> problems = new ArrayList<>();
		double severity;

		TileResult(CaseDef def, int zoom, int x, int y) {
			this.def = def;
			this.zoom = zoom;
			this.x = x;
			this.y = y;
		}

		boolean ok() {
			return problems.isEmpty();
		}
	}

	/** Aggregated numbers of one case. */
	public static class CaseStats {
		public int issue;
		public String title;
		public String url;
		public String check;
		public String group;
		public int tiles;
		public int comparedTiles;
		public int skippedTiles;
		public int failedTiles;
		public double worstExtraWater;
		public double worstMissingWater;
		public double sumExtraWater;
		public double sumMissingWater;
		public double styledSaltPonds;
		public String worstTile = "";

		public double avgExtraWater() {
			return comparedTiles == 0 ? 0 : sumExtraWater / comparedTiles;
		}

		public double avgMissingWater() {
			return comparedTiles == 0 ? 0 : sumMissingWater / comparedTiles;
		}
	}

	/** Failed / compared tiles of one group of cases. */
	public static class GroupTotals {
		public String group;
		public int tiles;
		public int failedTiles;
	}

	/** Result of a whole run, also written to {@code summary.json}. */
	public static class RunResult {
		public String style;
		public String mapsDir;
		public int loadedMaps;
		public long startedAt;
		public long durationMs;
		public int tiles;
		public int comparedTiles;
		public int failedTiles;
		public List<CaseStats> cases = new ArrayList<>();
		public List<GroupTotals> groups = new ArrayList<>();
	}

	// ----------------------------------------------------------------- parameters

	private final Map<String, String> options;
	private final File mapsDir;
	private final File outputDir;
	private final File referenceCacheDir;
	private final boolean loadAllMaps;
	private final boolean downloadMaps;
	private final boolean cacheReference;
	private final String saveImages;
	private final int tileSize;
	private final int maskTolerance;
	private final int threads;
	private final boolean writeHtml;
	private final int flushEvery;

	private CasesFile casesFile;
	private RunResult result;
	private int tilesSinceFlush;
	private long lastFlush;
	private NativeJavaRendering renderer;
	private final Set<String> initializedMaps = new LinkedHashSet<>();
	private final List<TileResult> reported = new ArrayList<>();
	private ExecutorService downloadPool;
	/** Reference tiles already handed to the download pool, so that nobody downloads them twice. */
	private final Map<String, Future<?>> pendingReferences = new ConcurrentHashMap<>();
	private long referenceWaitNs;

	public CoastlineRenderingTester(Map<String, String> options) {
		this.options = options;
		this.mapsDir = new File(opt("maps.dir", new File(System.getProperty("user.home"), "osmand/maps")
				.getAbsolutePath()));
		this.outputDir = new File(opt("out", "build/coastline-tiles"));
		// next to indexes.cache in the run folder, not inside the report - the report is rewritten
		// (and published) on every run, while the downloaded reference tiles are worth keeping so
		// that a rerun does not fetch them from tile.osmand.net again
		this.referenceCacheDir = new File(opt("referenceDir",
				new File(System.getProperty("user.dir"), "coastline-reference").getPath()));
		this.loadAllMaps = !"case".equalsIgnoreCase(opt("load", "all"));
		this.downloadMaps = Boolean.parseBoolean(opt("download", "true"));
		this.cacheReference = Boolean.parseBoolean(opt("referenceCache", "true"));
		this.saveImages = opt("save", "failed");
		this.tileSize = Integer.parseInt(opt("tileSize", "256"));
		this.maskTolerance = Integer.parseInt(opt("tolerance", "4"));
		this.threads = Integer.parseInt(opt("threads", "16"));
		this.writeHtml = Boolean.parseBoolean(opt("html", "true"));
		this.flushEvery = Integer.parseInt(opt("flushEvery", "1000"));
	}

	private String opt(String name, String def) {
		// an explicitly passed empty value means empty, e.g. -exclude= loads every map
		if (options.containsKey(name)) {
			return options.get(name);
		}
		String v = System.getProperty(name);
		return v == null ? def : v;
	}

	public static void main(String[] args) {
		Map<String, String> options = new LinkedHashMap<>();
		for (String a : args) {
			a = a.trim();
			if (!a.startsWith("-")) {
				continue;
			}
			a = a.substring(1);
			int eq = a.indexOf('=');
			if (eq > 0) {
				options.put(a.substring(0, eq), a.substring(eq + 1));
			} else {
				options.put(a, "true");
			}
		}
		int code;
		try {
			RunResult res = new CoastlineRenderingTester(options).run();
			code = res.failedTiles > 0 ? 2 : 0;
		} catch (Throwable e) {
			e.printStackTrace();
			code = 1;
		}
		System.exit(code);
	}

	// ----------------------------------------------------------------- run

	public RunResult run() throws Exception {
		long start = System.currentTimeMillis();
		outputDir.mkdirs();
		referenceCacheDir.mkdirs();
		casesFile = readCases();
		List<CaseDef> cases = selectCases();
		if (cases.isEmpty()) {
			throw new IllegalStateException("Nothing to check, no case matched the parameters");
		}
		initRenderer();
		// the JDK keeps only 5 pooled keep alive connections per host by default, everything above
		// that reconnects and does a TLS handshake per tile
		System.setProperty("http.maxConnections", String.valueOf(Math.max(5, threads)));
		downloadPool = Executors.newFixedThreadPool(threads);

		result = new RunResult();
		result.style = opt("style", "default.render.xml");
		result.mapsDir = mapsDir.getAbsolutePath();
		result.startedAt = start;
		try {
			// the seamarks cases close every map, so they go last
			cases.sort((a, b) -> Boolean.compare(a.isSeamarksCheck(), b.isSeamarksCheck()));
			for (CaseDef def : cases) {
				if (def.isSeamarksCheck()) {
					runSeamarksCase(def);
				} else {
					runWaterCase(def);
				}
			}
		} finally {
			downloadPool.shutdownNow();
		}
		result.loadedMaps = initializedMaps.size();
		recomputeTotals();
		result.durationMs = System.currentTimeMillis() - start;
		printSummary(result);
		writeSummaryJson(result);
		if (writeHtml) {
			writeHtmlReport(result);
		}
		return result;
	}

	/**
	 * Picks the random part of a run: tiles spread evenly over the zoom range, {@value
	 * #SHARE_COASTAL}% of them with a coastline in them, the rest split between deep ocean and deep
	 * land so that a break away from any coast is noticed too. A plain random tile of the world is
	 * almost always empty ocean or empty land, which is why the coastal ones are picked on purpose
	 * from the bundled oceantiles_12 bitmap.
	 *
	 * <p>The seed is the calendar month, so the same tiles are checked for a whole month: runs stay
	 * comparable and the reference tiles stay in the cache. Override with {@code -seed}.
	 */
	private CaseDef randomCase(int totalTiles) {
		CaseDef c = new CaseDef();
		c.issue = 3291;
		c.title = "Random tiles";
		c.group = GROUP_RANDOM;
		c.minzoom = Integer.parseInt(opt("minzoom", "1"));
		c.maxzoom = Integer.parseInt(opt("maxzoom", "17"));
		c.maxExtraWater = Double.parseDouble(opt("maxExtraWater", "0.02"));
		c.maxMissingWater = Double.parseDouble(opt("maxMissingWater", "0.02"));
		java.util.Calendar cal = java.util.Calendar.getInstance();
		long seed = Long.parseLong(opt("seed", String.valueOf(
				cal.get(java.util.Calendar.YEAR) * 100L + cal.get(java.util.Calendar.MONTH) + 1)));
		java.util.Random rnd = new java.util.Random(seed);
		CoastalTiles tiles = new CoastalTiles();

		List<int[]> picked = new ArrayList<>();
		int[] found = new int[3];
		int zoomsLeft = c.maxzoom - c.minzoom + 1;
		int left = totalTiles;
		for (int zoom = c.minzoom; zoom <= c.maxzoom; zoom++, zoomsLeft--) {
			// what a low zoom can not provide is spread over the zooms that follow
			int quota = Math.min(left, (int) Math.ceil(left / (double) zoomsLeft));
			int[] want = { quota * SHARE_COASTAL / 100, quota * SHARE_OCEAN / 100, 0 };
			want[2] = quota - want[0] - want[1];
			int got = 0;
			for (int kind = 0; kind < 3; kind++) {
				List<int[]> sel = tiles.pick(zoom, kind, want[kind], rnd);
				picked.addAll(sel);
				found[kind] += sel.size();
				got += sel.size();
			}
			left -= got;
		}
		// the quota has to be filled from the low zooms up - z1 has 4 tiles in the whole world and
		// what it can not provide is spread over the zooms that follow - but the run goes the other
		// way round, from the detailed zooms down, because the low zooms have been checked many
		// times already and the interesting tiles are at the top. Same seed, same set, other order.
		Collections.reverse(picked);
		c.tiles = picked;
		System.out.printf("Random tiles  : seed %d, %d tiles over zooms %d..%d (rendered %d down to %d) "
						+ "(%d coastal, %d ocean, %d land)%n",
				seed, picked.size(), c.minzoom, c.maxzoom, c.maxzoom, c.minzoom,
				found[0], found[1], found[2]);
		return c;
	}

	/**
	 * Sea/land bitmap of oceantiles_12, bundled into the jar. Kind 0 is a tile with a coastline in
	 * it, 1 is open ocean, 2 is inland.
	 */
	private static class CoastalTiles extends net.osmand.obf.preparation.BasemapProcessor {
		private static final int Z = net.osmand.obf.preparation.BasemapProcessor.TILE_ZOOMLEVEL;
		static final int COASTAL = 0, OCEAN = 1, LAND = 2;

		CoastalTiles() {
			constructBitSetInfo(null);
		}

		int kind(int zoom, int x, int y) {
			if (zoom < Z) {
				float sea = getSeaTile(x, y, zoom);
				return sea > 0.01f && sea < 0.99f ? COASTAL : (sea >= 0.99f ? OCEAN : LAND);
			}
			int shift = zoom - Z;
			int cx = x >> shift, cy = y >> shift, max = 1 << Z;
			float first = getSeaTile(cx, cy, Z);
			for (int dx = -1; dx <= 1; dx++) {
				for (int dy = -1; dy <= 1; dy++) {
					int nx = Math.max(0, Math.min(max - 1, cx + dx));
					int ny = Math.max(0, Math.min(max - 1, cy + dy));
					if (getSeaTile(nx, ny, Z) != first) {
						return COASTAL;
					}
				}
			}
			return first >= 0.99f ? OCEAN : LAND;
		}

		/** Up to {@code want} distinct tiles of that kind at that zoom. */
		List<int[]> pick(int zoom, int kind, int want, java.util.Random rnd) {
			List<int[]> res = new ArrayList<>();
			if (want <= 0) {
				return res;
			}
			int max = 1 << zoom;
			long total = (long) max * max;
			if (total <= 1 << 18) {
				// small zoom: take every tile of that kind and shuffle, so nothing is missed
				List<int[]> all = new ArrayList<>();
				for (int x = 0; x < max; x++) {
					for (int y = 0; y < max; y++) {
						if (kind(zoom, x, y) == kind) {
							all.add(new int[] { zoom, x, y });
						}
					}
				}
				Collections.shuffle(all, rnd);
				return all.subList(0, Math.min(want, all.size()));
			}
			Set<Long> seen = new LinkedHashSet<>();
			int attempts = want * 500 + 20000;
			while (res.size() < want && attempts-- > 0) {
				int x = rnd.nextInt(max);
				int y = rnd.nextInt(max);
				if (kind(zoom, x, y) == kind && seen.add(((long) x << 32) | y)) {
					res.add(new int[] { zoom, x, y });
				}
			}
			return res;
		}
	}

	private List<CaseDef> selectCases() {
		int randomTiles = Integer.parseInt(opt("randomTilesK", String.valueOf(DEFAULT_RANDOM_TILES_K))) * 1000;
		if (Boolean.parseBoolean(opt("random", "false"))) {
			return new ArrayList<>(Collections.singletonList(randomCase(randomTiles)));
		}
		if (Boolean.parseBoolean(opt("scan", "false"))) {
			CaseDef scan = new CaseDef();
			scan.issue = 3291;
			scan.title = "Full scan";
			scan.group = GROUP_SCAN;
			scan.minzoom = Integer.parseInt(opt("minzoom", "1"));
			scan.maxzoom = Integer.parseInt(opt("maxzoom", "10"));
			String bbox = opt("bbox", null);
			scan.bbox = bbox == null ? new double[] { -180, -85, 180, 85 } : parseBbox(bbox);
			scan.maxExtraWater = Double.parseDouble(opt("maxExtraWater", "0.02"));
			scan.maxMissingWater = Double.parseDouble(opt("maxMissingWater", "0.02"));
			return new ArrayList<>(Collections.singletonList(scan));
		}
		List<CaseDef> res = new ArrayList<>();
		String issue = opt("issue", null);
		if (issue != null && issue.isEmpty()) {
			issue = null;
		}
		for (CaseDef def : casesFile.cases) {
			if (issue == null || issue.equals(String.valueOf(def.issue))) {
				res.add(def);
			}
		}
		// the default run is the fixed cases of the json plus the random tiles
		if (issue == null && randomTiles > 0) {
			res.add(randomCase(randomTiles));
		}
		return res;
	}

	private static double[] parseBbox(String s) {
		String[] p = s.split(",");
		if (p.length != 4) {
			throw new IllegalArgumentException("bbox must be leftLon,bottomLat,rightLon,topLat but was " + s);
		}
		double[] res = new double[4];
		for (int i = 0; i < 4; i++) {
			res[i] = Double.parseDouble(p[i].trim());
		}
		return res;
	}

	private CasesFile readCases() throws IOException {
		String path = opt("cases", null);
		Gson gson = new Gson();
		if (path != null) {
			try (Reader r = new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8)) {
				return gson.fromJson(r, CasesFile.class);
			}
		}
		InputStream is = CoastlineRenderingTester.class.getResourceAsStream(BUNDLED_CASES);
		if (is == null) {
			throw new IOException("Can't find " + BUNDLED_CASES + " on the classpath, pass -cases=<file>");
		}
		try (Reader r = new InputStreamReader(is, StandardCharsets.UTF_8)) {
			return gson.fromJson(r, CasesFile.class);
		}
	}

	// ----------------------------------------------------------------- renderer setup

	private void initRenderer() throws Exception {
		// null lets NativeJavaRendering load the library bundled into OsmAndMapCreator
		File nativeLib = findNativeLibrary();
		File fonts = findFonts();
		String style = opt("style", "default.render.xml");
		System.out.println("Native library : " + (nativeLib == null
				? "bundled with OsmAndMapCreator" : nativeLib.getAbsolutePath()));
		System.out.println("Fonts          : " + (fonts == null ? "none" : fonts.getAbsolutePath()));
		System.out.println("Maps           : " + mapsDir.getAbsolutePath());
		System.out.println("Output         : " + outputDir.getAbsolutePath());
		System.out.println("Reference cache: " + referenceCacheDir.getAbsolutePath()
				+ " (" + countCachedReferences() + " tiles kept from the previous runs)");
		System.out.println("Style          : " + style);

		renderer = NativeJavaRendering.getDefault(nativeLib == null ? null : nativeLib.getAbsolutePath(), null,
				fonts == null ? null : fonts.getAbsolutePath());
		if (renderer == null) {
			throw new IllegalStateException("Native library could not be loaded"
					+ (nativeLib == null ? ", pass -native=<path to libosmand.dylib/so/dll>" : ": " + nativeLib));
		}
		renderer.loadRuleStorage(style, "density=1,textScale=1");
		if (loadAllMaps) {
			initAllMaps();
		} else {
			// the app always has the basemap, without it there is no ocean outside of a detailed map
			String basemap = opt("basemap", DEFAULT_BASEMAP);
			if (!basemap.isEmpty() && !initMap(basemap)) {
				System.err.println("No basemap - the sea will not be rendered outside of the detailed maps");
			}
		}
	}

	/**
	 * Initializes every map of the maps folder - the mode the server scan uses. The obf indexes are
	 * read through {@code indexes.cache}, otherwise the native library reports "File not
	 * initialized from cache" and re-reads the index of every single map on every run.
	 */
	private void initAllMaps() throws IOException {
		if (!mapsDir.isDirectory()) {
			System.err.println("Maps folder " + mapsDir.getAbsolutePath() + " does not exist");
			return;
		}
		// keeps the newest version of every region only
		MapsCollection collection = new MapsCollection(true);
		for (File obf : Algorithms.getSortedFilesVersions(mapsDir)) {
			if (!obf.isDirectory() && obf.getName().endsWith(".obf")) {
				collection.add(obf);
			}
		}
		String[] excluded = opt("exclude", DEFAULT_EXCLUDED_MAPS).split(",");
		List<File> maps = new ArrayList<>();
		List<String> skipped = new ArrayList<>();
		for (File f : collection.getFilesToUse()) {
			String n = f.getName();
			if (!f.isFile() || !n.endsWith(".obf") || n.endsWith(".road.obf") || n.endsWith(".srtm.obf")
					|| n.endsWith(".srtmf.obf") || n.endsWith(".wiki.obf") || n.endsWith(".depth.obf")) {
				continue;
			}
			boolean skip = false;
			for (String e : excluded) {
				if (!e.trim().isEmpty() && n.toLowerCase().contains(e.trim().toLowerCase())) {
					skip = true;
					break;
				}
			}
			if (skip) {
				skipped.add(n);
			} else {
				maps.add(f);
			}
		}
		Collections.sort(maps);

		// The cache lives in the folder the job runs in (the Jenkins workspace), so that it is wiped
		// together with it and is never written into the shared maps folder. Building it costs about
		// 20 ms per map; a run that finds the cache already there skips that entirely.
		long cacheStart = System.currentTimeMillis();
		File cacheFile = new File(System.getProperty("user.dir"), CachedOsmandIndexes.INDEXES_DEFAULT_FILENAME);
		boolean existed = cacheFile.isFile();
		CachedOsmandIndexes cache = new CachedOsmandIndexes();
		if (existed) {
			cache.readFromFile(cacheFile);
		}
		// A cache hit is a lookup by name and size, microseconds. A miss parses the whole obf index
		// and costs ~40 ms, so the misses are built in parallel - that is the only part worth
		// speeding up, and it disappears completely once the cache is warm.
		List<File> missing = new ArrayList<>();
		for (File f : maps) {
			if (cache.getFileIndex(f, false) == null) {
				missing.add(f);
			}
		}
		if (!missing.isEmpty()) {
			ExecutorService pool = Executors.newFixedThreadPool(Math.min(threads, missing.size()));
			List<Future<?>> futures = new ArrayList<>();
			for (File f : missing) {
				futures.add(pool.submit((Callable<Void>) () -> {
					try (RandomAccessFile raf = new RandomAccessFile(f.getPath(), "r")) {
						BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, f);
						synchronized (cache) {
							cache.addToCache(reader, f);
						}
						reader.close();
					}
					return null;
				}));
			}
			pool.shutdown();
			for (int i = 0; i < futures.size(); i++) {
				try {
					futures.get(i).get();
				} catch (ExecutionException e) {
					// a corrupt obf fails here on purpose - the map has to be fixed, not skipped
					throw new IOException("Can't read " + missing.get(i).getAbsolutePath() + ": "
							+ e.getCause().getMessage(), e.getCause());
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IOException(e);
				}
			}
		}
		cache.writeToFile(cacheFile);
		renderer.initCacheMapFile(cacheFile.getAbsolutePath());
		System.out.printf("Indexes cache : %s (%s, %d of %d maps indexed in %d ms)%n",
				cacheFile.getAbsolutePath(), existed ? "reused" : "created", missing.size(),
				maps.size(), System.currentTimeMillis() - cacheStart);

		for (File f : maps) {
			renderer.initMapFile(f.getAbsolutePath(), true);
			initializedMaps.add(f.getName());
		}
		System.out.println("Initialized " + maps.size() + " maps from " + mapsDir.getAbsolutePath());
		if (!skipped.isEmpty()) {
			Collections.sort(skipped);
			System.out.println("Skipped " + skipped.size() + " overlay maps (-exclude): "
					+ String.join(", ", skipped));
		}
	}

	/** Initializes one map, downloading it into the maps folder when it is missing. */
	private boolean initMap(String name) {
		if (initializedMaps.contains(name)) {
			return true;
		}
		File f = new File(mapsDir, name);
		if (!f.isFile() && downloadMaps) {
			f = downloadMap(name);
		}
		if (f == null || !f.isFile()) {
			System.err.println("Map " + name + " is not found in " + mapsDir.getAbsolutePath());
			return false;
		}
		System.out.println("Init map " + f.getAbsolutePath());
		renderer.initMapFile(f.getAbsolutePath(), true);
		initializedMaps.add(name);
		return true;
	}

	private void closeAllMaps() {
		for (String name : new ArrayList<>(initializedMaps)) {
			File f = new File(mapsDir, name);
			renderer.closeMapFile(f.getAbsolutePath());
			initializedMaps.remove(name);
		}
	}

	private File downloadMap(String name) {
		mapsDir.mkdirs();
		File target = new File(mapsDir, name);
		File zip = new File(mapsDir, name + ".zip.tmp");
		String url = casesFile.downloadUrl.replace("{name}", name);
		try {
			System.out.println("Downloading " + url + " ...");
			download(url, zip);
			try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zip))) {
				ZipEntry entry;
				while ((entry = zis.getNextEntry()) != null) {
					if (entry.getName().endsWith(".obf")) {
						try (FileOutputStream fos = new FileOutputStream(target)) {
							Algorithms.streamCopy(zis, fos);
						}
						break;
					}
				}
			}
			System.out.println("Unpacked " + target.getAbsolutePath() + " ("
					+ (target.length() >> 20) + " MB)");
		} catch (IOException e) {
			System.err.println("Can't download " + url + ": " + e.getMessage());
			return null;
		} finally {
			zip.delete();
		}
		return target.isFile() ? target : null;
	}

	// ----------------------------------------------------------------- water mask case

	private CaseStats runWaterCase(CaseDef def) throws Exception {
		System.out.println();
		System.out.println("=== " + def + (def.tiles != null ? " " + def.tiles.size() + " tiles"
				: def.bbox != null ? " bbox " + Arrays.toString(def.bbox)
				: String.format(" (%f, %f)", def.lat, def.lon)));
		CaseStats stats = newStats(def);
		if (!loadAllMaps) {
			for (String map : def.maps) {
				if (!initMap(map)) {
					throw new IllegalStateException("Map " + map + " of " + def + " is not available");
				}
			}
		} else {
			// the case still needs its own map even when everything else is already loaded
			for (String map : def.maps) {
				initMap(map);
			}
		}
		File dir = caseDir(def);
		int totalOfCase = countTiles(def);
		// the reference tiles of the next chunk are downloaded while this one is being rendered -
		// rendering is single threaded and the tile server is slow, so waiting for a whole chunk
		// before starting to render leaves either the net or the cpu idle all of the time
		Iterator<List<int[]>> it = chunks(def).iterator();
		List<int[]> chunk = it.hasNext() ? it.next() : null;
		prefetchReferences(chunk);
		while (chunk != null) {
			List<int[]> next = it.hasNext() ? it.next() : null;
			prefetchReferences(next);
			for (int[] t : chunk) {
				compareTile(def, stats, dir, t[0], t[1], t[2]);
				flush(stats, totalOfCase);
			}
			chunk = next;
		}
		System.out.printf("  %d tiles, %d compared, %d skipped, %d failed%n", stats.tiles,
				stats.comparedTiles, stats.skippedTiles, stats.failedTiles);
		return stats;
	}

	private void compareTile(CaseDef def, CaseStats stats, File dir, int zoom, int x, int y) throws IOException {
		stats.tiles++;
		awaitReference(zoom, x, y);
		BufferedImage reference = reference(zoom, x, y);
		if (reference == null) {
			stats.skippedTiles++;
			return;
		}
		BufferedImage rendered = render(zoom, x, y);
		reference = scaleDown(reference, rendered.getWidth(), rendered.getHeight());
		int w = rendered.getWidth(), h = rendered.getHeight();

		boolean[] renderedWater = waterMask(rendered, OSMAND_WATER_COLORS);
		boolean[] referenceWater = waterMask(reference, REFERENCE_WATER_COLORS);
		boolean[] shaded = dilate(waterMask(rendered, SHADED_WATER_COLORS), w, h, SHADED_WATER_SPREAD_PX);
		boolean[] ice = dilate(or(waterMask(rendered, OSMAND_ICE_COLORS),
				waterMask(reference, REFERENCE_ICE_COLORS)), w, h, maskTolerance);
		boolean[] extraAll = and(and(erode(renderedWater, w, h, maskTolerance),
				not(dilate(referenceWater, w, h, maskTolerance))), not(ice));
		boolean[] extra = and(extraAll, not(shaded));
		boolean[] missing = and(and(erode(referenceWater, w, h, maskTolerance),
				not(dilate(renderedWater, w, h, maskTolerance))), not(ice));
		double extraRatio = count(extra) * 1.0 / (w * h);
		double extraAllRatio = count(extraAll) * 1.0 / (w * h);
		double missingRatio = count(missing) * 1.0 / (w * h);

		stats.comparedTiles++;
		stats.sumExtraWater += extraRatio;
		stats.sumMissingWater += missingRatio;
		stats.styledSaltPonds += extraAllRatio - extraRatio;
		if (Math.max(extraRatio, missingRatio) > Math.max(stats.worstExtraWater, stats.worstMissingWater)) {
			stats.worstTile = zoom + "/" + x + "/" + y;
		}
		stats.worstExtraWater = Math.max(stats.worstExtraWater, extraRatio);
		stats.worstMissingWater = Math.max(stats.worstMissingWater, missingRatio);

		TileResult res = new TileResult(def, zoom, x, y);
		res.severity = Math.max(extraRatio, missingRatio);
		if (extraRatio > def.maxExtraWater) {
			res.problems.add(String.format("%.2f%% of water over the land (limit %.2f%%)",
					extraRatio * 100, def.maxExtraWater * 100));
		}
		if (missingRatio > def.maxMissingWater) {
			res.problems.add(String.format("%.2f%% of land over the water (limit %.2f%%)",
					missingRatio * 100, def.maxMissingWater * 100));
		}
		if (!res.ok()) {
			stats.failedTiles++;
			System.out.printf("  FAILED %d/%d/%d water: osmand %.1f%% reference %.1f%% - %s%n", zoom, x, y,
					count(renderedWater) * 100.0 / (w * h), count(referenceWater) * 100.0 / (w * h),
					String.join(", ", res.problems));
		}
		if (saveTile(res.ok())) {
			dir.mkdirs();
			ImageIO.write(rendered, "png", new File(dir, tileName(zoom, x, y, "rendered")));
			ImageIO.write(reference, "png", new File(dir, tileName(zoom, x, y, "reference")));
			ImageIO.write(diffImage(rendered, extra, missing), "png",
					new File(dir, tileName(zoom, x, y, "diff")));
			res.images.put("osmand", tileName(zoom, x, y, "rendered"));
			res.images.put("reference", tileName(zoom, x, y, "reference"));
			res.images.put("diff", tileName(zoom, x, y, "diff"));
		}
		res.metrics.put("water osmand", pct(count(renderedWater) * 1.0 / (w * h)));
		res.metrics.put("water reference", pct(count(referenceWater) * 1.0 / (w * h)));
		res.metrics.put("extra water", pct(extraRatio));
		if (extraAllRatio - extraRatio > 0.0001) {
			res.metrics.put("of it styled salt ponds", pct(extraAllRatio - extraRatio));
		}
		res.metrics.put("missing water", pct(missingRatio));
		keepForReport(res);
	}

	// ----------------------------------------------------------------- seamarks case

	/**
	 * Renders a tile with the maps of the case alone - every other map is closed - and requires it
	 * to stay empty. The empty tile of the native library carries a "Nothing found" placeholder,
	 * so only the pixels drawn <i>over the empty background</i> are counted.
	 */
	private CaseStats runSeamarksCase(CaseDef def) throws Exception {
		System.out.println();
		System.out.println("=== " + def + String.format(" (%f, %f)", def.lat, def.lon));
		CaseStats stats = newStats(def);
		File dir = caseDir(def);
		closeAllMaps();
		for (List<int[]> chunk : chunks(def)) {
			for (int[] t : chunk) {
				int zoom = t[0], x = t[1], y = t[2];
				stats.tiles++;
				closeAllMaps();
				BufferedImage empty = render(zoom, x, y);
				for (String map : def.maps) {
					if (!initMap(map)) {
						throw new IllegalStateException("Map " + map + " of " + def + " is not available");
					}
				}
				BufferedImage drawnImg = render(zoom, x, y);
				closeAllMaps();

				int background = dominantColor(empty);
				double ratio = countDrawnOverBackground(empty, drawnImg, background) * 1.0
						/ (empty.getWidth() * empty.getHeight());
				stats.comparedTiles++;
				stats.sumExtraWater += ratio;
				if (ratio > stats.worstExtraWater) {
					stats.worstTile = zoom + "/" + x + "/" + y;
				}
				stats.worstExtraWater = Math.max(stats.worstExtraWater, ratio);

				TileResult res = new TileResult(def, zoom, x, y);
				res.severity = ratio;
				res.metrics.put("drawn by the map", pct(ratio));
				if (ratio > def.maxDrawn) {
					stats.failedTiles++;
					res.problems.add(String.format("%s alone draws %.3f%% of an inland tile (limit %.3f%%)",
							String.join(", ", def.maps), ratio * 100, def.maxDrawn * 100));
					System.out.printf("  FAILED %d/%d/%d - %s%n", zoom, x, y, res.problems.get(0));
				}
				if (saveTile(res.ok())) {
					dir.mkdirs();
					ImageIO.write(empty, "png", new File(dir, tileName(zoom, x, y, "empty")));
					ImageIO.write(drawnImg, "png", new File(dir, tileName(zoom, x, y, "map-only")));
					res.images.put("no maps at all", tileName(zoom, x, y, "empty"));
					res.images.put(String.join(", ", def.maps) + " only", tileName(zoom, x, y, "map-only"));
				}
				keepForReport(res);
				flush(stats, countTiles(def));
			}
		}
		System.out.printf("  %d tiles, %d failed%n", stats.tiles, stats.failedTiles);
		if (loadAllMaps) {
			initAllMaps();
		}
		return stats;
	}

	// ----------------------------------------------------------------- tiles

	/**
	 * Walks the tiles of a case in chunks. A world wide z1-10 scan is more than a million tiles,
	 * so the chunks are generated lazily instead of being collected into one list.
	 */
	private Iterable<List<int[]>> chunks(CaseDef def) {
		if (def.tiles != null) {
			List<List<int[]>> res = new ArrayList<>();
			for (int i = 0; i < def.tiles.size(); i += CHUNK) {
				res.add(new ArrayList<>(def.tiles.subList(i, Math.min(i + CHUNK, def.tiles.size()))));
			}
			return res;
		}
		int[] zoomList = def.zoomList();
		return () -> new Iterator<List<int[]>>() {
			int zi = 0, x, y, leftX, rightX, topY, bottomY;
			boolean started = false;

			private boolean startZoom() {
				while (zi < zoomList.length) {
					int zoom = zoomList[zi];
					int max = 1 << zoom;
					if (def.bbox != null) {
						leftX = clamp((int) Math.floor(MapUtils.getTileNumberX(zoom, def.bbox[0])), max);
						rightX = clamp((int) Math.floor(MapUtils.getTileNumberX(zoom, def.bbox[2])), max);
						topY = clamp((int) Math.floor(MapUtils.getTileNumberY(zoom, def.bbox[3])), max);
						bottomY = clamp((int) Math.floor(MapUtils.getTileNumberY(zoom, def.bbox[1])), max);
					} else {
						int cx = (int) Math.floor(MapUtils.getTileNumberX(zoom, def.lon));
						int cy = (int) Math.floor(MapUtils.getTileNumberY(zoom, def.lat));
						leftX = clamp(cx - def.radius, max);
						rightX = clamp(cx + def.radius, max);
						topY = clamp(cy - def.radius, max);
						bottomY = clamp(cy + def.radius, max);
					}
					x = leftX;
					y = topY;
					if (leftX <= rightX && topY <= bottomY) {
						return true;
					}
					zi++;
				}
				return false;
			}

			@Override
			public boolean hasNext() {
				if (!started) {
					started = startZoom();
				}
				return zi < zoomList.length;
			}

			@Override
			public List<int[]> next() {
				List<int[]> chunk = new ArrayList<>(CHUNK);
				while (chunk.size() < CHUNK && zi < zoomList.length) {
					chunk.add(new int[] { zoomList[zi], x, y });
					if (++y > bottomY) {
						y = topY;
						if (++x > rightX) {
							zi++;
							if (!startZoom()) {
								break;
							}
						}
					}
				}
				return chunk;
			}
		};
	}

	/** How many tiles the case is going to check, for the progress line. */
	private static int countTiles(CaseDef def) {
		if (def.tiles != null) {
			return def.tiles.size();
		}
		int total = 0;
		for (int zoom : def.zoomList()) {
			int max = 1 << zoom;
			int w, h;
			if (def.bbox != null) {
				w = clamp((int) Math.floor(MapUtils.getTileNumberX(zoom, def.bbox[2])), max)
						- clamp((int) Math.floor(MapUtils.getTileNumberX(zoom, def.bbox[0])), max) + 1;
				h = clamp((int) Math.floor(MapUtils.getTileNumberY(zoom, def.bbox[1])), max)
						- clamp((int) Math.floor(MapUtils.getTileNumberY(zoom, def.bbox[3])), max) + 1;
			} else {
				w = h = 2 * def.radius + 1;
			}
			total += Math.max(0, w) * Math.max(0, h);
		}
		return total;
	}

	private static int clamp(int v, int max) {
		return v < 0 ? 0 : (v >= max ? max - 1 : v);
	}

	/**
	 * Renders one tile the way the server does it in VectorMetatile.renderMetaTile: from the tile
	 * aligned 31 bit bounds. The lat/lon constructor goes through RotatedTileBox and comes back a
	 * few 31 bit units off the tile grid, which is enough to flip the ocean/land fill of a tile -
	 * 6/4/62 and 6/58/19 come out as land through it and as water through this one.
	 */
	private BufferedImage render(int zoom, int x, int y) throws IOException {
		int shift = 31 - zoom;
		int left = x << shift;
		int top = y << shift;
		// the last column and the last row end exactly at 2^31, which does not fit a signed int -
		// VectorMetatile guards it the same way, without this the edge tiles render garbage
		int tileSize31 = 1 << shift;
		if (tileSize31 <= 0) {
			tileSize31 = Integer.MAX_VALUE;
		}
		int right = left + tileSize31;
		if (right <= 0) {
			right = Integer.MAX_VALUE;
		}
		int bottom = top + tileSize31;
		if (bottom <= 0) {
			bottom = Integer.MAX_VALUE;
		}
		RenderingImageContext ctx = new RenderingImageContext(left, right, top, bottom, zoom);
		return renderer.renderImage(ctx).getImage();
	}

	/**
	 * Hands the reference tiles of a chunk to the download pool and returns at once - the tile is
	 * awaited by {@link #awaitReference} right before it is needed, so the downloads of the next
	 * chunk overlap the rendering of the current one.
	 */
	private void prefetchReferences(List<int[]> chunk) {
		if (chunk == null) {
			return;
		}
		for (int[] t : chunk) {
			final int zoom = t[0], x = t[1], y = t[2];
			String key = zoom + "/" + x + "/" + y;
			if (referenceFile(zoom, x, y).isFile() || pendingReferences.containsKey(key)) {
				continue;
			}
			pendingReferences.put(key, downloadPool.submit((Callable<Void>) () -> {
				downloadReference(zoom, x, y);
				return null;
			}));
		}
	}

	/** Waits for the prefetch of one tile, measuring how much of the run is spent waiting for the net. */
	private void awaitReference(int zoom, int x, int y) {
		Future<?> f = pendingReferences.remove(zoom + "/" + x + "/" + y);
		if (f == null) {
			return;
		}
		long start = System.nanoTime();
		try {
			f.get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			// the tile is simply skipped later
		} finally {
			referenceWaitNs += System.nanoTime() - start;
		}
	}

	private String referenceUrl(int zoom, int x, int y) {
		return casesFile.referenceUrl.replace("{z}", String.valueOf(zoom)).replace("{x}", String.valueOf(x))
				.replace("{y}", String.valueOf(y));
	}

	/** How many reference tiles a previous run left behind - they are not downloaded again. */
	private int countCachedReferences() {
		int n = 0;
		File[] zooms = referenceCacheDir.listFiles();
		if (zooms == null) {
			return 0;
		}
		for (File z : zooms) {
			File[] columns = z.listFiles();
			if (columns == null) {
				continue;
			}
			for (File c : columns) {
				String[] tiles = c.list();
				n += tiles == null ? 0 : tiles.length;
			}
		}
		return n;
	}

	private File referenceFile(int zoom, int x, int y) {
		return new File(referenceCacheDir, zoom + "/" + x + "/" + y + ".png");
	}

	private void downloadReference(int zoom, int x, int y) throws IOException {
		File cached = referenceFile(zoom, x, y);
		cached.getParentFile().mkdirs();
		download(referenceUrl(zoom, x, y), cached);
	}

	private BufferedImage reference(int zoom, int x, int y) {
		File cached = referenceFile(zoom, x, y);
		if (!cached.isFile() || cached.length() == 0) {
			try {
				downloadReference(zoom, x, y);
			} catch (IOException e) {
				System.err.println("Can't download the reference tile " + zoom + "/" + x + "/" + y
						+ ": " + e.getMessage());
				cached.delete();
				return null;
			}
		}
		try {
			BufferedImage img = ImageIO.read(cached);
			if (!cacheReference) {
				cached.delete();
			}
			return img;
		} catch (IOException e) {
			cached.delete();
			return null;
		}
	}

	private static void download(String url, File target) throws IOException {
		HttpURLConnection cn = (HttpURLConnection) new URL(url).openConnection();
		cn.setRequestProperty("User-Agent", "OsmAnd-CoastlineRenderingTester");
		cn.setConnectTimeout(30000);
		// a reference tile is a few KB - a minute is already a stuck connection, and one stuck
		// download must not hold up the whole chunk
		cn.setReadTimeout(60000);
		if (cn.getResponseCode() != 200) {
			throw new IOException("HTTP " + cn.getResponseCode() + " for " + url);
		}
		File tmp = new File(target.getAbsolutePath() + ".tmp");
		try (InputStream is = cn.getInputStream(); FileOutputStream fos = new FileOutputStream(tmp)) {
			Algorithms.streamCopy(is, fos);
		}
		target.delete();
		if (!tmp.renameTo(target)) {
			throw new IOException("Can't rename " + tmp);
		}
	}

	// ----------------------------------------------------------------- masks

	private static boolean[] waterMask(BufferedImage img, int[] waterColors) {
		int w = img.getWidth(), h = img.getHeight();
		boolean[] mask = new boolean[w * h];
		for (int y = 0; y < h; y++) {
			for (int x = 0; x < w; x++) {
				mask[y * w + x] = isColor(img.getRGB(x, y), waterColors);
			}
		}
		return mask;
	}

	private static boolean isColor(int rgb, int[] colors) {
		int r = (rgb >> 16) & 0xff, g = (rgb >> 8) & 0xff, b = rgb & 0xff;
		for (int c : colors) {
			if (Math.abs(r - ((c >> 16) & 0xff)) <= COLOR_TOLERANCE
					&& Math.abs(g - ((c >> 8) & 0xff)) <= COLOR_TOLERANCE
					&& Math.abs(b - (c & 0xff)) <= COLOR_TOLERANCE) {
				return true;
			}
		}
		return false;
	}

	private static boolean[] erode(boolean[] m, int w, int h, int r) {
		return morph(m, w, h, r, true);
	}

	private static boolean[] dilate(boolean[] m, int w, int h, int r) {
		return morph(m, w, h, r, false);
	}

	/** Square erosion / dilation, border pixels are clamped. */
	private static boolean[] morph(boolean[] m, int w, int h, int r, boolean erode) {
		if (r <= 0) {
			return m;
		}
		boolean[] tmp = new boolean[w * h];
		for (int y = 0; y < h; y++) {
			for (int x = 0; x < w; x++) {
				boolean v = erode;
				for (int i = -r; i <= r; i++) {
					boolean p = m[y * w + clamp(x + i, w)];
					v = erode ? (v && p) : (v || p);
				}
				tmp[y * w + x] = v;
			}
		}
		boolean[] res = new boolean[w * h];
		for (int y = 0; y < h; y++) {
			for (int x = 0; x < w; x++) {
				boolean v = erode;
				for (int i = -r; i <= r; i++) {
					boolean p = tmp[clamp(y + i, h) * w + x];
					v = erode ? (v && p) : (v || p);
				}
				res[y * w + x] = v;
			}
		}
		return res;
	}

	private static boolean[] not(boolean[] m) {
		boolean[] r = new boolean[m.length];
		for (int i = 0; i < m.length; i++) {
			r[i] = !m[i];
		}
		return r;
	}

	private static boolean[] or(boolean[] a, boolean[] b) {
		boolean[] r = new boolean[a.length];
		for (int i = 0; i < a.length; i++) {
			r[i] = a[i] || b[i];
		}
		return r;
	}

	private static boolean[] and(boolean[] a, boolean[] b) {
		boolean[] r = new boolean[a.length];
		for (int i = 0; i < a.length; i++) {
			r[i] = a[i] && b[i];
		}
		return r;
	}

	private static int count(boolean[] m) {
		int c = 0;
		for (boolean b : m) {
			if (b) {
				c++;
			}
		}
		return c;
	}

	private static int countDrawnOverBackground(BufferedImage empty, BufferedImage img, int background) {
		int c = 0;
		for (int y = 0; y < empty.getHeight(); y++) {
			for (int x = 0; x < empty.getWidth(); x++) {
				if ((empty.getRGB(x, y) & 0xffffff) == background
						&& (img.getRGB(x, y) & 0xffffff) != background) {
					c++;
				}
			}
		}
		return c;
	}

	private static int dominantColor(BufferedImage img) {
		Map<Integer, Integer> colors = new LinkedHashMap<>();
		for (int y = 0; y < img.getHeight(); y++) {
			for (int x = 0; x < img.getWidth(); x++) {
				colors.merge(img.getRGB(x, y) & 0xffffff, 1, Integer::sum);
			}
		}
		return colors.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
	}

	/** Rendered tile, desaturated, with extra water in red and missing water in blue. */
	private static BufferedImage diffImage(BufferedImage rendered, boolean[] extra, boolean[] missing) {
		int w = rendered.getWidth(), h = rendered.getHeight();
		BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
		for (int y = 0; y < h; y++) {
			for (int x = 0; x < w; x++) {
				int rgb = rendered.getRGB(x, y);
				int gray = (((rgb >> 16) & 0xff) + ((rgb >> 8) & 0xff) + (rgb & 0xff)) / 3;
				gray = 128 + gray / 2;
				int v = (gray << 16) | (gray << 8) | gray;
				if (extra[y * w + x]) {
					v = 0xff0000;
				} else if (missing[y * w + x]) {
					v = 0x0000ff;
				}
				img.setRGB(x, y, v);
			}
		}
		return img;
	}

	/** Nearest neighbour downscale - keeps the exact colors of the style. */
	private static BufferedImage scaleDown(BufferedImage src, int w, int h) {
		if (src.getWidth() == w && src.getHeight() == h) {
			return src;
		}
		BufferedImage res = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
		for (int y = 0; y < h; y++) {
			for (int x = 0; x < w; x++) {
				res.setRGB(x, y, src.getRGB(x * src.getWidth() / w, y * src.getHeight() / h));
			}
		}
		return res;
	}

	// ----------------------------------------------------------------- reporting

	/** Cases in report order: the fixed ones first, then the rest - the run order differs. */
	private static List<CaseStats> orderedCases(RunResult result) {
		List<CaseStats> res = new ArrayList<>(result.cases);
		res.sort((a, b) -> {
			String ga = a.group == null ? GROUP_FIXED : a.group;
			String gb = b.group == null ? GROUP_FIXED : b.group;
			return GROUP_FIXED.equals(ga) == GROUP_FIXED.equals(gb) ? ga.compareTo(gb)
					: (GROUP_FIXED.equals(ga) ? -1 : 1);
		});
		return res;
	}

	private void recomputeTotals() {
		result.tiles = 0;
		result.comparedTiles = 0;
		result.failedTiles = 0;
		Map<String, GroupTotals> byGroup = new LinkedHashMap<>();
		for (CaseStats s : result.cases) {
			result.tiles += s.tiles;
			result.comparedTiles += s.comparedTiles;
			result.failedTiles += s.failedTiles;
			GroupTotals g = byGroup.computeIfAbsent(s.group == null ? GROUP_FIXED : s.group, k -> {
				GroupTotals t = new GroupTotals();
				t.group = k;
				return t;
			});
			g.tiles += s.comparedTiles;
			g.failedTiles += s.failedTiles;
		}
		result.groups = new ArrayList<>(byGroup.values());
	}

	/**
	 * Writes the html report and summary.json every {@code flushEvery} tiles, so that a long run can
	 * be watched while it goes and does not look stuck. Also prints how far it is and how long the
	 * rest is going to take.
	 */
	private void flush(CaseStats stats, int totalOfCase) throws IOException {
		if (++tilesSinceFlush < flushEvery) {
			return;
		}
		tilesSinceFlush = 0;
		long now = System.currentTimeMillis();
		recomputeTotals();
		result.loadedMaps = initializedMaps.size();
		result.durationMs = now - result.startedAt;
		writeSummaryJson(result, true);
		if (writeHtml) {
			writeHtmlReport(result, true);
		}
		double perSec = result.comparedTiles * 1000.0 / Math.max(1, now - result.startedAt);
		String eta = totalOfCase > 0 && perSec > 0
				? String.format(", eta %s", duration((long) ((totalOfCase - stats.tiles) / perSec * 1000)))
				: "";
		// how much of the wall clock went into waiting for a reference tile that the pool had not
		// finished yet - if this is high the run is bound by tile.osmand.net, not by the renderer
		long net = referenceWaitNs / 1000000 * 100 / Math.max(1, now - result.startedAt);
		System.out.printf("  ... %d of %d tiles, %d failed, %.1f tiles/s, %d%% waiting for references%s%n",
				stats.tiles, totalOfCase, stats.failedTiles, perSec, net, eta);
		lastFlush = now;
	}

	private static String duration(long ms) {
		long sec = ms / 1000;
		return sec < 60 ? sec + "s" : (sec < 3600 ? (sec / 60) + "m " + (sec % 60) + "s"
				: (sec / 3600) + "h " + ((sec % 3600) / 60) + "m");
	}

	private CaseStats newStats(CaseDef def) {
		CaseStats stats = new CaseStats();
		stats.issue = def.issue;
		stats.title = def.title;
		stats.url = def.url;
		stats.check = def.check;
		stats.group = def.group;
		result.cases.add(stats);
		return stats;
	}

	private boolean saveTile(boolean ok) {
		if ("none".equalsIgnoreCase(saveImages)) {
			return false;
		}
		return "all".equalsIgnoreCase(saveImages) || !ok;
	}

	private void keepForReport(TileResult res) {
		if (!res.ok() || "all".equalsIgnoreCase(saveImages)) {
			if (reported.size() < MAX_REPORTED_TILES) {
				reported.add(res);
			}
		}
	}

	private File caseDir(CaseDef def) {
		return new File(outputDir, String.valueOf(def.issue));
	}

	private static String tileName(int zoom, int x, int y, String suffix) {
		return String.format("%d_%d_%d_%s.png", zoom, x, y, suffix);
	}

	private static String pct(double v) {
		return String.format("%.2f%%", v * 100);
	}

	private void printSummary(RunResult result) {
		System.out.println();
		System.out.println("================================ coastline summary ================================");
		System.out.printf("%-58s %7s %7s %9s %9s%n", "case", "tiles", "failed", "worst+H2O", "worst-H2O");
		String printedGroup = null;
		for (CaseStats s : orderedCases(result)) {
			String g = s.group == null ? GROUP_FIXED : s.group;
			if (!g.equals(printedGroup)) {
				printedGroup = g;
				System.out.println("-- " + g);
			}
			boolean seamarks = CHECK_SEAMARKS_INLAND.equals(s.check);
			System.out.printf("%-58s %7d %7d %9s %9s%n",
					trim("#" + s.issue + " " + s.title, 58), s.comparedTiles, s.failedTiles,
					pct(s.worstExtraWater), seamarks ? "-" : pct(s.worstMissingWater));
			if (s.failedTiles > 0 && seamarks) {
				System.out.printf("%-58s worst tile %s, drawn by the map: worst %s, avg %s%n", "",
						s.worstTile, pct(s.worstExtraWater), pct(s.avgExtraWater()));
			} else if (s.failedTiles > 0) {
				System.out.printf("%-58s worst tile %s, avg +H2O %s, avg -H2O %s%n", "",
						s.worstTile, pct(s.avgExtraWater()), pct(s.avgMissingWater()));
			}
			if (s.styledSaltPonds > 0.0001) {
				System.out.printf("%-58s %s of the extra water is styled salt ponds (ignored)%n", "",
						pct(s.styledSaltPonds / Math.max(1, s.comparedTiles)));
			}
		}
		System.out.println("-----------------------------------------------------------------------------------");
		for (GroupTotals g : result.groups) {
			System.out.printf("%-58s %7d %7d%n", g.group, g.tiles, g.failedTiles);
		}
		System.out.printf("%d tiles compared, %d failed, %d maps loaded, %.1f s%n", result.comparedTiles,
				result.failedTiles, result.loadedMaps, result.durationMs / 1000.0);
		System.out.println(result.failedTiles > 0
				? "COASTLINE PROBLEMS REPRODUCED - exit code 2"
				: "No coastline problems found - exit code 0");
	}

	private static String trim(String s, int len) {
		return s.length() <= len ? s : s.substring(0, len - 1) + "…";
	}

	private void writeSummaryJson(RunResult result) throws IOException {
		writeSummaryJson(result, false);
	}

	private void writeSummaryJson(RunResult result, boolean quiet) throws IOException {
		File f = new File(outputDir, "summary.json");
		try (Writer w = new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8)) {
			new Gson().toJson(result, w);
		}
		if (!quiet) {
			System.out.println("Summary json: " + f.getAbsolutePath());
		}
	}

	/**
	 * Writes {@code <out>/index.html} - the per case statistics plus every failed tile with its
	 * rendered / reference / diff images.
	 */
	private void writeHtmlReport(RunResult result) throws IOException {
		writeHtmlReport(result, false);
	}

	private void writeHtmlReport(RunResult result, boolean quiet) throws IOException {
		reported.sort((a, b) -> {
			String ga = a.def.group == null ? GROUP_FIXED : a.def.group;
			String gb = b.def.group == null ? GROUP_FIXED : b.def.group;
			if (!ga.equals(gb)) {
				// fixed cases first, they are the known problems
				return GROUP_FIXED.equals(ga) ? -1 : (GROUP_FIXED.equals(gb) ? 1 : ga.compareTo(gb));
			}
			if (a.ok() != b.ok()) {
				return a.ok() ? 1 : -1;
			}
			return Double.compare(b.severity, a.severity);
		});
		StringBuilder sb = new StringBuilder();
		sb.append("<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n");
		sb.append("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n");
		// Jenkins serves the workspace with "style-src 'self'", which forbids an inline <style>,
		// so the css goes into a file next to index.html
		sb.append("<title>OsmAnd coastline tiles</title>\n");
		sb.append("<link rel=\"stylesheet\" href=\"" + REPORT_CSS_FILE + "\">\n</head><body>\n");
		sb.append("<header><h1>Coastline rendering &mdash; epic 3291</h1>");
		sb.append(String.format("<p class=\"sum\"><b class=\"%s\">%d failed</b> &middot; %d tiles compared "
						+ "&middot; %d maps &middot; style %s &middot; %.1f s &middot; %s</p>",
				result.failedTiles > 0 ? "bad" : "good", result.failedTiles, result.comparedTiles,
				result.loadedMaps, esc(result.style), result.durationMs / 1000.0, new java.util.Date()));
		if (result.groups.size() > 1) {
			StringBuilder g = new StringBuilder();
			for (GroupTotals t : result.groups) {
				g.append(g.length() == 0 ? "" : " &middot; ").append(String.format(
						"%s: <b class=\"%s\">%d failed</b> of %d tiles", esc(t.group),
						t.failedTiles > 0 ? "bad" : "good", t.failedTiles, t.tiles));
			}
			sb.append("<p class=\"sum groups\">").append(g).append("</p>");
		}
		sb.append("</header>\n<main>\n<table class=\"stats\"><tr><th>case</th><th>tiles</th><th>failed</th>"
				+ "<th>worst extra water</th><th>worst missing water</th><th>worst tile</th></tr>");
		String tableGroup = null;
		for (CaseStats s : orderedCases(result)) {
			String g = s.group == null ? GROUP_FIXED : s.group;
			if (!g.equals(tableGroup)) {
				tableGroup = g;
				sb.append(String.format("<tr class=\"grp\"><td colspan=\"6\">%s</td></tr>", esc(g)));
			}
			boolean seamarks = CHECK_SEAMARKS_INLAND.equals(s.check);
			sb.append(String.format("<tr class=\"%s\"><td>%s#%d</a> %s</td><td>%d</td><td>%d</td>"
							+ "<td>%s</td><td>%s</td><td>%s</td></tr>", s.failedTiles > 0 ? "bad" : "good",
					s.url == null ? "<a>" : "<a href=\"" + esc(s.url) + "\">", s.issue, esc(s.title),
					s.comparedTiles, s.failedTiles,
					seamarks ? pct(s.worstExtraWater) + " drawn" : pct(s.worstExtraWater),
					seamarks ? "&mdash;" : pct(s.worstMissingWater), esc(s.worstTile)));
		}
		sb.append("</table>\n");
		if (reported.isEmpty()) {
			sb.append("<p class=\"empty\">No failed tiles.</p>");
		}
		int lastIssue = -1;
		String lastTitle = null;
		String lastGroup = null;
		for (TileResult r : reported) {
			String g = r.def.group == null ? GROUP_FIXED : r.def.group;
			if (!g.equals(lastGroup)) {
				lastGroup = g;
				lastIssue = -1;
				lastTitle = null;
				sb.append(String.format("<h1 class=\"grp\">%s</h1>\n", esc(g)));
			}
			if (r.def.issue != lastIssue || !r.def.title.equals(lastTitle)) {
				lastIssue = r.def.issue;
				lastTitle = r.def.title;
				sb.append(String.format("<h2>%s#%d</a> %s</h2>\n",
						r.def.url == null ? "<a>" : "<a href=\"" + esc(r.def.url) + "\">",
						r.def.issue, esc(r.def.title)));
			}
			double lat = MapUtils.getLatitudeFromTile(r.zoom, r.y + 0.5);
			double lon = MapUtils.getLongitudeFromTile(r.zoom, r.x + 0.5);
			sb.append(String.format("<section class=\"tile %s\"><div class=\"hd\"><b>%d/%d/%d</b>"
							+ "<a href=\"%s/map/#%d/%.4f/%.4f\" title=\"open this place on the map\">map</a>"
							+ "<a href=\"%s/tile/df/%d/%d/%d.png\" title=\"the same tile rendered by the server\">"
							+ "server tile</a>"
							+ "<a href=\"%s\" title=\"the reference tile\">reference tile</a>"
							+ "<span class=\"badge\">%s</span></div>", r.ok() ? "good" : "bad",
					r.zoom, r.x, r.y, MAP_SERVER, r.zoom, lat, lon, MAP_SERVER, r.zoom, r.x, r.y,
					esc(referenceUrl(r.zoom, r.x, r.y)), r.ok() ? "ok" : "failed"));
			if (!r.images.isEmpty()) {
				sb.append("<div class=\"imgs\">");
				for (Map.Entry<String, String> e : r.images.entrySet()) {
					sb.append(String.format("<figure><img loading=\"lazy\" src=\"%d/%s\" alt=\"%s\">"
									+ "<figcaption>%s</figcaption></figure>", r.def.issue, e.getValue(),
							esc(e.getKey()), esc(e.getKey())));
				}
				sb.append("</div>");
			}
			sb.append("<dl>");
			for (Map.Entry<String, String> e : r.metrics.entrySet()) {
				sb.append(String.format("<dt>%s</dt><dd>%s</dd>", esc(e.getKey()), esc(e.getValue())));
			}
			sb.append("</dl>");
			for (String p : r.problems) {
				sb.append("<p class=\"problem\">").append(esc(p)).append("</p>");
			}
			sb.append("</section>\n");
		}
		sb.append("</main>\n</body></html>\n");
		File css = new File(outputDir, REPORT_CSS_FILE);
		try (Writer wr = new OutputStreamWriter(new FileOutputStream(css), StandardCharsets.UTF_8)) {
			wr.write(REPORT_CSS);
		}
		File report = new File(outputDir, "index.html");
		try (Writer wr = new OutputStreamWriter(new FileOutputStream(report), StandardCharsets.UTF_8)) {
			wr.write(sb.toString());
		}
		if (!quiet) {
			System.out.println("HTML report : " + report.getAbsolutePath());
		}
	}

	private static final String REPORT_CSS_FILE = "styles.css";

	private static final String REPORT_CSS =
			":root{--bg:#fff;--fg:#1b1b1f;--mut:#6a6a75;--line:#e2e2e8;--card:#fafafc;"
			+ "--bad:#c62828;--good:#2e7d32;--badbg:#fdecea;--goodbg:#edf7ed}\n"
			+ "@media(prefers-color-scheme:dark){:root{--bg:#16171a;--fg:#e8e8ec;--mut:#9a9aa5;"
			+ "--line:#2c2d32;--card:#1e1f23;--bad:#ff6b6b;--good:#7bd88f;--badbg:#2a1a1c;--goodbg:#18251b}}\n"
			+ "*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);"
			+ "font:14px/1.5 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif}\n"
			+ "header{border-bottom:1px solid var(--line);padding:14px 20px}h1{font-size:17px;margin:0 0 4px}\n"
			+ ".sum{margin:0;color:var(--mut)}.bad{color:var(--bad)}.good{color:var(--good)}\n"
			+ "main{padding:0 20px 40px}.empty{color:var(--mut)}\n"
			+ "table.stats{border-collapse:collapse;margin:18px 0;font-size:13px;max-width:100%;overflow-x:auto}\n"
			+ "table.stats th{text-align:left;color:var(--mut);font-weight:500;border-bottom:1px solid var(--line)}\n"
			+ "table.stats td,table.stats th{padding:5px 14px 5px 0;white-space:nowrap}\n"
			+ "table.stats td:nth-child(n+2){text-align:right;font-variant-numeric:tabular-nums}\n"
			+ "table.stats tr.bad td{color:var(--bad)}table.stats a{color:inherit}\n"
			+ "table.stats tr.grp td{padding-top:14px;color:var(--fg);font-weight:600;text-align:left}\n"
			+ "h1.grp{font-size:15px;margin:30px 0 0;padding:10px 0 0;border-top:2px solid var(--line)}\n"
			+ ".sum.groups{margin-top:6px}\n"
			+ "h2{font-size:15px;margin:26px 0 10px;padding-top:10px;border-top:1px solid var(--line)}\n"
			+ "h2 a{color:inherit}.tile{display:inline-block;vertical-align:top;margin:0 12px 12px 0;"
			+ "padding:10px;border:1px solid var(--line);border-radius:10px;background:var(--card)}\n"
			+ ".tile.bad{border-color:var(--bad);background:var(--badbg)}\n"
			+ ".hd{display:flex;align-items:center;gap:10px;margin-bottom:8px}\n"
			+ ".hd a{color:var(--mut);font-size:12px;white-space:nowrap}\n"
			+ ".badge{margin-left:auto;font-size:11px;text-transform:uppercase;letter-spacing:.06em;"
			+ "padding:2px 8px;border-radius:20px;background:var(--goodbg);color:var(--good)}\n"
			+ ".tile.bad .badge{background:var(--bad);color:#fff}\n"
			+ ".imgs{display:flex;gap:8px}figure{margin:0}\n"
			+ "img{display:block;width:256px;height:256px;image-rendering:pixelated;border-radius:4px;"
			+ "border:1px solid var(--line);background:#fff}\n"
			+ "figcaption{font-size:11px;color:var(--mut);text-align:center;padding-top:3px}\n"
			+ "dl{display:grid;grid-template-columns:auto auto;gap:1px 10px;margin:8px 0 0;font-size:12px}\n"
			+ "dt{color:var(--mut)}dd{margin:0;text-align:right;font-variant-numeric:tabular-nums}\n"
			+ ".problem{margin:8px 0 0;font-size:12px;color:var(--bad)}\n";

	private static String esc(String s) {
		return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
				.replace("\"", "&quot;");
	}

	// ----------------------------------------------------------------- environment

	/**
	 * Folder the running jar sits in - the root of an unpacked OsmAndMapCreator.zip, where
	 * {@code fonts/} and {@code lib/} live. Null when the classes are not run from a jar.
	 */
	private static File distributionDir() {
		try {
			File f = new File(CoastlineRenderingTester.class.getProtectionDomain().getCodeSource()
					.getLocation().toURI());
			return f.isFile() ? f.getParentFile() : null;
		} catch (Exception e) {
			return null;
		}
	}

	private File findFonts() {
		String explicit = opt("fonts", null);
		if (explicit != null) {
			File f = new File(explicit);
			return f.isDirectory() ? f : null;
		}
		List<File> candidates = new ArrayList<>();
		File dist = distributionDir();
		if (dist != null) {
			// unpacked OsmAndMapCreator.zip: <dir>/fonts, the jar itself is in <dir> or <dir>/lib
			candidates.add(new File(dist, "fonts"));
			candidates.add(new File(dist.getParentFile(), "fonts"));
		}
		candidates.add(new File(repoRoot(), "resources/rendering_styles/fonts"));
		for (File f : candidates) {
			if (f.isDirectory()) {
				return f;
			}
		}
		return null;
	}

	private File repoRoot() {
		File f = new File(System.getProperty("user.dir")).getAbsoluteFile();
		while (f != null) {
			if (new File(f, "resources/rendering_styles").isDirectory()
					&& new File(f, "core-legacy").isDirectory()) {
				return f;
			}
			f = f.getParentFile();
		}
		return new File(System.getProperty("user.dir"));
	}

	/**
	 * Explicit {@code -native=}, else the legacy core of a local repository checkout. Null means
	 * "let NativeJavaRendering load the library bundled into OsmAndMapCreator", which is how every
	 * utility of the distribution runs.
	 */
	private File findNativeLibrary() {
		String explicit = opt("native", null);
		if (explicit != null) {
			File f = new File(explicit);
			if (!f.exists()) {
				throw new IllegalStateException("-native=" + explicit + " does not exist");
			}
			return f;
		}
		String os = System.getProperty("os.name").toLowerCase();
		String ext = os.contains("mac") || os.contains("darwin") ? "dylib" : (os.contains("win") ? "dll" : "so");
		String libName = (os.contains("win") ? "" : "lib") + "osmand." + ext;
		List<File> found = new ArrayList<>();
		collect(new File(repoRoot(), "core-legacy/binaries"), libName, found, 4);
		return found.isEmpty() ? null : found.get(0);
	}

	private static void collect(File dir, String name, List<File> res, int depth) {
		if (depth < 0 || !dir.isDirectory()) {
			return;
		}
		File[] ls = dir.listFiles();
		if (ls == null) {
			return;
		}
		for (File f : ls) {
			if (f.isDirectory()) {
				collect(f, name, res, depth - 1);
			} else if (f.getName().equals(name)) {
				res.add(f);
			}
		}
	}
}
