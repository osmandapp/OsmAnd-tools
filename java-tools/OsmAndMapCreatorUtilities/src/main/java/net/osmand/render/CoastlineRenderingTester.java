package net.osmand.render;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.imageio.ImageIO;

import com.google.gson.Gson;

import net.osmand.NativeJavaRendering;
import net.osmand.NativeJavaRendering.RenderingImageContext;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

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
 * <b>1</b> - the tester could not run (no native library, no maps, broken json).
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
 * <li>{@code scan}, {@code minzoom}, {@code maxzoom}, {@code bbox} - scan every tile of a zoom
 * range instead of the cases; {@code bbox} is {@code leftLon,bottomLat,rightLon,topLat} and
 * defaults to the whole world;</li>
 * <li>{@code out} - output folder, default {@code build/coastline-tiles};</li>
 * <li>{@code save} - {@code failed} (default) writes the png tiles of the failed tiles only,
 * {@code all} writes everything, {@code none} keeps statistics only;</li>
 * <li>{@code native}, {@code fonts}, {@code style} - renderer setup, autodetected;</li>
 * <li>{@code threads} - parallel reference tile downloads, default 8;</li>
 * <li>{@code download} - {@code false} to never download a missing map;</li>
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

	/** Max per channel difference to still treat a pixel as water. */
	private static final int COLOR_TOLERANCE = 10;

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

	private CasesFile casesFile;
	private NativeJavaRendering renderer;
	private final Set<String> initializedMaps = new LinkedHashSet<>();
	private final List<TileResult> reported = new ArrayList<>();
	private ExecutorService downloadPool;

	public CoastlineRenderingTester(Map<String, String> options) {
		this.options = options;
		this.mapsDir = new File(opt("maps.dir", new File(System.getProperty("user.home"), "osmand/maps")
				.getAbsolutePath()));
		this.outputDir = new File(opt("out", "build/coastline-tiles"));
		this.referenceCacheDir = new File(opt("referenceDir", new File(outputDir, "reference").getPath()));
		this.loadAllMaps = !"case".equalsIgnoreCase(opt("load", "all"));
		this.downloadMaps = Boolean.parseBoolean(opt("download", "true"));
		this.cacheReference = Boolean.parseBoolean(opt("referenceCache", "true"));
		this.saveImages = opt("save", "failed");
		this.tileSize = Integer.parseInt(opt("tileSize", "256"));
		this.maskTolerance = Integer.parseInt(opt("tolerance", "4"));
		this.threads = Integer.parseInt(opt("threads", "8"));
		this.writeHtml = Boolean.parseBoolean(opt("html", "true"));
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
		downloadPool = Executors.newFixedThreadPool(threads);

		RunResult result = new RunResult();
		result.style = opt("style", "default.render.xml");
		result.mapsDir = mapsDir.getAbsolutePath();
		result.startedAt = start;
		try {
			// the seamarks cases close every map, so they go last
			cases.sort((a, b) -> Boolean.compare(a.isSeamarksCheck(), b.isSeamarksCheck()));
			for (CaseDef def : cases) {
				CaseStats stats = def.isSeamarksCheck() ? runSeamarksCase(def) : runWaterCase(def);
				result.cases.add(stats);
				result.tiles += stats.tiles;
				result.comparedTiles += stats.comparedTiles;
				result.failedTiles += stats.failedTiles;
			}
		} finally {
			downloadPool.shutdownNow();
		}
		result.loadedMaps = initializedMaps.size();
		result.durationMs = System.currentTimeMillis() - start;
		printSummary(result);
		writeSummaryJson(result);
		if (writeHtml) {
			writeHtmlReport(result);
		}
		return result;
	}

	private List<CaseDef> selectCases() {
		if (Boolean.parseBoolean(opt("scan", "false"))) {
			CaseDef scan = new CaseDef();
			scan.issue = 3291;
			scan.title = "Full scan";
			scan.url = "https://github.com/osmandapp/OsmAnd-Issues/issues/3291";
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
		for (CaseDef def : casesFile.cases) {
			if (issue == null || issue.equals(String.valueOf(def.issue))) {
				res.add(def);
			}
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
		File nativeLib = findNativeLibrary();
		if (nativeLib == null) {
			throw new IllegalStateException("Native library libosmand is not found, build core-legacy "
					+ "or pass -native=<path to libosmand.dylib/so/dll>");
		}
		File fonts = findFonts();
		String style = opt("style", "default.render.xml");
		System.out.println("Native library : " + nativeLib.getAbsolutePath());
		System.out.println("Fonts          : " + (fonts == null ? "none" : fonts.getAbsolutePath()));
		System.out.println("Maps           : " + mapsDir.getAbsolutePath());
		System.out.println("Output         : " + outputDir.getAbsolutePath());
		System.out.println("Style          : " + style);

		renderer = NativeJavaRendering.getDefault(nativeLib.getAbsolutePath(), null,
				fonts == null ? null : fonts.getAbsolutePath());
		if (renderer == null) {
			throw new IllegalStateException("Native library " + nativeLib + " could not be loaded");
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

	/** Initializes every map of the maps folder - the mode the server scan uses. */
	private void initAllMaps() {
		File[] ls = mapsDir.listFiles();
		if (ls == null) {
			System.err.println("Maps folder " + mapsDir.getAbsolutePath() + " does not exist");
			return;
		}
		String[] excluded = opt("exclude", DEFAULT_EXCLUDED_MAPS).split(",");
		List<File> maps = new ArrayList<>();
		List<String> skipped = new ArrayList<>();
		for (File f : ls) {
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
		System.out.println("=== " + def + (def.bbox != null ? " bbox " + Arrays.toString(def.bbox)
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
		for (List<int[]> chunk : chunks(def)) {
			prefetchReferences(chunk);
			for (int[] t : chunk) {
				compareTile(def, stats, dir, t[0], t[1], t[2]);
			}
			if (stats.comparedTiles > 0 && stats.tiles % (CHUNK * 4) == 0) {
				System.out.printf("  ... %d tiles, %d failed%n", stats.tiles, stats.failedTiles);
			}
		}
		System.out.printf("  %d tiles, %d compared, %d skipped, %d failed%n", stats.tiles,
				stats.comparedTiles, stats.skippedTiles, stats.failedTiles);
		return stats;
	}

	private void compareTile(CaseDef def, CaseStats stats, File dir, int zoom, int x, int y) throws IOException {
		stats.tiles++;
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
		boolean[] extraAll = and(erode(renderedWater, w, h, maskTolerance),
				not(dilate(referenceWater, w, h, maskTolerance)));
		boolean[] extra = and(extraAll, not(shaded));
		boolean[] missing = and(erode(referenceWater, w, h, maskTolerance),
				not(dilate(renderedWater, w, h, maskTolerance)));
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
		int[] zoomList = def.zoomList();
		return () -> new java.util.Iterator<List<int[]>>() {
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

	private static int clamp(int v, int max) {
		return v < 0 ? 0 : (v >= max ? max - 1 : v);
	}

	private BufferedImage render(int zoom, int x, int y) throws IOException {
		RenderingImageContext ctx = new RenderingImageContext(MapUtils.getLatitudeFromTile(zoom, y + 0.5),
				MapUtils.getLongitudeFromTile(zoom, x + 0.5), tileSize, tileSize, zoom, tileSize / 256);
		return renderer.renderImage(ctx).getImage();
	}

	/** Downloads the reference tiles of a chunk in parallel, so that rendering never waits for the net. */
	private void prefetchReferences(List<int[]> chunk) {
		List<Future<?>> futures = new ArrayList<>();
		for (int[] t : chunk) {
			final int zoom = t[0], x = t[1], y = t[2];
			if (referenceFile(zoom, x, y).isFile()) {
				continue;
			}
			futures.add(downloadPool.submit((Callable<Void>) () -> {
				downloadReference(zoom, x, y);
				return null;
			}));
		}
		for (Future<?> f : futures) {
			try {
				f.get();
			} catch (Exception e) {
				// the tile is simply skipped later
			}
		}
	}

	private File referenceFile(int zoom, int x, int y) {
		return new File(referenceCacheDir, zoom + "/" + x + "/" + y + ".png");
	}

	private void downloadReference(int zoom, int x, int y) throws IOException {
		File cached = referenceFile(zoom, x, y);
		cached.getParentFile().mkdirs();
		String url = casesFile.referenceUrl.replace("{z}", String.valueOf(zoom))
				.replace("{x}", String.valueOf(x)).replace("{y}", String.valueOf(y));
		download(url, cached);
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
		cn.setReadTimeout(300000);
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

	private CaseStats newStats(CaseDef def) {
		CaseStats stats = new CaseStats();
		stats.issue = def.issue;
		stats.title = def.title;
		stats.url = def.url;
		stats.check = def.check;
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
		for (CaseStats s : result.cases) {
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
		File f = new File(outputDir, "summary.json");
		try (Writer w = new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8)) {
			new Gson().toJson(result, w);
		}
		System.out.println("Summary json: " + f.getAbsolutePath());
	}

	/**
	 * Writes {@code <out>/index.html} - the per case statistics plus every failed tile with its
	 * rendered / reference / diff images.
	 */
	private void writeHtmlReport(RunResult result) throws IOException {
		reported.sort((a, b) -> {
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
		sb.append("</header>\n<main>\n<table class=\"stats\"><tr><th>case</th><th>tiles</th><th>failed</th>"
				+ "<th>worst extra water</th><th>worst missing water</th><th>worst tile</th></tr>");
		for (CaseStats s : result.cases) {
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
		for (TileResult r : reported) {
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
							+ "<a href=\"https://osmand.net/map?pin=%f,%f#%d/%f/%f\">map</a>"
							+ "<span class=\"badge\">%s</span></div>", r.ok() ? "good" : "bad",
					r.zoom, r.x, r.y, lat, lon, r.zoom, lat, lon, r.ok() ? "ok" : "failed"));
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
		System.out.println("HTML report : " + report.getAbsolutePath());
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
			+ "h2{font-size:15px;margin:26px 0 10px;padding-top:10px;border-top:1px solid var(--line)}\n"
			+ "h2 a{color:inherit}.tile{display:inline-block;vertical-align:top;margin:0 12px 12px 0;"
			+ "padding:10px;border:1px solid var(--line);border-radius:10px;background:var(--card)}\n"
			+ ".tile.bad{border-color:var(--bad);background:var(--badbg)}\n"
			+ ".hd{display:flex;align-items:center;gap:10px;margin-bottom:8px}\n"
			+ ".hd a{color:var(--mut);font-size:12px}\n"
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

	private File findNativeLibrary() {
		String explicit = opt("native", null);
		if (explicit != null) {
			File f = new File(explicit);
			return f.exists() ? f : null;
		}
		String os = System.getProperty("os.name").toLowerCase();
		String ext = os.contains("mac") || os.contains("darwin") ? "dylib" : (os.contains("win") ? "dll" : "so");
		String libName = (os.contains("win") ? "" : "lib") + "osmand." + ext;
		List<File> roots = new ArrayList<>();
		File dist = distributionDir();
		if (dist != null) {
			// unpacked OsmAndMapCreator.zip: the native libraries are shipped in <dir>/lib
			roots.add(dist);
			roots.add(dist.getParentFile());
		}
		roots.add(new File(repoRoot(), "core-legacy/binaries"));
		for (File root : roots) {
			List<File> found = new ArrayList<>();
			collect(root, libName, found, 4);
			if (!found.isEmpty()) {
				return found.get(0);
			}
		}
		return null;
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
