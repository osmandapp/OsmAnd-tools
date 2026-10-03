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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
 * Renders map tiles with the native (legacy, v1) renderer or with the OpenGL core renderer (v2, see
 * {@code renderer}) and compares their <b>water mask</b> with the water mask of the reference raster
 * tiles of {@code https://tile.osmand.net/hd/{z}/{x}/{y}.png},
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
 * # the same cases through the OpenGL engine of the apps instead of the legacy one
 * OsmAndMapCreator/utilities.sh test-coastline-rendering -renderer=opengl -maps.dir=/var/maps
 * </pre>
 * Exit code: <b>0</b> - nothing worse than {@code failAbove}, <b>2</b> - problems were reproduced (a
 * tile whose water difference is above {@code failAbove}, or a tile the renderer crashed on), <b>1</b> -
 * the tester could not run (native library could not be loaded, no maps, broken json).
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
 * {@code World_basemap_2.obf}, or {@code World_basemap_mini_2.obf} when only the mini one is in
 * {@code maps.dir};</li>
 * <li>{@code exclude} - comma separated name parts that {@code load=all} skips, default
 * {@code World_seamarks,basemap_mini} - an overlay and a second basemap would distort the
 * rendering; without {@code World_basemap_2.obf} in {@code maps.dir} the mini basemap is the
 * basemap and only {@code World_seamarks} is skipped; pass {@code -exclude=} to load literally
 * everything;</li>
 * <li>{@code cases} - path to the json with the cases, default the bundled
 * {@code coastline-tests.json}. Its {@code brokenReferences} list areas where the reference tile
 * itself is wrong; their tiles are compared with {@code secondReferenceUrl} (tile.openstreetmap.org)
 * only. Its {@code dataIssues} list areas where the OSM data is wrong, e.g. real islands mapped with
 * {@code place=island} but no coastline; their tiles are skipped;</li>
 * <li>{@code issue} - run only the cases of one issue, e.g. {@code -issue=25618};</li>
 * <li>{@code randomTilesK} - size of the random part of a run in thousands of tiles, default
 * {@value #DEFAULT_RANDOM_TILES_K}; {@code -randomTilesK=0} runs the json cases only. The tiles are
 * spread evenly over zooms {@value #RANDOM_MIN_ZOOM}..{@value #RANDOM_MAX_ZOOM},
 * {@value #SHARE_COASTAL}% of them with a coastline in them and the rest split between open ocean
 * and inland. {@code seed} is fixed, so the same tiles are checked by every run and their reference
 * tiles are cached. {@code minzoom}/{@code maxzoom} do not change what is drawn, they only skip the zooms
 * outside the range, so a slow run can be split into parts that add up to the whole one -
 * {@code -minzoom=1 -maxzoom=6} and {@code -minzoom=7} together are the full run. {@code -random}
 * runs that part alone;</li>
 * <li>{@code scan}, {@code minzoom}, {@code maxzoom}, {@code bbox} - scan every tile of a zoom
 * range instead of the cases; {@code bbox} is {@code leftLon,bottomLat,rightLon,topLat} and
 * defaults to the whole world;</li>
 * <li>{@code out} - output folder, default {@code build/coastline-tiles};</li>
 * <li>{@code save} - {@code failed} (default) writes the png tiles of the failed tiles only,
 * {@code all} writes everything, {@code none} keeps statistics only;</li>
 * <li>{@code renderer} - {@code legacy} (default) draws the tiles with the native library, the same
 * v1 engine the tile server runs; {@code opengl} draws them with OsmAndCore (v2 - the engine of the
 * apps) by driving the {@code eyepiece} tool. Everything else - the cases, the water masks, the
 * report and the exit codes - is the same, so the two runs are directly comparable;</li>
 * <li>{@code eyepiece}, {@code stylesPath} - only for {@code -renderer=opengl}: path to the
 * eyepiece binary (autodetected in {@code binaries} of a repository checkout and on the PATH) and
 * the folder with the {@code *.render.xml} styles (by default the styles built into OsmAndCore).
 * {@code -eyepieceLog=true} echoes everything eyepiece prints, {@code -eyepieceCheck=false} skips
 * the check that the binary has the batch tile mode at all;</li>
 * <li>{@code symbols} - only for {@code -renderer=opengl}: {@code false} (default) renders the
 * tiles without labels and icons. They are 94% of the time of a tile in the v2 engine (measured
 * 500 ms against 30 ms per tile) and say nothing about a coastline, and the fixed cases come out
 * identical either way; {@code -symbols=true} draws them;</li>
 * <li>{@code native} - path to {@code libosmand.dylib/so/dll}; by default the library bundled into
 * OsmAndMapCreator is used, a local repository checkout picks it up from {@code core-legacy}.
 * Ignored by {@code -renderer=opengl}, which needs no legacy library at all;</li>
 * <li>{@code fonts}, {@code style} - renderer setup, autodetected;</li>
 * <li>{@code hide} - comma separated {@code tag=value} objects that are not drawn, default
 * {@value #DEFAULT_HIDE}: the style is patched on the fly (a copy in {@code <out>/style}) because
 * the reference draws the sea over them. {@code -hide=} renders the style as it is;</li>
 * <li>{@code threads} - parallel reference tile downloads, default 16. The legacy renderer renders
 * that many tiles at once too (at most one per processor): the native library keeps global state,
 * so every thread gets its own copy of it, loaded by its own class loader, with its own maps.
 * {@code -renderer=opengl} runs an eyepiece process per thread;</li>
 * <li>{@code download} - {@code false} to never download a missing map;</li>
 * <li>{@code referenceDir} - where the downloaded reference tiles are kept, by default
 * {@code coastline-reference} in the current folder. It is reused by every run, so a rerun only
 * downloads the tiles it has not seen yet - delete the folder to force a refetch;</li>
 * <li>{@code referenceCache} - {@code false} to delete a reference tile once it was compared;</li>
 * <li>{@code recheck} - {@code true} (default): a failed tile is compared once more with
 * {@code secondReferenceUrl} of the cases file, tile.openstreetmap.org. Both draw
 * openstreetmap-carto, but tile.osmand.net keeps older water polygons, so where the second reference
 * agrees with the rendered tile the tile is counted as a <i>stale reference</i>, not as a failure.
 * Only the failed tiles are fetched from there - a few hundred per run, within its usage policy;</li>
 * <li>{@code failAbove} - share of a tile, default {@value #DEFAULT_FAIL_ABOVE}: a failed tile ends
 * the run with exit code 2 only when its water difference is above it. The smaller failures are
 * still reported, but a build does not go red for them;</li>
 * <li>{@code tileSize}, {@code tolerance} - size of the rendered tile and the mask tolerance. The
 * reference tile is scaled to the rendered one, and {@code tileSize} is honoured by
 * {@code -renderer=opengl} only - the legacy renderer always draws 256 px per tile.</li>
 * </ul>
 */
public class CoastlineRenderingTester {

	// ----------------------------------------------------------------- water detection

	/** Water color of default.render.xml (day mode). */
	private static final int[] OSMAND_WATER_COLORS = { 0x5cc3e5 };

	/** Water color of the reference tiles (tile.osmand.net renders openstreetmap-carto). */
	private static final int[] REFERENCE_WATER_COLORS = { 0xaad3df,
			// water under the tinted, hatched overlay of openstreetmap-carto (Novaya Zemlya 8/169/43, Rügen
			// 13/4401/2600 - a few units apart, within COLOR_TOLERANCE). Its hatch lines #b5c1cb and #babac3
			// are left out: both are within the tolerance of runways (#bbbbcc), and the mask tolerance
			// bridges the thin lines anyway
			0xb1c9d4 };

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

	/**
	 * Land cover both styles draw where the sea meets the land - sand, beach, tidal rock, mud, tidal
	 * flats - is ignored on both sides like the ice: the styles paint it differently over the sea,
	 * and the coastline is still checked all around it. The colors are measured on rendered tiles
	 * (the native renderer rounds the style colors): sand and shoals, beach, tidal rock, mud, the
	 * wetland_tidalflat shader, and the light water and hatch of reefs of default.render.xml; sand,
	 * beach and bare ground of openstreetmap-carto.
	 */
	private static final int[] OSMAND_LANDCOVER_COLORS = { 0xffe3bd, 0xfff3bd, 0xbde3ef, 0xcecac5, 0xbdc2c5,
			0xa5c2ce, 0x9c927b, 0x5ad2e6, 0x6b82c5 };
	private static final int[] REFERENCE_LANDCOVER_COLORS = { 0xf5e9c6, 0xfff1ba, 0xeee5dc };

	/** Default of {@code failAbove}: failed tiles up to 5% of water difference do not fail the run. */
	private static final double DEFAULT_FAIL_ABOVE = 0.05;

	/** Max per channel difference to still treat a pixel as water. */
	private static final int COLOR_TOLERANCE = 10;

	/** Server the report links to, so that a failed tile can be opened on the live map. */
	private static final String MAP_SERVER = "https://test.osmand.net";

	/** Default size of the random part of a run, in thousands of tiles - see {@code randomTilesK}. */
	private static final int DEFAULT_RANDOM_TILES_K = 10;

	/**
	 * Areas openstreetmap-carto does not fill over the sea, while OsmAnd does: islands mapped with
	 * their lagoon, reserves tagged desert, parks and archaeological sites with their bay, offshore oil
	 * fields tagged industrial, land reclamation sites tagged construction with their canals (Jeddah),
	 * offshore solar plants (Yellow River delta), aerodromes on sea ice (McMurdo), landfills with their
	 * ponds (Mykolaiv), mangroves mapped as wood into the sea (Para).
	 * Hiding them can't make water out of land - the land is still under them. Land cover both styles
	 * draw is checked instead, see OSMAND_LANDCOVER_COLORS.
	 */
	static final String DEFAULT_HIDE = "place=island,place=islet,natural=desert,leisure=park,historic=archaeological_site,"
			+ "landuse=industrial,landuse=construction,power=plant,aeroway=aerodrome,landuse=landfill,natural=wood";

	/** How the random tiles are split: coastal, open ocean, inland. */
	private static final int SHARE_COASTAL = 80, SHARE_OCEAN = 10;

	/** The random tiles are always drawn over this whole range - see {@code minzoom}/{@code maxzoom}. */
	private static final int RANDOM_MIN_ZOOM = 1, RANDOM_MAX_ZOOM = 17;

	/** Fixed, so that the random tiles and the reference cache built for them never move. */
	private static final long DEFAULT_SEED = 202608;

	/** How long one Picker keeps guessing before it declares a zoom exhausted for a kind. */
	private static final int PICK_ATTEMPTS = 20000;

	/** Folders the reference cache is spread over - see {@link #referenceFile}. */
	private static final int REFERENCE_BUCKETS = 1024;

	static final String GROUP_FIXED = "Fixed cases of coastline-tests.json";
	static final String GROUP_RANDOM = "Random tiles";
	static final String GROUP_SCAN = "Full scan";

	/** {@code renderer} - the legacy native renderer (v1) and the OpenGL core renderer (v2). */
	static final String RENDERER_LEGACY = "legacy", RENDERER_OPENGL = "opengl";

	private static final String BUNDLED_CASES = "/net/osmand/render/coastline-tests.json";
	private static final String CHECK_SEAMARKS_INLAND = "seamarksInland";

	/** How many reference tiles are kept downloading ahead of the tile being compared. */
	private static final int PREFETCH = 512;

	/** How many failed tiles at most are kept for the html report. */
	private static final int MAX_REPORTED_TILES = 3000;

	/**
	 * Maps that must not be loaded together with the normal ones: the seamarks overlay and the
	 * cut down basemap, which would be a second basemap next to World_basemap.
	 */
	private static final String DEFAULT_EXCLUDED_MAPS = "World_seamarks,basemap_mini";

	/** Without it the ocean is not rendered at all outside of the detailed maps. */
	private static final String DEFAULT_BASEMAP = "World_basemap_2.obf";

	/** The basemap of a maps folder that has no World_basemap, e.g. the prepare folder of the build server. */
	private static final String MINI_BASEMAP = "World_basemap_mini_2.obf";

	// ----------------------------------------------------------------- json model

	/** Content of coastline-tests.json. */
	public static class CasesFile {
		public String referenceUrl = "https://tile.osmand.net/hd/{z}/{x}/{y}.png";
		/** the same style with up to date water polygons, see {@code recheck} */
		public String secondReferenceUrl = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
		public String downloadUrl = "https://download.osmand.net/download?standard=yes&file={name}.zip";
		public List<CaseDef> cases = new ArrayList<>();
		/** areas where the reference itself is wrong - their tiles are compared with the second reference only */
		public List<BrokenReference> brokenReferences = new ArrayList<>();
		/** areas where the OSM data is wrong and both sides draw it differently - their tiles are skipped */
		public List<BrokenReference> dataIssues = new ArrayList<>();
	}

	/**
	 * An area of a known problem outside of the renderer: the reference is wrong (outdated water
	 * polygons) or the OSM data is.
	 */
	public static class BrokenReference {
		public String title;
		public String reason;
		/** leftLon, bottomLat, rightLon, topLat - every tile that touches it */
		public double[] bbox;
		/** the smaller zooms still see the area as a few pixels and are compared as usual */
		public int minzoom = 0;

		boolean covers(int zoom, int x, int y) {
			return zoom >= minzoom && bbox != null
					&& x <= MapUtils.getTileNumberX(zoom, bbox[2]) && x + 1 > MapUtils.getTileNumberX(zoom, bbox[0])
					&& y <= MapUtils.getTileNumberY(zoom, bbox[1]) && y + 1 > MapUtils.getTileNumberY(zoom, bbox[3]);
		}
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
		/** explicit {zoom, x, y} tiles instead of bbox/radius - the random mode and a list of single tiles */
		public List<int[]> tiles;
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
		/** failed against the reference but agrees with the second one - see {@code recheck} */
		boolean staleReference;

		TileResult(CaseDef def, int zoom, int x, int y) {
			this.def = def;
			this.zoom = zoom;
			this.x = x;
			this.y = y;
		}

		boolean ok() {
			return problems.isEmpty();
		}

		/** Index into {@link #SEVERITY_FILTERS}: the report filters the tiles by it. */
		int bucket() {
			if (staleReference) {
				return STALE_BUCKET;
			}
			return severity > 0.5 ? 1 : (severity > 0.1 ? 2 : (severity > 0.05 ? 3 : 4));
		}
	}

	/** Filters of the report: css id suffix and label, the first one shows everything. */
	private static final String[][] SEVERITY_FILTERS = { { "all", "all" }, { "s50", "&gt; 50%" },
			{ "s10", "10&ndash;50%" }, { "s5", "5&ndash;10%" }, { "s0", "&lt; 5%" },
			{ "stale", "stale reference" } };

	private static final int STALE_BUCKET = SEVERITY_FILTERS.length - 1;

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
		/** Tiles the renderer could not draw at all - it crashed on them. */
		public int renderErrors;
		public double worstExtraWater;
		public double worstMissingWater;
		public double sumExtraWater;
		public double sumMissingWater;
		public double styledSaltPonds;
		public String worstTile = "";
		/** Failed tiles by the ranges of {@link #SEVERITY_FILTERS} (index 0 is unused). */
		public int[] failedBySeverity = new int[SEVERITY_FILTERS.length];
		/** Failed tiles above {@code failAbove}. */
		public int failedAboveLimit;
		/** Tiles that differ from the reference only because the reference is outdated - see {@code recheck}. */
		public int staleReferences;
		/** The same numbers per zoom - the random tiles and the scans spread over many zooms. */
		public Map<Integer, ZoomStats> zooms = new TreeMap<>();

		ZoomStats zoom(int zoom) {
			return zooms.computeIfAbsent(zoom, z -> new ZoomStats());
		}

		public double avgExtraWater() {
			return comparedTiles == 0 ? 0 : sumExtraWater / comparedTiles;
		}

		public double avgMissingWater() {
			return comparedTiles == 0 ? 0 : sumMissingWater / comparedTiles;
		}
	}

	/** Numbers of one zoom of a case. */
	public static class ZoomStats {
		public int comparedTiles;
		public int failedTiles;
		public double worstExtraWater;
		public double worstMissingWater;
		public String worstTile = "";
		public int[] failedBySeverity = new int[SEVERITY_FILTERS.length];
	}

	/** Failed / compared tiles of one group of cases. */
	public static class GroupTotals {
		public String group;
		public int tiles;
		public int failedTiles;
		public int renderErrors;
	}

	/** Result of a whole run, also written to {@code summary.json}. */
	public static class RunResult {
		public String renderer;
		public String style;
		public String mapsDir;
		public int loadedMaps;
		public long startedAt;
		public long durationMs;
		public int tiles;
		public int comparedTiles;
		public int failedTiles;
		/** Failed tiles above {@code failAbove} - only these fail the run. */
		public int failedAboveLimit;
		public double failAbove;
		public int staleReferences;
		public int renderErrors;
		/** How many times the renderer died during the run, restarts included. */
		public int rendererDeaths;
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
	private final boolean openGl;
	private final double failAbove;
	private final boolean recheck;
	private final File secondReferenceCacheDir;

	private CasesFile casesFile;
	private RunResult result;
	private int tilesSinceFlush;
	private long lastFlush;
	private NativeJavaRendering renderer;
	private EyePieceTileRenderer eyePiece;
	private java.util.function.Function<File, EyePieceTileRenderer> newEyePiece;
	/** The eyepiece processes of the render pool, one per thread. */
	private final List<EyePieceTileRenderer> eyePieceWorkers = new java.util.concurrent.CopyOnWriteArrayList<>();
	private final Set<String> initializedMaps = new LinkedHashSet<>();
	private final List<TileResult> reported = new ArrayList<>();
	private ExecutorService downloadPool;
	/** Tiles rendered ahead of the comparison, null when rendering is single threaded. */
	private ExecutorService renderPool;
	private int renderThreads = 1;
	/** Idle renderers (copies of the native library or eyepiece processes), each one used by a single thread at a time. */
	private final java.util.concurrent.BlockingQueue<java.util.function.BiFunction<int[], Collection<String>, BufferedImage>> renderWorkers =
			new java.util.concurrent.LinkedBlockingQueue<>();
	private final Map<String, Future<PreparedTile>> pendingRenders = new HashMap<>();
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
		this.openGl = RENDERER_OPENGL.equalsIgnoreCase(opt("renderer", RENDERER_LEGACY));
		this.failAbove = Double.parseDouble(opt("failAbove", String.valueOf(DEFAULT_FAIL_ABOVE)));
		this.recheck = Boolean.parseBoolean(opt("recheck", "true"));
		this.secondReferenceCacheDir = new File(referenceCacheDir.getAbsoluteFile().getParentFile(),
				referenceCacheDir.getName() + "-osm");
		if (!openGl && !RENDERER_LEGACY.equalsIgnoreCase(opt("renderer", RENDERER_LEGACY))) {
			throw new IllegalArgumentException("-renderer must be " + RENDERER_LEGACY + " or "
					+ RENDERER_OPENGL + " but was " + opt("renderer", RENDERER_LEGACY));
		}
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
			// a renderer that crashes on a tile is a worse problem than a wrong coastline, so it
			// must not end in a green build either
			code = res.failedAboveLimit > 0 || res.renderErrors > 0 ? 2 : 0;
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
		int n = Math.max(1, Math.min(threads, Runtime.getRuntime().availableProcessors()));
		if (n > 1) {
			renderThreads = n;
			renderPool = Executors.newFixedThreadPool(renderThreads);
			// each copy initializes its own maps, about a second - all of them at once
			List<Future<java.util.function.BiFunction<int[], Collection<String>, BufferedImage>>> created = new ArrayList<>();
			for (int i = 0; i < n; i++) {
				int index = i;
				created.add(renderPool.submit(() -> newRenderWorker(index)));
			}
			for (Future<java.util.function.BiFunction<int[], Collection<String>, BufferedImage>> f : created) {
				renderWorkers.add(f.get());
			}
		}
		System.out.println("Render threads : " + renderThreads);

		result = new RunResult();
		result.renderer = openGl ? RENDERER_OPENGL : RENDERER_LEGACY;
		result.style = opt("style", "default.render.xml")
				+ (hiddenTags().isEmpty() ? "" : " without " + opt("hide", DEFAULT_HIDE));
		result.mapsDir = mapsDir.getAbsolutePath();
		result.startedAt = start;
		result.failAbove = failAbove;
		try {
			// the seamarks cases close every map, so they go last
			cases.sort((a, b) -> Boolean.compare(a.isSeamarksCheck(), b.isSeamarksCheck()));
			for (CaseDef def : cases) {
				if (def.isSeamarksCheck()) {
					runSeamarksCase(def);
				} else {
					runWaterCase(def);
				}
				// after every case, so that the fixed cases can be read while the random tiles -
				// hours of them - are still running, and so that a killed job leaves behind the
				// part of the report it did finish
				writeReport();
			}
		} finally {
			downloadPool.shutdownNow();
			if (renderPool != null) {
				renderPool.shutdownNow();
			}
			if (eyePiece != null) {
				eyePiece.close();
			}
			eyePieceWorkers.forEach(EyePieceTileRenderer::close);
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
		c.minzoom = Integer.parseInt(opt("minzoom", String.valueOf(RANDOM_MIN_ZOOM)));
		c.maxzoom = Integer.parseInt(opt("maxzoom", String.valueOf(RANDOM_MAX_ZOOM)));
		c.maxExtraWater = Double.parseDouble(opt("maxExtraWater", "0.02"));
		c.maxMissingWater = Double.parseDouble(opt("maxMissingWater", "0.02"));
		// a constant, so that the tiles - and the reference cache built for them - stay the same
		long seed = Long.parseLong(opt("seed", String.valueOf(DEFAULT_SEED)));
		CoastalTiles tiles = new CoastalTiles();

		// The sequence must not depend on totalTiles, otherwise raising -randomTilesK would draw a
		// completely different set and throw away the reference cache. So every (zoom, kind) has its
		// own stream seeded only by the seed, and the run takes tiles from them round robin, 8
		// coastal + 1 ocean + 1 land per zoom per round. The first N of that are the same N for any
		// N: going from 50k to 100k keeps the first 50k and only adds new tiles after them.
		List<int[]> picked = new ArrayList<>();
		int[] found = new int[3];
		int skipped = 0;
		int drawn = 0;
		CoastalTiles.Picker[][] pickers = new CoastalTiles.Picker[RANDOM_MAX_ZOOM + 1][3];
		for (int zoom = RANDOM_MIN_ZOOM; zoom <= RANDOM_MAX_ZOOM; zoom++) {
			for (int kind = 0; kind < 3; kind++) {
				pickers[zoom][kind] = tiles.picker(zoom, kind, seed);
			}
		}
		int[] perRound = { SHARE_COASTAL / 10, SHARE_OCEAN / 10, (100 - SHARE_COASTAL - SHARE_OCEAN) / 10 };
		boolean anyLeft = true;
		while (drawn < totalTiles && anyLeft) {
			anyLeft = false;
			for (int zoom = RANDOM_MIN_ZOOM; zoom <= RANDOM_MAX_ZOOM && drawn < totalTiles; zoom++) {
				boolean render = zoom >= c.minzoom && zoom <= c.maxzoom;
				for (int kind = 0; kind < 3 && drawn < totalTiles; kind++) {
					for (int i = 0; i < perRound[kind] && drawn < totalTiles; i++) {
						int[] t = pickers[zoom][kind].next();
						if (t == null) {
							break;
						}
						anyLeft = true;
						drawn++;
						if (render) {
							picked.add(t);
							found[kind]++;
						} else {
							skipped++;
						}
					}
				}
			}
		}
		Collections.reverse(picked);
		c.tiles = picked;
		if (c.minzoom > RANDOM_MIN_ZOOM || c.maxzoom < RANDOM_MAX_ZOOM) {
			c.title = "Random tiles z" + c.minzoom + "-" + c.maxzoom;
		}
		System.out.printf("Random tiles  : seed %d, %d of %d tiles, zooms %d..%d of %d..%d "
						+ "(rendered %d down to %d, %d skipped by the zoom filter) "
						+ "(%d coastal, %d ocean, %d land)%n",
				seed, picked.size(), picked.size() + skipped, c.minzoom, c.maxzoom,
				RANDOM_MIN_ZOOM, RANDOM_MAX_ZOOM, c.maxzoom, c.minzoom, skipped,
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

		/**
		 * An endless stream of distinct tiles of one kind at one zoom. It is seeded only by the run
		 * seed, the zoom and the kind - never by how many tiles are wanted - so the n-th tile it
		 * returns is always the same tile.
		 */
		Picker picker(int zoom, int kind, long seed) {
			return new Picker(zoom, kind, new java.util.Random(seed * 1000003L + zoom * 13L + kind));
		}

		class Picker {
			private final int zoom, kind, max;
			private final java.util.Random rnd;
			/** Small zooms are enumerated and shuffled, so that nothing is missed. */
			private final List<int[]> all;
			private int cursor;
			private final Set<Long> seen = new LinkedHashSet<>();

			Picker(int zoom, int kind, java.util.Random rnd) {
				this.zoom = zoom;
				this.kind = kind;
				this.rnd = rnd;
				this.max = 1 << zoom;
				if ((long) max * max <= 1 << 18) {
					all = new ArrayList<>();
					for (int x = 0; x < max; x++) {
						for (int y = 0; y < max; y++) {
							if (kind(zoom, x, y) == kind) {
								all.add(new int[] { zoom, x, y });
							}
						}
					}
					Collections.shuffle(all, rnd);
				} else {
					all = null;
				}
			}

			/** The next tile, or null once this zoom has no more of that kind to give. */
			int[] next() {
				if (all != null) {
					return cursor < all.size() ? all.get(cursor++) : null;
				}
				for (int attempt = 0; attempt < PICK_ATTEMPTS; attempt++) {
					int x = rnd.nextInt(max);
					int y = rnd.nextInt(max);
					if (kind(zoom, x, y) == kind && seen.add(((long) x << 32) | y)) {
						return new int[] { zoom, x, y };
					}
				}
				return null;
			}
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

	/** The {@code -hide} objects as {tag, value} pairs. */
	private List<String[]> hiddenTags() {
		List<String[]> res = new ArrayList<>();
		for (String s : opt("hide", DEFAULT_HIDE).split(",")) {
			int eq = s.indexOf('=');
			if (eq > 0) {
				res.add(new String[] { s.substring(0, eq).trim(), s.substring(eq + 1).trim() });
			}
		}
		return res;
	}

	private static String readStyle(File file, String name) throws IOException {
		if (file != null && file.isFile()) {
			return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
		}
		try (InputStream is = RenderingRulesStorage.class.getResourceAsStream(name)) {
			if (is == null) {
				throw new IOException("Can't find rendering style " + name);
			}
			return new String(is.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	/**
	 * The style with {@code order="-1"} for every {@code -hide} object in front of its order section,
	 * so they are not drawn at all. Null when the style has no order section of its own.
	 */
	static String hideInStyle(String xml, List<String[]> hidden) {
		for (int i = xml.indexOf("<order>"); i >= 0; i = xml.indexOf("<order>", i + 1)) {
			int comment = xml.lastIndexOf("<!--", i);
			if (comment >= 0 && xml.lastIndexOf("-->", i) < comment) {
				continue;
			}
			StringBuilder cases = new StringBuilder("<order>");
			for (String[] t : hidden) {
				cases.append("\n\t\t<case tag=\"").append(t[0]).append("\" value=\"").append(t[1])
						.append("\" order=\"-1\"/>");
			}
			return xml.substring(0, i) + cases + xml.substring(i + "<order>".length());
		}
		return null;
	}

	/**
	 * Writes the style with the {@code -hide} objects taken out into {@code dir}, next to copies of
	 * the other styles of its folder (its addons and the styles it depends on). Returns false when
	 * nothing is hidden or the style can't be patched, the original is used then.
	 */
	private boolean writeStyleWithoutHidden(File sourceDir, String fileName, File dir) throws IOException {
		List<String[]> hidden = hiddenTags();
		if (hidden.isEmpty()) {
			return false;
		}
		String patched = hideInStyle(readStyle(sourceDir == null ? null : new File(sourceDir, fileName), fileName),
				hidden);
		if (patched == null) {
			System.err.println("Style " + fileName + " has no <order> section, -hide is ignored");
			return false;
		}
		dir.mkdirs();
		File[] siblings = sourceDir == null ? null : sourceDir.listFiles();
		if (siblings != null) {
			for (File f : siblings) {
				if (f.isFile() && f.getName().endsWith(".render.xml") && !f.getName().equals(fileName)) {
					writeAtomically(Files.readAllBytes(f.toPath()), new File(dir, f.getName()));
				}
			}
		}
		writeAtomically(patched.getBytes(StandardCharsets.UTF_8), new File(dir, fileName));
		return true;
	}

	/** The render workers write the same files at the same time. */
	private static void writeAtomically(byte[] content, File file) throws IOException {
		File tmp = File.createTempFile(file.getName(), ".tmp", file.getParentFile());
		Files.write(tmp.toPath(), content);
		Files.move(tmp.toPath(), file.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING,
				java.nio.file.StandardCopyOption.ATOMIC_MOVE);
	}

	/** The v1 engine: {@code libosmand}, the same renderer the tile server runs today. */
	private void initLegacyRenderer(String style) throws Exception {
		File styleFile = new File(style);
		File dir = new File(outputDir, "style");
		if (writeStyleWithoutHidden(styleFile.isFile() ? styleFile.getParentFile() : null, styleFile.getName(), dir)) {
			style = new File(dir, styleFile.getName()).getAbsolutePath();
		}
		// null lets NativeJavaRendering load the library bundled into OsmAndMapCreator
		File nativeLib = findNativeLibrary();
		File fonts = findFonts();
		System.out.println("Native library : " + (nativeLib == null
				? "bundled with OsmAndMapCreator" : nativeLib.getAbsolutePath()));
		System.out.println("Fonts          : " + (fonts == null ? "none" : fonts.getAbsolutePath()));

		renderer = NativeJavaRendering.getDefault(nativeLib == null ? null : nativeLib.getAbsolutePath(), null,
				fonts == null ? null : fonts.getAbsolutePath());
		if (renderer == null) {
			throw new IllegalStateException("Native library could not be loaded"
					+ (nativeLib == null ? ", pass -native=<path to libosmand.dylib/so/dll>" : ": " + nativeLib));
		}
		renderer.loadRuleStorage(style, "density=1,textScale=1");
	}

	/**
	 * The v2 engine: OsmAndCore driven through the {@code eyepiece} tool, which needs no native
	 * library of the distribution, no fonts folder and no indexes cache - core does all of that
	 * itself. The style is resolved by name ({@code default.render.xml} is {@code default}), from
	 * {@code -stylesPath} when it is given and from the styles built into core otherwise.
	 */
	private void initOpenGlRenderer(String style) throws IOException {
		File binary = findEyePiece();
		File stylesPath = findStyles();
		String styleName = style.endsWith(".render.xml")
				? style.substring(0, style.length() - ".render.xml".length()) : style;
		File patchedStyles = new File(outputDir, "opengl-styles");
		if (writeStyleWithoutHidden(stylesPath, styleName + ".render.xml", patchedStyles)) {
			stylesPath = patchedStyles;
		}
		System.out.println("eyepiece       : " + binary.getAbsolutePath());
		System.out.println("Styles path    : " + (stylesPath == null
				? "built into OsmAndCore" : stylesPath.getAbsolutePath()));
		boolean symbols = Boolean.parseBoolean(opt("symbols", "false"));
		System.out.println("Map symbols    : " + (symbols
				? "drawn" : "off, they are 94% of the time of a tile (-symbols=true draws them)"));
		eyePiece = new EyePieceTileRenderer(binary, stylesPath, styleName, tileSize, symbols, outputDir,
				Boolean.parseBoolean(opt("eyepieceLog", "false")));
		File styles = stylesPath;
		newEyePiece = dir -> new EyePieceTileRenderer(binary, styles, styleName, tileSize, symbols, dir,
				Boolean.parseBoolean(opt("eyepieceLog", "false")));
		if (Boolean.parseBoolean(opt("eyepieceCheck", "true"))) {
			eyePiece.checkBatchTileMode();
		}
	}

	private void initRenderer() throws Exception {
		String style = opt("style", "default.render.xml");
		System.out.println("Renderer       : " + (openGl
				? "opengl (v2, OsmAndCore through eyepiece)" : "legacy (v1, native library)"));
		System.out.println("Maps           : " + mapsDir.getAbsolutePath());
		System.out.println("Output         : " + outputDir.getAbsolutePath());
		System.out.println("Reference cache: " + referenceCacheDir.getAbsolutePath()
				+ " (" + countCachedReferences() + " tiles kept from the previous runs)");
		System.out.println("Style          : " + style);
		System.out.println("Hidden         : " + (hiddenTags().isEmpty() ? "nothing" : opt("hide", DEFAULT_HIDE)));
		if (openGl) {
			initOpenGlRenderer(style);
		} else {
			initLegacyRenderer(style);
		}
		if (loadAllMaps) {
			initAllMaps();
		} else {
			// the app always has the basemap, without it there is no ocean outside of a detailed map
			String basemap = opt("basemap", defaultBasemap());
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
		String[] excluded = opt("exclude", defaultBasemap().equals(MINI_BASEMAP) ? "World_seamarks"
				: DEFAULT_EXCLUDED_MAPS).split(",");
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
		if (openGl) {
			// core keeps its own index cache and opens the maps of -obfsPath by itself
			for (File f : maps) {
				initializedMaps.add(f.getName());
			}
		} else {
			initLegacyMaps(maps);
		}
		System.out.println("Initialized " + maps.size() + " maps from " + mapsDir.getAbsolutePath());
		if (!skipped.isEmpty()) {
			Collections.sort(skipped);
			System.out.println("Skipped " + skipped.size() + " overlay maps (-exclude): "
					+ String.join(", ", skipped));
		}
	}

	/**
	 * Hands the maps to the native library. The obf indexes are read through {@code indexes.cache},
	 * otherwise the native library reports "File not initialized from cache" and re-reads the index
	 * of every single map on every run.
	 */
	private void initLegacyMaps(List<File> maps) throws IOException {
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
	}

	/** World_basemap, or the mini basemap when the maps folder has only that one. */
	private String defaultBasemap() {
		if (!new File(mapsDir, DEFAULT_BASEMAP).isFile() && new File(mapsDir, MINI_BASEMAP).isFile()) {
			return MINI_BASEMAP;
		}
		return DEFAULT_BASEMAP;
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
		if (!openGl) {
			renderer.initMapFile(f.getAbsolutePath(), true);
		}
		initializedMaps.add(name);
		return true;
	}

	private void closeAllMaps() {
		for (String name : new ArrayList<>(initializedMaps)) {
			File f = new File(mapsDir, name);
			if (!openGl) {
				renderer.closeMapFile(f.getAbsolutePath());
			}
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
		// rendering is single threaded and the tile server is slow, so the reference tiles are kept
		// downloading PREFETCH tiles ahead of the one being compared - neither side ever waits idle
		Iterator<int[]> it = tiles(def);
		Deque<int[]> ahead = new ArrayDeque<>();
		while (it.hasNext() || !ahead.isEmpty()) {
			while (ahead.size() < PREFETCH && it.hasNext()) {
				int[] t = it.next();
				if (brokenReference(t[0], t[1], t[2]) == null && covering(casesFile.dataIssues, t[0], t[1], t[2]) == null) {
					prefetchReference(t[0], t[1], t[2]);
				}
				ahead.add(t);
			}
			submitRenders(ahead);
			int[] t = ahead.poll();
			compareTile(def, stats, dir, t[0], t[1], t[2]);
			flush(stats, totalOfCase);
		}
		// a tile skipped for a missing reference; the next case may render it with other maps
		pendingRenders.values().forEach(f -> f.cancel(false));
		pendingRenders.clear();
		System.out.printf("  %d tiles, %d compared, %d skipped, %d failed, %d stale references%n", stats.tiles,
				stats.comparedTiles, stats.skippedTiles, stats.failedTiles, stats.staleReferences);
		return stats;
	}

	private BrokenReference brokenReference(int zoom, int x, int y) {
		return covering(casesFile.brokenReferences, zoom, x, y);
	}

	private static BrokenReference covering(List<BrokenReference> areas, int zoom, int x, int y) {
		for (BrokenReference b : areas) {
			if (b.covers(zoom, x, y)) {
				return b;
			}
		}
		return null;
	}

	/** Water masks of a rendered tile against one reference tile. */
	private class WaterDiff {
		final BufferedImage reference;
		final int w, h;
		final boolean[] renderedWater, referenceWater, extra, missing;
		final double extraRatio, extraAllRatio, missingRatio;

		WaterDiff(BufferedImage rendered, BufferedImage reference) {
			this.reference = scaleDown(reference, rendered.getWidth(), rendered.getHeight());
			w = rendered.getWidth();
			h = rendered.getHeight();
			renderedWater = waterMask(rendered, OSMAND_WATER_COLORS);
			referenceWater = waterMask(this.reference, REFERENCE_WATER_COLORS);
			boolean[] shaded = dilate(waterMask(rendered, SHADED_WATER_COLORS), w, h, SHADED_WATER_SPREAD_PX);
			boolean[] ice = dilate(or(or(waterMask(rendered, OSMAND_ICE_COLORS),
					waterMask(this.reference, REFERENCE_ICE_COLORS)), or(waterMask(rendered, OSMAND_LANDCOVER_COLORS),
					waterMask(this.reference, REFERENCE_LANDCOVER_COLORS))), w, h, maskTolerance);
			boolean[] extraAll = and(and(erode(renderedWater, w, h, maskTolerance),
					not(dilate(referenceWater, w, h, maskTolerance))), not(ice));
			extra = and(extraAll, not(shaded));
			missing = and(and(erode(referenceWater, w, h, maskTolerance),
					not(dilate(renderedWater, w, h, maskTolerance))), not(ice));
			extraRatio = count(extra) * 1.0 / (w * h);
			extraAllRatio = count(extraAll) * 1.0 / (w * h);
			missingRatio = count(missing) * 1.0 / (w * h);
		}

		boolean ok(CaseDef def) {
			return extraRatio <= def.maxExtraWater && missingRatio <= def.maxMissingWater;
		}

		double water(boolean[] mask) {
			return count(mask) * 1.0 / (w * h);
		}
	}

	private void compareTile(CaseDef def, CaseStats stats, File dir, int zoom, int x, int y) throws IOException {
		stats.tiles++;
		BrokenReference dataIssue = covering(casesFile.dataIssues, zoom, x, y);
		if (dataIssue != null) {
			stats.skippedTiles++;
			System.out.printf("  SKIPPED %d/%d/%d - data issue: %s%n", zoom, x, y, dataIssue.title);
			return;
		}
		BrokenReference broken = brokenReference(zoom, x, y);
		PreparedTile prepared = preparedTile(zoom, x, y);
		BufferedImage reference;
		if (broken != null) {
			// the reference is known to be wrong here - the second one is the only one worth comparing with
			reference = recheck ? secondReference(zoom, x, y) : null;
		} else {
			awaitReference(zoom, x, y);
			reference = prepared != null ? prepared.reference : reference(zoom, x, y);
		}
		if (reference == null) {
			stats.skippedTiles++;
			if (broken != null) {
				System.out.printf("  SKIPPED %d/%d/%d - broken reference: %s%n", zoom, x, y, broken.title);
			}
			return;
		}
		BufferedImage rendered;
		try {
			rendered = prepared != null ? prepared.rendered : render(zoom, x, y);
		} catch (EyePieceTileRenderer.TileRenderFailure e) {
			renderError(def, stats, zoom, x, y, e);
			return;
		}
		WaterDiff first = prepared != null && prepared.first != null ? prepared.first : new WaterDiff(rendered, reference);
		WaterDiff second = null;
		if (broken == null && recheck && !first.ok(def)) {
			BufferedImage img = secondReference(zoom, x, y);
			second = img == null ? null : new WaterDiff(rendered, img);
		}
		boolean stale = second != null && second.ok(def);
		// a stale reference is not the one to measure the renderer with
		WaterDiff d = stale ? second : first;
		double extraRatio = d.extraRatio, missingRatio = d.missingRatio;

		stats.comparedTiles++;
		stats.sumExtraWater += extraRatio;
		stats.sumMissingWater += missingRatio;
		stats.styledSaltPonds += d.extraAllRatio - extraRatio;
		if (Math.max(extraRatio, missingRatio) > Math.max(stats.worstExtraWater, stats.worstMissingWater)) {
			stats.worstTile = zoom + "/" + x + "/" + y;
		}
		stats.worstExtraWater = Math.max(stats.worstExtraWater, extraRatio);
		stats.worstMissingWater = Math.max(stats.worstMissingWater, missingRatio);
		ZoomStats zs = stats.zoom(zoom);
		zs.comparedTiles++;
		if (Math.max(extraRatio, missingRatio) > Math.max(zs.worstExtraWater, zs.worstMissingWater)) {
			zs.worstTile = zoom + "/" + x + "/" + y;
		}
		zs.worstExtraWater = Math.max(zs.worstExtraWater, extraRatio);
		zs.worstMissingWater = Math.max(zs.worstMissingWater, missingRatio);

		TileResult res = new TileResult(def, zoom, x, y);
		res.severity = Math.max(extraRatio, missingRatio);
		res.staleReference = stale;
		if (extraRatio > def.maxExtraWater) {
			res.problems.add(String.format("%.2f%% of water over the land (limit %.2f%%)",
					extraRatio * 100, def.maxExtraWater * 100));
		}
		if (missingRatio > def.maxMissingWater) {
			res.problems.add(String.format("%.2f%% of land over the water (limit %.2f%%)",
					missingRatio * 100, def.maxMissingWater * 100));
		}
		if (stale) {
			stats.staleReferences++;
			stats.failedBySeverity[res.bucket()]++;
			zs.failedBySeverity[res.bucket()]++;
			System.out.printf("  STALE REFERENCE %d/%d/%d water: osmand %.1f%% reference %.1f%% openstreetmap.org %.1f%%%n",
					zoom, x, y, first.water(first.renderedWater) * 100, first.water(first.referenceWater) * 100,
					second.water(second.referenceWater) * 100);
		} else if (!res.ok()) {
			stats.failedTiles++;
			zs.failedTiles++;
			stats.failedBySeverity[res.bucket()]++;
			zs.failedBySeverity[res.bucket()]++;
			if (res.severity > failAbove) {
				stats.failedAboveLimit++;
			}
			String secondWater = second != null
					? String.format(" openstreetmap.org %.1f%%", second.water(second.referenceWater) * 100)
					: (broken != null ? " (openstreetmap.org, broken reference)" : "");
			System.out.printf("  FAILED %d/%d/%d water: osmand %.1f%% reference %.1f%%%s - %s%n", zoom, x, y,
					d.water(d.renderedWater) * 100, d.water(d.referenceWater) * 100, secondWater,
					String.join(", ", res.problems));
		}
		if (saveTile(res.ok() && !stale)) {
			dir.mkdirs();
			ImageIO.write(rendered, "png", new File(dir, tileName(zoom, x, y, "rendered")));
			res.images.put("osmand", tileName(zoom, x, y, "rendered"));
			ImageIO.write(first.reference, "png", new File(dir, tileName(zoom, x, y, "reference")));
			res.images.put(broken != null ? "openstreetmap.org" : "reference", tileName(zoom, x, y, "reference"));
			if (second != null) {
				ImageIO.write(second.reference, "png", new File(dir, tileName(zoom, x, y, "osm")));
				res.images.put("openstreetmap.org", tileName(zoom, x, y, "osm"));
			}
			ImageIO.write(diffImage(rendered, d.extra, d.missing), "png", new File(dir, tileName(zoom, x, y, "diff")));
			res.images.put("diff", tileName(zoom, x, y, "diff"));
		}
		res.metrics.put("water osmand", pct(d.water(d.renderedWater)));
		res.metrics.put(broken != null ? "water openstreetmap.org" : "water reference",
				pct(first.water(first.referenceWater)));
		if (second != null) {
			res.metrics.put("water openstreetmap.org", pct(second.water(second.referenceWater)));
		}
		res.metrics.put("extra water", pct(extraRatio));
		if (d.extraAllRatio - extraRatio > 0.0001) {
			res.metrics.put("of it styled salt ponds", pct(d.extraAllRatio - extraRatio));
		}
		res.metrics.put("missing water", pct(missingRatio));
		if (broken != null) {
			res.metrics.put("broken reference", broken.title);
		}
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
		for (Iterator<int[]> it = tiles(def); it.hasNext(); ) {
			int[] t = it.next();
			int zoom = t[0], x = t[1], y = t[2];
			stats.tiles++;
			BufferedImage empty, drawnImg;
			try {
				closeAllMaps();
				empty = render(zoom, x, y);
				for (String map : def.maps) {
					if (!initMap(map)) {
						throw new IllegalStateException("Map " + map + " of " + def + " is not available");
					}
				}
				drawnImg = render(zoom, x, y);
			} catch (EyePieceTileRenderer.TileRenderFailure e) {
				closeAllMaps();
				renderError(def, stats, zoom, x, y, e);
				continue;
			}
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
				stats.failedBySeverity[res.bucket()]++;
				if (res.severity > failAbove) {
					stats.failedAboveLimit++;
				}
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
		System.out.printf("  %d tiles, %d failed%n", stats.tiles, stats.failedTiles);
		if (loadAllMaps) {
			initAllMaps();
		}
		return stats;
	}

	// ----------------------------------------------------------------- tiles

	/**
	 * Walks the tiles of a case one by one. A world wide z1-10 scan is more than a million tiles,
	 * so they are generated lazily instead of being collected into a list.
	 */
	private Iterator<int[]> tiles(CaseDef def) {
		if (def.tiles != null) {
			return def.tiles.iterator();
		}
		int[] zoomList = def.zoomList();
		return new Iterator<int[]>() {
			int zi = -1, x, y, leftX, rightX, topY, bottomY;

			/** Moves to the first zoom of the list that has any tile in it. */
			private boolean startZoom() {
				while (++zi < zoomList.length) {
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
				}
				return false;
			}

			@Override
			public boolean hasNext() {
				return zi < 0 ? startZoom() : zi < zoomList.length;
			}

			@Override
			public int[] next() {
				int[] tile = { zoomList[zi], x, y };
				if (++y > bottomY) {
					y = topY;
					if (++x > rightX) {
						startZoom();
					}
				}
				return tile;
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
	/** A tile rendered by the pool, with its reference and the comparison with it when there is one. */
	private static class PreparedTile {
		BufferedImage rendered;
		BufferedImage reference;
		WaterDiff first;
	}

	/**
	 * Keeps the render pool busy with the next tiles of the queue - a few per thread, so that the
	 * rendered images waiting for their comparison stay few. The pool renders a tile and also
	 * compares it with the reference: the comparison takes as long as the rendering.
	 */
	private void submitRenders(Deque<int[]> ahead) {
		if (renderPool == null) {
			return;
		}
		int n = 0;
		for (int[] t : ahead) {
			if (n++ >= renderThreads * 4) {
				break;
			}
			String key = t[0] + "/" + t[1] + "/" + t[2];
			// the tiles compareTile skips without rendering
			boolean skipped = covering(casesFile.dataIssues, t[0], t[1], t[2]) != null
					|| (!recheck && brokenReference(t[0], t[1], t[2]) != null);
			if (!skipped && !pendingRenders.containsKey(key)) {
				List<String> maps = new ArrayList<>(initializedMaps);
				boolean broken = brokenReference(t[0], t[1], t[2]) != null;
				Future<?> download = pendingReferences.get(key);
				pendingRenders.put(key, renderPool.submit(() -> {
					PreparedTile p = new PreparedTile();
					java.util.function.BiFunction<int[], Collection<String>, BufferedImage> worker = renderWorkers.take();
					try {
						p.rendered = worker.apply(t, maps);
					} finally {
						renderWorkers.add(worker);
					}
					if (!broken) {
						if (download != null) {
							try {
								download.get();
							} catch (ExecutionException e) {
								// reference() retries it and reports the failure
							}
						}
						p.reference = reference(t[0], t[1], t[2]);
						p.first = p.reference == null ? null : new WaterDiff(p.rendered, p.reference);
					}
					return p;
				}));
			}
		}
	}

	/**
	 * A copy of the native library with a renderer of its own. The JVM binds a library file to one
	 * class loader only, and the library keeps its maps, caches and output buffer in globals, so the
	 * library file is copied and loaded through a class loader of its own - that loader has its own
	 * {@code NativeLibrary} class and so its own set of natives.
	 */
	private java.util.function.BiFunction<int[], Collection<String>, BufferedImage> newRenderWorker(int index)
			throws Exception {
		if (openGl) {
			// a process of its own, with its own maps and tiles folders
			EyePieceTileRenderer e = newEyePiece.apply(new File(outputDir, "opengl-worker-" + index));
			eyePieceWorkers.add(e);
			return (tile, current) -> {
				List<File> maps = new ArrayList<>();
				for (String name : current) {
					maps.add(new File(mapsDir, name));
				}
				try {
					e.setMaps(maps);
					return e.render(tile[0], tile[1], tile[2]);
				} catch (IOException ex) {
					throw new java.io.UncheckedIOException(ex);
				}
			};
		}
		File lib = findNativeLibrary();
		Map<String, String> opts = new HashMap<>(options);
		// the bundled library is unpacked into a new temporary file on every load anyway
		if (lib != null) {
			File dir = Files.createTempDirectory("osmand-native-" + index).toFile();
			File copy = new File(dir, lib.getName());
			Files.copy(lib.toPath(), copy.toPath());
			copy.deleteOnExit();
			dir.deleteOnExit();
			opts.put("native", copy.getAbsolutePath());
		}
		List<java.net.URL> urls = new ArrayList<>();
		for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
			urls.add(new File(entry).toURI().toURL());
		}
		ClassLoader loader = new java.net.URLClassLoader(urls.toArray(new java.net.URL[0]),
				ClassLoader.getPlatformClassLoader());
		@SuppressWarnings("unchecked")
		java.util.function.BiFunction<int[], Collection<String>, BufferedImage> worker =
				(java.util.function.BiFunction<int[], Collection<String>, BufferedImage>) loader
						.loadClass(CoastlineRenderingTester.class.getName())
						.getMethod("renderWorker", Map.class, Collection.class)
						.invoke(null, opts, new ArrayList<>(initializedMaps));
		return worker;
	}

	/**
	 * Called through the class loader of {@link #newRenderWorker}: a renderer on the library of
	 * {@code -native} with the given maps. It renders a tile after initializing the maps the main
	 * renderer has got since, and is not thread safe.
	 */
	public static java.util.function.BiFunction<int[], Collection<String>, BufferedImage> renderWorker(
			Map<String, String> options, Collection<String> maps) throws Exception {
		CoastlineRenderingTester t = new CoastlineRenderingTester(options);
		t.initLegacyRenderer(t.opt("style", "default.render.xml"));
		if (t.loadAllMaps) {
			t.initAllMaps();
		}
		for (String map : maps) {
			t.initMap(map);
		}
		return (tile, current) -> {
			try {
				for (String map : current) {
					t.initMap(map);
				}
				return t.render(tile[0], tile[1], tile[2]);
			} catch (IOException e) {
				throw new java.io.UncheckedIOException(e);
			}
		};
	}

	/** The tile from the render pool when it was handed there, otherwise null. */
	private PreparedTile preparedTile(int zoom, int x, int y) throws IOException {
		Future<PreparedTile> f = pendingRenders.remove(zoom + "/" + x + "/" + y);
		if (f == null) {
			return null;
		}
		try {
			return f.get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException(e);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof java.io.UncheckedIOException) {
				throw ((java.io.UncheckedIOException) e.getCause()).getCause();
			}
			throw new IOException(e.getCause());
		}
	}

	private BufferedImage render(int zoom, int x, int y) throws IOException {
		if (openGl) {
			// eyepiece renders from the tile centre with the window sized to one tile, so the tile
			// bounds are its own business - all it needs is the current set of maps
			List<File> maps = new ArrayList<>();
			for (String name : initializedMaps) {
				maps.add(new File(mapsDir, name));
			}
			eyePiece.setMaps(maps);
			return eyePiece.render(zoom, x, y);
		}
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
	 * Hands one reference tile to the download pool and returns at once - it is awaited by
	 * {@link #awaitReference} right before it is compared.
	 */
	private void prefetchReference(int zoom, int x, int y) {
		String key = zoom + "/" + x + "/" + y;
		if (referenceFile(zoom, x, y).isFile() || pendingReferences.containsKey(key)) {
			return;
		}
		pendingReferences.put(key, downloadPool.submit((Callable<Void>) () -> {
			downloadReference(zoom, x, y);
			return null;
		}));
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
		File[] buckets = referenceCacheDir.listFiles();
		if (buckets == null) {
			return 0;
		}
		for (File b : buckets) {
			String[] tiles = b.list();
			n += tiles == null ? 0 : tiles.length;
		}
		return n;
	}

	/**
	 * A tile column per folder used to leave one or two files in each of tens of thousands of
	 * folders. The tiles are spread over a fixed number of numbered buckets instead: about 100
	 * files per folder for a 100k run and about 1000 for a million, and the file is still named by
	 * its coordinates so that a tile can be found without a lookup.
	 */
	private File referenceFile(int zoom, int x, int y) {
		int bucket = (((zoom * 31 + x) * 31 + y) & 0x7fffffff) % REFERENCE_BUCKETS;
		return new File(referenceCacheDir, bucket + "/" + zoom + "_" + x + "_" + y + ".png");
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

	/**
	 * The tile of {@code secondReferenceUrl}, fetched only for the tiles that failed or lie in a broken
	 * reference area, and cached next to the reference cache.
	 */
	private BufferedImage secondReference(int zoom, int x, int y) {
		File cached = new File(secondReferenceCacheDir, zoom + "/" + x + "_" + y + ".png");
		try {
			if (!cached.isFile() || cached.length() == 0) {
				cached.getParentFile().mkdirs();
				download(casesFile.secondReferenceUrl.replace("{z}", String.valueOf(zoom))
						.replace("{x}", String.valueOf(x)).replace("{y}", String.valueOf(y)), cached);
			}
			return ImageIO.read(cached);
		} catch (IOException e) {
			System.err.println("Can't get the second reference tile " + zoom + "/" + x + "/" + y + ": "
					+ e.getMessage());
			cached.delete();
			return null;
		}
	}

	private static void download(String url, File target) throws IOException {
		HttpURLConnection cn = (HttpURLConnection) new URL(url).openConnection();
		// tile.openstreetmap.org requires an agent that says who is asking
		cn.setRequestProperty("User-Agent", "OsmAnd-CoastlineRenderingTester (https://osmand.net)");
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
		result.failedAboveLimit = 0;
		result.staleReferences = 0;
		result.renderErrors = 0;
		result.rendererDeaths = eyePiece == null ? 0 : eyePiece.deaths();
		for (EyePieceTileRenderer e : eyePieceWorkers) {
			result.rendererDeaths += e.deaths();
		}
		Map<String, GroupTotals> byGroup = new LinkedHashMap<>();
		for (CaseStats s : result.cases) {
			result.tiles += s.tiles;
			result.comparedTiles += s.comparedTiles;
			result.failedTiles += s.failedTiles;
			result.failedAboveLimit += s.failedAboveLimit;
			result.staleReferences += s.staleReferences;
			result.renderErrors += s.renderErrors;
			GroupTotals g = byGroup.computeIfAbsent(s.group == null ? GROUP_FIXED : s.group, k -> {
				GroupTotals t = new GroupTotals();
				t.group = k;
				return t;
			});
			g.tiles += s.comparedTiles;
			g.failedTiles += s.failedTiles;
			g.renderErrors += s.renderErrors;
		}
		result.groups = new ArrayList<>(byGroup.values());
	}

	/** The report as it stands now - written after every case and every {@code flushEvery} tiles. */
	private void writeReport() throws IOException {
		recomputeTotals();
		result.loadedMaps = initializedMaps.size();
		result.durationMs = System.currentTimeMillis() - result.startedAt;
		writeSummaryJson(result, true);
		if (writeHtml) {
			writeHtmlReport(result, true);
		}
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
		writeReport();
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

	/**
	 * A tile the renderer crashed on. It is a defect of the renderer rather than of the coastline,
	 * but it is one the report has to carry - a run that quietly drops the tiles that killed the
	 * renderer would look better the more broken the renderer is.
	 */
	private void renderError(CaseDef def, CaseStats stats, int zoom, int x, int y,
			EyePieceTileRenderer.TileRenderFailure e) {
		stats.renderErrors++;
		TileResult res = new TileResult(def, zoom, x, y);
		// worse than any water difference, so that these tiles come first in the report
		res.severity = 2;
		res.problems.add("Renderer error: " + e.getMessage());
		keepForReport(res);
		System.out.printf("  CRASHED %d/%d/%d - %s%n", zoom, x, y, e.getMessage());
	}

	private void keepForReport(TileResult res) {
		if (!res.ok() || res.staleReference || "all".equalsIgnoreCase(saveImages)) {
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
			if (!GROUP_FIXED.equals(g) && s.zooms.size() > 1) {
				StringBuilder z = new StringBuilder();
				for (Map.Entry<Integer, ZoomStats> e : s.zooms.entrySet()) {
					z.append(String.format(" z%d %d/%d", e.getKey(), e.getValue().failedTiles,
							e.getValue().comparedTiles));
				}
				System.out.printf("%-58s failed by zoom:%s%n", "", z);
			}
			if (s.renderErrors > 0) {
				System.out.printf("%-58s %d tiles the renderer crashed on%n", "", s.renderErrors);
			}
			if (s.styledSaltPonds > 0.0001) {
				System.out.printf("%-58s %s of the extra water is styled salt ponds (ignored)%n", "",
						pct(s.styledSaltPonds / Math.max(1, s.comparedTiles)));
			}
		}
		System.out.println("-----------------------------------------------------------------------------------");
		for (GroupTotals g : result.groups) {
			System.out.printf("%-58s %7d %7d%s%n", g.group, g.tiles, g.failedTiles,
					g.renderErrors > 0 ? "   " + g.renderErrors + " renderer errors" : "");
		}
		System.out.printf("%d tiles compared, %d failed, %d maps loaded, %s renderer, %.1f s%n",
				result.comparedTiles, result.failedTiles, result.loadedMaps, result.renderer,
				result.durationMs / 1000.0);
		if (result.staleReferences > 0) {
			System.out.printf("%d tiles differ from the reference only - openstreetmap.org agrees with them%n",
					result.staleReferences);
		}
		if (result.renderErrors > 0) {
			System.out.printf("%d tiles could not be rendered at all, the renderer died %d times -"
					+ " see the report, that is a bug of the renderer%n",
					result.renderErrors, result.rendererDeaths);
		}
		System.out.println(result.failedAboveLimit > 0
				? String.format("COASTLINE PROBLEMS REPRODUCED - %d tiles above %s - exit code 2",
						result.failedAboveLimit, pct(failAbove))
				: result.renderErrors > 0
						? "RENDERER ERRORS - exit code 2"
						: result.failedTiles > 0
								? String.format("%d failed tiles, none above %s - exit code 0", result.failedTiles,
										pct(failAbove))
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
		// the severity filter: Jenkins allows no scripts, so it is radio buttons that the css reads with
		// ":checked ~ main"; they have to be siblings in front of <header> and <main>
		int[] total = new int[SEVERITY_FILTERS.length];
		for (TileResult r : reported) {
			total[0]++;
			total[r.bucket()]++;
		}
		for (int i = 0; i < SEVERITY_FILTERS.length; i++) {
			sb.append(String.format("<input type=\"radio\" name=\"sev\" class=\"sev\" id=\"f-%s\"%s>",
					SEVERITY_FILTERS[i][0], i == 0 ? " checked" : ""));
		}
		sb.append("\n<header><h1>Coastline rendering &mdash; epic 3291</h1>");
		sb.append(String.format("<p class=\"sum\"><b class=\"%s\">%d failed</b>, <b class=\"%s\">%d above %s</b> "
						+ "fail the build%s &middot; %d tiles compared "
						+ "&middot; %d maps &middot; %s renderer &middot; style %s &middot; %.1f s &middot; %s</p>",
				result.failedTiles > 0 ? "bad" : "good", result.failedTiles,
				result.failedAboveLimit > 0 ? "bad" : "good", result.failedAboveLimit, pct(result.failAbove),
				(result.renderErrors > 0 ? String.format(" &middot; <b class=\"bad\">%d renderer "
						+ "errors</b> (%d crashes)", result.renderErrors, result.rendererDeaths) : "")
						+ (result.staleReferences > 0 ? String.format(" &middot; %d stale references (the tile "
						+ "agrees with openstreetmap.org)", result.staleReferences) : ""),
				result.comparedTiles, result.loadedMaps, esc(result.renderer), esc(result.style),
				result.durationMs / 1000.0, new java.util.Date()));
		if (result.groups.size() > 1) {
			StringBuilder g = new StringBuilder();
			for (GroupTotals t : result.groups) {
				g.append(g.length() == 0 ? "" : " &middot; ").append(String.format(
						"%s: <b class=\"%s\">%d failed</b> of %d tiles", esc(t.group),
						t.failedTiles > 0 ? "bad" : "good", t.failedTiles, t.tiles));
			}
			sb.append("<p class=\"sum groups\">").append(g).append("</p>");
		}
		sb.append("<p class=\"filter\">tiles by the worst of extra / missing water:");
		for (int i = 0; i < SEVERITY_FILTERS.length; i++) {
			sb.append(String.format(" <label for=\"f-%s\">%s <b>%d</b></label>", SEVERITY_FILTERS[i][0],
					SEVERITY_FILTERS[i][1], total[i]));
		}
		sb.append("</p>");
		StringBuilder severityHeads = new StringBuilder();
		for (int i = 1; i < SEVERITY_FILTERS.length; i++) {
			severityHeads.append("<th>").append(SEVERITY_FILTERS[i][1]).append("</th>");
		}
		sb.append("</header>\n<main>\n<table class=\"stats\"><tr><th>case</th><th>tiles</th><th>failed</th>"
				+ severityHeads + "<th>worst extra water</th><th>worst missing water</th><th>worst tile</th></tr>");
		String tableGroup = null;
		for (CaseStats s : orderedCases(result)) {
			String g = s.group == null ? GROUP_FIXED : s.group;
			if (!g.equals(tableGroup)) {
				tableGroup = g;
				sb.append(String.format("<tr class=\"grp\"><td colspan=\"%d\">%s</td></tr>",
						5 + SEVERITY_FILTERS.length, esc(g)));
			}
			boolean seamarks = CHECK_SEAMARKS_INLAND.equals(s.check);
			sb.append(String.format("<tr class=\"%s\"><td>%s#%d</a> %s</td><td>%d</td><td>%d</td>%s"
							+ "<td>%s</td><td>%s</td><td>%s</td></tr>", s.failedTiles > 0 ? "bad" : "good",
					s.url == null ? "<a>" : "<a href=\"" + esc(s.url) + "\">", s.issue, esc(s.title),
					s.comparedTiles, s.failedTiles, severityCells(s.failedBySeverity),
					seamarks ? pct(s.worstExtraWater) + " drawn" : pct(s.worstExtraWater),
					seamarks ? "&mdash;" : pct(s.worstMissingWater), esc(s.worstTile)));
			if (!GROUP_FIXED.equals(g) && s.zooms.size() > 1) {
				// the random tiles and the scans: where the failures are is a question of the zoom
				for (Map.Entry<Integer, ZoomStats> e : s.zooms.entrySet()) {
					ZoomStats z = e.getValue();
					sb.append(String.format("<tr class=\"zoom %s\"><td>z%d</td><td>%d</td><td>%d</td>%s"
									+ "<td>%s</td><td>%s</td><td>%s</td></tr>", z.failedTiles > 0 ? "bad" : "good",
							e.getKey(), z.comparedTiles, z.failedTiles, severityCells(z.failedBySeverity),
							pct(z.worstExtraWater), pct(z.worstMissingWater), esc(z.worstTile)));
				}
			}
		}
		sb.append("</table>\n");
		if (reported.isEmpty()) {
			sb.append("<p class=\"empty\">No failed tiles.</p>");
		}
		// a block per fixed case and per zoom of the random tiles and the scans, each a <details> of its
		// own so that it can be folded away (Jenkins serves the report without scripts); worst tiles first
		Map<String, List<TileResult>> blocks = new LinkedHashMap<>();
		for (CaseStats s : orderedCases(result)) {
			blocks.put(s.issue + " " + s.title, new ArrayList<>());
		}
		for (TileResult r : reported) {
			blocks.computeIfAbsent(r.def.key(), k -> new ArrayList<>()).add(r);
		}
		String lastGroup = null;
		for (List<TileResult> caseTiles : blocks.values()) {
			if (caseTiles.isEmpty()) {
				continue;
			}
			CaseDef def = caseTiles.get(0).def;
			String g = def.group == null ? GROUP_FIXED : def.group;
			if (!g.equals(lastGroup)) {
				lastGroup = g;
				sb.append(String.format("<h1 class=\"grp\">%s</h1>\n", esc(g)));
			}
			String caseLink = String.format("%s#%d</a> %s", def.url == null ? "<a>"
					: "<a href=\"" + esc(def.url) + "\">", def.issue, esc(def.title));
			Map<Integer, List<TileResult>> byZoom = new TreeMap<>(Collections.reverseOrder());
			if (GROUP_FIXED.equals(g)) {
				byZoom.put(-1, caseTiles);
			} else {
				for (TileResult r : caseTiles) {
					byZoom.computeIfAbsent(r.zoom, z -> new ArrayList<>()).add(r);
				}
			}
			for (Map.Entry<Integer, List<TileResult>> e : byZoom.entrySet()) {
				List<TileResult> tiles = e.getValue();
				tiles.sort((a, b) -> a.ok() != b.ok() ? (a.ok() ? 1 : -1) : Double.compare(b.severity, a.severity));
				int failed = 0;
				int[] counts = new int[SEVERITY_FILTERS.length];
				for (TileResult r : tiles) {
					failed += r.ok() ? 0 : 1;
					counts[r.bucket()]++;
				}
				StringBuilder bucketCounts = new StringBuilder();
				for (int i = 1; i < SEVERITY_FILTERS.length; i++) {
					if (counts[i] > 0) {
						bucketCounts.append(String.format(" <i class=\"%s\">%s&nbsp;%d</i>", SEVERITY_FILTERS[i][0],
								SEVERITY_FILTERS[i][1], counts[i]));
					}
				}
				// folded: the report is read one block at a time
				sb.append(String.format("<details class=\"block\"><summary>%s%s <span class=\"%s\">%d failed"
								+ "</span><span class=\"bc\">%s</span></summary>\n", caseLink,
						e.getKey() < 0 ? "" : " &middot; z" + e.getKey(), failed > 0 ? "bad" : "good", failed,
						bucketCounts));
				for (TileResult r : tiles) {
					appendTile(sb, r);
				}
				sb.append("</details>\n");
			}
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

	/** The failed tiles of a table row by the ranges of {@link #SEVERITY_FILTERS}, zeros greyed out. */
	private static String severityCells(int[] counts) {
		StringBuilder res = new StringBuilder();
		for (int i = 1; i < SEVERITY_FILTERS.length; i++) {
			res.append(String.format("<td class=\"%s\">%d</td>", counts[i] == 0 ? "zero" : SEVERITY_FILTERS[i][0],
					counts[i]));
		}
		return res.toString();
	}

	private void appendTile(StringBuilder sb, TileResult r) {
		double lat = MapUtils.getLatitudeFromTile(r.zoom, r.y + 0.5);
		double lon = MapUtils.getLongitudeFromTile(r.zoom, r.x + 0.5);
		sb.append(String.format("<section class=\"tile " + SEVERITY_FILTERS[r.bucket()][0]
						+ " %s\"><div class=\"hd\"><b>%d/%d/%d</b>"
						+ "<a href=\"%s/map/#%d/%.4f/%.4f\" title=\"open this place on the map\">map</a>"
						+ "<a href=\"%s/tile/df/%d/%d/%d.png\" title=\"the same tile rendered by the server\">"
						+ "server tile</a>"
						+ "<a href=\"%s\" title=\"the reference tile\">reference tile</a>"
						+ "<span class=\"badge\">%s</span></div>", r.staleReference ? "stale" : r.ok() ? "good" : "bad",
				r.zoom, r.x, r.y, MAP_SERVER, r.zoom, lat, lon, MAP_SERVER, r.zoom, r.x, r.y,
				esc(referenceUrl(r.zoom, r.x, r.y)), r.staleReference ? "stale reference" : r.ok() ? "ok" : "failed"));
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
			+ ".problem{margin:8px 0 0;font-size:12px;color:var(--bad)}\n"
			+ "table.stats tr.zoom td{color:var(--mut);font-size:12px;padding-top:1px;padding-bottom:1px}\n"
			+ "table.stats tr.zoom td:first-child{padding-left:22px}\n"
			+ "table.stats tr.zoom.bad td{color:var(--bad)}\n"
			+ "details.block>summary{cursor:pointer;font-size:15px;font-weight:600;margin:26px 0 10px;"
			+ "padding-top:10px;border-top:1px solid var(--line)}\n"
			+ "details.block>summary a{color:inherit}details.block>summary span{font-weight:400;font-size:13px}\n"
			+ ".bc i{font-style:normal;font-weight:400;font-size:12px;color:var(--mut);margin-left:10px}\n"
			+ ".bc i.s50{color:var(--bad);font-weight:600}\n"
			+ "table.stats td.zero{color:var(--mut);opacity:.5}table.stats td.s50{font-weight:600}\n"
			+ "input.sev{position:absolute;opacity:0;pointer-events:none}\n"
			+ ".filter{margin:8px 0 0;color:var(--mut)}.filter label{cursor:pointer;margin-left:6px;padding:2px 10px;"
			+ "border:1px solid var(--line);border-radius:14px;white-space:nowrap}\n"
			+ ".tile.stale{border-color:var(--mut)}.tile.stale .badge{background:var(--line);color:var(--fg)}\n"
			+ filterCss();

	/** The css of the severity filter, one set of rules per range of {@link #SEVERITY_FILTERS}. */
	private static String filterCss() {
		StringBuilder checked = new StringBuilder(), tiles = new StringBuilder(), blocks = new StringBuilder();
		for (int i = 0; i < SEVERITY_FILTERS.length; i++) {
			String id = SEVERITY_FILTERS[i][0];
			checked.append(i == 0 ? "" : ",").append("#f-").append(id).append(":checked~header label[for=f-")
					.append(id).append("]");
			if (i > 0) {
				// the tiles of the other ranges disappear, and so do the blocks left without a tile
				tiles.append(i == 1 ? "" : ",").append("#f-").append(id).append(":checked~main .tile:not(.")
						.append(id).append(")");
				blocks.append(i == 1 ? "" : ",").append("#f-").append(id)
						.append(":checked~main details.block:not(:has(.").append(id).append("))");
			}
		}
		return checked + "{background:var(--fg);color:var(--bg);border-color:var(--fg)}\n" + tiles
				+ "{display:none}\n" + blocks + "{display:none}\n";
	}

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
	 * Explicit {@code -eyepiece=}, else the binary of a local repository checkout, else whatever is
	 * called {@code eyepiece} on the PATH - which is how the build server gets it.
	 */
	private File findEyePiece() {
		String explicit = opt("eyepiece", null);
		if (explicit != null) {
			File f = new File(explicit);
			if (!f.isFile()) {
				throw new IllegalStateException("-eyepiece=" + explicit + " does not exist");
			}
			return f;
		}
		String name = System.getProperty("os.name").toLowerCase().contains("win")
				? "eyepiece.exe" : "eyepiece";
		List<File> found = new ArrayList<>();
		collect(new File(repoRoot(), "binaries"), name, found, 4);
		if (!found.isEmpty()) {
			return found.get(0);
		}
		for (String dir : System.getenv().getOrDefault("PATH", "").split(File.pathSeparator)) {
			File f = new File(dir, name);
			if (f.isFile() && f.canExecute()) {
				return f;
			}
		}
		throw new IllegalStateException("eyepiece is not found, pass -eyepiece=<path to the binary>."
				+ " It is built from core/tools (OsmAndCore) and published by the build server as"
				+ " part of the core binaries");
	}

	/**
	 * Explicit {@code -stylesPath=}, else the styles of a local repository checkout. Null means "use
	 * the styles built into OsmAndCore", which is what the build server does.
	 */
	private File findStyles() {
		String explicit = opt("stylesPath", null);
		if (explicit != null) {
			File f = new File(explicit);
			if (!f.isDirectory()) {
				throw new IllegalStateException("-stylesPath=" + explicit + " is not a folder");
			}
			return f;
		}
		File f = new File(repoRoot(), "resources/rendering_styles");
		return f.isDirectory() ? f : null;
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
