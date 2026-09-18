package net.osmand.server.tileManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.osmand.NativeJavaRendering;
import net.osmand.render.RenderingRuleProperty;
import net.osmand.render.RenderingRulesStorage;
import net.osmand.server.tileManager.TileServerConfig.VectorStyle;
import net.osmand.util.Algorithms;

/**
 * Test only (OsmAnd-Issues#3341): depth OBFs rendered with the styles of this jar, in two sets to compare -
 * "depth-*" the maps of the builder and "work-*" the ones built locally. Three folders are used:
 * <ul>
 * <li>OBF_LOCATION - the ordinary maps (a basemap and the regions to look at); their own .depth.obf are closed;
 * <li>DEPTH_MAPS_DIR - the depth maps of the builder, downloaded here at start and again when their size changed
 *     (default: a folder in the temp directory);
 * <li>DEPTH_WORK_DIR - the depth OBFs built locally, any file name; nothing is downloaded there.
 * </ul>
 * The native library holds one set of files, so a style closes the other set before it renders.
 */
public class DepthTestMaps {

	private static final Log LOGGER = LogFactory.getLog(DepthTestMaps.class);

	public static final DepthTestMaps INSTANCE = new DepthTestMaps();

	public static final String SERVER = "depth";
	public static final String WORK = "work";
	// every published depth map, the _full_coverage_ ones left out: they are Europe_contours and World_contours
	// with the detailed regions not cut out, so they would double the contours of the regional maps
	private static final String[] MAPS = { "Netherlands_contours", "Ireland_contours", "France_contours",
			"Great_Britain_contours", "Norway_contours", "New-zealand_contours", "Gulf_of_Mexico_north-west_contours",
			"Europe_contours", "Europe_points", "World_contours", "World_Northern_hemisphere_points",
			"World_Southern_hemisphere_points" };
	private static final String[] STYLES = { "marine", "default", "nautical" };
	private static final String[] STYLE_FILES = { "default", "marine", "nautical", "depthcontourlines.addon" };
	private static final String MAP_URL = "https://builder.osmand.net/depth-data/build/%s.depth.obf";
	private static final int DOWNLOADS_AT_A_TIME = 3;

	private final File serverDir = System.getenv("DEPTH_MAPS_DIR") != null ? new File(System.getenv("DEPTH_MAPS_DIR"))
			: new File(System.getProperty("java.io.tmpdir"), "osmand-depth-test");
	private final File workDir = System.getenv("DEPTH_WORK_DIR") != null ? new File(System.getenv("DEPTH_WORK_DIR")) : null;
	private final HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
	/** Per set: what is still missing, or null once the set can be rendered; the work set is ready at once. */
	private final Map<String, String> status = new ConcurrentHashMap<>();
	private Thread downloader;
	private NativeJavaRendering lib;
	private Object renderLock;
	private String active;
	private boolean baseClosed;

	/** The styles of this jar, written beside the maps: depth-* for the server set, work-* for the local one. */
	public List<VectorStyle> createStyles(int tileSizeLog, int metaTileSizeLog, int maxZoomCache) {
		List<VectorStyle> styles = new ArrayList<>();
		File styleDir = new File(serverDir, "styles");
		try {
			styleDir.mkdirs();
			for (String f : STYLE_FILES) {
				InputStream in = RenderingRulesStorage.class.getResourceAsStream(f + ".render.xml");
				if (in == null) {
					throw new IOException("No rendering style " + f);
				}
				try (InputStream is = in) {
					Files.copy(is, new File(styleDir, f + ".render.xml").toPath(), StandardCopyOption.REPLACE_EXISTING);
				}
			}
			for (String set : workDir == null ? new String[] { SERVER } : new String[] { SERVER, WORK }) {
				for (String s : STYLES) {
					VectorStyle vs = new VectorStyle();
					vs.key = set + "-" + s;
					vs.name = vs.key;
					vs.file = new File(styleDir, s + ".render.xml").getAbsolutePath();
					vs.depth = set;
					vs.maxZoomCache = maxZoomCache;
					vs.tileSizeLog = tileSizeLog;
					vs.metaTileSizeLog = metaTileSizeLog;
					vs.storage = NativeJavaRendering.parseStorage(vs.file);
					for (RenderingRuleProperty p : vs.storage.PROPS.getPoperties()) {
						if (!Algorithms.isEmpty(p.getName()) && !Algorithms.isEmpty(p.getCategory())
								&& !"ui_hidden".equals(p.getCategory())) {
							vs.properties.add(p);
						}
					}
					styles.add(vs);
				}
			}
		} catch (Exception e) {
			LOGGER.error("Depth test styles: " + e.getMessage(), e);
		}
		LOGGER.info("Depth test styles: " + styles.size() + ", maps of the builder in " + serverDir.getAbsolutePath()
				+ (workDir == null ? ", no work folder" : ", work maps in " + workDir.getAbsolutePath()));
		startDownload();
		return styles;
	}

	/** Called under the rendering lock: returns an error while that set is not ready. */
	public synchronized String activate(NativeJavaRendering lib, Object renderLock, String set, String obfLocation) {
		String left = status.get(set);
		if (left != null) {
			startDownload();
			return left;
		}
		this.lib = lib;
		this.renderLock = renderLock;
		if (!baseClosed && obfLocation != null) {
			File[] base = new File(obfLocation).listFiles((d, name) -> name.endsWith(".depth.obf"));
			for (File f : base == null ? new File[0] : base) {
				lib.closeMapFile(f.getAbsolutePath());
			}
			baseClosed = true;
		}
		if (!set.equals(active)) {
			if (active != null) {
				for (File f : mapFiles(active)) {
					lib.closeMapFile(f.getAbsolutePath());
				}
			}
			int n = 0;
			for (File f : mapFiles(set)) {
				lib.initMapFile(f.getAbsolutePath(), true);
				n++;
			}
			active = set;
			LOGGER.info("Depth test set " + set + ": " + n + " maps opened");
		}
		return null;
	}

	/** The maps of a set: the published names in the server folder, every OBF of the work folder. */
	private List<File> mapFiles(String set) {
		List<File> files = new ArrayList<>();
		if (WORK.equals(set)) {
			File[] all = workDir == null ? null : workDir.listFiles((d, n) -> n.endsWith(".obf"));
			for (File f : all == null ? new File[0] : all) {
				files.add(f);
			}
			return files;
		}
		for (String m : MAPS) {
			File f = map(m);
			if (f.exists()) {  // a region that is not published yet
				files.add(f);
			}
		}
		return files;
	}

	/** Downloads the maps whose size on the builder changed (after a new build). */
	public synchronized void refresh() {
		if (downloader != null && downloader.isAlive()) {
			return;
		}
		downloader = new Thread(this::refreshChanged, "depth-test-refresh");
		downloader.start();
	}

	private void refreshChanged() {
		for (String m : MAPS) {
			File f = map(m);
			try {
				long size = http.send(HttpRequest.newBuilder(URI.create(String.format(MAP_URL, m)))
						.method("HEAD", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding())
						.headers().firstValueAsLong("Content-Length").orElse(-1);
				if (size > 0 && size != f.length()) {
					LOGGER.info("Depth test map changed on the builder: " + m);
					File tmp = new File(f.getPath() + ".tmp");
					download(String.format(MAP_URL, m), tmp);
					replace(tmp, f);
				}
			} catch (Exception e) {
				LOGGER.error("Depth test refresh " + m + ": " + e.getMessage(), e);
			}
		}
	}

	private void replace(File tmp, File f) throws IOException {
		// same lock order as rendering: render lock, then this
		Object lock = renderLock != null ? renderLock : this;
		synchronized (lock) {
			synchronized (this) {
				boolean opened = SERVER.equals(active) && lib != null;
				if (opened) {
					lib.closeMapFile(f.getAbsolutePath());
				}
				Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING);
				if (opened) {
					lib.initMapFile(f.getAbsolutePath(), true);
				}
			}
		}
		LOGGER.info("Depth test map updated: " + f.getName());
	}

	/** At start: downloads the missing maps of the builder, DOWNLOADS_AT_A_TIME at a time. */
	private synchronized void startDownload() {
		if (downloader != null && downloader.isAlive()) {
			return;
		}
		status.putIfAbsent(SERVER, "The depth maps of the builder are not downloaded yet");
		downloader = new Thread(() -> {
			try {
				downloadMissing();
				status.remove(SERVER);
				LOGGER.info("Depth test maps of the builder are ready");
			} catch (Exception e) {
				status.put(SERVER, "Depth test maps download failed: " + e.getMessage());
				LOGGER.error("Depth test maps: " + e.getMessage(), e);
				return;
			}
			refreshChanged();
		}, "depth-test-download");
		downloader.start();
	}

	private void downloadMissing() throws Exception {
		List<String> missing = new ArrayList<>();
		for (String m : MAPS) {
			if (!map(m).exists()) {
				missing.add(m);
			}
		}
		if (missing.isEmpty()) {
			return;
		}
		int[] left = { missing.size() };
		ExecutorService pool = Executors.newFixedThreadPool(Math.min(DOWNLOADS_AT_A_TIME, missing.size()));
		List<Future<?>> tasks = new ArrayList<>();
		try {
			for (String m : missing) {
				tasks.add(pool.submit(() -> {
					File f = map(m);
					f.getParentFile().mkdirs();
					File tmp = new File(f.getPath() + ".tmp");
					download(String.format(MAP_URL, m), tmp);
					Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING);
					synchronized (left) {
						left[0]--;
						status.put(SERVER, String.format("Downloading the depth maps of the builder: %d of %d left, now %s",
								left[0], missing.size(), m));
					}
					return null;
				}));
			}
			for (Future<?> t : tasks) {
				t.get();
			}
		} finally {
			pool.shutdownNow();
		}
	}

	private File map(String name) {
		return new File(serverDir, "maps/" + name + ".depth.obf");
	}

	private void download(String url, File target) throws IOException, InterruptedException {
		HttpResponse<InputStream> r = http.send(HttpRequest.newBuilder(URI.create(url)).build(),
				HttpResponse.BodyHandlers.ofInputStream());
		if (r.statusCode() != 200) {
			r.body().close();
			throw new IOException(url + " answered " + r.statusCode());
		}
		try (InputStream in = r.body(); FileOutputStream out = new FileOutputStream(target)) {
			Algorithms.streamCopy(in, out);
		}
	}
}
