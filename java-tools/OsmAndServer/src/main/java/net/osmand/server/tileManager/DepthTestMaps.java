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
 * Test only (OsmAnd-Issues#3341): the depth OBFs of the builder with the current styles, as depth-marine,
 * depth-default and depth-nautical. The maps are downloaded into the temp folder at start and again when their size
 * on the builder changed; env DEPTH_TEST_MAPS_DIR points at a folder of maps built locally instead.
 * The native library has one set of files, so the base depth maps are closed before these render.
 */
public class DepthTestMaps {

	private static final Log LOGGER = LogFactory.getLog(DepthTestMaps.class);

	public static final DepthTestMaps INSTANCE = new DepthTestMaps();

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

	// DEPTH_TEST_MAPS_DIR: a folder with maps/NAME.depth.obf built locally; nothing is downloaded or refreshed from
	// the builder then, so the local files stay
	private final String localDir = System.getenv("DEPTH_TEST_MAPS_DIR");
	private final File dir = localDir != null ? new File(localDir)
			: new File(System.getProperty("java.io.tmpdir"), "osmand-depth-test");
	private final HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
	private volatile String status = "Depth test maps are not downloaded yet";
	private Thread downloader;
	private NativeJavaRendering lib;
	private Object renderLock;
	private boolean opened;
	private boolean baseClosed;

	/** The current styles from the classpath, written next to the maps so that the renderer can load them by path. */
	public List<VectorStyle> createStyles(int tileSizeLog, int metaTileSizeLog, int maxZoomCache) {
		List<VectorStyle> styles = new ArrayList<>();
		File styleDir = new File(dir, "styles");
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
			for (String s : STYLES) {
				VectorStyle vs = new VectorStyle();
				vs.key = "depth-" + s;
				vs.name = vs.key;
				vs.file = new File(styleDir, s + ".render.xml").getAbsolutePath();
				vs.depth = s;
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
		} catch (Exception e) {
			LOGGER.error("Depth test styles: " + e.getMessage(), e);
		}
		LOGGER.info("Depth test styles: " + styles.size() + " in " + dir.getAbsolutePath());
		startDownload();
		return styles;
	}

	/** Called under the rendering lock: returns an error while the maps are not ready. */
	public synchronized String activate(NativeJavaRendering lib, Object renderLock, String obfLocation) {
		if (status != null) {
			startDownload();
			return status;
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
		if (!opened) {
			for (String m : MAPS) {
				File f = map(m);
				if (f.exists()) {  // a region that is not published yet, or a local folder with one region only
					lib.initMapFile(f.getAbsolutePath(), true);
				}
			}
			opened = true;
		}
		return null;
	}

	/** Downloads the maps whose size on the builder changed (after a new build). */
	public synchronized void refresh() {
		if (localDir != null || (downloader != null && downloader.isAlive())) {
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
				boolean wasOpen = opened && lib != null;
				if (wasOpen) {
					lib.closeMapFile(f.getAbsolutePath());
				}
				Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING);
				if (wasOpen) {
					lib.initMapFile(f.getAbsolutePath(), true);
				}
			}
		}
		LOGGER.info("Depth test map updated: " + f.getName());
	}

	/** At start: downloads the missing maps, DOWNLOADS_AT_A_TIME at a time, then looks for newly built ones. */
	private synchronized void startDownload() {
		if (downloader != null && downloader.isAlive()) {
			return;
		}
		downloader = new Thread(() -> {
			try {
				downloadMissing();
				status = null;
				LOGGER.info("Depth test maps are ready");
			} catch (Exception e) {
				status = "Depth test maps download failed: " + e.getMessage();
				LOGGER.error(status, e);
				return;
			}
			if (localDir == null) {
				refreshChanged();
			}
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
		if (missing.isEmpty() || localDir != null) {
			return;  // a local folder holds what it holds: whatever is missing is simply not shown
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
						status = String.format("Downloading the depth test maps: %d of %d left, now %s",
								left[0], missing.size(), m);
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
		return new File(dir, "maps/" + name + ".depth.obf");
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
