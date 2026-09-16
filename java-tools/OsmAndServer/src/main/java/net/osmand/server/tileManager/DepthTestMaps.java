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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.osmand.NativeJavaRendering;
import net.osmand.render.RenderingRuleProperty;
import net.osmand.server.tileManager.TileServerConfig.VectorStyle;
import net.osmand.util.Algorithms;

/**
 * Test only (OsmAnd-Issues#3341): published depth OBFs with the style before OsmAnd-resources#1478 as depth-old-*,
 * freshly built depth OBFs with the current style as depth-new-*. Everything is downloaded into the temp folder.
 * The native library has one set of files, so a depth style closes the other set before it renders.
 */
public class DepthTestMaps {

	private static final Log LOGGER = LogFactory.getLog(DepthTestMaps.class);

	public static final DepthTestMaps INSTANCE = new DepthTestMaps();

	public static final String OLD = "old";
	public static final String NEW = "new";
	private static final String[] MAPS = { "Netherlands_contours", "Europe_contours", "Europe_points", "World_contours",
			"World_Northern_hemisphere_points", "World_Southern_hemisphere_points", "Gulf_of_Mexico_north-west_contours" };
	private static final String[] STYLES = { "marine", "default", "nautical" };
	private static final String[] STYLE_FILES = { "default", "marine", "nautical", "depthcontourlines.addon" };
	private static final String OLD_STYLES_COMMIT = "cf87cb0661bb2669f6c38a581e89bd284eef8c28";
	private static final String STYLE_URL = "https://raw.githubusercontent.com/osmandapp/OsmAnd-resources/%s/rendering_styles/%s.render.xml";
	private static final String OLD_MAP_URL = "https://download.osmand.net/download?depth=yes&file=%s_2.depth.obf.zip";
	private static final String NEW_MAP_URL = "https://builder.osmand.net/depth-data/build/%s.depth.obf";

	private final File dir = new File(System.getProperty("java.io.tmpdir"), "osmand-depth-test");
	private final HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
	private volatile String status = "Depth test maps are not downloaded yet";
	private Thread downloader;
	private NativeJavaRendering lib;
	private Object renderLock;
	private String active;
	private boolean baseClosed;

	public List<VectorStyle> createStyles(VectorStyle template) {
		List<VectorStyle> styles = new ArrayList<>();
		for (String set : new String[] { OLD, NEW }) {
			File styleDir = new File(dir, set + "-styles");
			try {
				styleDir.mkdirs();
				for (String f : STYLE_FILES) {
					download(String.format(STYLE_URL, set.equals(OLD) ? OLD_STYLES_COMMIT : "master", f),
							new File(styleDir, f + ".render.xml"), false);
				}
				for (String s : STYLES) {
					VectorStyle vs = new VectorStyle();
					vs.key = "depth-" + set + "-" + s;
					vs.name = vs.key;
					vs.file = new File(styleDir, s + ".render.xml").getAbsolutePath();
					vs.depth = set;
					vs.maxZoomCache = template.maxZoomCache;
					vs.tileSizeLog = template.tileSizeLog;
					vs.metaTileSizeLog = template.metaTileSizeLog;
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
				LOGGER.error("Depth test styles " + set + ": " + e.getMessage(), e);
			}
		}
		return styles;
	}

	/** Called under the rendering lock: returns an error while the maps are not ready. */
	public synchronized String activate(NativeJavaRendering lib, Object renderLock, String set, String obfLocation) {
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
		if (!set.equals(active)) {
			if (active != null) {
				for (String m : MAPS) {
					lib.closeMapFile(map(active, m).getAbsolutePath());
				}
			}
			for (String m : MAPS) {
				lib.initMapFile(map(set, m).getAbsolutePath(), true);
			}
			active = set;
		}
		return null;
	}

	/** Downloads new depth maps whose size on the builder changed (after a new build). */
	public synchronized void refresh() {
		if (downloader != null && downloader.isAlive()) {
			return;
		}
		downloader = new Thread(() -> {
			for (String m : MAPS) {
				File f = map(NEW, m);
				try {
					long size = http.send(HttpRequest.newBuilder(URI.create(String.format(NEW_MAP_URL, m)))
							.method("HEAD", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding())
							.headers().firstValueAsLong("Content-Length").orElse(-1);
					if (size > 0 && size != f.length()) {
						File tmp = new File(f.getPath() + ".tmp");
						download(String.format(NEW_MAP_URL, m), tmp, false);
						replace(tmp, f);
					}
				} catch (Exception e) {
					LOGGER.error("Depth test refresh " + m + ": " + e.getMessage(), e);
				}
			}
		}, "depth-test-refresh");
		downloader.start();
	}

	private void replace(File tmp, File f) throws IOException {
		// same lock order as rendering: render lock, then this
		Object lock = renderLock != null ? renderLock : this;
		synchronized (lock) {
			synchronized (this) {
				boolean opened = NEW.equals(active) && lib != null;
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

	private synchronized void startDownload() {
		if (downloader != null && downloader.isAlive()) {
			return;
		}
		downloader = new Thread(() -> {
			try {
				int i = 0;
				for (String set : new String[] { OLD, NEW }) {
					for (String m : MAPS) {
						status = String.format("Downloading depth test maps %d of %d: %s %s", ++i, MAPS.length * 2, set, m);
						File f = map(set, m);
						if (!f.exists()) {
							f.getParentFile().mkdirs();
							File tmp = new File(f.getPath() + ".tmp");
							download(String.format(set.equals(OLD) ? OLD_MAP_URL : NEW_MAP_URL, m), tmp, set.equals(OLD));
							Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING);
						}
					}
				}
				status = null;
			} catch (Exception e) {
				status = "Depth test maps download failed: " + e.getMessage();
				LOGGER.error(status, e);
			}
		}, "depth-test-download");
		downloader.start();
	}

	private File map(String set, String name) {
		return new File(dir, set + "/" + name + ".depth.obf");
	}

	private void download(String url, File target, boolean unzip) throws IOException, InterruptedException {
		HttpResponse<InputStream> r = http.send(HttpRequest.newBuilder(URI.create(url)).build(),
				HttpResponse.BodyHandlers.ofInputStream());
		if (r.statusCode() != 200) {
			r.body().close();
			throw new IOException(url + " answered " + r.statusCode());
		}
		try (InputStream in = r.body()) {
			InputStream src = in;
			if (unzip) {
				ZipInputStream zis = new ZipInputStream(in);
				ZipEntry ze;
				while ((ze = zis.getNextEntry()) != null && !ze.getName().endsWith(".obf")) {
					zis.closeEntry();
				}
				if (ze == null) {
					throw new IOException(url + " has no obf");
				}
				src = zis;
			}
			try (FileOutputStream out = new FileOutputStream(target)) {
				Algorithms.streamCopy(src, out);
			}
		}
	}
}
