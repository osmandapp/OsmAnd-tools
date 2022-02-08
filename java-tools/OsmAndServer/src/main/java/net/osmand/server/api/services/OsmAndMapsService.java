package net.osmand.server.api.services;


import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.NativeJavaRendering;
import net.osmand.NativeJavaRendering.RenderingImageContext;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.CachedOsmandIndexes;
import net.osmand.binary.OsmandIndex.FileIndex;
import net.osmand.binary.OsmandIndex.RoutingPart;
import net.osmand.binary.OsmandIndex.RoutingSubregion;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.router.PrecalculatedRouteDirection;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.router.RoutingContext;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

@Service
public class OsmAndMapsService {
	private static final Log LOGGER = LogFactory.getLog(OsmAndMapsService.class);

	private static final int MAX_RUNTIME_IMAGE_CACHE_SIZE = 80;

	private static final int MAX_RUNTIME_TILES_CACHE_SIZE = 10000;

	private static final int MAX_FILES_PER_FOLDER = 1 << 12; // 4096

	private static final int ZOOM_EN_PREFERRED_LANG = 6;

	private static final boolean DEFAULT_USE_ROUTING_NATIVE_LIB = false;
	
	private static final int MEM_LIMIT = RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT * 8;

	Map<String, BinaryMapIndexReaderReference> obfRoutingFiles = new LinkedHashMap<>();

	AtomicInteger cacheTouch = new AtomicInteger(0);

	Map<String, VectorMetatile> tileCache = new ConcurrentHashMap<>();

	NativeJavaRendering nativelib;

	File tempDir;

	@Autowired
	VectorTileServerConfig config;

	public static class BinaryMapIndexReaderReference {
		File file;
		BinaryMapIndexReader reader;
		public FileIndex fileIndex;
	}

	@Configuration
	@ConfigurationProperties("tile-server")
	public static class VectorTileServerConfig {

		@Value("${tile-server.obf.location}")
		String obfLocation;

		@Value("${tile-server.cache.location}")
		String cacheLocation;

		@Value("${tile-server.cache.max-zoom}")
		int maxZoomCache = 16;

		@Value("${tile-server.metatile-size}")
		int metatileSize;

		public String initErrorMessage;

		public Map<String, VectorStyle> style = new TreeMap<String, VectorStyle>();

		public void setStyle(Map<String, String> style) {
			for (Entry<String, String> e : style.entrySet()) {
				VectorStyle vectorStyle = new VectorStyle();
				vectorStyle.key = e.getKey();
				vectorStyle.name = "";
				vectorStyle.maxZoomCache = maxZoomCache;
				// fast log_2_n calculation
				vectorStyle.metaTileSizeLog = 31 - Integer.numberOfLeadingZeros(Math.max(256, metatileSize)) - 8;
				vectorStyle.tileSizeLog = 31 - Integer.numberOfLeadingZeros(256) - 8;
				for (String s : e.getValue().split(",")) {
					String value = s.substring(s.indexOf('=') + 1);
					if (s.startsWith("style=")) {
						vectorStyle.name = value;
					} else if (s.startsWith("tilesize=")) {
						vectorStyle.tileSizeLog = 31 - Integer.numberOfLeadingZeros(Integer.parseInt(value)) - 8;
					} else if (s.startsWith("metatilesize=")) {
						vectorStyle.metaTileSizeLog = 31 - Integer.numberOfLeadingZeros(Integer.parseInt(value)) - 8;
					}
				}
				this.style.put(vectorStyle.key, vectorStyle);
			}
		}

	}

	public static class VectorStyle {
		public String key;
		public String name;
		public int maxZoomCache;
		public int tileSizeLog;
		public int metaTileSizeLog;
	}

	public static class VectorMetatile implements Comparable<VectorMetatile> {

		public BufferedImage runtimeImage;
		public long lastAccess;
		public final String key;
		public final int z;
		public final int left;
		public final int top;
		public final int metaSizeLog;
		public final int tileSizeLog;
		public final VectorStyle style;
		private final VectorTileServerConfig cfg;

		public VectorMetatile(VectorTileServerConfig cfg, String tileId, VectorStyle style, int z, int left, int top,
				int metaSizeLog, int tileSizeLog) {
			this.cfg = cfg;
			this.style = style;
			this.metaSizeLog = metaSizeLog;
			this.tileSizeLog = tileSizeLog;
			this.key = tileId;
			this.left = left;
			this.top = top;
			this.z = z;
			touch();
		}

		public void touch() {
			lastAccess = System.currentTimeMillis();
		}

		@Override
		public int compareTo(VectorMetatile o) {
			return Long.compare(lastAccess, o.lastAccess);
		}

		public BufferedImage readSubImage(BufferedImage img, int x, int y) {
			int subl = x - ((x >> metaSizeLog) << metaSizeLog);
			int subt = y - ((y >> metaSizeLog) << metaSizeLog);
			int tilesize = 256 << tileSizeLog;
			return img.getSubimage(subl * tilesize, subt * tilesize, tilesize, tilesize);
		}

		public BufferedImage getCacheRuntimeImage() throws IOException {
			BufferedImage img = runtimeImage;
			if (img != null) {
				return img;
			}
			File cf = getCacheFile();
			if (cf != null && cf.exists()) {
				runtimeImage = ImageIO.read(cf);
				return runtimeImage;
			}
			return null;
		}

		public File getCacheFile() {
			if (z > cfg.maxZoomCache || cfg.cacheLocation == null || cfg.cacheLocation.length() == 0) {
				return null;
			}
			int x = left >> (31 - z) >> metaSizeLog;
			int y = top >> (31 - z) >> metaSizeLog;
			StringBuilder loc = new StringBuilder();
			loc.append(style.key).append("/").append(z);
			while (x >= MAX_FILES_PER_FOLDER) {
				int nx = x % MAX_FILES_PER_FOLDER;
				loc.append("/").append(nx);
				x = (x - nx) / MAX_FILES_PER_FOLDER;
			}
			loc.append("/").append(x);
			while (y >= MAX_FILES_PER_FOLDER) {
				int ny = y % MAX_FILES_PER_FOLDER;
				loc.append("/").append(ny);
				y = (y - ny) / MAX_FILES_PER_FOLDER;
			}
			loc.append("/").append(y);
			loc.append("-").append(metaSizeLog).append("-").append(tileSizeLog).append(".png");
			return new File(cfg.cacheLocation, loc.toString());
		}
	}

	public VectorTileServerConfig getConfig() {
		return config;
	}

	public VectorStyle getStyle(String style) {
		return config.style.get(style);
	}

	public VectorMetatile getMetaTile(VectorStyle vectorStyle, int z, int x, int y) {
		int metaSizeLog = Math.min(vectorStyle.metaTileSizeLog, z - 1);
		int left = ((x >> metaSizeLog) << metaSizeLog) << (31 - z);
		if (left < 0) {
			left = 0;
		}
		int top = ((y >> metaSizeLog) << metaSizeLog) << (31 - z);
		if (top < 0) {
			top = 0;
		}
		String tileId = encode(vectorStyle.key, left >> (31 - z), top >> (31 - z), z, metaSizeLog,
				vectorStyle.tileSizeLog);
		VectorMetatile tile = tileCache.get(tileId);
		if (tile == null) {
			tile = new VectorMetatile(config, tileId, vectorStyle, z, left, top, metaSizeLog, vectorStyle.tileSizeLog);
			tileCache.put(tile.key, tile);
		}
		return tile;
	}

	public boolean validateAndInitConfig() throws IOException {
		if (nativelib == null && config.initErrorMessage == null) {
			synchronized (this) {
				if (!(nativelib == null && config.initErrorMessage == null)) {
					return config.initErrorMessage == null;
				}
				if (config.obfLocation == null || config.obfLocation.isEmpty()) {
					config.initErrorMessage = "Files location is not specified";
				} else {
					File obfLocationF = new File(config.obfLocation);
					if (!obfLocationF.exists()) {
						config.initErrorMessage = "Files location is not specified";
					}
				}
				tempDir = Files.createTempDirectory("osmandserver").toFile();
				LOGGER.info("Init temp rendering directory for libs / fonts: " + tempDir.getAbsolutePath());
				tempDir.deleteOnExit();
				ClassLoader cl = NativeJavaRendering.class.getClassLoader();
				ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(cl);
				Resource[] resources = resolver.getResources("classpath:/map/fonts/*.ttf");
				File fontsFolder = new File(tempDir, "fonts");
				fontsFolder.mkdirs();
				fontsFolder.deleteOnExit();
				for (Resource resource : resources) {
					InputStream ios = resource.getInputStream();
					File file = new File(fontsFolder, resource.getFilename());
					file.deleteOnExit();
					FileOutputStream fous = new FileOutputStream(file);
					Algorithms.streamCopy(ios, fous);
					fous.close();
					ios.close();
				}
				nativelib = NativeJavaRendering.getDefault(null, config.obfLocation, fontsFolder.getAbsolutePath());
				if (nativelib == null) {
					config.initErrorMessage = "Tile rendering engine is not initialized";
				}
			}
		}
		return config.initErrorMessage == null;
	}

	public ResponseEntity<String> renderMetaTile(VectorMetatile tile)
			throws IOException, XmlPullParserException, SAXException {
		// don't synchronize this to not block routing
		synchronized (config) {
			VectorMetatile rendered = tileCache.get(tile.key);
			if (rendered != null && rendered.runtimeImage != null) {
				tile.runtimeImage = rendered.runtimeImage;
				return null;
			}
			int imgTileSize = (256 << tile.tileSizeLog) << Math.min(tile.z, tile.metaSizeLog);
			int tilesize = (1 << Math.min(31 - tile.z + tile.metaSizeLog, 31));
			if (tilesize <= 0) {
				tilesize = Integer.MAX_VALUE;
			}
			int right = tile.left + tilesize;
			if (right <= 0) {
				right = Integer.MAX_VALUE;
			}
			int bottom = tile.top + tilesize;
			if (bottom <= 0) {
				bottom = Integer.MAX_VALUE;
			}
			long now = System.currentTimeMillis();
			String props = String.format("density=%d,textScale=%d", 1 << tile.tileSizeLog, 1 << tile.tileSizeLog);
			if (tile.z < ZOOM_EN_PREFERRED_LANG) {
				props += ",lang=en";
			}
			if (nativelib == null) {
				return null;
			}
			if (!tile.style.name.equalsIgnoreCase(nativelib.getRenderingRuleStorage().getName())) {
				nativelib.loadRuleStorage(tile.style.name + ".render.xml", props);
			} else {
				nativelib.setRenderingProps(props);
			}
			RenderingImageContext ctx = new RenderingImageContext(tile.left, right, tile.top, bottom, tile.z);
			if (ctx.width > 8192) {
				return ResponseEntity.badRequest().body("Metatile exceeds 8192x8192 size");

			}
			if (imgTileSize != ctx.width << tile.tileSizeLog || imgTileSize != ctx.height << tile.tileSizeLog) {
				return ResponseEntity.badRequest().body(String.format("Metatile has wrong size (%d != %d)", imgTileSize,
						ctx.width << tile.tileSizeLog));
			}
			tile.runtimeImage = nativelib.renderImage(ctx);
			if (tile.runtimeImage != null) {
				File cacheFile = tile.getCacheFile();
				if (cacheFile != null) {
					cacheFile.getParentFile().mkdirs();
					if (cacheFile.getParentFile().exists()) {
						ImageIO.write(tile.runtimeImage, "png", cacheFile);
					}
				}
			}
			String msg = String.format("Rendered %d %d at %d (%s %s): %dx%d - %d ms", tile.left, tile.top, tile.z,
					tile.style.name, props, ctx.width, ctx.height, (int) (System.currentTimeMillis() - now));
			System.out.println(msg);
			// LOGGER.debug();
			return null;
		}
	}

	public void cleanupCache() {
		int version = cacheTouch.incrementAndGet();
		// so with atomic only 1 thread will get % X == 0
		if (version % MAX_RUNTIME_IMAGE_CACHE_SIZE == 0 && version > 0) {
			cacheTouch.set(0);
			TreeSet<VectorMetatile> sortCache = new TreeSet<>(tileCache.values());
			List<VectorMetatile> imageTiles = new ArrayList<>();
			for (VectorMetatile vm : sortCache) {
				if (vm.runtimeImage != null) {
					imageTiles.add(vm);
				}
			}
			if (imageTiles.size() >= MAX_RUNTIME_IMAGE_CACHE_SIZE) {
				for (int i = 0; i < MAX_RUNTIME_IMAGE_CACHE_SIZE / 2; i++) {
					VectorMetatile vm = imageTiles.get(i);
					vm.runtimeImage = null;
				}
			}
			if (tileCache.size() >= MAX_RUNTIME_TILES_CACHE_SIZE) {
				Iterator<VectorMetatile> it = sortCache.iterator();
				while (tileCache.size() >= MAX_RUNTIME_TILES_CACHE_SIZE / 2 && it.hasNext()) {
					VectorMetatile metatTile = it.next();
					tileCache.remove(metatTile.key);
				}
			}
		}
	}

	private static String encode(String style, int x, int y, int z, int metasizeLog, int tileSizeLog) {
		// long l = 0 ;
		// int shift = 0;
		// l += ((long) tileSizeLog) << shift; // 0-2
		// shift += 2;
		// l += ((long) metasizeLog) << shift; // 0-4
		// shift += 3;
		// l += ((long) z) << shift; // 1-22
		// shift += 5;
		// l += ((long) x) << shift;
		// shift += z;
		// l += ((long) y) << shift;
		// shift += z;
		StringBuilder sb = new StringBuilder();
		return sb.append(style).append('-').append(metasizeLog).append('-').append(tileSizeLog).append('/').append(z)
				.append('/').append(x).append('/').append(y).toString();
	}

	public synchronized List<RouteSegmentResult> routing(String routeMode, LatLon start, LatLon end, List<LatLon> intermediates)
			throws IOException, InterruptedException {
		String[] props = routeMode.split("\\,");
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Map<String, String> paramsR = new LinkedHashMap<String, String>();
		boolean useNativeLib = DEFAULT_USE_ROUTING_NATIVE_LIB;
		for (String p : props) {
			if (p.startsWith("nativerouting")) {
				useNativeLib = true;
			} else if (p.startsWith("nonnativerouting")) {
				useNativeLib = false;
			} else if (p.contains("=")) {
				paramsR.put(p.split("=")[0], p.split("=")[1]);
			} else {
				paramsR.put(p, "true");
			}
		}
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(MEM_LIMIT, MEM_LIMIT);
		RoutingConfiguration config = RoutingConfiguration.getDefault().
		// addImpassableRoad(6859437l).
		// setDirectionPoints(directionPointsFile).
				build(props[0], /* RoutingConfiguration.DEFAULT_MEMORY_LIMIT */ memoryLimit, paramsR);
		PrecalculatedRouteDirection precalculatedRouteDirection = null;
		config.routeCalculationTime = System.currentTimeMillis();
		if (!validateAndInitConfig()) {
			return Collections.emptyList();
		}
		final RoutingContext ctx = router.buildRoutingContext(config, useNativeLib ? nativelib : null, 
				getObfReaders(points(intermediates, start, end)), RouteCalculationMode.COMPLEX);
		ctx.leftSideNavigation = false;
		// ctx.previouslyCalculatedRoute = previousRoute;
		// LOGGER.info("Use " + config.routerName + " mode for routing");
		// GpxRouteApproximation gctx = new GpxRouteApproximation(ctx);
		// List<GpxPoint> gpxPoints = router.generateGpxPoints(gctx, new LocationsHolder(intermediates));
		List<RouteSegmentResult> route = // gpx ? getGpxAproximation(router, gctx, gpxPoints) :
				router.searchRoute(ctx, start, end, intermediates, precalculatedRouteDirection);
		return route;
	}

	private QuadRect points(List<LatLon> intermediates, LatLon start, LatLon end) {
		int y = MapUtils.get31TileNumberY(start.getLatitude());
		int x = MapUtils.get31TileNumberX(start.getLongitude());
		QuadRect upd = new QuadRect(x, y, x, y);
		addPnt(end, upd);
		if(intermediates != null) {
			intermediates.forEach(item -> addPnt(item, upd));
		}
		return upd;
	}

	private void addPnt(LatLon pnt, QuadRect upd) {
		int y = MapUtils.get31TileNumberY(pnt.getLatitude());
		int x = MapUtils.get31TileNumberX(pnt.getLongitude());
		if (upd.left > x) {
			upd.left = x;
		}
		if (upd.right < x) {
			upd.right = x;
		}
		if (upd.bottom < y) {
			upd.bottom = y;
		}
		if (upd.top > y) {
			upd.top = y;
		}
	}

	public synchronized BinaryMapIndexReader[] getObfReaders(QuadRect quadRect) throws IOException {
		List<BinaryMapIndexReader> files = new ArrayList<>();
		if (obfRoutingFiles.isEmpty()) {
			initObfReaders();
		}
		for (BinaryMapIndexReaderReference ref : obfRoutingFiles.values()) {
			boolean intersects = false;
			mainLoop: for (RoutingPart rp : ref.fileIndex.getRoutingIndexList()) {
				for (RoutingSubregion s : rp.getSubregionsList()) {
					intersects = quadRect.left <= s.getRight() && quadRect.right >= s.getLeft()
							&& quadRect.top <= s.getBottom() && quadRect.bottom >= s.getTop();
					if (intersects) {
						if (ref.reader == null) {
							long val = System.currentTimeMillis();
							RandomAccessFile raf = new RandomAccessFile(ref.file, "r"); //$NON-NLS-1$ //$NON-NLS-2$
							ref.reader = CachedOsmandIndexes.initReaderFromFileIndex(ref.fileIndex, raf, ref.file);
							LOGGER.info("Initializing routing file " + ref.file.getName() + " " + (System.currentTimeMillis() - val) + " ms"); 
						}
						files.add(ref.reader);
						break mainLoop;
					}
				}
			}
		};
		return files.toArray(new BinaryMapIndexReader[files.size()]);
	}
	
	public synchronized void initObfReaders() throws IOException {
		File mapsFolder = new File(config.obfLocation);
		CachedOsmandIndexes cache = null;
		File cacheFile = new File(mapsFolder, CachedOsmandIndexes.INDEXES_DEFAULT_FILENAME);
		if (mapsFolder.exists()) {
			cache = new CachedOsmandIndexes();
			if (cacheFile.exists()) {
				cache.readFromFile(cacheFile, 2);
			}
			for (File obf : Algorithms.getSortedFilesVersions(mapsFolder)) {
				if (obf.getName().endsWith(".obf")) {
					BinaryMapIndexReaderReference ref = obfRoutingFiles.get(obf.getAbsolutePath());
					if (ref == null) {
						ref = new BinaryMapIndexReaderReference();
						ref.file = obf;
						ref.fileIndex = cache.getFileIndex(obf, true);
						obfRoutingFiles.put(obf.getAbsolutePath(), ref);
					}
				}
			}
			cache.writeToFile(cacheFile);
		}
		
	}
}
