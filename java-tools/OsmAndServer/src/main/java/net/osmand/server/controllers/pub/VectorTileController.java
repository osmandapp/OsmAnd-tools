package net.osmand.server.controllers.pub;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.Gson;

import net.osmand.NativeJavaRendering;
import net.osmand.NativeJavaRendering.RenderingImageContext;
import net.osmand.util.Algorithms;

@Controller
@RequestMapping("/tile")
public class VectorTileController {
	
    private static final Log LOGGER = LogFactory.getLog(VectorTileController.class);

	private static final int MAX_RUNTIME_IMAGE_CACHE_SIZE = 40;
	
	private static final int MAX_RUNTIME_TILES_CACHE_SIZE = 10000;
	
	private static final int MAX_FILES_PER_FOLDER = 1 << 12; // 4096

	private static final int ZOOM_EN_PREFERRED_LANG = 6;
	
	AtomicInteger cacheTouch = new AtomicInteger(0);
	
	Gson gson = new Gson();
	
	@Autowired
	VectorTileServerConfig config; 
	

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
		
		Map<String, VectorMetatile> tileCache = new ConcurrentHashMap<>();
		
		NativeJavaRendering nativelib;
		
		String initErrorMessage;
		
		File tempDir;
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
		
		public VectorMetatile(VectorTileServerConfig cfg, String tileId, VectorStyle style, 
				int z, int left, int top,  int metaSizeLog, int tileSizeLog) {
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
	
	private ResponseEntity<?> errorConfig() {
		return ResponseEntity.badRequest()
				.body("Tile service is not initialized: " + (config == null ? "" : config.initErrorMessage));
	}
	
	public boolean validateConfig() throws IOException {
		if (config.nativelib == null && config.initErrorMessage == null) {
			synchronized (this) {
				if (!(config.nativelib == null && config.initErrorMessage == null)) {
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
				config.tempDir = Files.createTempDirectory("osmandserver").toFile();
				LOGGER.info("Init temp rendering directory for libs / fonts: " + config.tempDir.getAbsolutePath());
				config.tempDir.deleteOnExit();
				ClassLoader cl = NativeJavaRendering.class.getClassLoader(); 
				ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(cl);
				Resource[] resources = resolver.getResources("classpath:/map/fonts/*.ttf") ;
				File fontsFolder = new File(config.tempDir, "fonts");
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
				config.nativelib = NativeJavaRendering.getDefault(null, config.obfLocation, fontsFolder.getAbsolutePath());
				if (config.nativelib == null) {
					config.initErrorMessage = "Tile rendering engine is not initialized";
				}
			}
		}
		return config.initErrorMessage == null;
	}
	
	private static String encode(String style, int x, int y, int z, int metasizeLog, int tileSizeLog) {
//		long l = 0 ;
//		int shift = 0;
//		l += ((long) tileSizeLog) << shift; // 0-2
//		shift += 2;
//		l += ((long) metasizeLog) << shift; // 0-4
//		shift += 3;
//		l += ((long) z) << shift; // 1-22
//		shift += 5;
//		l += ((long) x) << shift;
//		shift += z;
//		l += ((long) y) << shift;
//		shift += z;
		StringBuilder sb = new StringBuilder();
		return sb.append(style).append('-').append(metasizeLog).append('-').append(tileSizeLog).
				append('/').append(z).append('/').append(x).append('/').append(y).toString();
	}
	
	private synchronized ResponseEntity<String> renderMetaTile(VectorMetatile tile) throws IOException, XmlPullParserException, SAXException {
		VectorMetatile rendered = config.tileCache.get(tile.key);
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
		if (!tile.style.name.equalsIgnoreCase(config.nativelib.getRenderingRuleStorage().getName())) {
			config.nativelib.loadRuleStorage(tile.style.name + ".render.xml", props);
		} else {
			config.nativelib.setRenderingProps(props);
		}
		RenderingImageContext ctx = new RenderingImageContext(tile.left, right, tile.top, bottom, tile.z);
		if (ctx.width > 8192) {
			return ResponseEntity.badRequest().body("Metatile exceeds 8192x8192 size");

		}
		if (imgTileSize != ctx.width << tile.tileSizeLog || imgTileSize != ctx.height << tile.tileSizeLog) {
			return ResponseEntity.badRequest()
					.body(String.format("Metatile has wrong size (%d != %d)", imgTileSize, ctx.width << tile.tileSizeLog));
		}
		tile.runtimeImage = config.nativelib.renderImage(ctx);
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
				tile.style.name, props,
				ctx.width, ctx.height,
				(int) (System.currentTimeMillis() - now));
		System.out.println(msg);
//		LOGGER.debug();
		return null;
	}

	private void cleanupCache() {
		int version = cacheTouch.incrementAndGet();
		// so with atomic only 1 thread will get % X == 0
		if (version % MAX_RUNTIME_IMAGE_CACHE_SIZE == 0 && version > 0) {
			cacheTouch.set(0);
			TreeSet<VectorMetatile> sortCache = new TreeSet<>(config.tileCache.values());
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
			if (config.tileCache.size() >= MAX_RUNTIME_TILES_CACHE_SIZE) {
				Iterator<VectorMetatile> it = sortCache.iterator();
				while (config.tileCache.size() >= MAX_RUNTIME_TILES_CACHE_SIZE / 2 && it.hasNext()) {
					VectorMetatile metatTile = it.next();
					config.tileCache.remove(metatTile.key);
				}
			}
		}
	}

	@RequestMapping(path = "/styles", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<?> getStyles() {
		return ResponseEntity.ok(gson.toJson(config.style));
	}

	@RequestMapping(path = "/{style}/{z}/{x}/{y}.png", produces = MediaType.IMAGE_PNG_VALUE)
	public ResponseEntity<?> getTile(@PathVariable String style, @PathVariable int z, @PathVariable int x, @PathVariable int y)
			throws IOException, XmlPullParserException, SAXException {
		if (!validateConfig()) {
			return errorConfig();
		}
		VectorStyle vectorStyle = config.style.get(style);
		if (vectorStyle == null) {
			return ResponseEntity.badRequest().body("Rendering style is undefined: " + style);
		}
		int metaSizeLog = Math.min(vectorStyle.metaTileSizeLog, z - 1);
		int left = ((x >> metaSizeLog) << metaSizeLog) << (31 - z);
		if (left < 0) {
			left = 0;
		}
		int top = ((y >> metaSizeLog) << metaSizeLog) << (31 - z);
		if (top < 0) {
			top = 0;
		}
		String tileId = encode(style, left >> (31 - z), top >> (31 - z), z, metaSizeLog, vectorStyle.tileSizeLog);
		VectorMetatile tile = config.tileCache.get(tileId);
		if (tile == null) {
			tile = new VectorMetatile(config, tileId, vectorStyle, z, left, top, metaSizeLog, vectorStyle.tileSizeLog);
			config.tileCache.put(tile.key, tile);
		}
		BufferedImage img = tile.getCacheRuntimeImage();
		if (img == null) {
			ResponseEntity<String> err = renderMetaTile(tile);
			img = tile.runtimeImage;
			if (err != null) {
				return err;
			} else if (img == null) {
				return ResponseEntity.badRequest().body("Unexpected error during rendering");
			}
		}
		cleanupCache();
		tile.touch();
		BufferedImage subimage = tile.readSubImage(img, x, y);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(subimage, "png", baos);
		return ResponseEntity.ok(new ByteArrayResource(baos.toByteArray()));
	}

}
