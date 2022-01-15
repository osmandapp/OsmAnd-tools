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
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

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

	@Value("${tile-server.obf.location}")
	String obfLocation;
	
	@Value("${tile-server.cache.location}")
	String cacheLocation;
	
	@Value("${tile-server.cache.max-zoom}")
	int maxZoomCache = 16;
	
	@Value("${tile-server.metatile-powsize}")
	int metatileSize;
	
	@Value("${tile-server.tile-size}")
	int singleTileSize = 256;
	
	AtomicInteger cacheTouch = new AtomicInteger(0);
	
	VectorTileServerConfig config; 
	
	public static class VectorTileServerConfig {
		NativeJavaRendering nativelib;
		Map<Long, VectorMetatile> tileCache = new ConcurrentHashMap<>();
		String error;
		File tempDir;
	}

	
	public class VectorMetatile implements Comparable<VectorMetatile> {
		
		public BufferedImage runtimeImage;
		public long lastAccess;
		public final long key;
		public final int z;
		public final int left;
		public final int top;
		
		public VectorMetatile(int left, int top, int z, long tileId) {
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

		public BufferedImage readSubImage(BufferedImage img, int x, int y, int tileSize) {
			int subl = x - ((x >> metatileSize) << metatileSize);
			int subt = y - ((y >> metatileSize) << metatileSize);
			return img.getSubimage(subl * tileSize, subt * tileSize, tileSize, tileSize);
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
			if (z > maxZoomCache || cacheLocation == null || cacheLocation.length() == 0) {
				return null;
			}
			int x = left >> (31 - z) >> metatileSize;
			int y = top >> (31 - z) >> metatileSize;
			StringBuilder loc = new StringBuilder();
			loc.append(z);
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
			loc.append("/").append(y).append(".png");
			return new File(cacheLocation, loc.toString());
		}
	}
	
	private ResponseEntity<?> errorConfig() {
		return ResponseEntity.badRequest()
				.body("Tile service is not initialized: " + (config == null ? "" : config.error));
	}
	
	public boolean validateConfig() throws IOException {
		if (config == null) {
			synchronized (this) {
				if (config != null) {
					return config.error == null;
				}
				config = new VectorTileServerConfig();
				if (obfLocation == null || obfLocation.isEmpty()) {
					config.error = "Files location is not specified";
				} else {
					File obfLocationF = new File(obfLocation);
					if (!obfLocationF.exists()) {
						config.error = "Files location is not specified";
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
				config.nativelib = NativeJavaRendering.getDefault(null, obfLocation, fontsFolder.getAbsolutePath());
				if (config.nativelib == null) {
					config.error = "Tile rendering engine is not initialized";
				}
			}
		}
		return config.error == null;
	}
	
	private static long encode(int x, int y, int z) {
		long l = 0 ;
		l += z ;
		l += (((long) x) << (z + 5)) ;
		l += (((long) y) << (z + z + 5)) ;
		return l;
	}
	
	private synchronized ResponseEntity<String> renderMetaTile(VectorMetatile tile) throws IOException {
		VectorMetatile rendered = config.tileCache.get(tile.key);
		if (rendered != null && rendered.runtimeImage != null) {
			tile.runtimeImage = rendered.runtimeImage;
			return null;
		}
		int tilesize = (1 << Math.min(31 - tile.z + metatileSize, 31));
		if(tilesize <= 0) {
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
		RenderingImageContext ctx = new RenderingImageContext(tile.left, right, tile.top, bottom, tile.z,
				// TODO doesn't work correctly
				(singleTileSize >> 9), 1);
		if (ctx.width > 8192) {
			return ResponseEntity.badRequest().body("Metatile exceeds 8192x8192 size");

		}
		if (singleTileSize << metatileSize != ctx.width || singleTileSize << metatileSize != ctx.height) {
			return ResponseEntity.badRequest()
					.body(String.format("Metatile has wrong size (%d != %d)", tilesize << metatileSize, ctx.width));
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
		LOGGER.debug(String.format("Rendered %d %d at %d: %dx%d - %d ms", tile.left, tile.top, tile.z, ctx.width, ctx.height,
				(int) (System.currentTimeMillis() - now)));
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

	@RequestMapping(path = "/{z}/{x}/{y}.png", produces = "image/png")
	public ResponseEntity<?> getTile(@PathVariable int z, @PathVariable int x, @PathVariable int y)
			throws IOException {
		if (!validateConfig()) {
			return errorConfig();
		}
		int left = ((x >> metatileSize) << metatileSize) << (31 - z);
		if (left < 0) {
			left = 0;
		}
		int top = ((y >> metatileSize) << metatileSize) << (31 - z);
		if (top < 0) {
			top = 0;
		}
		long tileId = encode(left >> (31 - z), top >> (31 - z), z);
		VectorMetatile tile = config.tileCache.get(tileId);
		if (tile == null) {
			tile = new VectorMetatile(left, top, z, tileId);
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
		BufferedImage subimage = tile.readSubImage(img, x, y, singleTileSize);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(subimage, "png", baos);
		return ResponseEntity.ok(new ByteArrayResource(baos.toByteArray()));
	}

}
