package net.osmand.server.controllers.pub;

import java.awt.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.*;

import net.osmand.server.tileManager.TileMemoryCache;
import net.osmand.server.tileManager.TileServerConfig;
import net.osmand.server.tileManager.MapboxVectorTile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import net.osmand.server.api.services.OsmAndMapsService;

@Controller
@RequestMapping("/vector")
public class MapboxVectorTileController {

	protected static final Log LOGGER = LogFactory.getLog(MapboxVectorTileController .class);

	private static final boolean DEBUG = true;

	private static final long CLEANUP_CACHE_AFTER_ZOOM_7 = TimeUnit.DAYS.toMillis(10);
	private static final long CLEANUP_CACHE_BEFORE_ZOOM_7 = TimeUnit.DAYS.toMillis(400);
	private static final long CLEANUP_INTERVAL_MILLIS = 12 * 60 * 60 * 1000L; // 12 hours

	@Autowired
	OsmAndMapsService osmAndMapsService;

	@Autowired
	TileServerConfig config;

	private final TileMemoryCache<MapboxVectorTile> tileMemoryCache = new TileMemoryCache<>();

	private ResponseEntity<?> errorConfig(String msg) {
		return ResponseEntity.badRequest()
				.body(msg);
	}

	@RequestMapping(path = "/{z}/{x}/{y}.mvt", produces = MediaType.APPLICATION_PROTOBUF_VALUE)
	public ResponseEntity<?> getTile(@PathVariable int z, @PathVariable int x, @PathVariable int y)
			throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return errorConfig("Tile service is not initialized");
		}
		if (osmAndMapsService.validateNativeLib() != null) {
			return errorConfig("Tile service is not initialized: " + osmAndMapsService.validateNativeLib());
		}

		String tileId = config.createTileId("vector", x, y, z, -1, -1);
        MapboxVectorTile tile = tileMemoryCache.getTile(tileId, k -> new MapboxVectorTile(config, x, y, z));
        // for testing
        //MapboxVectorTile tile = new MapboxVectorTile(config, x, y, z);
        tileMemoryCache.cleanupCache();
		byte[] data = tile.getCacheRuntimeTile();
        tile.touch();

		if (data == null) {
            data = getTileFromService(tile);
		}
		if (data == null) {
			return ResponseEntity.badRequest().body("Failed to get tile");
		}
		return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_ENCODING, "gzip")
				.header(HttpHeaders.CACHE_CONTROL, "public, max-age=2592000")
				.body(new ByteArrayResource(data));
	}

	@Scheduled(fixedRate = CLEANUP_INTERVAL_MILLIS)
	public synchronized void cleanUpCache() {
		File cacheDir = new File(config.mvtsLocation);
		if (!cacheDir.exists() || !cacheDir.isDirectory()) {
			return; // Nothing to clean up
		}
		cleanUpDirectory(cacheDir);
	}

	private void cleanUpDirectory(File dir) {
		long now = System.currentTimeMillis();
		File[] files = dir.listFiles();

		if (files != null) {
			for (File file : files) {
				if (file.isDirectory()) {
					cleanUpDirectory(file);
				} else if (file.isFile() && isValidMapboxVectorTileFile(file)) {
					int zoom = parseZoomFromFileName(file.getName());
					if (zoom == -1) {
						continue;
					}
					if ((zoom <= 7 && now - file.lastModified() >= CLEANUP_CACHE_BEFORE_ZOOM_7) ||
							(zoom > 7 && now - file.lastModified() >= CLEANUP_CACHE_AFTER_ZOOM_7)) {
						try {
							Path filePath = file.toPath();
							Files.delete(filePath);
						} catch (IOException e) {
							LOGGER.warn("Failed to delete file: " + file.getAbsolutePath(), e);
						}

					}
				}
			}
		}
	}

	private int parseZoomFromFileName(String fileName) {
		String[] parts = fileName.split(File.separator);
		if (parts.length < 2) {
			return -1;
		}
		for (int i = 0; i < parts.length; i++) {
			if (parts[i].equals("mvts") && i + 2 < parts.length) {
				return Integer.parseInt(parts[i + 2]);
			}
		}
		return -1;
	}

	private boolean isValidMapboxVectorTileFile(File file) {
		String name = file.getName();
		return name.endsWith(".mvt") && name.contains("mvts");
	}

	/* TODO */ synchronized /* TESTING */ private byte[] getTileFromService(MapboxVectorTile tile) throws IOException {
		long startTime = DEBUG ? System.currentTimeMillis() : 0;
		ExecutorService executor = Executors.newSingleThreadExecutor();
		if (DEBUG) {
			LOGGER.info("Start rendering tile [" + tile.getTileId() + "] on thread: " + Thread.currentThread().getId());
		}
        Future<byte[]> future = executor.submit(() -> osmAndMapsService.renderMapboxVectorTile(tile.z, tile.x, tile.y));
        byte[] data;
		try {
            data = future.get(30, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			LOGGER.warn("Rendering tile [" + tile.getTileId() + "] timed out on thread: " + Thread.currentThread().getId());
			future.cancel(true);
            data = null;
		} catch (InterruptedException e) {
			LOGGER.error("Rendering tile [" + tile.getTileId() + "] was interrupted on thread: " + Thread.currentThread().getId(), e);
			Thread.currentThread().interrupt();
            data = null;
		} catch (ExecutionException e) {
			LOGGER.error("Error during rendering tile [" + tile.getTileId() + "] on thread: " + Thread.currentThread().getId(), e);
            data = null;
		} finally {
			executor.shutdown();
		}
		if (DEBUG) {
			long endTime = System.currentTimeMillis();
			long duration = endTime - startTime;
			LOGGER.info("Finish rendering tile [" + tile.getTileId() + "] on thread: " + Thread.currentThread().getId() +
					" (duration: " + duration + " ms)");
		}

		if (data == null) {
			return null;
		}

		return saveToCache(tile, data);
	}

	private byte[] saveToCache(MapboxVectorTile tile, byte[] data) throws IOException {
		File cacheFile = tile.getCacheFile(".mvt");
		if (cacheFile == null) {
			return null;
		}
		tile.setRuntimeTile(data);
		tile.saveTileToCache(tile, cacheFile);
		return data;
	}
}
