package net.osmand.server.tileManager;

import net.osmand.server.controllers.pub.MapboxVectorTileController;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class MapboxVectorTile implements TileCacheProvider, Comparable<MapboxVectorTile> {
	public static final int MIN_SHIFT = -3;
	public static final int MAX_SHIFT = 3;
	public static final int MAX_ZOOM = 22;
	private static final int MAX_MVT_TILES_ZOOM = 15;
	private static final int MVT_TILE_INCREASE_DETAILS_BEFORE_DETAILED_ZOOM = 9;
	private byte[] runtimeTile;
	private long lastAccess;
	private String tileId;
	private final TileServerConfig cfg;
	public final int x;
	public final int y;
	public final int z;
	public final int shift;
	private static final long THREE_HOURS_IN_MILLIS = 3 * 60 * 60 * 1000L;

	public MapboxVectorTile(TileServerConfig cfg, int x, int y, int z, int shift) {
		this.cfg = cfg;
		this.x = x;
		this.y = y;
		this.z = z;
		this.shift = normalizeShift(z, shift);
		setTileId();
		touch();
	}

	public synchronized void setRuntimeTile(byte[] runtimeTile) {
		this.runtimeTile = runtimeTile;
	}

	public synchronized void touch() {
		lastAccess = System.currentTimeMillis();
		File cacheFile = getCacheFile(".mvt");
		if (cacheFile != null && cacheFile.exists()) {
			long lastModifiedTime = cacheFile.lastModified();
			if (lastAccess > lastModifiedTime + THREE_HOURS_IN_MILLIS) {
				boolean success = cacheFile.setLastModified(lastAccess);
				if (!success) {
					lastAccess = lastModifiedTime;
				}
			}
		}
	}

	public static int normalizeShift(int mapZoom, int shift) {
		// Keep the data zoom calculation in sync with getMapboxVectorTileData in core-legacy.
		int baseDataZoom = mapZoom < MVT_TILE_INCREASE_DETAILS_BEFORE_DETAILED_ZOOM - 1 ? mapZoom + 1 : mapZoom;
		int dataZoom = Math.max(1, Math.min(MAX_MVT_TILES_ZOOM, baseDataZoom + shift));
		// Reuse the unshifted cache when clamping makes the shift ineffective.
		return dataZoom == Math.min(MAX_MVT_TILES_ZOOM, baseDataZoom) ? 0 : dataZoom - baseDataZoom;
	}

	// vector, vector-shift-1, vector-shift+1, etc.
	public static String getCacheNamespace(int shift) {
		return shift == 0 ? "vector" : "vector-shift" + (shift > 0 ? "+" : "") + shift;
	}

	public void setTileId() {
		this.tileId = this.cfg.createTileId(getCacheNamespace(shift), x, y, z, -1, -1);
	}

	public synchronized byte[] getCacheRuntimeTile() throws IOException {
        byte[] tile = runtimeTile;
		if (tile != null) {
			return tile;
		}
		File cf = getCacheFile(".mvt");
		if (cf != null && cf.exists() && cf.length() > 0) {
			runtimeTile = Files.readAllBytes(cf.toPath());
    		return runtimeTile;
		}
		return null;
	}

	public File getCacheFile(String ext) {
		return TileCacheProvider.super.getCacheFile(
				cfg.mvtsLocation, ext, z, x, y,
				-1, -1,
				getCacheNamespace(shift), null, MAX_ZOOM
		);
	}

	@Override
	public void saveTileToCache(Object tile, File cacheFile) throws IOException {
		if (tile instanceof MapboxVectorTile mvt) {
			if (mvt.runtimeTile != null) {
				cacheFile.getParentFile().mkdirs();
				if (cacheFile.getParentFile().exists()) {
                    Files.write(cacheFile.toPath(), mvt.runtimeTile);
				}
			}
		}
	}

	@Override
	public String getTileId() {
		return tileId;
	}

	@Override
	public Object getTile() {
		return runtimeTile;
	}

	@Override
	public void setTile(Object tile) {
		runtimeTile = (byte[]) tile;
	}

	@Override
	public int compareTo(@NotNull MapboxVectorTile o) {
		return Long.compare(lastAccess, o.lastAccess);
	}
}
