package net.osmand.server.tileManager;

import net.osmand.server.controllers.pub.MapboxVectorTileController;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class MapboxVectorTile implements TileCacheProvider, Comparable<MapboxVectorTile> {
	private byte[] runtimeTile;
	private long lastAccess;
	private String tileId;
	private final TileServerConfig cfg;
	public final int x;
	public final int y;
	public final int z;
	private static final long THREE_HOURS_IN_MILLIS = 3 * 60 * 60 * 1000L;

	public MapboxVectorTile(TileServerConfig cfg, int x, int y, int z) {
		this.cfg = cfg;
		this.x = x;
		this.y = y;
		this.z = z;
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

	public void setTileId() {
		this.tileId = this.cfg.createTileId("vector", x, y, z, -1, -1);
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
				"vector", null, 22
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
