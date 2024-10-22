package net.osmand.server.tileManager;

import net.osmand.server.controllers.pub.GeotiffTileController;
import org.jetbrains.annotations.NotNull;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

public class GeotiffTile implements TileCacheProvider, Comparable<GeotiffTile> {
	private BufferedImage runtimeImage;
	private long lastAccess;
	private String tileId;
	private final TileServerConfig cfg;
	private final GeotiffTileController.TileType tileType;
	public final int x;
	public final int y;
	public final int z;
	private static final long THREE_HOURS_IN_MILLIS = 3 * 60 * 60 * 1000L;

	public GeotiffTile(TileServerConfig cfg, GeotiffTileController.TileType tileType, int x, int y, int z) {
		this.cfg = cfg;
		this.tileType = tileType;
		this.x = x;
		this.y = y;
		this.z = z;
		setTileId();
		touch();
	}

	public void setRuntimeImage(BufferedImage runtimeImage) {
		this.runtimeImage = runtimeImage;
	}

	public void touch() {
		lastAccess = System.currentTimeMillis();
		File cacheFile = getCacheFile(".png");
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

	public GeotiffTileController.TileType getTileType() {
		return tileType;
	}

	public void setTileId() {
		this.tileId = this.cfg.createTileId(tileType.getType(), x, y, z, -1, -1);
	}

	public BufferedImage getCacheRuntimeImage() throws IOException {
		BufferedImage img = runtimeImage;
		if (img != null) {
			return img;
		}
		File cf = getCacheFile(".png");
		if (cf != null && cf.exists()) {
			runtimeImage = ImageIO.read(cf);
			return runtimeImage;
		}
		return null;
	}

	public File getCacheFile(String ext) {
		return TileCacheProvider.super.getCacheFile(
				cfg.heightmapLocation, ext, z, x, y,
				-1, -1,
				tileType.getType(), null
		);
	}

	@Override
	public void saveImageToCache(Object tile, File cacheFile) throws IOException {
		if (tile instanceof GeotiffTile gt) {
			if (gt.runtimeImage != null) {
				cacheFile.getParentFile().mkdirs();
				if (cacheFile.getParentFile().exists()) {
					ImageIO.write(gt.runtimeImage, "png", cacheFile);
				}
			}
		}
	}

	@Override
	public String getTileId() {
		return tileId;
	}

	@Override
	public BufferedImage getImg() {
		return runtimeImage;
	}

	@Override
	public void setImg(BufferedImage img) {
		runtimeImage = img;
	}

	@Override
	public int compareTo(@NotNull GeotiffTile o) {
		return Long.compare(lastAccess, o.lastAccess);
	}
}
