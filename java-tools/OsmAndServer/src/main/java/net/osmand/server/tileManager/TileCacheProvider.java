package net.osmand.server.tileManager;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

public interface TileCacheProvider {

	int MAX_FILES_PER_FOLDER = 4096;

	default File getCacheFile(String cacheLocation, String ext, int z, int x, int y,
	                          int metaSizeLog, int tileSizeLog, String styleKey, String interactiveKey, int maxZoom) {
		if (z > maxZoom || cacheLocation == null || cacheLocation.isEmpty()) {
			return null;
		}

		int cacheX = x >> metaSizeLog;
		int cacheY = y >> metaSizeLog;
		StringBuilder loc = new StringBuilder();

		loc.append(interactiveKey != null ? interactiveKey : styleKey)
				.append("/").append(z);

		if (metaSizeLog == -1 && tileSizeLog == -1) {
			int folderX = x / MAX_FILES_PER_FOLDER;
			int folderY = y / MAX_FILES_PER_FOLDER;
			loc.append("/").append(folderX)
					.append("/").append(folderY)
					.append("/").append(x)
					.append("-").append(y)
					.append(ext);
			return new File(cacheLocation, loc.toString());
		}

		while (cacheX >= MAX_FILES_PER_FOLDER) {
			int nx = cacheX % MAX_FILES_PER_FOLDER;
			loc.append("/").append(nx);
			cacheX = (cacheX - nx) / MAX_FILES_PER_FOLDER;
		}
		loc.append("/").append(cacheX);

		while (cacheY >= MAX_FILES_PER_FOLDER) {
			int ny = cacheY % MAX_FILES_PER_FOLDER;
			loc.append("/").append(ny);
			cacheY = (cacheY - ny) / MAX_FILES_PER_FOLDER;
		}
		loc.append("/").append(cacheY);

		loc.append("-").append(metaSizeLog)
				.append("-").append(tileSizeLog)
				.append(ext);

		return new File(cacheLocation, loc.toString());
	}

	void saveImageToCache(Object tile, File cacheFile) throws IOException;

	String getTileId();

	BufferedImage getImg();

	void setImg(BufferedImage img);
}
