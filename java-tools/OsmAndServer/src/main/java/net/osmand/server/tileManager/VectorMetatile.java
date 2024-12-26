package net.osmand.server.tileManager;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.osmand.NativeJavaRendering;
import net.osmand.server.tileManager.TileServerConfig.VectorStyle;
import org.springframework.http.ResponseEntity;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class VectorMetatile implements TileCacheProvider, Comparable<VectorMetatile> {

	public BufferedImage runtimeImage;
	public long lastAccess;
	public final String key;
	public final int z;
	public final int left;
	public final int top;
	public final int metaSizeLog;
	public final int tileSizeLog;
	public final VectorStyle style;
	private final TileServerConfig cfg;
	private JsonObject info;
	private final String interactiveKey;

	public VectorMetatile(TileServerConfig cfg, String tileId, VectorStyle style, int z, int x, int y,
	                      int metaSizeLog, int tileSizeLog, String interactiveKey) {
		this.cfg = cfg;
		this.style = style;
		this.metaSizeLog = metaSizeLog;
		this.tileSizeLog = tileSizeLog;
		this.key = tileId;
		this.left = getLeft(x, z, metaSizeLog);
		this.top = getTop(y, z, metaSizeLog);
		this.z = z;
		this.interactiveKey = interactiveKey;
		touch();
	}

	public int getLeft(int x, int z, int metaSizeLog) {
		int left = ((x >> metaSizeLog) << metaSizeLog) << (31 - z);
		if (left < 0) {
			left = 0;
		}
		return left;
	}

	public int getTop(int y, int z, int metaSizeLog) {
		int top = ((y >> metaSizeLog) << metaSizeLog) << (31 - z);
		if (top < 0) {
			top = 0;
		}
		return top;
	}

	public String getInteractiveKey() {
		return interactiveKey;
	}

	public void setInfo(JsonObject info) {
		this.info = info;
	}

	public JsonObject getInfo() {
		return info;
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
		File cf = getCacheFile(".png");
		if (cf != null && cf.exists()) {
			runtimeImage = ImageIO.read(cf);
			return runtimeImage;
		}
		return null;
	}

	public JsonObject getCacheRuntimeInfo() throws IOException {
		JsonObject tileInfo = getInfo();
		if (tileInfo != null) {
			return tileInfo;
		}
		File cf = getCacheFile(".json.gz");
		if (cf != null && cf.exists()) {
			try (GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(cf));
			     Reader reader = new InputStreamReader(gzip, StandardCharsets.UTF_8)) {
				JsonParser parser = new JsonParser();
				tileInfo = parser.parse(reader).getAsJsonObject();
				return tileInfo;
			}
		}
		return null;
	}

	public File getCacheFile(String ext) {
		return TileCacheProvider.super.getCacheFile(
				cfg.cacheLocation, ext, z,
				left >> (31 - z), top >> (31 - z),
				metaSizeLog, tileSizeLog,
				style.key, interactiveKey, 16
		);
	}

	public void buildCacheFileInfo(VectorMetatile tile) throws IOException {
		File cacheFileInfo = tile.getCacheFile(".json.gz");
		if (cacheFileInfo != null) {
			cacheFileInfo.getParentFile().mkdirs();
			if (cacheFileInfo.getParentFile().exists()) {
				JsonObject info = tile.getInfo();
				if (info != null) {
					try (GZIPOutputStream gzip = new GZIPOutputStream(new FileOutputStream(cacheFileInfo));
					     Writer writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)) {
						writer.write(info.toString());
					}
				}
			}
		}
	}

	public ResponseEntity<String> renderMetaTile(NativeJavaRendering nativelib, TileMemoryCache<VectorMetatile> tileCache)
			throws IOException, XmlPullParserException, SAXException {

		int ZOOM_EN_PREFERRED_LANG = 6;

		// don't synchronize this to not block routing
		synchronized (this.cfg) {
			// for local debug :
			// VectorMetatile rendered = null;
			VectorMetatile rendered = tileCache.get(this.key);
			if (rendered != null && rendered.runtimeImage != null) {
				this.runtimeImage = rendered.runtimeImage;
				this.setInfo(rendered.getInfo());
				return null;
			}
			int imgTileSize = (256 << this.tileSizeLog) << Math.min(this.z, this.metaSizeLog);
			int tilesize = (1 << Math.min(31 - this.z + this.metaSizeLog, 31));
			if (tilesize <= 0) {
				tilesize = Integer.MAX_VALUE;
			}
			int right = this.left + tilesize;
			if (right <= 0) {
				right = Integer.MAX_VALUE;
			}
			int bottom = this.top + tilesize;
			if (bottom <= 0) {
				bottom = Integer.MAX_VALUE;
			}
			long now = System.currentTimeMillis();
			String props = String.format("density=%d,textScale=%d", 1 << this.tileSizeLog, 1 << this.tileSizeLog);

			if (this.z < ZOOM_EN_PREFERRED_LANG) {
				props += ",lang=en";
			}
			if (nativelib == null) {
				return null;
			}
			if (!this.style.name.equalsIgnoreCase(nativelib.getRenderingRuleStorage().getName())) {
				nativelib.loadRuleStorage(this.style.name + ".render.xml", props);
			} else {
				nativelib.setRenderingProps(props);
			}
			NativeJavaRendering.RenderingImageContext ctx = new NativeJavaRendering.RenderingImageContext(this.left, right, this.top, bottom, this.z);

			if (this.getInteractiveKey() != null) {
				ctx.saveTextTile = true;
			}

			if (ctx.width > 8192) {
				return ResponseEntity.badRequest().body("Metatile exceeds 8192x8192 size");

			}
			if (imgTileSize != ctx.width << this.tileSizeLog || imgTileSize != ctx.height << this.tileSizeLog) {
				return ResponseEntity.badRequest().body(String.format("Metatile has wrong size (%d != %d)", imgTileSize,
						ctx.width << this.tileSizeLog));
			}

			NativeJavaRendering.RenderingImageResult result = nativelib.renderImage(ctx);
			this.runtimeImage = result.getImage();
			if (this.runtimeImage != null) {
				this.setInfo(result.getGenerationResult().getInfo());
				File cacheFile = this.getCacheFile(".png");
				if (cacheFile != null) {
					this.saveImageToCache(this, cacheFile);
				}
			}
			String msg = String.format("Rendered %d %d at %d (%s %s): %dx%d - %d ms", this.left, this.top, this.z,
					this.style.name, props, ctx.width, ctx.height, (int) (System.currentTimeMillis() - now));
			System.out.println(msg);
			// LOGGER.debug();
			return null;
		}
	}

	@Override
	public void saveImageToCache(Object tile, File cacheFile) throws IOException {
		if (tile instanceof VectorMetatile vm) {
			if (vm.runtimeImage != null) {
				cacheFile.getParentFile().mkdirs();
				if (cacheFile.getParentFile().exists()) {
					ImageIO.write(vm.runtimeImage, "png", cacheFile);
					vm.buildCacheFileInfo(vm);
				}
			}
		}
	}

	@Override
	public String getTileId() {
		return key;
	}

	@Override
	public BufferedImage getImg() {
		return runtimeImage;
	}

	@Override
	public void setImg(BufferedImage img) {
		runtimeImage = img;
	}
}
