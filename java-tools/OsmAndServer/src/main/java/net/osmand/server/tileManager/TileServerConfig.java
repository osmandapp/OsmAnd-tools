package net.osmand.server.tileManager;

import net.osmand.NativeJavaRendering;
import net.osmand.render.RenderingRuleProperty;
import net.osmand.render.RenderingRulesStorage;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Configuration
@ConfigurationProperties("tile-server")
public class TileServerConfig {

	@Value("${tile-server.obf.location}")
	public String obfLocation;

	@Value("${tile-server.obf.ziplocation}")
	public String obfZipLocation;

	@Value("${tile-server.cache.location}")
	public String cacheLocation;

	@Value("${tile-server.cache.heightmap-location}")
	public String heightmapLocation;

	@Value("${tile-server.cache.max-zoom}")
	int maxZoomCache = 16;

	@Value("${tile-server.metatile-size}")
	int metatileSize;

	public String initErrorMessage;

	protected static final Log LOGGER = LogFactory.getLog(TileServerConfig.class);

	public final Map<String, VectorStyle> style = new TreeMap<>();

	public void setStyle(Map<String, String> style) {
		for (Map.Entry<String, String> e : style.entrySet()) {
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
			try {
				vectorStyle.storage = NativeJavaRendering.parseStorage(vectorStyle.name + ".render.xml");
				for (RenderingRuleProperty p : vectorStyle.storage.PROPS.getPoperties()) {
					if (!Algorithms.isEmpty(p.getName()) && !Algorithms.isEmpty(p.getCategory())
							&& !"ui_hidden".equals(p.getCategory())) {
						vectorStyle.properties.add(p);
					}
				}
			} catch (Exception e1) {
				LOGGER.error(String.format("Error init rendering style %s: %s", vectorStyle.name + ".render.xml",
						e1.getMessage()), e1);
			}
			this.style.put(vectorStyle.key, vectorStyle);
		}
	}

	public String createTileId(String style, int x, int y, int z, int metaSizeLog, int tileSizeLog) {
		int left;
		int top;
		final int shiftZoom = 31 - z;
		if (metaSizeLog != -1) {
			left = ((x >> metaSizeLog) << metaSizeLog) << shiftZoom;
			if (left < 0) {
				left = 0;
			}
			top = ((y >> metaSizeLog) << metaSizeLog) << shiftZoom;
			if (top < 0) {
				top = 0;
			}
		} else {
			left = x << shiftZoom;
			top = y << shiftZoom;
		}

		if (tileSizeLog != -1 && metaSizeLog != -1) {
			return style + '-' + metaSizeLog + '-' + tileSizeLog + '/' + z + '/' + (left >> (31 - z)) + '/' + (top >> shiftZoom);
		}
		return style + '-' + z + '-' + (left >> shiftZoom) + '-' + (top >> shiftZoom);
	}

	public TileServerConfig getConfig() {
		return this;
	}

	public VectorStyle getStyle(String style) {
		return style != null ? this.style.get(style) : null;
	}

	public static class VectorStyle {
		public transient RenderingRulesStorage storage;
		public List<RenderingRuleProperty> properties = new ArrayList<>();
		public String key;
		public String name;
		public int maxZoomCache;
		public int tileSizeLog;
		public int metaTileSizeLog;
	}
}
