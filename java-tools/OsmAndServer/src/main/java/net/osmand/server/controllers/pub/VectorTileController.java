package net.osmand.server.controllers.pub;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import javax.imageio.ImageIO;

import com.google.gson.JsonObject;
import net.osmand.server.tileManager.TileMemoryCache;
import net.osmand.server.tileManager.TileServerConfig;
import net.osmand.server.tileManager.VectorMetatile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.Gson;

import net.osmand.render.RenderingRuleProperty;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.tileManager.TileServerConfig.VectorStyle;
import net.osmand.util.Algorithms;

@Controller
@RequestMapping("/tile")
public class VectorTileController {

	protected static final Log LOGGER = LogFactory.getLog(VectorTileController.class);

	@Autowired
	OsmAndMapsService osmAndMapsService;

	@Autowired
	TileServerConfig config;

	private final TileMemoryCache<VectorMetatile> tileMemoryCache = new TileMemoryCache<>();

	Gson gson = new Gson();

	private ResponseEntity<?> errorConfig(String msg) {
		return ResponseEntity.badRequest()
				.body(msg);
	}

	@RequestMapping(path = "/styles", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<?> getStyles() {
		Map<String, VectorStyle> snapshot;
		synchronized (config.style) {
			for (VectorStyle vectorStyle : config.style.values()) {
				vectorStyle.properties.clear();
				for (RenderingRuleProperty p : vectorStyle.storage.PROPS.getPoperties()) {
					if (!Algorithms.isEmpty(p.getName()) && !Algorithms.isEmpty(p.getCategory())
							&& !"ui_hidden".equals(p.getCategory())) {
						p.setCategory(Algorithms.capitalizeFirstLetter(p.getCategory().replace('_', ' ')));
						vectorStyle.properties.add(p);
					}
				}
			}
			snapshot = new TreeMap<>(config.style);
		}
		return ResponseEntity.ok(gson.toJson(snapshot));
	}

	@RequestMapping(path = "/{style}/{z}/{x}/{y}.png", produces = MediaType.IMAGE_PNG_VALUE)
	public ResponseEntity<?> getTile(@PathVariable String style, @PathVariable int z, @PathVariable int x, @PathVariable int y)
			throws IOException, XmlPullParserException, SAXException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return errorConfig("Tile service is not initialized: " + (config == null ? "" : config.initErrorMessage));
		}
		if (osmAndMapsService.validateNativeLib() != null) {
			return errorConfig("Tile service is not initialized: " + osmAndMapsService.validateNativeLib());
		}

		String interactiveKey = osmAndMapsService.getInteractiveKeyMap(style);
		String currentStyle = osmAndMapsService.getMapStyle(style, interactiveKey);
		VectorStyle vectorStyle = config.getStyle(currentStyle);

		if (vectorStyle == null) {
			return ResponseEntity.badRequest().body("Rendering style is undefined: " + currentStyle);
		}

		tileMemoryCache.conditionalCleanupCache();
		VectorMetatile tile = getMetaTile(vectorStyle, z, x, y, interactiveKey);
		// for local debug :
		//BufferedImage img = null;
		BufferedImage img = tile.getCacheRuntimeImage();
		tile.touch();
		if (img == null) {
			ResponseEntity<String> err = osmAndMapsService.renderMetaTile(tile, tileMemoryCache);
			img = tile.runtimeImage;
			if (err != null) {
				return err;
			} else if (img == null) {
				return ResponseEntity.badRequest().body("Unexpected error during rendering");
			}
		}
		BufferedImage subimage = tile.readSubImage(img, x, y);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(subimage, "png", baos);
		return ResponseEntity.ok()
				.header("Cache-Control", "public, max-age=2592000")
				.body(new ByteArrayResource(baos.toByteArray()));
	}

	@GetMapping(path = "/info/{style}/{z}/{x}/{y}.json", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getTileInfo(@PathVariable String style, @PathVariable int z, @PathVariable int x, @PathVariable int y) throws IOException {

		String interactiveKey = osmAndMapsService.getInteractiveKeyMap(style);
		String currentStyle = osmAndMapsService.getMapStyle(style, interactiveKey);
		VectorStyle vectorStyle = config.getStyle(currentStyle);

		if (vectorStyle == null) {
			return ResponseEntity.badRequest().body("Rendering style is undefined: " + currentStyle);
		}
		VectorMetatile tile = getMetaTile(vectorStyle, z, x, y, interactiveKey);
		JsonObject tileInfo = osmAndMapsService.getTileInfo(tile.getCacheRuntimeInfo(), x, y, z);
		tile.touch();
		if (tileInfo == null) {
			return ResponseEntity.badRequest().body("Unexpected error during rendering");
		}
		tileMemoryCache.conditionalCleanupCache();

		return ResponseEntity.ok()
				.header("Cache-Control", "public, max-age=2592000")
				.body(String.valueOf(tileInfo));
	}

	public VectorMetatile getMetaTile(VectorStyle vectorStyle, int z, int x, int y, String interactiveKey) {
		int metaSizeLog = Math.min(vectorStyle.metaTileSizeLog, z - 1);
		String key = interactiveKey != null ? interactiveKey : vectorStyle.key;
		String tileId = config.createTileId(key, x, y, z, metaSizeLog, vectorStyle.tileSizeLog);
		VectorMetatile tile = tileMemoryCache.get(tileId);
		if (tile == null) {
			tile = new VectorMetatile(config, tileId, vectorStyle, z, x, y, metaSizeLog, vectorStyle.tileSizeLog, interactiveKey);
			tileMemoryCache.put(tile.key, tile);
		}
		return tile;
	}
}
