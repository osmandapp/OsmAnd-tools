package net.osmand.server.controllers.pub;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import com.google.gson.JsonObject;
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
import net.osmand.server.api.services.OsmAndMapsService.VectorMetatile;
import net.osmand.server.api.services.OsmAndMapsService.VectorStyle;
import net.osmand.server.api.services.OsmAndMapsService.VectorTileServerConfig;
import net.osmand.util.Algorithms;

@Controller
@RequestMapping("/tile")
public class VectorTileController {
	
    protected static final Log LOGGER = LogFactory.getLog(VectorTileController.class);

	@Autowired
	OsmAndMapsService osmAndMapsService;
	
	Gson gson = new Gson();

	private ResponseEntity<?> errorConfig(String msg) {
		return ResponseEntity.badRequest()
				.body(msg);
	}
	@RequestMapping(path = "/styles", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<?> getStyles() {
		VectorTileServerConfig config = osmAndMapsService.getConfig();
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
		return ResponseEntity.ok(gson.toJson(config.style));
	}
	
	@RequestMapping(path = "/{style}/{z}/{x}/{y}.png", produces = MediaType.IMAGE_PNG_VALUE)
	public ResponseEntity<?> getTile(@PathVariable String style, @PathVariable int z, @PathVariable int x, @PathVariable int y)
			throws IOException, XmlPullParserException, SAXException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			VectorTileServerConfig config = osmAndMapsService.getConfig();
			return errorConfig("Tile service is not initialized: " + (config == null ? "" : config.initErrorMessage));
		}
		if (osmAndMapsService.validateNativeLib() != null) {
			return errorConfig("Tile service is not initialized: " + osmAndMapsService.validateNativeLib());
		}
		
		String interactiveKey = osmAndMapsService.getInteractiveKeyMap(style);
		String currentStyle = osmAndMapsService.getMapStyle(style, interactiveKey);
		VectorStyle vectorStyle = osmAndMapsService.getStyle(currentStyle);
		
		if (vectorStyle == null) {
			return ResponseEntity.badRequest().body("Rendering style is undefined: " + currentStyle);
		}
		
		VectorMetatile tile = osmAndMapsService.getMetaTile(vectorStyle, z, x, y, interactiveKey);
		// for local debug :
		// BufferedImage img = null;
		BufferedImage img = tile.getCacheRuntimeImage();
		tile.touch();
		if (img == null) {
			ResponseEntity<String> err = osmAndMapsService.renderMetaTile(tile);
			img = tile.runtimeImage;
			if (err != null) {
				return err;
			} else if (img == null) {
				return ResponseEntity.badRequest().body("Unexpected error during rendering");
			}
		}
		osmAndMapsService.cleanupCache();
		BufferedImage subimage = tile.readSubImage(img, x, y);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(subimage, "png", baos);
		return ResponseEntity.ok(new ByteArrayResource(baos.toByteArray()));
	}
	
	@GetMapping(path = "/info/{style}/{z}/{x}/{y}.json", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getTileInfo(@PathVariable String style, @PathVariable int z, @PathVariable int x, @PathVariable int y) throws IOException {
		
		String interactiveKey = osmAndMapsService.getInteractiveKeyMap(style);
		String currentStyle = osmAndMapsService.getMapStyle(style, interactiveKey);
		VectorStyle vectorStyle = osmAndMapsService.getStyle(currentStyle);
		
		if (vectorStyle == null) {
			return ResponseEntity.badRequest().body("Rendering style is undefined: " + currentStyle);
		}
		VectorMetatile tile = osmAndMapsService.getMetaTile(vectorStyle, z, x, y, interactiveKey);
		JsonObject tileInfo = osmAndMapsService.getTileInfo(tile.getCacheRuntimeInfo(), x, y, z);
		tile.touch();
		if (tileInfo == null) {
			return ResponseEntity.badRequest().body("Unexpected error during rendering");
		}
		osmAndMapsService.cleanupCache();
		
		return ResponseEntity.ok(String.valueOf(tileInfo));
	}
}
