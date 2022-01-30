package net.osmand.server.controllers.pub;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.Gson;

import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.OsmAndMapsService.VectorMetatile;
import net.osmand.server.api.services.OsmAndMapsService.VectorStyle;
import net.osmand.server.api.services.OsmAndMapsService.VectorTileServerConfig;

@Controller
@RequestMapping("/tile")
public class VectorTileController {
	
    protected static final Log LOGGER = LogFactory.getLog(VectorTileController.class);

	@Autowired
	OsmAndMapsService osmAndMapsService;
	
	Gson gson = new Gson();

	private ResponseEntity<?> errorConfig() {
		VectorTileServerConfig config = osmAndMapsService.getConfig();
		return ResponseEntity.badRequest()
				.body("Tile service is not initialized: " + (config == null ? "" : config.initErrorMessage));
	}
	@RequestMapping(path = "/styles", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<?> getStyles() {
		VectorTileServerConfig config = osmAndMapsService.getConfig();
		return ResponseEntity.ok(gson.toJson(config.style));
	}
	
	@RequestMapping(path = "/{style}/{z}/{x}/{y}.png", produces = MediaType.IMAGE_PNG_VALUE)
	public ResponseEntity<?> getTile(@PathVariable String style, @PathVariable int z, @PathVariable int x, @PathVariable int y)
			throws IOException, XmlPullParserException, SAXException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return errorConfig();
		}
		VectorStyle vectorStyle = osmAndMapsService.getStyle(style);
		if (vectorStyle == null) {
			return ResponseEntity.badRequest().body("Rendering style is undefined: " + style);
		}
		VectorMetatile tile = osmAndMapsService.getMetaTile(vectorStyle, z, x, y);
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

}
