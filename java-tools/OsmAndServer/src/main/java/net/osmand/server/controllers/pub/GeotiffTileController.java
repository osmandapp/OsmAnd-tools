package net.osmand.server.controllers.pub;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.server.api.services.OsmAndMapsService;

@Controller
@RequestMapping("/heightmap")
public class GeotiffTileController {
	
    protected static final Log LOGGER = LogFactory.getLog(GeotiffTileController.class);

	private static final String HILLSHADE_TYPE = "hillshade";
	private static final String SLOPE_TYPE = "slope";

	private static final String geotiffTilesFilepath = "../../../../heightmap/tiles/";
	private static final String resultsColorFilepath = "../../resources/color-palette/";

	@Autowired
	OsmAndMapsService osmAndMapsService;

	private ResponseEntity<?> errorConfig(String msg) {
		return ResponseEntity.badRequest()
				.body(msg);
	}
	
	@RequestMapping(path = "/{type}/{z}/{x}/{y}.png", produces = MediaType.IMAGE_PNG_VALUE)
	public ResponseEntity<?> getTile(@PathVariable String type, @PathVariable int z, @PathVariable int x, @PathVariable int y)
			throws IOException, XmlPullParserException, SAXException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return errorConfig("Tile service is not initialized");
		}
		if (osmAndMapsService.validateNativeLib() != null) {
			return errorConfig("Tile service is not initialized: " + osmAndMapsService.validateNativeLib());
		}

		int resType = type.equals(HILLSHADE_TYPE) ? 1 : (type.equals(SLOPE_TYPE) ? 2 : 3);
		String resultColorsFilename;
		String intermediateColorsFilename = "";
		switch(resType)
		{
			case 1:
				resultColorsFilename = resultsColorFilepath + "hillshade_main_default.txt";
				intermediateColorsFilename = resultsColorFilepath + "hillshade_color_default.txt";
				break;
			case 2:
				resultColorsFilename = resultsColorFilepath + "slope_default.txt";
				break;
			default:
				resultColorsFilename = resultsColorFilepath + "height_altitude_default.txt";
		}
	
		BufferedImage img = osmAndMapsService.getGeotiffTile(
			geotiffTilesFilepath, resultColorsFilename, intermediateColorsFilename, resType, 256, z, x, y);
		BufferedImage subimage = img;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(subimage, "png", baos);
		return ResponseEntity.ok(new ByteArrayResource(baos.toByteArray()));
	}
}
