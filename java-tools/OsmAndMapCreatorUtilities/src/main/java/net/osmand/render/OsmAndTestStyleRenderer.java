package net.osmand.render;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.NativeJavaRendering;
import net.osmand.NativeJavaRendering.RenderingImageContext;
import net.osmand.map.TileSourceManager;
import net.osmand.map.TileSourceManager.TileSourceTemplate;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class OsmAndTestStyleRenderer {


	private static class OsmAndTestStyleRendererParams {
		double topLat = 0;
		double leftLon = 0;
		double bottomLat = 0;
		double rightLon = 0;
		int zoom = 0;
		String nativeLib = null;
		String fontsFolder;
		File obf = null;
		File outputDir = null;
		List<TileSourceGenTemplate> tilesource = new ArrayList<>();
		
		NativeJavaRendering nsr;
		String selectedVectorStyle = "";
 	}
	
	protected static class TileSourceGenTemplate {
		String template = null;
		boolean echo = false;
		boolean vector = false;
		String vectorStyle ="";
		int vectorSize = 256;
		
		public static TileSourceGenTemplate raster(String rasterTemplate) {
			TileSourceGenTemplate t = new TileSourceGenTemplate();
			t.template = rasterTemplate;
			return t;
		}
		
		public static TileSourceGenTemplate echo(String rasterTemplate) {
			TileSourceGenTemplate t = new TileSourceGenTemplate();
			t.template = rasterTemplate;
			t.echo = true;
			return t;
		}
		
		public static TileSourceGenTemplate vector(String input) {
			TileSourceGenTemplate t = new TileSourceGenTemplate();
			t.vector = true;
			for(String a :input.split(":")) {
				String[] keys = a.split("-");
				if (keys.length > 1) {
					String key = keys[0];
					String value = keys[1];
					if (key.equals("style")) {
						t.vectorStyle = value;
					} else if (key.equals("sz")) {
						t.vectorSize = Integer.parseInt(value);
					}
				}
			}
			return t;
		}
		
		
		public boolean isFile() {
			return !echo;
		}
		
	}

	public static void main(String[] args) throws IOException, XmlPullParserException, SAXException, ParserConfigurationException, TransformerException {
		OsmAndTestStyleRendererParams pms = new OsmAndTestStyleRendererParams();
		for (String a : args) {
			a = a.trim();
			if (a.startsWith("-native=")) {
				pms.nativeLib = a.substring("-native=".length());
			} else if (a.startsWith("-fonts=")) {
				pms.fontsFolder = a.substring("-fonts=".length());
			} else if (a.startsWith("-obf=")) {
				pms.obf = new File(a.substring("-obf=".length()));
			} else if (a.startsWith("-bbox=")) {
				// left, top, right, bottom
				String[] bbox = a.substring("-bbox=".length()).split(",");
				pms.leftLon = Double.parseDouble(bbox[0]);
				pms.bottomLat = Double.parseDouble(bbox[1]);
				pms.rightLon = Double.parseDouble(bbox[2]);
				pms.topLat  = Double.parseDouble(bbox[3]);
			} else if (a.startsWith("-zoom=")) {
				pms.zoom = Integer.parseInt(a.substring("-zoom=".length()));
			} else if (a.startsWith("-output=")) {
				pms.outputDir = new File(a.substring("-output=".length()));
				pms.outputDir.mkdirs();
			} else if (a.startsWith("-raster=")) {
				pms.tilesource.add(TileSourceGenTemplate.raster(a.substring("-raster=".length())));
			} else if (a.startsWith("-vector=")) {
				pms.tilesource.add(TileSourceGenTemplate.vector(a.substring("-vector=".length())));
			}
		}
		if(pms.tilesource.size() == 0) {
			pms.tilesource.add(TileSourceGenTemplate.echo("https://tile.osmand.net/hd/{z}/{x}/{y}.png"));
		}
		
		if(pms.zoom == 0) {
			System.out.println("Please specify --zoom");
			return;
		}
		if(pms.leftLon == pms.rightLon || pms.topLat == pms.bottomLat) {
			System.out.println("Please specify --bbox in format 'leftlon,bottomlat,rightlon,toplat'");
			return;
		}

//		if (nativeLib != null) {
//			boolean old = NativeLibrary.loadOldLib(nativeLib);
//			NativeLibrary nl = new NativeLibrary();
//			nl.loadFontData(new File(NativeSwingRendering.findFontFolder()));
//			if (!old) {
//				throw new UnsupportedOperationException("Not supported");
//			}
//		}
		
		pms.nsr = NativeJavaRendering.getDefault(pms.nativeLib, null, pms.fontsFolder);
		
		if (pms.nsr != null && pms.obf != null) {
			if (pms.obf.isFile()) {
				pms.nsr.initMapFile(pms.obf.getAbsolutePath(), true);
			} else {
				pms.nsr.initFilesInDir(pms.obf);
			}
		}
		
		int leftx = (int) Math.floor(MapUtils.getTileNumberX(pms.zoom, pms.leftLon));
		int rightx = (int) Math.ceil(MapUtils.getTileNumberX(pms.zoom, pms.rightLon));
		int topY = (int) Math.floor(MapUtils.getTileNumberY(pms.zoom, pms.topLat));
		int bottomY = (int) Math.ceil(MapUtils.getTileNumberY(pms.zoom, pms.bottomLat));
		for (int x = leftx; x <= rightx; x++) {
			for (int y = topY; y <= bottomY; y++) {
				downloadTiles(x, y, pms.zoom, pms);
			}
		}
	}
	

	private static void downloadTiles(int x, int y, int zoom, OsmAndTestStyleRendererParams pms) throws IOException, XmlPullParserException, SAXException {
		int ind = 0;
		for (TileSourceGenTemplate t : pms.tilesource) {
			ind++;
			if (t.vector) {
//				RenderingImageContext ctx = new RenderingImageContext(x << (31 - zoom), (x + 1) << (31 - zoom),
//						y << (31 - zoom), (y + 1) << (31 - zoom), zoom);
				
				RenderingImageContext ctx = new RenderingImageContext(MapUtils.getLatitudeFromTile(zoom, y + 0.5),
						MapUtils.getLongitudeFromTile(zoom, x + 0.5),
						t.vectorSize, t.vectorSize, zoom, t.vectorSize / 256);
//				String props = String.format("density=%d,textScale=%d", 1 << tile.tileSizeLog, 1 << tile.tileSizeLog);
				String props = "density=1";
				if (!Algorithms.stringsEqual(t.vectorStyle, pms.selectedVectorStyle)) {
					pms.nsr.loadRuleStorage(t.vectorStyle, props);
					pms.selectedVectorStyle = t.vectorStyle;
				} else {
					pms.nsr.setRenderingProps(props);
				}
				BufferedImage img = pms.nsr.renderImage(ctx);
				String ext = "png";
				ImageIO.write(img, ext, new File(pms.outputDir, formatTile(pms.zoom, x, y, ind, ext)));
			} else {
				String template = TileSourceTemplate.normalizeUrl(t.template);
				int bingQuadKeyParamIndex = template.indexOf(TileSourceManager.PARAM_BING_QUAD_KEY);
				if (bingQuadKeyParamIndex != -1) {
					template = template.replace(TileSourceManager.PARAM_BING_QUAD_KEY,
							TileSourceTemplate.eqtBingQuadKey(zoom, x, y));
				}
				String tileUrl = MessageFormat.format(template, pms.zoom + "", x + "", y + "");
				System.out.println("Downloading " + tileUrl + " ...");
				if (t.isFile() && pms.outputDir != null) {
					URL url = new URL(tileUrl);
					URLConnection cn = url.openConnection();
					String ext = tileUrl.substring(tileUrl.lastIndexOf('.') + 1);
					if (ext.indexOf("?") != -1) {
						ext = ext.substring(0, ext.indexOf('?'));
					}
					File f = new File(pms.outputDir, formatTile(pms.zoom, x, y, ind, ext));
					FileOutputStream fous = new FileOutputStream(f);
					Algorithms.streamCopy(cn.getInputStream(), fous);
					fous.close();
				}
			}
		}
	}

	private static String formatTile(int zoom, int x, int y, int ind, String ext) {
		return String.format("%d_%d_%d_%d.%s", zoom, x, y, ind, ext);
	}


}
