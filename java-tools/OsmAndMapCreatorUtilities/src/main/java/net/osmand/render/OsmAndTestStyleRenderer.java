package net.osmand.render;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import gnu.trove.list.array.TIntArrayList;
import net.osmand.NativeJavaRendering;
import net.osmand.NativeJavaRendering.RenderingImageContext;
import net.osmand.binary.MapZooms;
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
		boolean overwrite = false;
		MapZooms zooms;
		int partTotal = 1, total = 1 ; // 1/1, 1/2, 0/2
		String nativeLib = null;
		String fontsFolder;
		File obf = null;
		File outputDir = null;
		List<TileSourceGenTemplate> tilesource = new ArrayList<>();
		
		NativeJavaRendering nsr;
		String selectedVectorStyle = "";
 	}
	
	protected static class TileSourceGenTemplate {
		String url = null;
		boolean echo = false;
		boolean vector = false;
		String vectorStyle ="";
		int vectorSize = 256;
		String suffix;
		
		public static TileSourceGenTemplate raster(String rasterTemplate) {
			TileSourceGenTemplate t = new TileSourceGenTemplate();
			Map<String, String> params = getParams(rasterTemplate);
			t.url = params.get("url");
			t.suffix = params.get("suffix");
			return t;
		}
		
		public static TileSourceGenTemplate echo(String rasterTemplate) {
			TileSourceGenTemplate t = new TileSourceGenTemplate();
			Map<String, String> params = getParams(rasterTemplate);
			t.url = params.get("url");
			t.suffix = params.get("suffix");
			t.echo = true;
			return t;
		}
		
		public static TileSourceGenTemplate vector(String input) {
			TileSourceGenTemplate t = new TileSourceGenTemplate();
			Map<String, String> params = getParams(input);
			t.vector = true;
			t.vectorStyle = params.get("style");
			t.suffix = params.get("suffix");
			if (params.containsKey("size")) {
				t.vectorSize = Integer.parseInt(params.get("size"));
			}
			return t;
		}

		private static Map<String, String> getParams(String input) {
			Map<String, String> parms = new HashMap<>();
			for (String a : input.split("::")) {
				int indexOf = a.indexOf("-");
				if(indexOf > 0) {
					parms.put(a.substring(0, indexOf), a.substring(indexOf + 1));
				}
			}
			return parms;
		}
		
		
		public boolean isFile() {
			return !echo;
		}

		public String getExt() {
			if(vector) {
				return "png";
			} else {
				String ext = url.substring(url.lastIndexOf('.') + 1);
				if (ext.indexOf("?") != -1) {
					ext = ext.substring(0, ext.indexOf('?'));
				}
				return ext;
			}
			
		}
		
	}

	public static void main(String[] args) throws Exception {
		OsmAndTestStyleRendererParams pms = new OsmAndTestStyleRendererParams();
		for (String a : args) {
			a = a.trim();
			if (a.startsWith("-native=")) {
				pms.nativeLib = a.substring("-native=".length());
			} else if (a.startsWith("-fonts=")) {
				pms.fontsFolder = a.substring("-fonts=".length());
			} else if (a.startsWith("-obf=")) {
				pms.obf = new File(a.substring("-obf=".length()));
			} else if (a.equals("-overwrite")) {
				pms.overwrite = true;
			} else if (a.startsWith("-part=")) {
				String[] s = a.substring("-part=".length()).split("/");
				pms.partTotal = Integer.parseInt(s[0]) - 1;
				pms.total = Integer.parseInt(s[1]);
			} else if (a.startsWith("-bbox=")) {
				// left, top, right, bottom
				String[] bbox = a.substring("-bbox=".length()).split(",");
				pms.leftLon = Double.parseDouble(bbox[0]);
				pms.bottomLat = Double.parseDouble(bbox[1]);
				pms.rightLon = Double.parseDouble(bbox[2]);
				pms.topLat = Double.parseDouble(bbox[3]);
			} else if (a.startsWith("-zoom=")) {
				String s = a.substring("-zoom=".length());
				pms.zooms = MapZooms.parseZooms(s);
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
		
		if(pms.zooms == null) {
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
		
		TIntArrayList zooms = new TIntArrayList();
		for(int k = 0; k < pms.zooms.getLevels().size(); k++) {
			for(int z = pms.zooms.getLevel(k).getMinZoom(); z <= pms.zooms.getLevel(k).getMaxZoom(); z++) {
				zooms.add(z);
			}
		}
		for (int z : zooms.toArray()) {
			int leftx = (int) Math.floor(MapUtils.getTileNumberX(z, pms.leftLon));
			int rightx = (int) Math.ceil(MapUtils.getTileNumberX(z, pms.rightLon));
			int topY = (int) Math.floor(MapUtils.getTileNumberY(z, pms.topLat));
			int bottomY = (int) Math.ceil(MapUtils.getTileNumberY(z, pms.bottomLat));
			int cnt = (rightx - leftx + 1) * (bottomY - topY + 1);
			int ind = 0;
			if (pms.total == 1) {
				System.out.printf("Generating %d tiles zoom %d ...\n", cnt, z);
			} else {
				cnt = cnt / pms.total;
				System.out.printf("Generating %d tiles zoom %d - part %d / %d ...\n", cnt, z, (pms.partTotal + 1),
						pms.total);
			}

			for (int x = leftx; x <= rightx; x++) {
				for (int y = topY; y <= bottomY; y++) {
					if (pms.total > 1 && (x + y + z) % pms.total != pms.partTotal) {
						continue;
					}
					ind++;
					System.out.printf("Generating  %.2f%% (%d / %d tiles) %d/%d/%d...\n", ind * 100.0 / cnt, ind, cnt,
							z, x, y);
					downloadTiles(x, y, z, pms);
				}
			}
		}
	}
	

	private static void downloadTiles(int x, int y, int zoom, OsmAndTestStyleRendererParams pms) throws Exception {
		int ind = 0;
		File outFile = null;
		try {
			for (TileSourceGenTemplate t : pms.tilesource) {
				ind++;
				String suffix = t.suffix == null ? ind + "" : t.suffix;
				String ext = t.getExt();
				outFile = new File(pms.outputDir, formatTile(zoom, x, y, suffix, ext));
				outFile.getParentFile().mkdirs();
				if (outFile.exists() && !pms.overwrite) {
					continue;
				}
				if (t.vector) {
//				RenderingImageContext ctx = new RenderingImageContext(x << (31 - zoom), (x + 1) << (31 - zoom),
//						y << (31 - zoom), (y + 1) << (31 - zoom), zoom);
					RenderingImageContext ctx = new RenderingImageContext(MapUtils.getLatitudeFromTile(zoom, y + 0.5),
							MapUtils.getLongitudeFromTile(zoom, x + 0.5), t.vectorSize, t.vectorSize, zoom,
							t.vectorSize / 256);
//				String props = String.format("density=%d,textScale=%d", 1 << tile.tileSizeLog, 1 << tile.tileSizeLog);
					String props = "density=1";
					if (!Algorithms.stringsEqual(t.vectorStyle, pms.selectedVectorStyle)) {
						pms.nsr.loadRuleStorage(t.vectorStyle, props);
						pms.selectedVectorStyle = t.vectorStyle;
					} else {
						pms.nsr.setRenderingProps(props);
					}
					BufferedImage img = pms.nsr.renderImage(ctx);
					ImageIO.write(img, ext, outFile);
				} else {
					String template = TileSourceTemplate.normalizeUrl(t.url);
					int bingQuadKeyParamIndex = template.indexOf(TileSourceManager.PARAM_BING_QUAD_KEY);
					if (bingQuadKeyParamIndex != -1) {
						template = template.replace(TileSourceManager.PARAM_BING_QUAD_KEY,
								TileSourceTemplate.eqtBingQuadKey(zoom, x, y));
					}
					String tileUrl = MessageFormat.format(template, zoom + "", x + "", y + "");
					System.out.println("  Downloading " + tileUrl + " ...");
					if (t.isFile() && pms.outputDir != null) {
						URL url = new URL(tileUrl);
						URLConnection cn = url.openConnection();
						FileOutputStream fous = new FileOutputStream(outFile);
						Algorithms.streamCopy(cn.getInputStream(), fous);
						fous.close();
					}
				}
			}
		} catch (Exception e) {
			System.err.printf("Error dealing with tile %s \n",
					outFile != null ? outFile.getAbsolutePath() : zoom + "/" + x + "/" + y);
			throw e;

		}
	}

	private static String formatTile(int zoom, int x, int y, String suffix, String ext) {
//		return String.format("%d_%d_%d_%s.%s", zoom, x, y, suffix, ext);
		return String.format("%d/%d/%d_%s.%s", zoom, x, y, suffix, ext);
	}


}
