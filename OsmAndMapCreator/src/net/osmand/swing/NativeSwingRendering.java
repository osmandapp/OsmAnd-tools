package net.osmand.swing;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import net.osmand.NativeLibrary;
import net.osmand.RenderingContext;
import net.osmand.data.QuadPointDouble;
import net.osmand.data.QuadRect;
import net.osmand.data.RotatedTileBox;
import net.osmand.data.RotatedTileBox.RotatedTileBoxBuilder;
import net.osmand.render.RenderingRuleProperty;
import net.osmand.render.RenderingRuleSearchRequest;
import net.osmand.render.RenderingRuleStorageProperties;
import net.osmand.render.RenderingRulesStorage;
import net.osmand.render.RenderingRulesStorage.RenderingRulesStorageResolver;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xmlpull.v1.XmlPullParserException;

import resources._R;

public class NativeSwingRendering extends NativeLibrary {

	
	RenderingRulesStorage storage;
	private HashMap<String, String> renderingProps;
	
	public static Boolean loaded = null;
	private static NativeSwingRendering defaultLoadedLibrary; 
	
	private void loadRenderingAttributes(InputStream is, final Map<String, String> renderingConstants) throws SAXException, IOException{
		try {
			final SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
			saxParser.parse(is, new DefaultHandler() {
				@Override
				public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
					String tagName = saxParser.isNamespaceAware() ? localName : qName;
					if ("renderingConstant".equals(tagName)) { //$NON-NLS-1$
						if (!renderingConstants.containsKey(attributes.getValue("name"))) {
							renderingConstants.put(attributes.getValue("name"), attributes.getValue("value"));
						}
					}
				}
			});
		} catch (ParserConfigurationException e1) {
			throw new IllegalStateException(e1);
		} finally {
			is.close();
		}
	}
	
	public void loadRuleStorage(String path, String renderingProperties) throws IOException, XmlPullParserException, SAXException{
		final LinkedHashMap<String, String> renderingConstants = new LinkedHashMap<String, String>();
		
		final RenderingRulesStorageResolver resolver = new RenderingRulesStorageResolver() {
			@Override
			public RenderingRulesStorage resolve(String name, RenderingRulesStorageResolver ref) throws XmlPullParserException, IOException {
				RenderingRulesStorage depends = new RenderingRulesStorage(name, renderingConstants);
					depends.parseRulesFromXmlInputStream(RenderingRulesStorage.class.getResourceAsStream(name+".render.xml"),
							ref);
				return depends;
			}
		};
		if(path == null || path.equals("default.render.xml")) {
			loadRenderingAttributes(RenderingRulesStorage.class.getResourceAsStream("default.render.xml"), 
					renderingConstants);
			storage = new RenderingRulesStorage("default", renderingConstants);
			storage.parseRulesFromXmlInputStream(RenderingRulesStorage.class.getResourceAsStream("default.render.xml"), resolver);
		} else {
			loadRenderingAttributes(new FileInputStream(path),renderingConstants);
			storage = new RenderingRulesStorage("default", renderingConstants);
			storage.parseRulesFromXmlInputStream(new FileInputStream(path), resolver);
		}
		renderingProps = new HashMap<String, String>();
		String[] props = renderingProperties.split(",");
		for (String s : props) {
			int i = s.indexOf('=');
			if (i > 0) {
				renderingProps.put(s.substring(0, i).trim(), s.substring(i + 1).trim());
			}
		}
		initRenderingRulesStorage(storage);
	}
	
	public NativeSwingRendering(boolean newLibrary){
        super(newLibrary);
        if (!newLibrary) {
            try {
                loadRuleStorage(null, "");
            } catch (SAXException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (XmlPullParserException e) {
                throw new RuntimeException(e);
            }
        }
    }
	
	
	
	public static class RenderingImageContext {
		public int zoom;
		public double zoomDelta;
		public int sleft;
		public int sright;
		public int stop;
		public int sbottom;
		private double leftX;
		private double topY;
		private int width;
		private int height;
		public long searchTime;
		public long renderingTime;
		
		public RenderingImageContext(int sleft, int sright, int stop, int sbottom, int zoom) {
			this.sleft = sleft;
			this.sright = sright;
			this.stop = stop;
			this.sbottom = sbottom;
			this.zoom = zoom;
			leftX =  (((double) sleft) / MapUtils.getPowZoom(31 - zoom));
			topY = (((double) stop) / MapUtils.getPowZoom(31 - zoom));
			width = (int) ((sright - sleft) / MapUtils.getPowZoom(31 - zoom - 8));
			height = (int) ((sbottom - stop) / MapUtils.getPowZoom(31 - zoom - 8));
		}
		
		public RenderingImageContext(double lat, double lon, int width, int height, int zoom, 
				double zoomDelta) {
			this.width = width;
			this.height = height;
			this.zoomDelta = zoomDelta;
			this.zoom = zoom;
			RotatedTileBoxBuilder bld = new RotatedTileBox.RotatedTileBoxBuilder();
			RotatedTileBox tb = bld.setPixelDimensions(width, height).setZoomAndScale(zoom, (float) zoomDelta).
				setLocation(lat, lon).build();
			final QuadPointDouble lt = tb.getLeftTopTile(tb.getZoom());
			
			
			this.leftX = lt.x /** MapUtils.getPowZoom(tb.getZoomScale())*/;
			this.topY = lt.y /** MapUtils.getPowZoom(tb.getZoomScale())*/;
			QuadRect ll = tb.getLatLonBounds();
			this.sleft = MapUtils.get31TileNumberX(ll.left);
			this.sright = MapUtils.get31TileNumberX(ll.right);
			this.sbottom = MapUtils.get31TileNumberY(ll.bottom);
			this.stop = MapUtils.get31TileNumberY(ll.top);
		}
	}
	
	public BufferedImage renderImage(int sleft, int sright, int stop, int sbottom, int zoom) throws IOException {
		return renderImage(new RenderingImageContext(sleft, sright, stop, sbottom, zoom));	
	}
	
	public BufferedImage renderImage(RenderingImageContext ctx) throws IOException {
		long time = -System.currentTimeMillis();
		RenderingContext rctx = new RenderingContext() {
			@Override
			protected byte[] getIconRawData(String data) {
				return _R.getIconData(data);
			}
		};
		rctx.nightMode = "true".equals(renderingProps.get("nightMode"));
		RenderingRuleSearchRequest request = new RenderingRuleSearchRequest(storage);
		request.setBooleanFilter(request.ALL.R_NIGHT_MODE, rctx.nightMode);
		for (RenderingRuleProperty customProp : storage.PROPS.getCustomRules()) {
			String res = renderingProps.get(customProp.getAttrName());
			if (!Algorithms.isEmpty(res)) {
				if (customProp.isString()) {
					request.setStringFilter(customProp, res);
				} else if (customProp.isBoolean()) {
					request.setBooleanFilter(customProp, "true".equalsIgnoreCase(res));
				} else {
					try {
						request.setIntFilter(customProp, Integer.parseInt(res));
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
				}
			}
		}
		request.setIntFilter(request.ALL.R_MINZOOM, ctx.zoom);
		request.saveState();
		
		NativeSearchResult res = searchObjectsForRendering(ctx.sleft, ctx.sright, ctx.stop, ctx.sbottom, ctx.zoom, request, true, 
					rctx, "Nothing found");
		
		rctx.leftX = ctx.leftX * MapUtils.getPowZoom((float) ctx.zoomDelta);
		rctx.topY = ctx.topY * MapUtils.getPowZoom((float) ctx.zoomDelta);
		rctx.width = ctx.width;
		rctx.height = ctx.height;
		final float mapDensity = (float) Math.pow(2,  ctx.zoomDelta);
		rctx.setDensityValue(mapDensity);
		rctx.screenDensityRatio = mapDensity / Math.max(1, 1/*requestedBox.getDensity()*/) ;
		final double tileDivisor = MapUtils.getPowZoom((float) (31 - ctx.zoom - ctx.zoomDelta));
		request.clearState();
		
		if(request.searchRenderingAttribute(RenderingRuleStorageProperties.A_DEFAULT_COLOR)) {
			rctx.defaultColor = request.getIntPropertyValue(request.ALL.R_ATTR_COLOR_VALUE);
		}
		request.clearState();
		request.setIntFilter(request.ALL.R_MINZOOM, ctx.zoom);
		if(request.searchRenderingAttribute(RenderingRuleStorageProperties.A_SHADOW_RENDERING)) {
			rctx.shadowRenderingMode = request.getIntPropertyValue(request.ALL.R_ATTR_INT_VALUE);
			rctx.shadowRenderingColor = request.getIntPropertyValue(request.ALL.R_SHADOW_COLOR);
			
		}
		rctx.zoom = ctx.zoom;
		rctx.tileDivisor = tileDivisor;
		long search = time + System.currentTimeMillis();
		final RenderingGenerationResult rres = NativeSwingRendering.generateRenderingIndirect(rctx, res.nativeHandler,  
				false, request, true);
		long rendering = time + System.currentTimeMillis() - search;
		InputStream inputStream = new InputStream() {
			int nextInd = 0;
			@Override
			public int read() throws IOException {
				if(nextInd >= rres.bitmapBuffer.capacity()) {
					return -1;
				}
				byte b = rres.bitmapBuffer.get(nextInd++) ;
				if(b < 0) {
					return b + 256;
				} else {
					return b;
				}
			}
		};
		Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("png");
		ImageReader reader = readers.next();
		reader.setInput(new MemoryCacheImageInputStream(inputStream), true);
		BufferedImage img = reader.read(0);
		ctx.searchTime = search;
		ctx.renderingTime = rendering;
		long last = time + System.currentTimeMillis() - rendering;
		System.out.println(" TIMES search - " + search + " rendering - " + rendering + " unpack - " + last);
		return img;
	}
	
	public void initFilesInDir(File filesDir){
		File[] lf = filesDir.listFiles();
		for(File f : lf){
			if(f.getName().endsWith(".obf")) {
				initMapFile(f.getAbsolutePath());
			}
		}
	}
	
	
	public static NativeSwingRendering getDefaultFromSettings() {
		if (defaultLoadedLibrary != null) {
			return defaultLoadedLibrary;
		}
		String filename = DataExtractionSettings.getSettings().getNativeLibFile();
		File f = new File(filename);
		if (filename.length() == 0 || !(f.exists())) {
			filename = null;
		}
		boolean loaded;
        boolean newLib = !f.isFile() || f.getName().contains("JNI");
		if(!newLib){
			loaded = NativeLibrary.loadOldLib(f.getParentFile().getAbsolutePath());
		} else {
			loaded = NativeLibrary.loadNewLib(f.getParentFile().getAbsolutePath());
		}
		if (loaded) {
			defaultLoadedLibrary = new NativeSwingRendering(newLib);
			defaultLoadedLibrary.initFilesInDir(new File(DataExtractionSettings.getSettings().getBinaryFilesDir()));
		}
		return defaultLoadedLibrary;
	}
	


	

}
