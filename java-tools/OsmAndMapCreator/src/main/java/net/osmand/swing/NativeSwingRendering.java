package net.osmand.swing;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import net.osmand.NativeLibrary;
import net.osmand.RenderingContext;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapRoot;
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
	
	private Map<String, MapDiff> diffs = new LinkedHashMap<String, MapDiff>();
	
	public static class MapDiff {
		String baseName;
		File baseFile;
		QuadRect bounds;
		Map<String, File> diffs = new TreeMap<String, File>();
		Set<String> disabled = new TreeSet<String>();
		boolean enableBaseMap = true; 
		String selected;
		public long timestamp; 
	}

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
	
	public void closeAllFiles() {
		for(String s : diffs.keySet()) {
			MapDiff md = diffs.get(s);
			if(md.baseFile != null) {
				closeBinaryMapFile(md.baseFile.getAbsolutePath());
			}
			if (md.diffs != null) {
				for (File l : md.diffs.values()) {
					closeBinaryMapFile(l.getAbsolutePath());
				}
			}
		}
		diffs.clear();
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
			InputStream is = null;
			InputStream is2 = null;
			if (new File(path).exists()) {
				is = new FileInputStream(new File(path));
				is2 = new FileInputStream(new File(path));
			} else {
				is = RenderingRulesStorage.class.getResourceAsStream(path );
				is2 = RenderingRulesStorage.class.getResourceAsStream(path);
			}
			if(is == null) {
				throw new IllegalArgumentException("Can't find rendering style '" + path + "'");
			}
			loadRenderingAttributes(is, renderingConstants);
			storage = new RenderingRulesStorage("default", renderingConstants);
			storage.parseRulesFromXmlInputStream(is2, resolver);
			is.close();
			is2.close();
		}
		renderingProps = new HashMap<String, String>();
		String[] props = renderingProperties.split(",");
		for (String s : props) {
			int i = s.indexOf('=');
			if (i > 0) {
				String key = s.substring(0, i).trim();
				String value = s.substring(i + 1).trim();
				if(value.contains(";")) {
					value = value.substring(0, value.indexOf(';'));
				}
				renderingProps.put(key, value);
			}
		}
		initRenderingRulesStorage(storage);
	}

	public NativeSwingRendering() {
		super();
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



	public static class RenderingImageContext {
		public int zoom;
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
		public double mapDensity;
		public RenderingContext context;

		public RenderingImageContext(int sleft, int sright, int stop, int sbottom, int zoom, double mapDensity) {
			this.sleft = sleft;
			this.sright = sright;
			this.stop = stop;
			this.sbottom = sbottom;
			this.zoom = zoom;
			this.mapDensity = mapDensity;
			leftX =  (((double) sleft) / MapUtils.getPowZoom(31 - zoom));
			topY = (((double) stop) / MapUtils.getPowZoom(31 - zoom));
			width = (int) ((sright - sleft) / MapUtils.getPowZoom(31 - zoom - 8));
			height = (int) ((sbottom - stop) / MapUtils.getPowZoom(31 - zoom - 8));
		}

		public RenderingImageContext(double lat, double lon, int width, int height, int zoom,
				double mapDensity) {
			this.width = width;
			this.height = height;
			this.zoom = zoom;
			RotatedTileBoxBuilder bld = new RotatedTileBox.RotatedTileBoxBuilder();
			RotatedTileBox tb = bld.setPixelDimensions(width, height).setZoom(zoom).
				setLocation(lat, lon).build();
			tb.setMapDensity(mapDensity);
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

	public BufferedImage renderImage(RenderingImageContext ctx) throws IOException {
		long time = -System.currentTimeMillis();
		RenderingContext rctx = new RenderingContext() {
			@Override
			protected byte[] getIconRawData(String data) {
				return _R.getIconData(data);
			}
		};
		rctx.preferredLocale = renderingProps.get("lang") != null ? renderingProps.get("lang") : "";
		rctx.nightMode = "true".equals(renderingProps.get("nightMode"));
		RenderingRuleSearchRequest request = new RenderingRuleSearchRequest(storage);
		request.setBooleanFilter(request.ALL.R_NIGHT_MODE, rctx.nightMode);
		for (RenderingRuleProperty customProp : storage.PROPS.getCustomRules()) {
			String res = renderingProps.get(customProp.getAttrName());
			if(customProp.getAttrName().equals(RenderingRuleStorageProperties.A_ENGINE_V1)){
				request.setBooleanFilter(customProp, true);
			} else if (!Algorithms.isEmpty(res)) {
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
			} else {
				if (customProp.isString()) {
					request.setStringFilter(customProp, "");
				} else if (customProp.isBoolean()) {
					request.setBooleanFilter(customProp, false);
				} else {
					request.setIntFilter(customProp, -1);
				}
			}
		}
		request.setIntFilter(request.ALL.R_MINZOOM, ctx.zoom);
		request.saveState();
		NativeSearchResult res = searchObjectsForRendering(ctx.sleft, ctx.sright, ctx.stop, ctx.sbottom, ctx.zoom, request, true,
					rctx, "Nothing found");
		// ctx.zoomDelta =  1;
//		double scale = MapUtils.getPowZoom((float) ctx.zoomDelta);
		float scale = 1;
		if(renderingProps.get("density") != null ) {
			scale *= Float.parseFloat(renderingProps.get("density"));

		}
		rctx.leftX = ctx.leftX * scale;
		rctx.topY = ctx.topY * scale;
		rctx.width = (int) (ctx.width * scale);
		rctx.height = (int) (ctx.height * scale);
		// map density scales corresponding to zoom delta
		// (so the distance between the road is the same)
		rctx.setDensityValue(scale);
		//rctx.textScale = 1f;//Text/icon scales according to mapDensity
		rctx.textScale = 1 / scale; //Text/icon stays same for all sizes
		if(renderingProps.get("textScale") != null ) {
			rctx.textScale *= Float.parseFloat(renderingProps.get("textScale"));
		}
		rctx.screenDensityRatio = 1 / Math.max(1, 1f /*requestedBox.getDensity()*/);
		final double tileDivisor = MapUtils.getPowZoom((float) (31 - ctx.zoom)) / scale;
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
		ctx.context = rctx;
		long last = time + System.currentTimeMillis() - rendering;
		System.out.println(" TIMES search - " + search + " rendering - " + rendering + " unpack - " + last);
		res.deleteNativeResult();
		return img;
	}

	public void initFilesInDir(File filesDir) {
		File[] lf = Algorithms.getSortedFilesVersions(filesDir);
		for(File f : lf){
			if(f.getName().endsWith(".obf")) {
				// "_16_05_06.obf"
				int l = f.getName().length() - 4;
				if(f.getName().charAt(l - 3) == '_' && f.getName().charAt(l - 6) == '_' && 
						f.getName().charAt(l - 9) == '_') {
					String baseName = f.getName().substring(0, l - 9);
					if(!diffs.containsKey(baseName)) {
						MapDiff md = new MapDiff();
						md.baseName = baseName;
						diffs.put(baseName, md);
					}
					MapDiff md = diffs.get(baseName);
					md.diffs.put(f.getName().substring(l - 8, l), f);
				} else if( !f.getName().contains("basemap")){
					if(f.getName().endsWith("_2.obf")) {
						updateBoundaries(f, f.getName().substring(0, f.getName().length() - 6));	
					} else {
						updateBoundaries(f, f.getName().substring(0, f.getName().length() - 4));
					}
				}
				initMapFile(f.getAbsolutePath(), true);
			}
		}
	}

	private void updateBoundaries(File f, String nm) {
		try {
			BinaryMapIndexReader r = new BinaryMapIndexReader(new RandomAccessFile(f, "r"), f);
			try {
				if (r.getMapIndexes().isEmpty() || r.getMapIndexes().get(0).getRoots().isEmpty()) {
					return;
				}
				MapRoot rt = r.getMapIndexes().get(0).getRoots().get(0);
				if (!diffs.containsKey(nm)) {
					MapDiff mm = new MapDiff();
					mm.baseName = nm;
					diffs.put(nm, mm);
				}
				MapDiff dd = diffs.get(nm);
				dd.baseFile = f;
				dd.timestamp = r.getDateCreated();
				dd.bounds = new QuadRect(MapUtils.get31LongitudeX(rt.getLeft()), MapUtils.get31LatitudeY(rt.getTop()),
						MapUtils.get31LongitudeX(rt.getRight()), MapUtils.get31LatitudeY(rt.getBottom()));
				Iterator<String> iterator = dd.diffs.keySet().iterator();
				while (iterator.hasNext()) {
					dd.selected = iterator.next();
				}
			} finally {
				r.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void enableBaseFile(MapDiff m, boolean enable) {
		if(enable) {
			if(!m.enableBaseMap) {
				initBinaryMapFile(m.baseFile.getAbsolutePath(), true, false);
				m.enableBaseMap = true;
			}
		} else {
			if(m.enableBaseMap) {
				closeBinaryMapFile(m.baseFile.getAbsolutePath());
				m.enableBaseMap = false;
			}
		}
	}
	
	public void enableMapFile(MapDiff md, String df) {
		closeBinaryMapFile(md.baseFile.getAbsolutePath());
		Set<String> ks = md.diffs.keySet();
		LinkedList<String> reverse = new LinkedList<>();
		for(String s : ks) {
			String fp = md.diffs.get(s).getAbsolutePath();
			if(!md.disabled.contains(fp)) {
				closeBinaryMapFile(fp);
				md.disabled.add(fp);
			}
			reverse.addFirst(s);
		}
		boolean enable = false;
		for(String s : reverse) {
			String fp = md.diffs.get(s).getAbsolutePath();
			enable = enable || s.equals(df);
			if(!enable) {
				if(!md.disabled.contains(fp)) {
					closeBinaryMapFile(fp);
					md.disabled.add(fp);
				}
			} else {
				if(md.disabled.contains(fp)) {
					initBinaryMapFile(fp, true, false);
					md.disabled.remove(fp);
				}
			}
			 
		}
		md.selected = df;
		if(md.enableBaseMap) {
			initBinaryMapFile(md.baseFile.getAbsolutePath(), true, false);
		}
	}
	
	public MapDiff getMapDiffs(double lat, double lon) {
		for(MapDiff md : diffs.values()) {
			if(md.bounds != null && md.bounds.top > lat && md.bounds.bottom < lat && 
					md.bounds.left < lon && md.bounds.right > lon) {
				return md;
			}
		}
		return null;
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
        String path =  filename == null ? null : f.getParentFile().getAbsolutePath();
		loaded = NativeLibrary.loadOldLib(path);
		if (loaded) {
			defaultLoadedLibrary = new NativeSwingRendering();
			defaultLoadedLibrary.initFilesInDir(new File(DataExtractionSettings.getSettings().getBinaryFilesDir()));
			defaultLoadedLibrary.loadFontData(new File("fonts"));
		}
		return defaultLoadedLibrary;
	}



}
