package net.osmand;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import net.osmand.util.MapsCollection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.binary.CachedOsmandIndexes;
import net.osmand.binary.OsmandIndex.FileIndex;
import net.osmand.binary.OsmandIndex.MapLevel;
import net.osmand.data.QuadPointDouble;
import net.osmand.data.QuadRect;
import net.osmand.data.RotatedTileBox;
import net.osmand.data.RotatedTileBox.RotatedTileBoxBuilder;
import net.osmand.render.RenderingClass;
import net.osmand.render.RenderingRuleProperty;
import net.osmand.render.RenderingRuleSearchRequest;
import net.osmand.render.RenderingRuleStorageProperties;
import net.osmand.render.RenderingRulesStorage;
import net.osmand.render.RenderingRulesStorage.RenderingRulesStorageResolver;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import resources._R;

public class NativeJavaRendering extends NativeLibrary {

	private static final String INDEXES_CACHE = "indexes.cache";
	
	public static Boolean loaded = null;

	private final Map<String, Object> tilePathLocks = new ConcurrentHashMap<>();
	
	private static NativeJavaRendering defaultLoadedLibrary;
	
	private static final Log log = LogFactory.getLog(NativeJavaRendering.class);
	
	private RenderingRulesStorage storage;
	
	private Map<String, String> renderingProps;
	
	private Map<String, MapDiff> diffs = new LinkedHashMap<String, MapDiff>();
	
	public static class MapDiff {
		public String baseName;
		public File baseFile;
		public QuadRect bounds;
		public Map<String, File> diffs = new TreeMap<String, File>();
		public Set<String> disabled = new TreeSet<String>();
		public boolean enableBaseMap = true; 
		public String selected;
		public long timestamp; 
	}

	private static void loadRenderingAttributes(InputStream is, final Map<String, String> renderingConstants) throws SAXException, IOException{
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
		for (String s : diffs.keySet()) {
			MapDiff md = diffs.get(s);
			if (md.baseFile != null) {
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
		storage = parseStorage(path);
		setRenderingProps(renderingProperties);
		clearRenderingRulesStorage();
		initRenderingRulesStorage(storage);
	}

	public static RenderingRulesStorage parseStorage(String path) throws SAXException, IOException, XmlPullParserException{ 
		RenderingRulesStorage storage;
		final LinkedHashMap<String, String> renderingConstants = new LinkedHashMap<String, String>();

		final RenderingRulesStorageResolver resolver = new RenderingRulesStorageResolver() {
			@Override
			public RenderingRulesStorage resolve(String name, RenderingRulesStorageResolver ref) throws XmlPullParserException, IOException {
				RenderingRulesStorage depends = new RenderingRulesStorage(name, renderingConstants);
				depends.parseRulesFromXmlInputStream(RenderingRulesStorage.class.getResourceAsStream(name+".render.xml"),
							ref, false);
				return depends;
			}
		};
		if (path == null || path.equals("default.render.xml")) {
			loadRenderingAttributes(RenderingRulesStorage.class.getResourceAsStream("default.render.xml"),
					renderingConstants);
			storage = new RenderingRulesStorage("default", renderingConstants);
			storage.parseRulesFromXmlInputStream(RenderingRulesStorage.class.getResourceAsStream("default.render.xml"),
					resolver, false);
		} else {
			InputStream is = null;
			InputStream is2 = null;
			File stylesDir = null;
			if (new File(path).exists()) {
				is = new FileInputStream(new File(path));
				is2 = new FileInputStream(new File(path));
				stylesDir = new File(path).getParentFile();
			} else {
				is = RenderingRulesStorage.class.getResourceAsStream(path );
				is2 = RenderingRulesStorage.class.getResourceAsStream(path);
			}
			if(is == null) {
				throw new IllegalArgumentException("Can't find rendering style '" + path + "'");
			}
			loadRenderingAttributes(is, renderingConstants);
			String name = path;
			if (name.endsWith(".render.xml")) {
				name = name.substring(0, name.length() - ".render.xml".length());
			}
			if (name.lastIndexOf('/') != -1) {
				name = name.substring(name.lastIndexOf('/') + 1);
			}
			storage = new RenderingRulesStorage(name, renderingConstants);
			storage.parseRulesFromXmlInputStream(is2, resolver, false);
			is.close();
			is2.close();
			if (stylesDir != null) {
				for (File file : stylesDir.listFiles()) {
					if (file.isFile() && file.getName().endsWith("addon.render.xml")) {
						InputStream is3 = new FileInputStream(file);
						storage.parseRulesFromXmlInputStream(is3, resolver, true);
						is3.close();
					}
				}
			}
		}
		return storage;
	}


	public NativeJavaRendering() {
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

	public void setRenderingProps(String renderingProperties) {
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
	}


	public static class RenderingImageContext {
		public final int zoom;
		public final int sleft;
		public final int sright;
		public final int stop;
		public final int sbottom;
		private double leftX;
		private double topY;
		public final int width;
		public final int height;
		public long searchTime;
		public long renderingTime;
		public boolean saveTextTile = false;
		public RenderingContext context;

		public RenderingImageContext(int sleft, int sright, int stop, int sbottom, int zoom) {
			this.sleft = sleft;
			this.sright = sright;
			this.stop = stop;
			this.sbottom = sbottom;
			this.zoom = zoom;
			leftX =  (((double) sleft) / MapUtils.getPowZoom(31 - zoom));
			topY = (((double) stop) / MapUtils.getPowZoom(31 - zoom));
			width = (int) Math.round((sright - sleft) / MapUtils.getPowZoom(31 - zoom - 8));
			height = (int) Math.round((sbottom - stop) / MapUtils.getPowZoom(31 - zoom - 8));
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
	
	public RenderingRulesStorage getRenderingRuleStorage() {
		return storage;
	}

	public RenderingGenerationResult render(RenderingImageContext renderingImageContext) throws IOException {
		long time = -System.currentTimeMillis();
		RenderingContext renderingContext = new RenderingContext() {
			@Override
			protected byte[] getIconRawData(String data) {
				return _R.getIconData(data);
			}
		};
		renderingContext.preferredLocale = renderingProps.get("lang") != null ? renderingProps.get("lang") : "";
		renderingContext.nightMode = "true".equals(renderingProps.get("nightMode"));
		
		RenderingRuleSearchRequest request = new RenderingRuleSearchRequest(storage);
		
		request.setBooleanFilter(request.ALL.R_NIGHT_MODE, renderingContext.nightMode);
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
		request.setIntFilter(request.ALL.R_MINZOOM, renderingImageContext.zoom);
		
		Map<String, Boolean> parentsStates = new HashMap<>();
		Map<String, RenderingClass> renderingClasses = storage.getRenderingClasses();
		
		for (Map.Entry<String, RenderingClass> entry : renderingClasses.entrySet()) {
			String name = entry.getKey();
			RenderingClass renderingClass = entry.getValue();

			boolean enabled = renderingClass.isEnabledByDefault();
			
			String parentName = renderingClass.getParentName();
			if (parentName != null && parentsStates.containsKey(parentName) && !parentsStates.get(parentName)) {
				enabled = false;
			}
			
			request.setClassProperty(name, String.valueOf(enabled));
			parentsStates.put(name, enabled);
		}
		
		request.saveState();
		NativeSearchResult res = searchObjectsForRendering(renderingImageContext.sleft, renderingImageContext.sright, renderingImageContext.stop, renderingImageContext.sbottom, renderingImageContext.zoom, request, true,
					renderingContext, "Nothing found");
		// ctx.zoomDelta =  1;
//		double scale = MapUtils.getPowZoom((float) ctx.zoomDelta);
		float scale = 1;
		if (renderingProps.get("density") != null) {
			scale *= Float.parseFloat(renderingProps.get("density"));
		}
		renderingContext.leftX = renderingImageContext.leftX * scale;
		renderingContext.topY = renderingImageContext.topY * scale;
		renderingContext.width = (int) (renderingImageContext.width * scale);
		renderingContext.height = (int) (renderingImageContext.height * scale);
		renderingContext.renderingContextHandle = res.nativeHandler;
		// map density scales corresponding to zoom delta
		// (so the distance between the road is the same)
		renderingContext.setDensityValue(scale);
		//rctx.textScale = 1f;//Text/icon scales according to mapDensity
		renderingContext.textScale = 1 / scale; //Text/icon stays same for all sizes
		if(renderingProps.get("textScale") != null ) {
			renderingContext.textScale *= Float.parseFloat(renderingProps.get("textScale"));
		}
		renderingContext.screenDensityRatio = 1 / Math.max(1, 1f /*requestedBox.getDensity()*/);
		final double tileDivisor = MapUtils.getPowZoom((float) (31 - renderingImageContext.zoom)) / scale;
		request.clearState();

		if(request.searchRenderingAttribute(RenderingRuleStorageProperties.A_DEFAULT_COLOR)) {
			renderingContext.defaultColor = request.getIntPropertyValue(request.ALL.R_ATTR_COLOR_VALUE);
		}
		request.clearState();
		request.setIntFilter(request.ALL.R_MINZOOM, renderingImageContext.zoom);
		if(request.searchRenderingAttribute(RenderingRuleStorageProperties.A_SHADOW_RENDERING)) {
			renderingContext.shadowRenderingMode = request.getIntPropertyValue(request.ALL.R_ATTR_INT_VALUE);
			renderingContext.shadowRenderingColor = request.getIntPropertyValue(request.ALL.R_SHADOW_COLOR);

		}
		renderingContext.zoom = renderingImageContext.zoom;
		renderingContext.tileDivisor = tileDivisor;
		renderingContext.saveTextTile = renderingImageContext.saveTextTile;
		long search = time + System.currentTimeMillis();
		
		RenderingGenerationResult generationResult = NativeLibrary.generateRenderingIndirect(renderingContext, res.nativeHandler,
				false, request, true);
		List<RenderableObject> renderableObjects = new Gson().fromJson(renderingContext.textTile,  new TypeToken<List<RenderableObject>>(){}.getType());
		if (renderingContext.saveTextTile) {
			generationResult.setInfo(RenderableObject.createGeoJson(renderableObjects));
		}
		
		long rendering = time + System.currentTimeMillis() - search;
		renderingImageContext.searchTime = search;
		renderingImageContext.renderingTime = rendering;
		renderingImageContext.context = renderingContext;
		res.deleteNativeResult();
		
		return generationResult;
	}
	
	public static class RenderingImageResult {
		private final BufferedImage img;
		private final RenderingGenerationResult generationResult;
		
		RenderingImageResult(BufferedImage img, RenderingGenerationResult result) {
			this.img = img;
			this.generationResult = result;
		}
		
		public BufferedImage getImage() {
			return img;
		}
		
		public RenderingGenerationResult getGenerationResult() {
			return generationResult;
		}
		
	}
	
	public RenderingImageResult renderImage(RenderingImageContext renderingImageContext) throws IOException {
		RenderingGenerationResult generationResult = render(renderingImageContext);
		InputStream inputStream = new InputStream() {
			int nextInd = 0;
			@Override
			public int read() {
				if(nextInd >= generationResult.bitmapBuffer.capacity()) {
					return -1;
				}
				byte b = generationResult.bitmapBuffer.get(nextInd++) ;
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
		AllocationUtil.freeDirectBuffer(generationResult.bitmapBuffer);
		
		return new RenderingImageResult(img, generationResult);
	}

	public void initFilesInDir(File filesDir) throws IOException {
		List<File> obfFiles = new ArrayList<>();
		Map<String, FileIndex> mp = initIndexesCache(filesDir, obfFiles, false);
		initFilesInDir(obfFiles, mp);
	}
	
	private void initFilesInDir(List<File> obfFiles, Map<String, FileIndex> boundaries) {
		for (File f : obfFiles) {
			// "_16_05_06.obf"
			int l = f.getName().length() - 4;
			if (f.getName().charAt(l - 3) == '_' && f.getName().charAt(l - 6) == '_'
					&& f.getName().charAt(l - 9) == '_') {
				String baseName = f.getName().substring(0, l - 9);
				if (!diffs.containsKey(baseName)) {
					MapDiff md = new MapDiff();
					md.baseName = baseName;
					diffs.put(baseName, md);
				}
				MapDiff md = diffs.get(baseName);
				md.diffs.put(f.getName().substring(l - 8, l), f);
			} else if (!f.getName().contains("basemap") && boundaries != null) {
				FileIndex mapIndex = boundaries.get(f.getAbsolutePath());
				if (f.getName().endsWith("_2.obf")) {
					updateBoundaries(f, mapIndex, f.getName().substring(0, f.getName().length() - 6));
				} else {
					updateBoundaries(f, mapIndex, f.getName().substring(0, f.getName().length() - 4));
				}
			}
			initMapFile(f.getAbsolutePath(), true);
		}
	}

	private void updateBoundaries(File f, FileIndex mapIndex, String nm) {
		if (mapIndex == null || mapIndex.getMapIndexCount() == 0 || mapIndex.getMapIndex(0).getLevelsCount() == 0) {
			return;
		}
		if (!diffs.containsKey(nm)) {
			MapDiff mm = new MapDiff();
			mm.baseName = nm;
			diffs.put(nm, mm);
		}
		MapLevel rt = mapIndex.getMapIndex(0).getLevels(0);
		MapDiff dd = diffs.get(nm);
		dd.baseFile = f;
		dd.timestamp = mapIndex.getDateModified();
		dd.bounds = new QuadRect(MapUtils.get31LongitudeX(rt.getLeft()), MapUtils.get31LatitudeY(rt.getTop()),
				MapUtils.get31LongitudeX(rt.getRight()), MapUtils.get31LatitudeY(rt.getBottom()));
		Iterator<String> iterator = dd.diffs.keySet().iterator();
		while (iterator.hasNext()) {
			dd.selected = iterator.next();
		}
	}
	
	public void enableBaseFile(MapDiff m, boolean enable) {
		if (enable) {
			if (!m.enableBaseMap) {
				initBinaryMapFile(m.baseFile.getAbsolutePath(), true, false);
				m.enableBaseMap = true;
			}
		} else {
			if (m.enableBaseMap) {
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
		for (String s : reverse) {
			String fp = md.diffs.get(s).getAbsolutePath();
			enable = enable || s.equals(df);
			if (!enable) {
				if (!md.disabled.contains(fp)) {
					closeBinaryMapFile(fp);
					md.disabled.add(fp);
				}
			} else {
				if (md.disabled.contains(fp)) {
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
	
	public Map<String, FileIndex> initIndexesCache(File dir, List<File> filesToUse, boolean filterDuplicates) throws IOException {
		Map<String, FileIndex> map = new TreeMap<>();
		File cacheFile = new File(dir, INDEXES_CACHE);
		CachedOsmandIndexes cache = new CachedOsmandIndexes();
		if (cacheFile.exists()) {
			cache.readFromFile(cacheFile);
		}
		if (dir.exists() && dir.listFiles() != null) {
			MapsCollection mapsCollection = new MapsCollection(filterDuplicates);
			for (File obf : Algorithms.getSortedFilesVersions(dir)) {
				if (!obf.isDirectory() && obf.getName().endsWith(".obf")) {
					mapsCollection.add(obf);
				}
			}
			List<File> filteredMaps = mapsCollection.getFilesToUse();
			for (File file : filteredMaps) {
				FileIndex fileIndex = cache.getFileIndex(file, true);
				if (fileIndex != null) {
					map.put(file.getAbsolutePath(), fileIndex);
				}
			}
			if (filesToUse != null) {
				filesToUse.addAll(filteredMaps);
			}
		}
		cache.writeToFile(cacheFile);
		return map;
	}

	public static NativeJavaRendering getDefault(String filename, String obfFolder,
			String fontsFolder) throws IOException {
		if (defaultLoadedLibrary != null) {
			return defaultLoadedLibrary;
		}
		File f = filename == null ? null : new File(filename);
		if (filename == null || filename.length() == 0 || !(f.exists())) {
			filename = null;
		}
		boolean loaded;
        String path =  filename == null ? null : f.getParentFile().getAbsolutePath();
		loaded = NativeLibrary.loadOldLib(path);
		if (loaded) {
			defaultLoadedLibrary = new NativeJavaRendering();
			long now = System.currentTimeMillis();
			if (obfFolder != null) {
				File filesFolder = new File(obfFolder);
				List<File> obfFiles = new ArrayList<>();
				Map<String, FileIndex> map = defaultLoadedLibrary.initIndexesCache(filesFolder, obfFiles, true);
				//defaultLoadedLibrary.initCacheMapFile(new File(filesFolder, INDEXES_CACHE).getAbsolutePath());
				defaultLoadedLibrary.initFilesInDir(obfFiles, map);
			}
			log.info(String.format("Init native library with maps: %d ms", System.currentTimeMillis() - now));
			if (fontsFolder != null) {
				defaultLoadedLibrary.loadFontData(new File(fontsFolder));
			}
		}
		return defaultLoadedLibrary;
	}

	public BufferedImage getGeotiffImage(String tilePath, String outColorFilename, String midColorFilename,
	                                                  int type, int size, int zoom, int x, int y) throws IOException {
		Object lock = tilePathLocks.computeIfAbsent(tilePath, k -> new Object());
		ByteBuffer geotiffBuffer;
		synchronized (lock) {
			geotiffBuffer = NativeLibrary.getGeotiffTile(tilePath, outColorFilename, midColorFilename, type, size, zoom, x, y);
		}
		try (InputStream inputStream = new InputStream() {
			int nextInd = 0;

			@Override
			public int read() {
				if (nextInd >= geotiffBuffer.capacity()) {
					return -1;
				}
				byte b = geotiffBuffer.get(nextInd++);
				return b & 0xFF;
			}
		};
		     MemoryCacheImageInputStream memoryCacheStream = new MemoryCacheImageInputStream(inputStream)) {

			Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("png");
			if (!readers.hasNext()) {
				throw new IOException("No PNG ImageReader found");
			}
			ImageReader reader = readers.next();
			reader.setInput(memoryCacheStream, true);

			BufferedImage img = reader.read(0);

			synchronized (lock) {
				AllocationUtil.freeDirectBuffer(geotiffBuffer);
			}

			return img;
		}
	}
}
