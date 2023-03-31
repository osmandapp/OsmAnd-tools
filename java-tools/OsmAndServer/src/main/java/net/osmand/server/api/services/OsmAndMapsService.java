package net.osmand.server.api.services;




import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpSession;

import net.osmand.IndexConstants;
import net.osmand.data.Amenity;
import net.osmand.obf.OsmGpxWriteContext;
import net.osmand.osm.PoiType;
import net.osmand.server.controllers.pub.RoutingController;
import net.osmand.server.controllers.pub.UserSessionResources;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.LocationsHolder;
import net.osmand.NativeJavaRendering;
import net.osmand.NativeJavaRendering.RenderingImageContext;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.CachedOsmandIndexes;
import net.osmand.binary.GeocodingUtilities;
import net.osmand.binary.GeocodingUtilities.GeocodingResult;
import net.osmand.binary.OsmandIndex.FileIndex;
import net.osmand.binary.OsmandIndex.RoutingPart;
import net.osmand.binary.OsmandIndex.RoutingSubregion;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.gpx.GPXUtilities.TrkSegment;
import net.osmand.gpx.GPXUtilities.WptPt;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.MapPoiTypes;
import net.osmand.render.RenderingRuleProperty;
import net.osmand.render.RenderingRulesStorage;
import net.osmand.router.GeneralRouter.GeneralRouterProfile;
import net.osmand.router.PrecalculatedRouteDirection;
import net.osmand.router.RouteCalculationProgress;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.GpxPoint;
import net.osmand.router.RoutePlannerFrontEnd.GpxRouteApproximation;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RouteResultPreparation;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.router.RoutingContext;
import net.osmand.search.SearchUICore;
import net.osmand.search.SearchUICore.SearchResultCollection;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.osmand.util.MapsCollection;

import static net.osmand.binary.BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER;
import static net.osmand.binary.BinaryMapIndexReader.buildSearchPoiRequest;

@Service
public class OsmAndMapsService {
	private static final Log LOGGER = LogFactory.getLog(OsmAndMapsService.class);

	private static final int MAX_RUNTIME_IMAGE_CACHE_SIZE = 80;
	private static final int MAX_RUNTIME_TILES_CACHE_SIZE = 10000;
	private static final int MAX_FILES_PER_FOLDER = 1 << 12; // 4096
	private static final int ZOOM_EN_PREFERRED_LANG = 6;
	
	private static final boolean DEFAULT_USE_ROUTING_NATIVE_LIB = false;
	private static final int MEM_LIMIT = RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT * 8;
	
	private static final long INTERVAL_TO_MONITOR_ZIP = 15 * 60 * 1000;
	
	private static final int SEARCH_RADIUS_LEVEL = 1;
	private static final double SEARCH_RADIUS_DEGREE = 1.5;
	private static final double MAX_SIZE_POI = 500;
	private static final String SEARCH_LOCALE = "en";
	
	Map<String, BinaryMapIndexReaderReference> obfFiles = new LinkedHashMap<>();
	
	CachedOsmandIndexes cacheFiles = null; 

	AtomicInteger cacheTouch = new AtomicInteger(0);

	Map<String, VectorMetatile> tileCache = new ConcurrentHashMap<>();

	NativeJavaRendering nativelib;

	File tempDir;

	@Autowired
	VectorTileServerConfig config;
	
	@Autowired
	RoutingServerConfig routingConfig;

	private OsmandRegions osmandRegions;


	public static class BinaryMapIndexReaderReference {
		File file;
		BinaryMapIndexReader reader;
		public FileIndex fileIndex;
	}
	
	
	public static class RoutingServerConfigEntry {
		public String type;
		public String url;
		public String name;
		public String profile = "car";
	}
	
	@Configuration
	@ConfigurationProperties("osmand.routing")
	public static class RoutingServerConfig {
		
		public Map<String, RoutingServerConfigEntry> config = new TreeMap<String, RoutingServerConfigEntry>();
		
		public void setConfig(Map<String, String> style) {
			for (Entry<String, String> e : style.entrySet()) {
				RoutingServerConfigEntry src = new RoutingServerConfigEntry();
				src.name = e.getKey();
				for (String s : e.getValue().split(",")) {
					String value = s.substring(s.indexOf('=') + 1);
					if (s.startsWith("type=")) {
						src.type = value;
					} else if (s.startsWith("url=")) {
						src.url = value;
					} else if (s.startsWith("profile=")) {
						src.profile = value;
					}
				}
				config.put(src.name, src);
			}
		}
	}

	@Configuration
	@ConfigurationProperties("tile-server")
	public static class VectorTileServerConfig {

		@Value("${tile-server.obf.location}")
		String obfLocation;
		
		@Value("${tile-server.obf.ziplocation}")
		String obfZipLocation;

		@Value("${tile-server.cache.location}")
		String cacheLocation;

		@Value("${tile-server.cache.max-zoom}")
		int maxZoomCache = 16;

		@Value("${tile-server.metatile-size}")
		int metatileSize;

		public String initErrorMessage;

		public Map<String, VectorStyle> style = new TreeMap<String, VectorStyle>();

		public void setStyle(Map<String, String> style) {
			for (Entry<String, String> e : style.entrySet()) {
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

	}

	public static class VectorStyle {
		public transient RenderingRulesStorage storage;
		public List<RenderingRuleProperty> properties = new ArrayList<RenderingRuleProperty>();
		public String key;
		public String name;
		public int maxZoomCache;
		public int tileSizeLog;
		public int metaTileSizeLog;
	}

	public static class VectorMetatile implements Comparable<VectorMetatile> {

		public BufferedImage runtimeImage;
		public long lastAccess;
		public final String key;
		public final int z;
		public final int left;
		public final int top;
		public final int metaSizeLog;
		public final int tileSizeLog;
		public final VectorStyle style;
		private final VectorTileServerConfig cfg;

		public VectorMetatile(VectorTileServerConfig cfg, String tileId, VectorStyle style, int z, int left, int top,
				int metaSizeLog, int tileSizeLog) {
			this.cfg = cfg;
			this.style = style;
			this.metaSizeLog = metaSizeLog;
			this.tileSizeLog = tileSizeLog;
			this.key = tileId;
			this.left = left;
			this.top = top;
			this.z = z;
			touch();
		}

		public void touch() {
			lastAccess = System.currentTimeMillis();
		}

		@Override
		public int compareTo(VectorMetatile o) {
			return Long.compare(lastAccess, o.lastAccess);
		}

		public BufferedImage readSubImage(BufferedImage img, int x, int y) {
			int subl = x - ((x >> metaSizeLog) << metaSizeLog);
			int subt = y - ((y >> metaSizeLog) << metaSizeLog);
			int tilesize = 256 << tileSizeLog;
			return img.getSubimage(subl * tilesize, subt * tilesize, tilesize, tilesize);
		}

		public BufferedImage getCacheRuntimeImage() throws IOException {
			BufferedImage img = runtimeImage;
			if (img != null) {
				return img;
			}
			File cf = getCacheFile();
			if (cf != null && cf.exists()) {
				runtimeImage = ImageIO.read(cf);
				return runtimeImage;
			}
			return null;
		}

		public File getCacheFile() {
			if (z > cfg.maxZoomCache || cfg.cacheLocation == null || cfg.cacheLocation.length() == 0) {
				return null;
			}
			int x = left >> (31 - z) >> metaSizeLog;
			int y = top >> (31 - z) >> metaSizeLog;
			StringBuilder loc = new StringBuilder();
			loc.append(style.key).append("/").append(z);
			while (x >= MAX_FILES_PER_FOLDER) {
				int nx = x % MAX_FILES_PER_FOLDER;
				loc.append("/").append(nx);
				x = (x - nx) / MAX_FILES_PER_FOLDER;
			}
			loc.append("/").append(x);
			while (y >= MAX_FILES_PER_FOLDER) {
				int ny = y % MAX_FILES_PER_FOLDER;
				loc.append("/").append(ny);
				y = (y - ny) / MAX_FILES_PER_FOLDER;
			}
			loc.append("/").append(y);
			loc.append("-").append(metaSizeLog).append("-").append(tileSizeLog).append(".png");
			return new File(cfg.cacheLocation, loc.toString());
		}
	}
	
	@Scheduled(fixedRate = INTERVAL_TO_MONITOR_ZIP)
	public void checkZippedFiles() throws IOException {
		if (config != null && !Algorithms.isEmpty(config.obfZipLocation) && !Algorithms.isEmpty(config.obfLocation)) {
			for (File zipFile : new File(config.obfZipLocation).listFiles()) {
				if (zipFile.getName().endsWith(".obf.zip")) {
					String fn = zipFile.getName().substring(0, zipFile.getName().length() - ".zip".length());
					File target = new File(config.obfLocation, fn);
					File targetTemp = new File(config.obfLocation, fn + ".new");
					if (!target.exists() || target.lastModified() != zipFile.lastModified()
							|| zipFile.length() > target.length()) {
						long val = System.currentTimeMillis();
						ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
						ZipEntry ze = zis.getNextEntry();
						boolean success = false;
						while (ze != null && !success) {
							if (!ze.isDirectory() && ze.getName().endsWith(".obf")) {
								targetTemp.delete();
								FileOutputStream fous = new FileOutputStream(targetTemp);
								byte[] b = new byte[1 << 20];
								int read;
								while ((read = zis.read(b)) != -1) {
									fous.write(b, 0, read);
								}
								fous.close();
								targetTemp.setLastModified(zipFile.lastModified());
								success = true;
								zis.closeEntry();
							}
							ze = zis.getNextEntry();
						}
						zis.close();
						LOGGER.info("Unzip new obf file " + target.getName() + " " + (System.currentTimeMillis() - val)
								+ " ms");
						initNewObfFiles(target, targetTemp);
					}
				}
			}
		}
	}

	public VectorTileServerConfig getConfig() {
		return config;
	}
	
	public RoutingServerConfig getRoutingConfig() {
		return routingConfig;
	}

	public VectorStyle getStyle(String style) {
		return config.style.get(style);
	}

	public VectorMetatile getMetaTile(VectorStyle vectorStyle, int z, int x, int y) {
		int metaSizeLog = Math.min(vectorStyle.metaTileSizeLog, z - 1);
		int left = ((x >> metaSizeLog) << metaSizeLog) << (31 - z);
		if (left < 0) {
			left = 0;
		}
		int top = ((y >> metaSizeLog) << metaSizeLog) << (31 - z);
		if (top < 0) {
			top = 0;
		}
		String tileId = encode(vectorStyle.key, left >> (31 - z), top >> (31 - z), z, metaSizeLog,
				vectorStyle.tileSizeLog);
		VectorMetatile tile = tileCache.get(tileId);
		if (tile == null) {
			tile = new VectorMetatile(config, tileId, vectorStyle, z, left, top, metaSizeLog, vectorStyle.tileSizeLog);
			tileCache.put(tile.key, tile);
		}
		return tile;
	}

	public boolean validateAndInitConfig() throws IOException {
		if (nativelib == null && config.initErrorMessage == null) {
			osmandRegions = new OsmandRegions();
			osmandRegions.prepareFile();
			synchronized (this) {
				if (!(nativelib == null && config.initErrorMessage == null)) {
					return config.initErrorMessage == null;
				}
				if (config.obfLocation == null || config.obfLocation.isEmpty()) {
					config.initErrorMessage = "Files location is not specified";
				} else {
					File obfLocationF = new File(config.obfLocation);
					if (!obfLocationF.exists()) {
						config.initErrorMessage = "Files location is not specified";
					}
				}
				tempDir = Files.createTempDirectory("osmandserver").toFile();
				LOGGER.info("Init temp rendering directory for libs / fonts: " + tempDir.getAbsolutePath());
				tempDir.deleteOnExit();
				ClassLoader cl = NativeJavaRendering.class.getClassLoader();
				ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(cl);
				Resource[] resources = resolver.getResources("classpath:/map/fonts/*.ttf");
				File fontsFolder = new File(tempDir, "fonts");
				fontsFolder.mkdirs();
				fontsFolder.deleteOnExit();
				for (Resource resource : resources) {
					InputStream ios = resource.getInputStream();
					File file = new File(fontsFolder, resource.getFilename());
					file.deleteOnExit();
					FileOutputStream fous = new FileOutputStream(file);
					Algorithms.streamCopy(ios, fous);
					fous.close();
					ios.close();
				}
				nativelib = NativeJavaRendering.getDefault(null, config.obfLocation, fontsFolder.getAbsolutePath());
				if (nativelib == null) {
					config.initErrorMessage = "Tile rendering engine is not initialized";
				}
			}
		}
		return config.initErrorMessage == null;
	}

	public ResponseEntity<String> renderMetaTile(VectorMetatile tile)
			throws IOException, XmlPullParserException, SAXException {
		// don't synchronize this to not block routing
		synchronized (config) {
			VectorMetatile rendered = tileCache.get(tile.key);
			if (rendered != null && rendered.runtimeImage != null) {
				tile.runtimeImage = rendered.runtimeImage;
				return null;
			}
			int imgTileSize = (256 << tile.tileSizeLog) << Math.min(tile.z, tile.metaSizeLog);
			int tilesize = (1 << Math.min(31 - tile.z + tile.metaSizeLog, 31));
			if (tilesize <= 0) {
				tilesize = Integer.MAX_VALUE;
			}
			int right = tile.left + tilesize;
			if (right <= 0) {
				right = Integer.MAX_VALUE;
			}
			int bottom = tile.top + tilesize;
			if (bottom <= 0) {
				bottom = Integer.MAX_VALUE;
			}
			long now = System.currentTimeMillis();
			String props = String.format("density=%d,textScale=%d", 1 << tile.tileSizeLog, 1 << tile.tileSizeLog);
			if (tile.z < ZOOM_EN_PREFERRED_LANG) {
				props += ",lang=en";
			}
			if (nativelib == null) {
				return null;
			}
			if (!tile.style.name.equalsIgnoreCase(nativelib.getRenderingRuleStorage().getName())) {
				nativelib.loadRuleStorage(tile.style.name + ".render.xml", props);
			} else {
				nativelib.setRenderingProps(props);
			}
			RenderingImageContext ctx = new RenderingImageContext(tile.left, right, tile.top, bottom, tile.z);
			if (ctx.width > 8192) {
				return ResponseEntity.badRequest().body("Metatile exceeds 8192x8192 size");

			}
			if (imgTileSize != ctx.width << tile.tileSizeLog || imgTileSize != ctx.height << tile.tileSizeLog) {
				return ResponseEntity.badRequest().body(String.format("Metatile has wrong size (%d != %d)", imgTileSize,
						ctx.width << tile.tileSizeLog));
			}
			tile.runtimeImage = nativelib.renderImage(ctx);
			if (tile.runtimeImage != null) {
				File cacheFile = tile.getCacheFile();
				if (cacheFile != null) {
					cacheFile.getParentFile().mkdirs();
					if (cacheFile.getParentFile().exists()) {
						ImageIO.write(tile.runtimeImage, "png", cacheFile);
					}
				}
			}
			String msg = String.format("Rendered %d %d at %d (%s %s): %dx%d - %d ms", tile.left, tile.top, tile.z,
					tile.style.name, props, ctx.width, ctx.height, (int) (System.currentTimeMillis() - now));
			System.out.println(msg);
			// LOGGER.debug();
			return null;
		}
	}

	public void cleanupCache() {
		int version = cacheTouch.incrementAndGet();
		// so with atomic only 1 thread will get % X == 0
		if (version % MAX_RUNTIME_IMAGE_CACHE_SIZE == 0 && version > 0) {
			cacheTouch.set(0);
			TreeSet<VectorMetatile> sortCache = new TreeSet<>(tileCache.values());
			List<VectorMetatile> imageTiles = new ArrayList<>();
			for (VectorMetatile vm : sortCache) {
				if (vm.runtimeImage != null) {
					imageTiles.add(vm);
				}
			}
			if (imageTiles.size() >= MAX_RUNTIME_IMAGE_CACHE_SIZE) {
				for (int i = 0; i < MAX_RUNTIME_IMAGE_CACHE_SIZE / 2; i++) {
					VectorMetatile vm = imageTiles.get(i);
					vm.runtimeImage = null;
				}
			}
			if (tileCache.size() >= MAX_RUNTIME_TILES_CACHE_SIZE) {
				Iterator<VectorMetatile> it = sortCache.iterator();
				while (tileCache.size() >= MAX_RUNTIME_TILES_CACHE_SIZE / 2 && it.hasNext()) {
					VectorMetatile metatTile = it.next();
					tileCache.remove(metatTile.key);
				}
			}
		}
	}

	private static String encode(String style, int x, int y, int z, int metasizeLog, int tileSizeLog) {
		// long l = 0 ;
		// int shift = 0;
		// l += ((long) tileSizeLog) << shift; // 0-2
		// shift += 2;
		// l += ((long) metasizeLog) << shift; // 0-4
		// shift += 3;
		// l += ((long) z) << shift; // 1-22
		// shift += 5;
		// l += ((long) x) << shift;
		// shift += z;
		// l += ((long) y) << shift;
		// shift += z;
		StringBuilder sb = new StringBuilder();
		return sb.append(style).append('-').append(metasizeLog).append('-').append(tileSizeLog).append('/').append(z)
				.append('/').append(x).append('/').append(y).toString();
	}
	
	public synchronized List<SearchResult> search(double lat, double lon, String text) throws IOException, InterruptedException {
		if (!validateAndInitConfig()) {
			return null;
		}
		SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), SEARCH_LOCALE, false);
		searchUICore.getSearchSettings().setRegions(osmandRegions);
		QuadRect points = points(null, new LatLon(lat + SEARCH_RADIUS_DEGREE, lon - SEARCH_RADIUS_DEGREE), 
				new LatLon(lat - SEARCH_RADIUS_DEGREE, lon + SEARCH_RADIUS_DEGREE));
		List<BinaryMapIndexReader> list = Arrays.asList(getObfReaders(points));
		searchUICore.getSearchSettings().setOfflineIndexes(list);
	    searchUICore.init();
	    searchUICore.registerAPI(new SearchCoreFactory.SearchRegionByNameAPI());
	    
	    SearchSettings settings = searchUICore.getPhrase().getSettings();
	    searchUICore.updateSettings(settings.setRadiusLevel(SEARCH_RADIUS_LEVEL));
		SearchResultCollection r = searchUICore.immediateSearch(text, new LatLon(lat, lon));
		List<SearchResult> results = r.getCurrentSearchResults();
		
	    return results;
	}
	
	public synchronized List<GeocodingResult> geocoding(double lat, double lon) throws IOException, InterruptedException {
		QuadRect points = points(null, new LatLon(lat, lon), new LatLon(lat, lon));
		List<BinaryMapIndexReader> list = Arrays.asList(getObfReaders(points));
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		RoutingContext ctx = prepareRouterContext("geocoding", points, router, null, null);
		GeocodingUtilities su = new GeocodingUtilities();
		List<GeocodingResult> res = su.reverseGeocodingSearch(ctx, lat, lon, false);
		List<GeocodingResult> complete = su.sortGeocodingResults(list, res);
//		complete.addAll(res);
//		Collections.sort(complete, GeocodingUtilities.DISTANCE_COMPARATOR);
		
		return complete;
	}
	

	public synchronized List<RouteSegmentResult> gpxApproximation(String routeMode, Map<String, Object> props, GPXFile file) throws IOException, InterruptedException {
		if (!file.hasTrkPt()) {
			return Collections.emptyList();
		}
		TrkSegment trkSegment = file.tracks.get(0).segments.get(0);
		List<LatLon> polyline = new ArrayList<LatLon>(trkSegment.points.size());
		for (WptPt p : trkSegment.points) {
			polyline.add(new LatLon(p.lat, p.lon));
		}
		QuadRect points = points(polyline, null, null);
		if (!validateAndInitConfig() || points == null) {
			return Collections.emptyList();
		}
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		RoutingContext ctx = prepareRouterContext(routeMode, points, router, null, null);
		List<RouteSegmentResult> route = approximate(ctx, router, props, polyline);
		return route;
	}

	private List<RouteSegmentResult> approximate(RoutingContext ctx, RoutePlannerFrontEnd router, 
			Map<String, Object> props, List<LatLon> polyline) throws IOException, InterruptedException {
		GpxRouteApproximation gctx = new GpxRouteApproximation(ctx);
		List<GpxPoint> gpxPoints = router.generateGpxPoints(gctx, new LocationsHolder(polyline));
		GpxRouteApproximation r = router.searchGpxRoute(gctx, gpxPoints, null);
		List<RouteSegmentResult> route = new ArrayList<RouteSegmentResult>();
		for (GpxPoint pnt : r.finalPoints) {
			route.addAll(pnt.routeToTarget);
		}
		if (router.useNativeApproximation) {
			RouteResultPreparation preparation = new RouteResultPreparation();
			// preparation.prepareTurnResults(gctx.ctx, route);
			preparation.addTurnInfoDescriptions(route);
		}
		putResultProps(ctx, route, props);
		return route;
	}

	private RoutingContext prepareRouterContext(String routeMode, QuadRect points, RoutePlannerFrontEnd router, 
			RoutingServerConfigEntry[] serverEntry, List<String> avoidRoadsIds) throws IOException {
		String[] props = routeMode.split("\\,");
		Map<String, String> paramsR = new LinkedHashMap<String, String>();
		boolean useNativeLib = DEFAULT_USE_ROUTING_NATIVE_LIB;
		router.setUseNativeApproximation(false);
		RouteCalculationMode paramMode = null;
		for (String p : props) {
			if (p.length() == 0) {
				continue;
			}
			int ind = p.indexOf('=');
			String key = p;
			String value = "true";
			if (ind != -1) {
				key = p.substring(0, ind);
				value = p.substring(ind + 1);
			}
			if (key.equals("nativerouting")) {
				useNativeLib = Boolean.parseBoolean(value);
			} else if (key.equals("nativeapproximation")) {
				router.setUseNativeApproximation(Boolean.parseBoolean(value));
			} else if (key.equals("calcmode")) {
				if (value.length() > 0) {
					paramMode = RouteCalculationMode.valueOf(value.toUpperCase());
				}
			} else {
				paramsR.put(key, value);
			}
		}
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(MEM_LIMIT, MEM_LIMIT);
		String routeModeKey = props[0];
		if (routingConfig.config.containsKey(routeModeKey)) {
			RoutingServerConfigEntry entry = routingConfig.config.get(routeModeKey);
			routeModeKey = entry.profile;
			if (serverEntry != null && serverEntry.length > 0) {
				serverEntry[0] = entry;
			}
		}
		Builder cfgBuilder = RoutingConfiguration.getDefault();
		cfgBuilder.clearImpassableRoadLocations();
		if (avoidRoadsIds != null) {
			for (String s : avoidRoadsIds) {
				cfgBuilder.addImpassableRoad(Long.parseLong(s));
			}
		}
		// addImpassableRoad(6859437l).
		// setDirectionPoints(directionPointsFile).
		RoutingConfiguration config =  cfgBuilder.build(routeModeKey, /* RoutingConfiguration.DEFAULT_MEMORY_LIMIT */ memoryLimit, paramsR);
		config.routeCalculationTime = System.currentTimeMillis();
		if (paramMode == null) {
			paramMode = GeneralRouterProfile.CAR == config.router.getProfile() ? RouteCalculationMode.COMPLEX
					: RouteCalculationMode.NORMAL;
		}
		final RoutingContext ctx = router.buildRoutingContext(config, useNativeLib ? nativelib : null,
				getObfReaders(points), paramMode); // RouteCalculationMode.BASE
		ctx.leftSideNavigation = false;
		return ctx;
	}
	
	
	public synchronized List<RouteSegmentResult> routing(String routeMode, Map<String, Object> props, LatLon start,
			LatLon end, List<LatLon> intermediates, List<String> avoidRoadsIds)
			throws IOException, InterruptedException {
		QuadRect points = points(intermediates, start, end);
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		RoutingServerConfigEntry[] rsc = new RoutingServerConfigEntry[1];
		RoutingContext ctx = prepareRouterContext(routeMode, points, router, rsc, avoidRoadsIds);
		if (rsc[0] != null) {
			StringBuilder url = new StringBuilder(rsc[0].url);
			url.append(String.format("?point=%.6f,%.6f", start.getLatitude(), start.getLongitude()));
			url.append(String.format("&point=%.6f,%.6f", end.getLatitude(), end.getLongitude()));
			
	        RestTemplate restTemplate = new RestTemplate();
	        restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
				
				@Override
				public void handleError(ClientHttpResponse response) throws IOException {
					LOGGER.error(String.format("Error handling url for %s : %s", routeMode, url.toString()));
					super.handleError(response);
				}
			});
	        String gpx = restTemplate.getForObject(url.toString(), String.class);
	        
	        GPXFile file = GPXUtilities.loadGPXFile(new ByteArrayInputStream(gpx.getBytes()));
			TrkSegment trkSegment = file.tracks.get(0).segments.get(0);
			List<LatLon> polyline = new ArrayList<LatLon>(trkSegment.points.size());
			for (WptPt p : trkSegment.points) {
				polyline.add(new LatLon(p.lat, p.lon));
			}
			return approximate(ctx, router, props, polyline);
		} else { 
			PrecalculatedRouteDirection precalculatedRouteDirection = null;
			List<RouteSegmentResult> route = router.searchRoute(ctx, start, end, intermediates, precalculatedRouteDirection);
			putResultProps(ctx, route, props);
			return route;
		}
	}
	

	private void putResultProps(RoutingContext ctx, List<RouteSegmentResult> route, Map<String, Object> props) {
		float completeTime = 0;
		float completeDistance = 0;
		if (route != null) {
			for (RouteSegmentResult r : route) {
				completeTime += r.getSegmentTime();
				completeDistance += r.getDistance();
			}
		}
		TreeMap<String, Object> overall = new TreeMap<String, Object>();
		props.put("overall", overall);
		overall.put("distance", completeDistance);
		overall.put("time", completeTime);
		overall.put("routingTime", ctx.routingTime);
		TreeMap<String, Object> dev = new TreeMap<String, Object>();
		props.put("dev", dev);
		dev.put("nativeLib", ctx.nativeLib != null);
		dev.put("alertFasterRoadToVisitedSegments", ctx.alertFasterRoadToVisitedSegments);
		dev.put("alertSlowerSegmentedWasVisitedEarlier", ctx.alertSlowerSegmentedWasVisitedEarlier);
		RouteCalculationProgress p = ctx.calculationProgress;
		if (p != null) {
			overall.put("calcTime", (float) p.timeToCalculate / 1.0e9);
			dev.putAll(p.getInfo(ctx.calculationProgressFirstPhase));
		}
		if (ctx.calculationProgressFirstPhase != null) {
			props.put("devbase", ctx.calculationProgressFirstPhase.getInfo(null));
		}
	}

	private QuadRect points(List<LatLon> intermediates, LatLon start, LatLon end) {
		QuadRect upd = null;
		upd = addPnt(start, upd);
		upd = addPnt(end, upd);
		if (intermediates != null) {
			for (LatLon i : intermediates) {
				upd = addPnt(i, upd);
			}
		}
		return upd;
	}

	private QuadRect addPnt(LatLon pnt, QuadRect upd) {
		if (pnt == null) {
			return upd;
		}
		int y = MapUtils.get31TileNumberY(pnt.getLatitude());
		int x = MapUtils.get31TileNumberX(pnt.getLongitude());
		if (upd == null) {
			upd = new QuadRect(x, y, x, y);
		} else {
			if (upd.left > x) {
				upd.left = x;
			}
			if (upd.right < x) {
				upd.right = x;
			}
			if (upd.bottom < y) {
				upd.bottom = y;
			}
			if (upd.top > y) {
				upd.top = y;
			}
		}
		return upd;
	}
	

	private synchronized void initNewObfFiles(File target, File targetTemp) throws IOException, FileNotFoundException {
		initObfReaders();
		long val = System.currentTimeMillis();
		BinaryMapIndexReaderReference ref = obfFiles.get(target.getAbsolutePath());
		if(ref == null) {
			ref = new BinaryMapIndexReaderReference();
			ref.file = target;
			obfFiles.put(target.getAbsolutePath(), ref);
		}
		if (ref.fileIndex != null) {
			ref.fileIndex = null;
		}
		if (ref.reader != null) {
			ref.reader.close();
		}
		if (nativelib != null) {
			nativelib.closeMapFile(target.getAbsolutePath());
		}
		target.delete();
		targetTemp.renameTo(target);
		RandomAccessFile raf = new RandomAccessFile(target, "r"); //$NON-NLS-1$ //$NON-NLS-2$
		ref.reader = new BinaryMapIndexReader(raf, target);
		ref.fileIndex = cacheFiles.addToCache(ref.reader, target);
		cacheFiles.writeToFile(new File(config.cacheLocation, CachedOsmandIndexes.INDEXES_DEFAULT_FILENAME));
		if (nativelib != null) {
			nativelib.initMapFile(target.getAbsolutePath(), false);
		}
		LOGGER.info("Init new obf file " + target.getName() + " " + (System.currentTimeMillis() - val) + " ms");
	}
	
	public synchronized BinaryMapIndexReader[] getObfReaders(QuadRect quadRect) throws IOException {
		initObfReaders();
		List<BinaryMapIndexReader> files = new ArrayList<>();
		MapsCollection mapsCollection = new MapsCollection(true);
		for (BinaryMapIndexReaderReference ref : obfFiles.values()) {
			boolean intersects;
			fileOverlaps: for (RoutingPart rp : ref.fileIndex.getRoutingIndexList()) {
				for (RoutingSubregion s : rp.getSubregionsList()) {
					intersects = quadRect.left <= s.getRight() && quadRect.right >= s.getLeft()
							&& quadRect.top <= s.getBottom() && quadRect.bottom >= s.getTop();
					if (intersects) {
						mapsCollection.add(ref.file);
						break fileOverlaps;
					}
				}
			}
		}
		for (File f : mapsCollection.getFilesToUse()) {
			BinaryMapIndexReaderReference ref = obfFiles.get(f.getAbsolutePath());
			if (ref.reader == null) {
				long val = System.currentTimeMillis();
				RandomAccessFile raf = new RandomAccessFile(ref.file, "r"); //$NON-NLS-1$ //$NON-NLS-2$
				ref.reader = cacheFiles.initReaderFromFileIndex(ref.fileIndex, raf, ref.file);
				LOGGER.info("Initializing routing file " + ref.file.getName() + " " + (System.currentTimeMillis() - val) + " ms");
			}
			files.add(ref.reader);
		}
		return files.toArray(new BinaryMapIndexReader[0]);
	}
	
	public synchronized void initObfReaders() throws IOException {
		if (cacheFiles != null) {
			return;
		}
		File mapsFolder = new File(config.obfLocation);
		cacheFiles = new CachedOsmandIndexes();
		if (mapsFolder.exists()) {
			File cacheFile = new File(mapsFolder, CachedOsmandIndexes.INDEXES_DEFAULT_FILENAME);
			if (cacheFile.exists()) {
				cacheFiles.readFromFile(cacheFile, 2);
			}
			for (File obf : Algorithms.getSortedFilesVersions(mapsFolder)) {
				if (obf.getName().endsWith(".obf")) {
					BinaryMapIndexReaderReference ref = obfFiles.get(obf.getAbsolutePath());
					if (ref == null) {
						ref = new BinaryMapIndexReaderReference();
						ref.file = obf;
						ref.fileIndex = cacheFiles.getFileIndex(obf, true);
						obfFiles.put(obf.getAbsolutePath(), ref);
					}
				}
			}
			cacheFiles.writeToFile(cacheFile);
		}
	}
	
	public OsmandRegions getOsmandRegions() {
		return osmandRegions;
	}
	
	public File getObf(Map<String, GPXFile> files)
			throws IOException, SQLException, XmlPullParserException, InterruptedException {
		File tmpOsm = File.createTempFile("gpx_obf", ".osm.gz");
		File tmpFolder = new File(tmpOsm.getParentFile(), String.valueOf(System.currentTimeMillis()));
		String fileName = "gpx_" + System.currentTimeMillis();
		OsmGpxWriteContext.QueryParams qp = new OsmGpxWriteContext.QueryParams();
		qp.osmFile = tmpOsm;
		qp.details = OsmGpxWriteContext.QueryParams.DETAILS_ELE_SPEED;
		OsmGpxWriteContext writeCtx = new OsmGpxWriteContext(qp);
		File targetObf = new File(tmpFolder.getParentFile(), fileName + IndexConstants.BINARY_MAP_INDEX_EXT);
		writeCtx.writeObf(files, null, tmpFolder, fileName, targetObf);
		
		if (!qp.osmFile.delete()) {
			qp.osmFile.deleteOnExit();
		}
		
		return targetObf;
	}
	
	public synchronized RoutingController.FeatureCollection searchPoi(double lat, double lon, int zoom, int radius) throws IOException {
		QuadRect bbox = getBboxRadius(lat, lon, radius, zoom);
		BinaryMapIndexReader[] list = getObfReaders(bbox);
		List<Amenity> results = new ArrayList<>();
		for (BinaryMapIndexReader reader : list) {
			results.addAll(reader.searchPoi(buildSearchPoiRequest((int) bbox.left,
					(int) bbox.right, (int) bbox.top, (int) bbox.bottom, zoom, ACCEPT_ALL_POI_TYPE_FILTER, null)));
		}
		if (!results.isEmpty() && results.size() < MAX_SIZE_POI) {
			List<RoutingController.Feature> features = new ArrayList<>();
			for (Amenity amenity : results) {
				PoiType poiType = amenity.getType().getPoiTypeByKeyName(amenity.getSubType());
				RoutingController.Feature feature;
				if (poiType != null) {
					feature = new RoutingController.Feature(RoutingController.Geometry.point(amenity.getLocation()))
							.prop("name", amenity.getName())
							.prop("color", amenity.getColor())
							.prop("iconKeyName", poiType.getIconKeyName())
							.prop("typeOsmTag", poiType.getOsmTag())
							.prop("typeOsmValue", poiType.getOsmValue())
							.prop("iconName", getIconName(poiType))
							.prop("type", amenity.getType().getKeyName())
							.prop("subType", amenity.getSubType());
					
					for (String e : amenity.getAdditionalInfoKeys()) {
						feature.prop(e, amenity.getAdditionalInfo(e));
					}
					features.add(feature);
				}
			}
			return new RoutingController.FeatureCollection(features.toArray(new RoutingController.Feature[0]));
		} else {
			return null;
		}
	}
	
	private String getIconName(PoiType poiType) {
		if (poiType != null) {
			if (poiType.getParentType() != null) {
				return poiType.getParentType().getIconKeyName();
			} else if (poiType.getFilter() != null) {
				return poiType.getFilter().getIconKeyName();
			} else if (poiType.getCategory() != null) {
				return poiType.getCategory().getIconKeyName();
			}
		}
		return null;
	}
	
	private QuadRect getBboxRadius(double lat, double lon, int radiusMeters, int zoom) {
		double dx = MapUtils.getTileNumberX(zoom, lon);
		double half16t = MapUtils.getDistance(lat, MapUtils.getLongitudeFromTile(zoom, ((int) dx) + 0.5),
				lat, MapUtils.getLongitudeFromTile(zoom, (int) dx));
		double cf31 = (radiusMeters / (half16t * 2)) * (1 << 15);
		double y = MapUtils.get31TileNumberY(lat);
		double x = MapUtils.get31TileNumberX(lon);
		double left = (int) (x - cf31);
		double top = (int) (x + cf31);
		double right = (int) (y - cf31);
		double bottom = (int) (y + cf31);
		
		return new QuadRect(left, right, top, bottom);
	}
	
}
