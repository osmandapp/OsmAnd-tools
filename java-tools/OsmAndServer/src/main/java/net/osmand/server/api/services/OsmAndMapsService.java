package net.osmand.server.api.services;

import static net.osmand.util.MapUtils.rhumbDestinationPoint;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.annotation.Nullable;
import javax.imageio.ImageIO;

import net.osmand.router.*;
import net.osmand.shared.gpx.GpxFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.osmand.IndexConstants;
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
import net.osmand.map.WorldRegion;
import net.osmand.obf.OsmGpxWriteContext;
import net.osmand.render.RenderingRuleProperty;
import net.osmand.render.RenderingRulesStorage;
import net.osmand.router.HHRouteDataStructure.HHRoutingConfig;
import net.osmand.router.HHRouteDataStructure.HHRoutingContext;
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;
import net.osmand.router.RoutePlannerFrontEnd.GpxPoint;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RouteResultPreparation.RouteCalcResult;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

@Service
public class OsmAndMapsService {
	private static final Log LOGGER = LogFactory.getLog(OsmAndMapsService.class);

	private static final int MAX_RUNTIME_IMAGE_CACHE_SIZE = 80;
	private static final int MAX_RUNTIME_TILES_CACHE_SIZE = 10000;
	private static final int MAX_FILES_PER_FOLDER = 1 << 12; // 4096
	private static final int ZOOM_EN_PREFERRED_LANG = 6;

	private static final int MEM_LIMIT = RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT * 8;

	private static final long INTERVAL_TO_MONITOR_ZIP = 15 * 60 * 1000;

	// counts only files open for Java (doesn't fit for rendering / routing)
	private static final int MAX_SAME_FILE_OPEN = 15;
	private static final long MAX_TIME_ROUTING_FILE = 4 * 60 * 60;
	private static final int HINT_SAME_ROUTING_CONTEXT_OPEN = 5;
	private static final int MAX_SAME_PROFILE_DF = 1;
	private static final int MAX_SAME_ROUTING_CONTEXT_OPEN = 8;
	private static final Map<String, Integer> MAX_SAME_PROFILE = Map.of("car", 2, "bicycle", 2, "pedestrian", 2);
	private static final long MAX_SAME_PROFILE_WAIT_MS = 6000;


	private static final String INTERACTIVE_KEY = "int";
	private static final String DEFAULT_INTERACTIVE_STYLE = "hd";
	private static final String INTERACTIVE_STYLE_DELIMITER = "-";
	
	
	Map<String, BinaryMapIndexReaderReference> obfFiles = new LinkedHashMap<>();

	CachedOsmandIndexes cacheFiles = null;

	AtomicInteger cacheTouch = new AtomicInteger(0);

	Map<String, VectorMetatile> tileCache = new ConcurrentHashMap<>();

	List<RoutingCacheContext> routingCaches = new ArrayList<>();

	NativeJavaRendering nativelib;

	File tempDir;

	Integer geotiffTileLock = 0;

	@Autowired
	VectorTileServerConfig config;

	@Autowired
	RoutingServerConfig routingConfig;

	OsmandRegions osmandRegions;

	@Value("${tile-server.routeObf.location}")
	String routeObfLocation;

	public enum ServerRoutingTypes {
		HH_JAVA("HH Java"),
		HH_CPP("HH C++"),
		ASTAR_NORMAL_JAVA("A* Normal Java"),
		ASTAR_NORMAL_CPP("A* Normal C++"),
		ASTAR_2PHASE_JAVA("A* 2-phase Java"),
		ASTAR_2PHASE_CPP("A* 2-phase C++");
		private final String description;
		ServerRoutingTypes(String description) {
			this.description = description;
		}
		public static Map<String, String> getSelectList(boolean car) {
			Map<String, String> list = new LinkedHashMap<>();
			for (ServerRoutingTypes type : ServerRoutingTypes.values()) {
				if (!car && (type == ASTAR_2PHASE_JAVA || type == ASTAR_2PHASE_CPP)) {
					continue;
				}
				list.put(type.name().toLowerCase(), type.description);
			}
			return list;
		}

		public boolean isUsingNativeLib() {
			return this == HH_CPP || this == ASTAR_NORMAL_CPP || this == ASTAR_2PHASE_CPP;
		}

		public boolean isOldRouting() {
			return this != HH_JAVA && this != HH_CPP;
		}

		public boolean is2phaseRouting() {
			return this == ASTAR_2PHASE_JAVA || this == ASTAR_2PHASE_CPP || this == HH_JAVA || this == HH_CPP;
		}
	}

	public enum ServerApproximationTypes {
		GEO_JAVA("Geometry-based Java"),
		GEO_CPP("Geometry-based C++"),
		ROUTING_JAVA("Routing-based Java"),
		ROUTING_CPP("Routing-based C++");
		private final String description;
		ServerApproximationTypes(String description) {
			this.description = description;
		}
		public static Map<String, String> getSelectList() {
			Map<String, String> list = new LinkedHashMap<>();
			for (ServerApproximationTypes type : ServerApproximationTypes.values()) {
				list.put(type.name().toLowerCase(), type.description);
			}
			return list;
		}

		public boolean isGeometryBased() {
			return this == GEO_JAVA || this == GEO_CPP;
		}

		public boolean isUsingNativeLib() {
			return this == GEO_CPP || this == ROUTING_CPP;
		}
	}

	public class RoutingCacheContext {
		String profile;
		String routeParamsStr = "";
		RoutingContext rCtx;
		HHRoutingContext<NetworkDBPoint> hCtx;
		long locked;
		long created;
		int used;
		HHRoutingConfig hhConfig;

		@Override
		public String toString() {
			String p = profile.length() > 5 ? profile.substring(0, 5) : profile;
			return (locked == 0 ? '\u25FB' : '\u25FC')
					+ String.format("%s %s %d, %s min", p, routeParamsStr, used, (System.currentTimeMillis() - created) / 60 / 1000);
		}

		public double importance() {
			double minutesAlive = (System.currentTimeMillis() - created) / 1000.0 / 60.0;
			return (used + 1) / minutesAlive;
		}

	}

	public class BinaryMapIndexReaderReference {
		File file;
		private static final int WAIT_LOCK_CHECK = 10;
		ConcurrentHashMap<BinaryMapIndexReader, Boolean> readers = new ConcurrentHashMap<>();
		public FileIndex fileIndex;

		private synchronized void closeUnusedReaders() {
			readers.forEach((reader, open) -> {
				if (Boolean.TRUE.equals(open)) {
					try {
						reader.close();
						readers.remove(reader);
					} catch (IOException e) {
						LOGGER.error(e.getMessage(), e);
					}
				}
			});
		}

		private synchronized BinaryMapIndexReader lockReader() {
			for (Entry<BinaryMapIndexReader, Boolean> r : readers.entrySet()) {
				if (Boolean.TRUE.equals(r.getValue())) {
					r.setValue(false);
					return r.getKey();
				}
			}
			return null;
		}

		public void unlockReader(BinaryMapIndexReader reader) {
			if (reader != null && !readers.isEmpty()) {
				readers.computeIfPresent(reader, (key, value) -> true);
			}
		}

		public BinaryMapIndexReader getReader(CachedOsmandIndexes cacheFiles, int maxWaitMs) throws IOException, InterruptedException {
			BinaryMapIndexReader resReader = lockReader();
			if (resReader != null) {
				return resReader;
			}
			if (readers.size() < MAX_SAME_FILE_OPEN) {
				if (cacheFiles == null) {
					initObfReaders();
				}
				BinaryMapIndexReader newReader = createReader();
				if (newReader != null) {
					readers.put(newReader, true);
				}
			}
			int ms = 0;
			while (ms < maxWaitMs) {
				Thread.sleep(WAIT_LOCK_CHECK);
				ms += WAIT_LOCK_CHECK;
				resReader = lockReader();
				if (resReader != null) {
					return resReader;
				}
			}
			LOGGER.info("Failed to get a reader for the file " + file.getName());
			return null;
		}

		private BinaryMapIndexReader createReader() throws IOException {
			if (cacheFiles != null) {
				RandomAccessFile raf = new RandomAccessFile(file, "r");
				BinaryMapIndexReader reader = cacheFiles.initReaderFromFileIndex(fileIndex, raf, file);
				return reader;
			}
			LOGGER.info("Failed to create a reader for the file " + file.getName());
			return null;
		}

		public int getOpenFiles() {
			int cnt = 0;
			for (Boolean b : readers.values()) {
				if (Boolean.TRUE.equals(b)) {
					cnt++;
				}
			}
			return cnt;
		}
	}

	public List<BinaryMapIndexReader> getReaders(List<BinaryMapIndexReaderReference> refs, boolean[] incompleteFlag) {
		List<BinaryMapIndexReader> res = new ArrayList<>();
		for (BinaryMapIndexReaderReference ref : refs) {
			BinaryMapIndexReader reader = null;
			try {
				reader = ref.getReader(cacheFiles, 1000);
			} catch (IOException | InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			}
			if (reader != null) {
				res.add(reader);
			} else if (incompleteFlag != null) {
				incompleteFlag[0] = true;
			}
		};
		return res;
	}

	public void unlockReaders(List<BinaryMapIndexReader> mapsReaders) {
		obfFiles.values().forEach(ref -> mapsReaders.forEach(ref::unlockReader));
	}

	@Scheduled(fixedRate = INTERVAL_TO_MONITOR_ZIP)
	public void closeMapReaders() {
		obfFiles.forEach((name, ref) -> {
			if (!ref.readers.isEmpty()) {
				ref.closeUnusedReaders();
			}
		});
	}

	@Scheduled(fixedRate = INTERVAL_TO_MONITOR_ZIP)
	public void cleanUpRoutingFiles() {
		List<RoutingCacheContext> removed = new ArrayList<>();
		synchronized (routingCaches) {
			Collections.sort(routingCaches, new Comparator<RoutingCacheContext>() {

				@Override
				public int compare(RoutingCacheContext o1, RoutingCacheContext o2) {
					return Double.compare(o1.importance(), o2.importance());
				}
			});
			System.out.println("Prepare to clean up global routing contexts " + routingCaches);
			while (routingCaches.size() > HINT_SAME_ROUTING_CONTEXT_OPEN) {
				RoutingCacheContext r = routingCaches.remove(routingCaches.size() - 1);
				removed.add(r);
			}
			for (int i = 0; i < routingCaches.size();) {
				RoutingCacheContext check = routingCaches.get(i);
				if ((System.currentTimeMillis() - check.created) / 1000l >= MAX_TIME_ROUTING_FILE) {
					removed.add(routingCaches.remove(i));
				} else {
					i++;
					if (check.locked == 0 && check.hCtx != null) {
						check.hCtx.clearSegments();
					}
				}
			}
		}
		if (removed.size() > 0) {
			for (RoutingCacheContext r : removed) {
				BinaryMapIndexReader reader = r.rCtx.map.keySet().iterator().next();
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			System.gc();
			System.out.printf("Clean up %d global routing contexts, state - %s \n", removed.size(), routingCaches);
			long MEMORY_LAST_USED_MB = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20;
			System.out.printf("***** Memory used %d MB *****\n", MEMORY_LAST_USED_MB);
		}
	}


	public static class RoutingServerConfigEntry {
		public String type;
		public String url;
		public String name;
		public String profile = "car";
	}


	public int getCurrentOpenJavaFiles() {
		int cnt = 0;
		for(BinaryMapIndexReaderReference r: obfFiles.values()) {
			cnt+= r.getOpenFiles();
		}
		return cnt;
	}

	@Configuration
	@ConfigurationProperties("osmand.routing")
	public static class RoutingServerConfig {

		@Value("${osmand.routing.hh-only-limit}") // --osmand.routing.hh-only-limit= or $HH_ONLY_LIMIT=
		public int hhOnlyLimit; // See application.yml, set 100 for production, or 1000 for testing server (km)

		public Map<String, RoutingServerConfigEntry> config = new TreeMap<>(new ProfileComparator());

		private class ProfileComparator implements Comparator<String> {
			// reorder *-bike, *-car, *-foot to car, bike, foot
			@Override
			public int compare(String s1, String s2) {
				// -car -> 0-car, -bike -> 1-bike, -foot -> 2-foot
				s1 = reorder(s1, "-car", "-bike", "-foot");
				s2 = reorder(s2, "-car", "-bike", "-foot");
				return s1.compareTo(s2);
			}
			private String reorder(String s, String... search) {
				String result = s;
				for (int i = 0; i < search.length; i++) {
					if(s.contains(search[i])) {
						result = Integer.toString(i) + s;
					}
				}
				return result;
			}
		}

		public void setConfig(Map<String, String> style) {
			for (Entry<String, String> e : style.entrySet()) {

				/**
				 * Example boot/command-line parameters (osmand-server-boot.conf):
				 *
				 * --osmand.routing.config.osrm-bicycle=type=osrm,profile=bicycle,url=https://zlzk.biz/route/v1/bike/
				 * --osmand.routing.config.rescuetrack=type=online,url=https://apps.rescuetrack.com/api/routing/osmand
				 *
				 * name= osrm-bicycle|rescuetrack (Name, as a part of the option)
				 * type= osrm|online (Type of routing provider, osrm and online are supported)
				 * profile= [car]|bicycle (Profile for route-approximation params, default is car)
				 */

				RoutingServerConfigEntry src = new RoutingServerConfigEntry();
				src.name = e.getKey(); // name --osmand.routing.config.[rescuetrack]
				for (String s : e.getValue().split(",")) {
					String value = s.substring(s.indexOf('=') + 1);
					if (s.startsWith("type=")) { // osrm|online
						src.type = value;
					} else if (s.startsWith("url=")) { // online-provider base url
						src.url = value;
					} else if (s.startsWith("profile=")) { // osmand profile to catch routeMode params
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
		private JsonObject info;
		private final String interactiveKey;

		public VectorMetatile(VectorTileServerConfig cfg, String tileId, VectorStyle style, int z, int left, int top,
				int metaSizeLog, int tileSizeLog, String interactiveKey) {
			this.cfg = cfg;
			this.style = style;
			this.metaSizeLog = metaSizeLog;
			this.tileSizeLog = tileSizeLog;
			this.key = tileId;
			this.left = left;
			this.top = top;
			this.z = z;
			this.interactiveKey = interactiveKey;
			touch();
		}
		
		public String getInteractiveKey() {
			return interactiveKey;
		}
		
		public void setInfo(JsonObject info) {
			this.info = info;
		}
		
		public JsonObject getInfo() {
			return info;
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
			File cf = getCacheFile(".png");
			if (cf != null && cf.exists()) {
				runtimeImage = ImageIO.read(cf);
				return runtimeImage;
			}
			return null;
		}
		
		public JsonObject getCacheRuntimeInfo() throws IOException {
			JsonObject tileInfo = getInfo();
			if (tileInfo != null) {
				return tileInfo;
			}
			File cf = getCacheFile(".json.gz");
			if (cf != null && cf.exists()) {
				try (GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(cf));
				     Reader reader = new InputStreamReader(gzip, StandardCharsets.UTF_8)) {
					JsonParser parser = new JsonParser();
					tileInfo = parser.parse(reader).getAsJsonObject();
					return tileInfo;
				}
			}
			return null;
		}

		public File getCacheFile(String ext) {
			if (z > cfg.maxZoomCache || cfg.cacheLocation == null || cfg.cacheLocation.isEmpty()) {
				return null;
			}
			int x = left >> (31 - z) >> metaSizeLog;
			int y = top >> (31 - z) >> metaSizeLog;
			StringBuilder loc = new StringBuilder();
			
//			if (ext.equals(".json")) {
//				loc.append("info").append("/");
//			}
			
			loc.append(interactiveKey != null ? interactiveKey : style.key).append("/").append(z);
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
			loc.append("-").append(metaSizeLog).append("-").append(tileSizeLog).append(ext);
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
					if (!target.exists() || target.lastModified() < zipFile.lastModified()
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

	public VectorMetatile getMetaTile(VectorStyle vectorStyle, int z, int x, int y, String interactiveKey) {
		int metaSizeLog = Math.min(vectorStyle.metaTileSizeLog, z - 1);
		int left = ((x >> metaSizeLog) << metaSizeLog) << (31 - z);
		if (left < 0) {
			left = 0;
		}
		int top = ((y >> metaSizeLog) << metaSizeLog) << (31 - z);
		if (top < 0) {
			top = 0;
		}
		String key = interactiveKey != null ? interactiveKey : vectorStyle.key;
		String tileId = encode(key, left >> (31 - z), top >> (31 - z), z, metaSizeLog,
				vectorStyle.tileSizeLog);
		VectorMetatile tile = tileCache.get(tileId);
		if (tile == null) {
			tile = new VectorMetatile(config, tileId, vectorStyle, z, left, top, metaSizeLog, vectorStyle.tileSizeLog, interactiveKey);
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

			}
		}
		return config.initErrorMessage == null;
	}
	public String validateNativeLib() {
		if (nativelib == null) {
			return "Tile rendering engine is not initialized";
		}
		return null;
	}

	public ResponseEntity<String> renderMetaTile(VectorMetatile tile)
			throws IOException, XmlPullParserException, SAXException {
		// don't synchronize this to not block routing
		synchronized (config) {
			// for local debug :
			// VectorMetatile rendered = null;
			VectorMetatile rendered = tileCache.get(tile.key);
			if (rendered != null && rendered.runtimeImage != null) {
				tile.runtimeImage = rendered.runtimeImage;
				tile.setInfo(rendered.getInfo());
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
			
			if (tile.getInteractiveKey() != null) {
				ctx.saveTextTile = true;
			}
			
			if (ctx.width > 8192) {
				return ResponseEntity.badRequest().body("Metatile exceeds 8192x8192 size");

			}
			if (imgTileSize != ctx.width << tile.tileSizeLog || imgTileSize != ctx.height << tile.tileSizeLog) {
				return ResponseEntity.badRequest().body(String.format("Metatile has wrong size (%d != %d)", imgTileSize,
						ctx.width << tile.tileSizeLog));
			}
			
			NativeJavaRendering.RenderingImageResult result = nativelib.renderImage(ctx);
			tile.runtimeImage = result.getImage();
			if (tile.runtimeImage != null) {
				tile.setInfo(result.getGenerationResult().getInfo());
				File cacheFile = tile.getCacheFile(".png");
				if (cacheFile != null) {
					cacheFile.getParentFile().mkdirs();
					if (cacheFile.getParentFile().exists()) {
						ImageIO.write(tile.runtimeImage, "png", cacheFile);
						buildCacheFileInfo(tile);
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
	
	private void buildCacheFileInfo(VectorMetatile tile) throws IOException {
		File cacheFileInfo = tile.getCacheFile(".json.gz");
		if (cacheFileInfo != null) {
			cacheFileInfo.getParentFile().mkdirs();
			if (cacheFileInfo.getParentFile().exists()) {
				JsonObject info = tile.getInfo();
				if (info != null) {
					try (GZIPOutputStream gzip = new GZIPOutputStream(new FileOutputStream(cacheFileInfo));
					     Writer writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8)) {
						writer.write(info.toString());
					}
				}
			}
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

	public List<GeocodingResult> geocoding(double lat, double lon) throws IOException, InterruptedException {
		QuadRect points = points(null, new LatLon(lat, lon), new LatLon(lat, lon));
		List<GeocodingResult> complete;
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		try {
			List<BinaryMapIndexReaderReference> list = getObfReaders(points, null, 0, "geocoding");
			boolean[] incomplete = new boolean[1];
			usedMapList = getReaders(list, incomplete);
			if (incomplete[0]) {
				return Collections.emptyList();
			}
			RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
			RoutingContext ctx = prepareRouterContext(new RouteParameters("geocoding"), router, usedMapList, false);
			GeocodingUtilities su = new GeocodingUtilities();
			List<GeocodingResult> res = su.reverseGeocodingSearch(ctx, lat, lon, false);
			complete = su.sortGeocodingResults(usedMapList, res);
		} finally {
			unlockReaders(usedMapList);
		}
		return complete != null ? complete : Collections.emptyList();
	}


	public List<RouteSegmentResult> gpxApproximation(String routeMode, Map<String, Object> props, GPXFile file) throws IOException, InterruptedException {
		if (!file.hasTrkPt()) {
			return Collections.emptyList();
		}
		TrkSegment trkSegment = file.tracks.get(0).segments.get(0);
		return approximateByWaypoints(trkSegment.points, routeMode, props);
	}

	public List<RouteSegmentResult> approximateRoute(List<WebGpxParser.Point> points, String routeMode) throws IOException, InterruptedException {
		List<WptPt> waypoints = new ArrayList<>();
		for (WebGpxParser.Point p : points) {
			waypoints.add(new WptPt(p.lat, p.lng));
		}
		Map<String, Object> props = new HashMap<>();
		return approximateByWaypoints(waypoints, routeMode, props);
	}

	private List<RouteSegmentResult> approximateByWaypoints(List<WptPt> waypoints, String routeMode, Map<String, Object> props) throws IOException, InterruptedException {
		List<RouteSegmentResult> route;
		List<LatLon> polyline = new ArrayList<>();
		for (WptPt wpt : waypoints) {
			polyline.add(new LatLon(wpt.lat, wpt.lon));
		}
		QuadRect quadRect = points(polyline, null, null);
		if (!validateAndInitConfig()) {
			return Collections.emptyList();
		}
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		try {
			List<BinaryMapIndexReaderReference> list = getObfReaders(quadRect, null, 0, "approximate");
			boolean[] incomplete = new boolean[1];
			usedMapList = getReaders(list, incomplete);
			if (incomplete[0]) {
				return new ArrayList<RouteSegmentResult>();
			}
			RouteParameters rp = parseRouteParameters(routeMode);
			RoutingContext ctx = prepareRouterContext(rp, router, usedMapList, true);
			route = approximate(ctx, router, props, waypoints, rp.useExternalTimestamps);
		} finally {
			unlockReaders(usedMapList);
		}
		return route;
	}

	public List<RouteSegmentResult> approximate(RoutingContext ctx, RoutePlannerFrontEnd router,
	                                            Map<String, Object> props, List<WptPt> waypoints,
	                                            boolean useExternalTimestamps)
			throws IOException, InterruptedException {
		if (ctx.nativeLib != null || router.isUseNativeApproximation()) {
			return approximateSyncNative(ctx, router, props, waypoints, useExternalTimestamps);
		}
		return approximateInternal(ctx, router, props, waypoints, useExternalTimestamps);
	}

	private synchronized List<RouteSegmentResult> approximateSyncNative(RoutingContext ctx, RoutePlannerFrontEnd router,
	                                                                    Map<String, Object> props, List<WptPt> waypoints,
	                                                                    boolean useExternalTimestamps)
			throws IOException, InterruptedException {
		return approximateInternal(ctx, router, props, waypoints, useExternalTimestamps);
	}

	private synchronized List<RouteSegmentResult> approximateInternal(RoutingContext ctx, RoutePlannerFrontEnd router,
	                                                                  Map<String, Object> props, List<WptPt> waypoints,
	                                                                  boolean useExternalTimestamps)
			throws IOException, InterruptedException {
		GpxRouteApproximation gctx = new GpxRouteApproximation(ctx);
		List<GpxPoint> gpxPoints = router.generateGpxPoints(gctx, new LocationsHolder(waypoints));
		GpxRouteApproximation r = router.searchGpxRoute(gctx, gpxPoints, null, useExternalTimestamps);
		List<RouteSegmentResult> route = r.collectFinalPointsAsRoute();
		if (router.isUseNativeApproximation()) {
			RouteResultPreparation preparation = new RouteResultPreparation();
			// preparation.prepareTurnResults(gctx.ctx, route);
			preparation.addTurnInfoDescriptions(route);
		}
		putResultProps(ctx, route, props);
		return route;
	}


	private static class RouteParameters {
		String routeProfile;
		Map<String, String> routeParams = new LinkedHashMap<String, String>();
		public RouteParameters(String p) {
			this.routeProfile = p;
			updateRoutingType(defaultRoutingType);
			updateApproximationType(defaultApproximationType);
		}

		public void updateRoutingType(ServerRoutingTypes type) {
			this.disableHHRouting = type.isOldRouting();
			this.useNativeRouting = type.isUsingNativeLib();
			this.calcMode = type.is2phaseRouting() ? null /* auto-COMPLEX-mode */ : RouteCalculationMode.NORMAL;
		}

		public void updateApproximationType(ServerApproximationTypes type) {
			this.useGeometryBasedApproximation = type.isGeometryBased();
			this.useNativeApproximation = type.isUsingNativeLib();
		}

		boolean useOnlyHHRouting = false;
		boolean useNativeApproximation = false;
		boolean useGeometryBasedApproximation = false;
		boolean useExternalTimestamps = false;
		boolean useNativeRouting = false;
		boolean noGlobalFile = false; // "noglobalfile"
		RouteCalculationMode calcMode = null;
		public boolean disableHHRouting;
		public RoutingServerConfigEntry onlineRouting;

		private final ServerRoutingTypes defaultRoutingType = ServerRoutingTypes.HH_JAVA;
		private final ServerApproximationTypes defaultApproximationType = ServerApproximationTypes.GEO_JAVA;
	}

	private RouteParameters parseRouteParameters(String routeMode) {
		String[] props = routeMode.split("\\,");
		RouteParameters r = new RouteParameters(props[0]);
		for (int i = 1; i < props.length; i++) {
			String p = props[i];
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
			if ("routing".equals(key)) {
				ServerRoutingTypes type = value.isEmpty()
						? r.defaultRoutingType
						: ServerRoutingTypes.valueOf(value.toUpperCase());
				r.updateRoutingType(type);
			} else if ("approximation".equals(key)) {
				ServerApproximationTypes type = value.isEmpty()
						? r.defaultApproximationType
						: ServerApproximationTypes.valueOf(value.toUpperCase());
				r.updateApproximationType(type);
			} else if ("noglobalfile".equals(key)) {
				r.noGlobalFile = Boolean.parseBoolean(value);
			} else if ("hhonly".equals(key)) {
				r.useOnlyHHRouting = Boolean.parseBoolean(value);
			} else if ("gpxtimestamps".equals(key)) {
				r.useExternalTimestamps = Boolean.parseBoolean(value);
			} else {
				r.routeParams.put(key, value);
			}
		}
		if (routingConfig.config.containsKey(r.routeProfile)) {
			RoutingServerConfigEntry entry = routingConfig.config.get(r.routeProfile);
			r.routeProfile = entry.profile;
			r.onlineRouting = entry;
		}

		return r;
	}

	private RoutingContext prepareRouterContext(RouteParameters rp, RoutePlannerFrontEnd router,
	                                            List<BinaryMapIndexReader> usedMapList,
	                                            boolean approximation) throws IOException, InterruptedException
	{
		boolean useNativeLib = approximation ? rp.useNativeApproximation : rp.useNativeRouting;

		if (rp.onlineRouting != null && rp.useNativeApproximation) {
			useNativeLib = true; // rescuetrack + approximation
		}

		router.setUseNativeApproximation(rp.useNativeApproximation);
		router.setUseGeometryBasedApproximation(rp.useGeometryBasedApproximation);

		router.CALCULATE_MISSING_MAPS = false;
		if (rp.disableHHRouting) {
			router.disableHHRoutingConfig();
		} else {
			router.setHHRouteCpp(rp.useNativeRouting);
			router.setUseOnlyHHRouting(rp.useOnlyHHRouting);
			router.setDefaultHHRoutingConfig();
		}

		Builder cfgBuilder = RoutingConfiguration.getDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(MEM_LIMIT, MEM_LIMIT);
		RoutingConfiguration config = cfgBuilder.build(rp.routeProfile, /* RoutingConfiguration.DEFAULT_MEMORY_LIMIT */ memoryLimit, rp.routeParams);

		String minPointApproximationString = rp.routeParams.get("minPointApproximation");
		if (minPointApproximationString != null) {
			config.minPointApproximation = Float.parseFloat(minPointApproximationString);
		}

		config.routeCalculationTime = System.currentTimeMillis();
		final RoutingContext ctx = router.buildRoutingContext(config, useNativeLib ? nativelib : null,
				usedMapList.toArray(new BinaryMapIndexReader[0]), rp.calcMode);
		ctx.leftSideNavigation = false;
		return ctx;
	}

	@Nullable
	private List<RouteSegmentResult> onlineRouting(RoutingServerConfigEntry rsc, RoutingContext ctx,
	                                               RoutePlannerFrontEnd router, Map<String, Object> props,
	                                               LatLon start, LatLon end, List<LatLon> intermediates,
	                                               boolean useExternalTimestamps)
			throws IOException, InterruptedException {

		// OSRM by type, all others treated as "rescuetrack"
		if (rsc.type != null && "osrm".equalsIgnoreCase(rsc.type)) {
			return onlineRoutingOSRM(rsc.url, ctx, router, props, start, end, intermediates);
		} else {
			return onlineRoutingRescuetrack(rsc.url, ctx, router, props, start, end, useExternalTimestamps);
		}
	}

	@Nullable
	private List<RouteSegmentResult> onlineRoutingOSRM(String baseurl, RoutingContext ctx,
	                                                   RoutePlannerFrontEnd router, Map<String, Object> props,
	                                                   LatLon start, LatLon end, List<LatLon> intermediates)
			throws IOException, InterruptedException {

		// OSRM requires lon,lat not lat,lon
		List<String> points = new ArrayList<>();
		points.add(String.format("%f,%f", start.getLongitude(), start.getLatitude()));
		if (intermediates != null) {
			intermediates.forEach(p -> points.add(String.format("%f,%f", p.getLongitude(), p.getLatitude())));
		}
		points.add(String.format("%f,%f", end.getLongitude(), end.getLatitude()));

		StringBuilder url = new StringBuilder(baseurl); // base url
		url.append(String.join(";", points)); // points;points;points
		url.append("?geometries=geojson&overview=full&steps=true"); // OSRM options

		List<WptPt> waypoints = new ArrayList<>();
		try {
			String body = new RestTemplate().getForObject(url.toString(), String.class);
			if (body != null && body.contains("Ok") && body.contains("coordinates")) {
				JSONArray coordinates = new JSONObject(body)
						.getJSONArray("routes")
						.getJSONObject(0)
						.getJSONObject("geometry")
						.getJSONArray("coordinates");

				for (int i = 0; i < coordinates.length(); i++) {
					JSONArray ll = coordinates.getJSONArray(i);
					final double lon = ll.getDouble(0);
					final double lat = ll.getDouble(1);
					waypoints.add(new WptPt(lat, lon));
				}
			}
		} catch(RestClientException | JSONException error) {
			LOGGER.error(String.format("OSRM get/parse error (%s)", url));
			return null;
		}

		if (waypoints.size() >= 2) {
			return approximate(ctx, router, props, waypoints, false);
		}

		return null;
	}

	private List<RouteSegmentResult> onlineRoutingRescuetrack(String baseurl, RoutingContext ctx,
	                                               RoutePlannerFrontEnd router, Map<String, Object> props,
	                                               LatLon start, LatLon end, boolean useExternalTimestamps)
			throws IOException, InterruptedException {

		List<RouteSegmentResult> routeRes;
		StringBuilder url = new StringBuilder(baseurl);
		url.append(String.format("?point=%.6f,%.6f", start.getLatitude(), start.getLongitude()));
		url.append(String.format("&point=%.6f,%.6f", end.getLatitude(), end.getLongitude()));

		RestTemplate restTemplate = new RestTemplate();
		restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
			@Override
			public void handleError(ClientHttpResponse response) throws IOException {
				LOGGER.error(String.format("Error GET url (%s)", url));
				super.handleError(response);
			}
		});
		LOGGER.info("Online request: " + url.toString());
		String gpx = restTemplate.getForObject(url.toString(), String.class);
		GPXFile file = GPXUtilities.loadGPXFile(new ByteArrayInputStream(gpx.getBytes()));
		TrkSegment trkSegment = file.tracks.get(0).segments.get(0);
		routeRes = approximate(ctx, router, props, trkSegment.points, useExternalTimestamps);
		return routeRes;
	}

	@Nullable
	public List<RouteSegmentResult> routing(boolean disableOldRouting, String routeMode, Map<String, Object> props,
			LatLon start, LatLon end, List<LatLon> intermediates, List<String> avoidRoadsIds,
			RouteCalculationProgress progress) throws IOException, InterruptedException {
		String profile = routeMode.split("\\,")[0];
		QuadRect points = points(intermediates, start, end);
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		List<RouteSegmentResult> routeRes;
		RoutingContext ctx = null;
		try {
			RouteParameters rp = parseRouteParameters(routeMode);
			String routingCacheStr = "";
			synchronized (routingCaches) {
				routingCacheStr = routingCaches.toString();
			}
			ctx = lockCacheRoutingContext(router, rp);
			LOGGER.info(String.format("Route %s: %s -> %s (%s) - cache %s", profile, start, end, routeMode, routingCacheStr));
			if (ctx == null) {
				validateAndInitConfig();
				List<BinaryMapIndexReaderReference> list = getObfReaders(points, null, 0, "routing");
				boolean[] incomplete = new boolean[1];
				usedMapList = getReaders(list, incomplete);
				if (incomplete[0]) {
					return Collections.emptyList();
				}
				ctx = prepareRouterContext(rp, router, usedMapList, false);
			}
			HashSet<Long> impassableRoads = new HashSet<>();
			for (String s : avoidRoadsIds) {
				impassableRoads.add(Long.parseLong(s));
			}
			((GeneralRouter) ctx.getRouter()).setImpassableRoads(impassableRoads);
			if (disableOldRouting && !router.isHHRoutingConfigured()) {
				// BRP is disabled (limited) and HH is disabled (hhoff)
				return null;
			}
			ctx.routingTime = 0;
			ctx.calculationProgress = progress;
			if (rp.onlineRouting != null) {
				routeRes = onlineRouting(rp.onlineRouting, ctx, router, props, start, end, intermediates,
						rp.useExternalTimestamps);
			} else {
				RouteCalcResult rc = ctx.nativeLib != null ? runRoutingSync(start, end, intermediates, router, ctx)
						: router.searchRoute(ctx, start, end, intermediates, null);
				routeRes = rc == null ? null : rc.getList();
				putResultProps(ctx, routeRes, props);
			}
		} finally {
			unlockReaders(usedMapList);
			unlockCacheRoutingContext(ctx);
		}
		return routeRes;
	}


	private RoutingContext lockCacheRoutingContext(RoutePlannerFrontEnd router, RouteParameters rp) throws IOException, InterruptedException {
		if (routeObfLocation == null || routeObfLocation.length() == 0) {
			return null;
		}
		if (rp.useNativeRouting || rp.useNativeApproximation || rp.noGlobalFile || rp.calcMode != null) {
			return null;
		}
		RoutingCacheContext cache = lockRoutingCache(router, rp);
		if (cache == null) {
			return null;
		}
		RoutingContext c = cache.rCtx;
		c.unloadAllData();
		c.calculationProgress = new RouteCalculationProgress();
		return c;
	}

	private int maxProfileMaps(String profile) {
		Integer i = MAX_SAME_PROFILE.get(profile);
		if (i != null) {
			return i;
		}
		return MAX_SAME_PROFILE_DF;
	}

	private RoutingCacheContext lockRoutingCache(RoutePlannerFrontEnd router, RouteParameters rp) throws IOException, InterruptedException {
		long waitTime = System.currentTimeMillis();
		while ((System.currentTimeMillis() - waitTime) < MAX_SAME_PROFILE_WAIT_MS) {
			RoutingCacheContext best = null;
			synchronized (routingCaches) {
				for (RoutingCacheContext c : routingCaches) {
					if (c.locked == 0 && rp.routeProfile.equals(c.profile)) {
						if (c.routeParamsStr.equals(rp.routeParams.toString()) || best == null) {
							best = c;
						}
					}
				}
				if (best != null) {
					best.used++;
					best.locked = System.currentTimeMillis();
					router.setHHRoutingConfig(best.hhConfig);
				}
			}
			if (best != null) {
				best.rCtx.unloadAllData();
				if (!best.routeParamsStr.equals(rp.routeParams.toString())) {
					best.routeParamsStr = rp.routeParams.toString();
					GeneralRouter oldRouter = best.rCtx.config.router;
					oldRouter.clearCaches();
					GeneralRouter newRouter = new GeneralRouter(oldRouter, rp.routeParams);
					best.rCtx.setRouter(newRouter);
					newRouter.clearCaches();
					if (best.hCtx != null) {
						best.hCtx.clearSegments(); // segments could be affected by params recalculation
					}
				}
				if (rp.disableHHRouting) {
					router.disableHHRoutingConfig();
				} else {
					router.setHHRouteCpp(rp.useNativeRouting);
					router.setUseOnlyHHRouting(rp.useOnlyHHRouting);
					router.setHHRoutingConfig(best.hhConfig); // after prepare
				}
				return best;
			}
			Thread.sleep(1000);
		}

		RoutingCacheContext cs = new RoutingCacheContext();
		cs.locked = System.currentTimeMillis();
		cs.created = System.currentTimeMillis();
		cs.hhConfig = RoutePlannerFrontEnd.defaultHHConfig().cacheContext(cs.hCtx);
		cs.routeParamsStr = rp.routeParams.toString();
		cs.profile = rp.routeProfile;
		int sameProfileSize = 0, all = 0;
		synchronized (routingCaches) {
			all = sameProfileSize = 0;
			for (RoutingCacheContext c : routingCaches) {
				all++;
				if (rp.routeProfile.equals(c.profile)) {
					sameProfileSize++;
				}
			}
			if (sameProfileSize >= maxProfileMaps(rp.routeProfile) || all >= MAX_SAME_ROUTING_CONTEXT_OPEN) {
				System.out.printf("Global routing cache %s is not available (using old files)\n", rp.routeProfile);
				return null;
			}
			routingCaches.add(cs);
		}
		// do outside synchronized to not block
		File target = new File(routeObfLocation);
		File targetIndex = new File(routeObfLocation + ".index");
		CachedOsmandIndexes cache = new CachedOsmandIndexes();
		if (targetIndex.exists()) {
			cache.readFromFile(targetIndex);
		}
		BinaryMapIndexReader reader = cache.getReader(target, true);
		cache.writeToFile(targetIndex);
		cs.rCtx = prepareRouterContext(rp, router, Collections.singletonList(reader), false);
		router.setHHRoutingConfig(cs.hhConfig); // after prepare
		System.out.printf("Use new routing context for %s profile (%s params) - all %d\n", cs.profile,
				cs.routeParamsStr, sameProfileSize + 1);
		return cs;
	}

	private boolean unlockCacheRoutingContext(RoutingContext ctx) {
		synchronized (routingCaches) {
			for (RoutingCacheContext c : routingCaches) {
				if (c.rCtx == ctx) {
					c.hCtx = c.hhConfig.cacheCtx;
					c.locked = 0;
					return true;
				}
			}
		}
		return false;
	}

	private synchronized RouteCalcResult runRoutingSync(LatLon start, LatLon end, List<LatLon> intermediates,
			RoutePlannerFrontEnd router, RoutingContext ctx) throws IOException, InterruptedException {
		return router.searchRoute(ctx, start, end, intermediates, null);
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

	public QuadRect points(List<LatLon> intermediates, LatLon start, LatLon end) {
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


	private void initNewObfFiles(File target, File targetTemp) throws IOException {
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

		if (!ref.readers.isEmpty()) {
			for (Entry<BinaryMapIndexReader, Boolean> r : ref.readers.entrySet()) {
				r.getKey().close();
			}
		}
		if (nativelib != null) {
			nativelib.closeMapFile(target.getAbsolutePath());
		}
		target.delete();
		targetTemp.renameTo(target);
		RandomAccessFile raf = new RandomAccessFile(target, "r");
		BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, target);
		ref.readers.put(reader, true);
		ref.fileIndex = cacheFiles.addToCache(reader, target);
		cacheFiles.writeToFile(new File(config.cacheLocation, CachedOsmandIndexes.INDEXES_DEFAULT_FILENAME));
		if (nativelib != null) {
			nativelib.initMapFile(target.getAbsolutePath(), false);
		}
		LOGGER.info("Init new obf file " + target.getName() + " " + (System.currentTimeMillis() - val) + " ms");
	}

	public List<BinaryMapIndexReaderReference> getObfReaders(QuadRect quadRect, List<LatLon> bbox, int maxNumberMaps, String reason) throws IOException {
		initObfReaders();
		List<BinaryMapIndexReaderReference> files = new ArrayList<>();
		List<File> filesToUse = getMaps(quadRect, bbox, maxNumberMaps);
		if (!filesToUse.isEmpty()) {
			for (File f : filesToUse) {
				BinaryMapIndexReaderReference ref = obfFiles.get(f.getAbsolutePath());
				files.add(ref);
			}
		}
		LOGGER.info(String.format("Preparing %d files for %s", files.size(), reason));
		return files;
	}

	private List<File> getMaps(QuadRect quadRect, List<LatLon> bbox, int maxNumberMaps) throws IOException {
		List<File> files = new ArrayList<>();
		for (BinaryMapIndexReaderReference ref : obfFiles.values()) {
			boolean intersects;
			fileOverlaps:
			for (RoutingPart rp : ref.fileIndex.getRoutingIndexList()) {
				for (RoutingSubregion s : rp.getSubregionsList()) {
					intersects = quadRect.left <= s.getRight() && quadRect.right >= s.getLeft()
							&& quadRect.top <= s.getBottom() && quadRect.bottom >= s.getTop();
					if (intersects) {
						files.add(ref.file);
						break fileOverlaps;
					}
				}
			}
		}
		return prepareMaps(files, bbox, maxNumberMaps);
	}
	
	public BinaryMapIndexReaderReference getBaseMap() throws IOException {
		initObfReaders();
		for (BinaryMapIndexReaderReference ref : obfFiles.values()) {
			if (ref.file.getName().contains("basemap")) {
				LOGGER.info("Base map successfully found " + ref.file.getName());
				return ref;
			}
		}
		return null;
	}

	private List<File> prepareMaps(List<File> files, List<LatLon> bbox, int maxNumberMaps) throws IOException {
		List<File> filesToUse = filterMap(files);
		List<File> res;

		if (!filesToUse.isEmpty() && maxNumberMaps != 0 && filesToUse.size() >= 4 && bbox != null) {
			res = filterMapsByName(filesToUse, bbox);
		} else {
			res = filesToUse;
		}
		return res;
	}

	private List<File> filterMap(List<File> files) throws IOException {
		List<File> res = new ArrayList<>();
		if (osmandRegions == null) {
			osmandRegions = new OsmandRegions();
			osmandRegions.prepareFile();
		}
		TreeSet<String> allDwNames = new TreeSet<>();
		for (File file : files) {
			allDwNames.add(getDownloadNameByFileName(file.getName()));
		}
		for (File file : files) {
			String dwName = getDownloadNameByFileName(file.getName());
			WorldRegion wr = osmandRegions.getRegionDataByDownloadName(dwName);
			if (wr != null && wr.getSuperregion() != null && wr.getSuperregion().getRegionDownloadName() != null
					&& allDwNames.contains(wr.getSuperregion().getRegionDownloadName())) {
			} else {
				res.add(file);
			}
		}
		return res;
	}

	private String getDownloadNameByFileName(String fileName) {
		String dwName = fileName.substring(0, fileName.indexOf('.')).toLowerCase();
		if (dwName.endsWith("_2")) {
			dwName = dwName.substring(0, dwName.length() - 2);
		}
		return dwName;
	}

	public List<File> filterMapsByName(List<File> filesToUse, List<LatLon> bbox) throws IOException {
		List<File> res = new ArrayList<>();
		HashSet<String> regions = getRegionsNameByBbox(bbox);
		for (File f : filesToUse) {
			BinaryMapIndexReaderReference ref = obfFiles.get(f.getAbsolutePath());
			String name = ref.file.getName().toLowerCase().replace("_2.obf", "");
			if (regions.contains(name)) {
				res.add(f);
			}
		}
		return res;
	}

	private HashSet<String> getRegionsNameByBbox(List<LatLon> bbox) throws IOException {
		HashSet<String> regions = new HashSet<>();
		if (osmandRegions == null) {
			osmandRegions = new OsmandRegions();
			osmandRegions.prepareFile();
		}
		for (LatLon point : getPointsByBbox(bbox)) {
			List<String> res = new ArrayList<>();
			res = osmandRegions.getRegionsToDownload(point.getLatitude(), point.getLongitude(), res);
			regions.addAll(res);
		}
		LOGGER.debug("Regions by bbox size " + regions.size());
		return regions;
	}

	private List<LatLon> getPointsByBbox(List<LatLon> bbox) {
		final int POINT_STEP_M = 20000;

		List<LatLon> points = new ArrayList<>(bbox);
		LatLon p1 = bbox.get(0);
		LatLon p2 = bbox.get(1);

		LatLon start = p1;
		LatLon pointStep = start;
		while (pointStep != null) {
			LatLon res = rhumbDestinationPoint(pointStep, POINT_STEP_M, 180);
			if (res.getLatitude() > p2.getLatitude()) {
				points.add(res);
				pointStep = res;
			} else {
				start = rhumbDestinationPoint(start, POINT_STEP_M, 90);
				if (start.getLongitude() < p2.getLongitude()) {
					pointStep = start;
				} else {
					pointStep = null;
				}
			}
		}
		return points;
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
				cacheFiles.readFromFile(cacheFile);
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

	public ResponseEntity<String> errorConfig() {
		VectorTileServerConfig config = getConfig();
		return ResponseEntity.badRequest()
				.body("Tile service is not initialized: " + (config == null ? "" : config.initErrorMessage));
	}

	public OsmandRegions getOsmandRegions() {
		return osmandRegions;
	}

	public File getObf(Map<String, GpxFile> files)
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
	
	public JsonObject getTileInfo(JsonObject tileInfo, int x, int y, int z) {
		JsonObject subInfo = new JsonObject();
		JsonArray featuresArray = tileInfo.getAsJsonArray("features");
		JsonArray newFeaturesArray = new JsonArray();
		for (int i = 0; i < featuresArray.size(); i++) {
			JsonObject featureObject = featuresArray.get(i).getAsJsonObject();
			JsonObject geometryObject = featureObject.getAsJsonObject("geometry");
			JsonArray coordinatesArray = geometryObject.getAsJsonArray("coordinates");
			
			for (int j = 0; j < coordinatesArray.size(); j++) {
				JsonArray pointArray = coordinatesArray.get(j).getAsJsonArray();
				double lat = pointArray.get(0).getAsDouble();
				double lon = pointArray.get(1).getAsDouble();
				
				if (isPointInTile(lat, lon, x, y, z)) {
					newFeaturesArray.add(featureObject);
					break;
				}
			}
		}
		
		subInfo.addProperty("type", "FeatureCollection");
		subInfo.add("features", newFeaturesArray);
		
		return subInfo;
	}
	
	private boolean isPointInTile(double lat, double lon, int tileX, int tileY, int zoom) {
		
		double xmin = MapUtils.getLongitudeFromTile(zoom, tileX);
		double xmax = MapUtils.getLongitudeFromTile(zoom, tileX + 1);
		double ymin = MapUtils.getLatitudeFromTile(zoom, tileY + 1);
		double ymax = MapUtils.getLatitudeFromTile(zoom, tileY);
		
		if (lat < ymin || lat >= ymax) {
			return false;
		}
		
		if (lon < xmin || lon >= xmax) {
			return false;
		}
		return true;
	}
	
	public String getInteractiveKeyMap(String style) {
		return style.startsWith(INTERACTIVE_KEY) ? style + INTERACTIVE_STYLE_DELIMITER + DEFAULT_INTERACTIVE_STYLE : null;
	}
	
	public String getMapStyle(String style, String interactiveKey) {
		if (interactiveKey == null) {
			return style;
		}
		return style.equals(INTERACTIVE_KEY) ? DEFAULT_INTERACTIVE_STYLE : style.split(INTERACTIVE_KEY + INTERACTIVE_STYLE_DELIMITER)[1];
	}

	public BufferedImage getGeotiffTile(String tilePath, String outColorFilename, String midColorFilename,
		int type, int size, int zoom, int x, int y) throws IOException {
		BufferedImage image = null;
		synchronized (geotiffTileLock) {
			if (nativelib != null) {
				image = nativelib.getGeotiffImage(tilePath, outColorFilename, midColorFilename, type, size, zoom, x, y);
			}	
		}
		return image;
	}
}
