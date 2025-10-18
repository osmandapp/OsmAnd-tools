package net.osmand.server.api.services;


import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.annotation.Nullable;

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
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import net.osmand.IndexConstants;
import net.osmand.LocationsHolder;
import net.osmand.NativeJavaRendering;
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
import net.osmand.router.GeneralRouter;
import net.osmand.router.GeneralRouter.RoutingParameter;
import net.osmand.router.GeneralRouter.RoutingParameterType;
import net.osmand.router.GpxRouteApproximation;
import net.osmand.router.HHRouteDataStructure.HHRoutingConfig;
import net.osmand.router.HHRouteDataStructure.HHRoutingContext;
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;
import net.osmand.router.RouteCalculationProgress;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.GpxPoint;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RouteResultPreparation;
import net.osmand.router.RouteResultPreparation.RouteCalcResult;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.router.RoutingContext;
import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.tileManager.TileMemoryCache;
import net.osmand.server.tileManager.TileServerConfig;
import net.osmand.server.tileManager.VectorMetatile;
import net.osmand.server.utils.TimezoneMapper;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

@Service
public class OsmAndMapsService {
	private static final Log LOGGER = LogFactory.getLog(OsmAndMapsService.class);

	protected static final int MAX_FILES_PER_FOLDER = 1 << 12; // 4096

	private static final int MEM_LIMIT = RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT * 8;
	private static final int MEM_MAX_HITS_PER_RUN = 5;

	private static final long INTERVAL_TO_MONITOR_ZIP = 5 * 60 * 1000;
	private static final long INTERVAL_TO_CLEANUP_ROUTING_CACHE = 10 * 60 * 1000;

	// counts only files open for Java (doesn't fit for rendering / routing)
	private static final int MAX_SAME_FILE_OPEN = 15;
	private static final long CACHE_MAX_ROUTING_CONTEXT_SEC = Integer.MAX_VALUE; //12 * 60 * 60; // 12h
	
	private static final List<String> ALWAYS_IN_MEMORY = new ArrayList<String>();
	static {
		ALWAYS_IN_MEMORY.add("car:{}");
		ALWAYS_IN_MEMORY.add("car:{}");
//		ALWAYS_IN_MEMORY.add("car:{avoid_motorway=true, prefer_unpaved=true}"); // test
//		ALWAYS_IN_MEMORY.add("car:{prefer_unpaved=true}"); // test
		ALWAYS_IN_MEMORY.add("motorcycle:{}");
		ALWAYS_IN_MEMORY.add("motorcycle:{}");
//		ALWAYS_IN_MEMORY.add("motorcycle:{avoid_motorway=true, prefer_unpaved=true}"); // test
//		ALWAYS_IN_MEMORY.add("motorcycle:{avoid_4wd_only=true, avoid_motorway=true, prefer_unpaved=true}"); // test
//		ALWAYS_IN_MEMORY.add("motorcycle:{prefer_unpaved=true}"); // test
		
		ALWAYS_IN_MEMORY.add("bicycle:{}");
		ALWAYS_IN_MEMORY.add("bicycle:{}");
		ALWAYS_IN_MEMORY.add("bicycle:{height_obstacles=true}");
		ALWAYS_IN_MEMORY.add("pedestrian:{}");
		ALWAYS_IN_MEMORY.add("pedestrian:{}");
	}
	
	
	
	private static final long MAX_PROFILE_WAIT_MS = 6000;


	private static final String INTERACTIVE_KEY = "int";
	private static final String DEFAULT_INTERACTIVE_STYLE = "hd";
	private static final String INTERACTIVE_STYLE_DELIMITER = "-";


	Map<String, BinaryMapIndexReaderReference> obfFiles = new LinkedHashMap<>();

	CachedOsmandIndexes cacheFiles = null;

	final List<RoutingCacheContext> routingCaches = new ArrayList<>();

	NativeJavaRendering nativelib;

	File tempDir;

	@Autowired
	TileServerConfig tileConfig;

	@Autowired
	RoutingServerConfig routingConfig;

	OsmandRegions osmandRegions;

	@Value("${tile-server.routeObf.location}")
	String routeObfLocation;

	public CloudUserDevicesRepository.CloudUserDevice checkUser() {
		Object user = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
		if (user instanceof WebSecurityConfiguration.OsmAndProUser osmAndProUser) {
			return osmAndProUser.getUserDevice();
		}
		return null;
	}

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
			return (locked == 0 ? '\u25FB' : '\u25FC')
					+ (ALWAYS_IN_MEMORY.contains(profile + ":" + routeParamsStr) ? "*" : "")
					+ String.format("%s %s %s %d, %s min", profile, hCtx == null ? "-" : hCtx.hashCode() + "",
							routeParamsStr, used, (System.currentTimeMillis() - created) / 60 / 1000);
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
		}
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

	@Scheduled(fixedRate = INTERVAL_TO_CLEANUP_ROUTING_CACHE)
	public void cleanUpRoutingFiles() {
		Runtime rt = Runtime.getRuntime();
		List<RoutingCacheContext> removed = new ArrayList<>();
		long usedBeforeCleanup = rt.totalMemory() - rt.freeMemory();

		synchronized (routingCaches) {
			Collections.sort(routingCaches, new Comparator<RoutingCacheContext>() {

				@Override
				public int compare(RoutingCacheContext o1, RoutingCacheContext o2) {
					return Double.compare(o1.importance(), o2.importance());
				}
			});
			System.out.println("Clean fast global routing contexts " + routingCaches);
			for (RoutingCacheContext survivor : routingCaches) {
				if (survivor.locked == 0) {
					if (survivor.hCtx != null) {
						survivor.hCtx.clearSegments();
					}
					survivor.rCtx.unloadAllData();
//					survivor.rCtx.unloadUnusedTiles(survivor.rCtx.config.memoryLimitation);
				}
			}
			System.gc();
			long brpReservedBytes = routingCaches.size() == 0 ? 0 : routingCaches.size() * routingCaches.get(0).rCtx.config.memoryLimitation;
			long futureFreeMemory = rt.maxMemory() - rt.totalMemory() + rt.freeMemory();
			boolean criticalMemory = futureFreeMemory < brpReservedBytes;
			if (criticalMemory) {
				System.out.printf("Trigger brpReservedBytes (future free %d MB, need %d MB)\n", futureFreeMemory >> 20, brpReservedBytes >> 20);
			}
			Iterator<RoutingCacheContext> it = routingCaches.iterator();
			while (it.hasNext()) {
				RoutingCacheContext check = it.next();
				if (check.locked == 0 && (criticalMemory
						|| (System.currentTimeMillis() - check.created) / 1000L >= CACHE_MAX_ROUTING_CONTEXT_SEC)) {
					removed.add(check); // lower importance() means higher removal priority
					System.out.printf("Delete %s global routing context from cache\n", check);
					it.remove();
					if (criticalMemory) {
						criticalMemory = false;
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
			System.out.printf("Clean up %d global routing contexts, state - %s\n", removed.size(), routingCaches);
			System.gc();
		}
		long maxMemory = rt.maxMemory();
		long totalMemory = rt.totalMemory();
		long freeMemory = rt.freeMemory();
		long bytesReleased = usedBeforeCleanup - (totalMemory - freeMemory);
		System.out.printf("Cache-GC: [%d] released %d MB (max %d MB, total %d MB, free %d MB)\n",
				routingCaches.size(), bytesReleased >> 20, maxMemory >> 20, totalMemory >> 20, freeMemory >> 20);
	}


	public static class RoutingServerConfigEntry {
		public String type;
		public String url;
		public String name;
		public String profile = "car";
	}


	public int getCurrentOpenJavaFiles() {
		int cnt = 0;
		for (BinaryMapIndexReaderReference r : obfFiles.values()) {
			cnt += r.getOpenFiles();
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
					if (s.contains(search[i])) {
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

	@Scheduled(fixedRate = INTERVAL_TO_MONITOR_ZIP)
	public synchronized void checkZippedFiles() throws IOException {
		if (tileConfig != null && !Algorithms.isEmpty(tileConfig.obfZipLocation) && !Algorithms.isEmpty(tileConfig.obfLocation)) {
			LOGGER.info("Checking new files at " + tileConfig.obfZipLocation + " " + tileConfig.obfLocation);
			for (File zipFile : new File(tileConfig.obfZipLocation).listFiles()) {
				if (zipFile.getName().endsWith(".obf.zip")) {
					String fn = zipFile.getName().substring(0, zipFile.getName().length() - ".zip".length());
					File target = new File(tileConfig.obfLocation, fn);
					File targetTemp = new File(tileConfig.obfLocation, fn + ".new");
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

	public RoutingServerConfig getRoutingConfig() {
		return routingConfig;
	}

	public synchronized boolean validateAndInitConfig() throws IOException {
		if (nativelib == null && tileConfig.initErrorMessage == null) {
			if (osmandRegions == null) {
				osmandRegions = new OsmandRegions();
				osmandRegions.prepareFile();
			}
			if (tileConfig.obfLocation == null || tileConfig.obfLocation.isEmpty()) {
				tileConfig.initErrorMessage = "Files location is not specified";
			} else {
				File obfLocationF = new File(tileConfig.obfLocation);
				if (!obfLocationF.exists()) {
					tileConfig.initErrorMessage = "Files location is not specified";
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
			String nativeLibraryPath = System.getenv("NATIVE_LIBRARY_PATH"); // use native library from file, optional
			nativelib = NativeJavaRendering.getDefault(nativeLibraryPath, tileConfig.obfLocation, fontsFolder.getAbsolutePath());
		}
		return tileConfig.initErrorMessage == null;
	}

	public String validateNativeLib() {
		if (nativelib == null) {
			return "Tile rendering engine is not initialized";
		}
		return null;
	}

	public ResponseEntity<String> renderMetaTile(VectorMetatile tile, TileMemoryCache<VectorMetatile> tileMemoryCache) throws XmlPullParserException, IOException, SAXException {
		return tile.renderMetaTile(nativelib, tileMemoryCache);
	}

	public BufferedImage renderGeotiffTile(String tilePath, String outColorFilename, String midColorFilename,
	                                       int type, int size, int zoom, int x, int y) throws IOException {
		BufferedImage image = null;
		if (nativelib != null) {
			image = nativelib.getGeotiffImage(tilePath, outColorFilename, midColorFilename, type, size, zoom, x, y);
		}
		return image;
	}

	public List<GeocodingResult> geocoding(double lat, double lon) throws IOException, InterruptedException {
		QuadRect points = points(null, new LatLon(lat, lon), new LatLon(lat, lon));
		List<GeocodingResult> complete;
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		try {
			List<BinaryMapIndexReaderReference> list = getObfReaders(points, null, "geocoding");
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
			List<BinaryMapIndexReaderReference> list = getObfReaders(quadRect, null, "approximate");
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
		boolean noGlobalFile = false;
		boolean noConditionals = false;
		long routeCalculationTime = -1;
		float minPointApproximation = -1;
		RouteCalculationMode calcMode = null;
		public boolean disableHHRouting;
		public RoutingServerConfigEntry onlineRouting;
		private static final int RESCUETRACK_DEFAULT_HEADING = 0;
		int headingForRescuetrack = RESCUETRACK_DEFAULT_HEADING;

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
			} else if ("noconditionals".equals(key)) {
				r.noConditionals = Boolean.parseBoolean(value);
			} else if ("heading".equals(key)) {
				r.headingForRescuetrack = Integer.parseInt(value);
			} else if ("minPointApproximation".equals(key)) {
				r.minPointApproximation = Float.parseFloat(value);
			} else if ("routeCalculationTime".equals(key)) {
				r.routeCalculationTime = Long.parseLong(value);
			} else if ("hhonly".equals(key)) {
				r.useOnlyHHRouting = Boolean.parseBoolean(value);
			} else if ("gpxtimestamps".equals(key)) {
				r.useExternalTimestamps = Boolean.parseBoolean(value);
			} else {
				r.routeParams.put(key, value); // pass directly to the router
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
	                                            boolean approximation) throws IOException, InterruptedException {
		boolean useNativeLib = approximation ? rp.useNativeApproximation : rp.useNativeRouting;

		if (rp.onlineRouting != null && rp.useNativeApproximation) {
			useNativeLib = true; // rescuetrack + approximation
		}

		router.setUseNativeApproximation(rp.useNativeApproximation);
		router.setUseGeometryBasedApproximation(rp.useGeometryBasedApproximation);

		RoutePlannerFrontEnd.CALCULATE_MISSING_MAPS = false;
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
		config.memoryMaxHits = MEM_MAX_HITS_PER_RUN;
		if (rp.minPointApproximation >= 0) {
			config.minPointApproximation = rp.minPointApproximation;
		}

		if (!rp.noConditionals) {
			config.routeCalculationTime = rp.routeCalculationTime >= 0
					? rp.routeCalculationTime
					: System.currentTimeMillis();
		}

		final RoutingContext ctx = router.buildRoutingContext(config, useNativeLib ? nativelib : null,
				usedMapList.toArray(new BinaryMapIndexReader[0]), rp.calcMode);
		ctx.leftSideNavigation = false;
		return ctx;
	}

	@Nullable
	private List<RouteSegmentResult> onlineRouting(RouteParameters rp, RoutingContext ctx,
	                                               RoutePlannerFrontEnd router, Map<String, Object> props,
	                                               LatLon start, LatLon end, List<LatLon> intermediates)
			throws IOException, InterruptedException {
		RoutingServerConfigEntry rsc = rp.onlineRouting;

		// OSRM by type, all others treated as "rescuetrack"
		if (rsc.type != null && "osrm".equalsIgnoreCase(rsc.type)) {
			return onlineRoutingOSRM(rsc.url, ctx, router, props, start, end, intermediates);
		} else {
			int heading = rp.headingForRescuetrack;
			boolean useExternalTimestamps = rp.useExternalTimestamps;
			return onlineRoutingRescuetrack(rsc.url, ctx, router, props, start, end, useExternalTimestamps, heading);
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
		} catch (RestClientException | JSONException error) {
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
	                                                          LatLon start, LatLon end, boolean useExternalTimestamps,
	                                                          int heading)
			throws IOException, InterruptedException {

		List<RouteSegmentResult> routeRes;
		StringBuilder url = new StringBuilder(baseurl);
		url.append(String.format("?point=%.6f,%.6f", start.getLatitude(), start.getLongitude()));
		url.append(String.format("&point=%.6f,%.6f", end.getLatitude(), end.getLongitude()));
		if (heading != RouteParameters.RESCUETRACK_DEFAULT_HEADING) {
			url.append(String.format("&heading=%d", heading));
		}

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
		if (file.error == null) {
			TrkSegment trkSegment = file.tracks.get(0).segments.get(0);
			routeRes = approximate(ctx, router, props, trkSegment.points, useExternalTimestamps);
			return routeRes;
		}
		LOGGER.error("Empty GPX from Rescuetrack: " + url);
		return new ArrayList<>();
	}
	
	private static class DebugInfo {
		String routingCacheInfo = "";
		String selectedCache = "SEPARATE";
		String routeParametersStr = "";
		public long waitTime;
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
			DebugInfo di = new DebugInfo();
			ctx = lockCacheRoutingContext(router, rp, di);
			LOGGER.info(String.format("REQ routing %s (%s - %.1f sec, %s): %s -> %s - cache %s", profile, 
					di.selectedCache, di.waitTime / 1e3, di.routeParametersStr, start, end, di.routingCacheInfo));
			if (ctx == null) {
				validateAndInitConfig();
				List<BinaryMapIndexReaderReference> list = getObfReaders(points, null, "routing");
				boolean[] incomplete = new boolean[1];
				usedMapList = getReaders(list, incomplete);
				if (incomplete[0]) {
					return Collections.emptyList();
				}
				ctx = prepareRouterContext(rp, router, usedMapList, false);
			}
			if (!rp.noConditionals && rp.routeCalculationTime < 0) {
				// update TIME_CONDITIONAL_ROUTING if the conditional time is not disabled and is not enforced
				ctx.config.routeCalculationTime = getLocalTimeMillisByLatLon(start.getLatitude(), start.getLongitude());
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
				routeRes = onlineRouting(rp, ctx, router, props, start, end, intermediates);
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

	private static long getLocalTimeMillisByLatLon(double lat, double lon) {
		String tz = TimezoneMapper.latLngToTimezoneString(lat, lon);
		ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of(tz));
//		System.out.printf("TimezoneMapper (%.5f, %.5f) = %s\n", lat, lon, zonedDateTime);
		return zonedDateTime.toInstant().toEpochMilli();
	}

	private RoutingContext lockCacheRoutingContext(RoutePlannerFrontEnd router, RouteParameters rp, DebugInfo di) throws IOException, InterruptedException {
		if (routeObfLocation == null || routeObfLocation.length() == 0) {
			return null;
		}
		if (rp.useNativeRouting || rp.useNativeApproximation || rp.noGlobalFile || rp.calcMode != null) {
			return null;
		}
		RoutingCacheContext cache = lockRoutingCache(router, rp, di);
		if (cache == null) {
			return null;
		}
		RoutingContext c = cache.rCtx;
		c.unloadAllData();
		c.calculationProgress = new RouteCalculationProgress();
		return c;
	}



	private RoutingCacheContext lockRoutingCache(RoutePlannerFrontEnd router, RouteParameters rp, DebugInfo di) throws IOException, InterruptedException {
		long waitTime = System.currentTimeMillis();
		String rProfile = rp.routeProfile;
		String rParamsStr = getCleanRouteParams(rp, rProfile);
		String rProfileKey = rProfile + ":" + rParamsStr;
		di.routeParametersStr = rParamsStr;
		List<String> sameInMemoryProfiles = new ArrayList<>(ALWAYS_IN_MEMORY); // don't reuse
		if (!sameInMemoryProfiles.contains(rProfileKey)) {
			di.waitTime = System.currentTimeMillis() - waitTime;
			LOGGER.info(String.format("Global routing cache %s is not available (using separate files)", rProfileKey));
			return null;
		}
		// Wait for availability
		while ((System.currentTimeMillis() - waitTime) < MAX_PROFILE_WAIT_MS) {
			RoutingCacheContext best = null;
			sameInMemoryProfiles = new ArrayList<>(ALWAYS_IN_MEMORY); // don't reuse
			synchronized (routingCaches) {
				di.routingCacheInfo = routingCaches.toString();
				for (RoutingCacheContext c : routingCaches) {
					if (rProfile.equals(c.profile) && c.routeParamsStr.equals(rParamsStr)) {
						sameInMemoryProfiles.remove(rProfileKey);
						if (c.locked == 0) {
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
				if (!best.routeParamsStr.equals(rParamsStr)) {
					// this is not used any more cause we always match exactly route params 
					best.routeParamsStr = rParamsStr;
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
				di.selectedCache = best.hCtx != null ? best.hCtx.hashCode() + "" : "RENEW";
				di.waitTime = System.currentTimeMillis() - waitTime;
				return best;
			}
			if (sameInMemoryProfiles.contains(rProfileKey)) {
				// immediately start 2nd copy of cache as it's supposed
				break;
			}
			Thread.sleep(1000);
		}
		if (!sameInMemoryProfiles.contains(rProfileKey)) {
			di.waitTime = System.currentTimeMillis() - waitTime;
			LOGGER.info(String.format("Global routing cache %s limits exceeded (using separate files)", rProfileKey));
			return null;
		}
		// Create new global cache
		RoutingCacheContext cs = new RoutingCacheContext();
		cs.locked = System.currentTimeMillis();
		cs.created = System.currentTimeMillis();
		cs.hhConfig = RoutePlannerFrontEnd.defaultHHConfig().cacheContext(cs.hCtx);
		cs.routeParamsStr = rParamsStr;
		cs.profile = rProfile;
		synchronized (routingCaches) {
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
		LOGGER.info(String.format("Use new routing context for %s profile (%s params)", rProfile, rParamsStr));
		di.waitTime = System.currentTimeMillis() - waitTime;
		di.selectedCache = "NEW";
		return cs;
	}

	private String getCleanRouteParams(RouteParameters rp, String rProfile) {
		GeneralRouter defaultRouter = RoutingConfiguration.getDefault().build(rProfile, new RoutingMemoryLimits(MEM_LIMIT, MEM_LIMIT)).router;
		String rParamsStr = rp.routeParams.toString();
		if (defaultRouter != null) {
			// clean up parameters string key for routing
			Map<String, String> cleanRouteParams = new TreeMap<String, String>();
			Map<String, RoutingParameter> defaultParams = defaultRouter.getParameters();
			for (String key : rp.routeParams.keySet()) {
				String value = rp.routeParams.get(key).trim();
				// add only existing parameter and non-default params
				RoutingParameter pm = defaultParams.get(key);
				if (pm != null && pm.getType() == RoutingParameterType.BOOLEAN) {
					if (!(pm.getDefaultBoolean() + "").equalsIgnoreCase(value)) {
						cleanRouteParams.put(key, value);
					}
				} else if (pm != null && !value.equals("") && !value.equals("0") && !value.equals("0.0")) {
					cleanRouteParams.put(key, value);
				} else if (defaultRouter.containsAttribute(key)
						&& !Algorithms.objectEquals(value, defaultRouter.getAttribute(key))) {
					cleanRouteParams.put(key, value);
				}
			}
			rParamsStr = cleanRouteParams.toString();
		}
		return rParamsStr;
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
		if (ref == null) {
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
		cacheFiles.writeToFile(new File(tileConfig.cacheLocation, CachedOsmandIndexes.INDEXES_DEFAULT_FILENAME));
		if (nativelib != null) {
			nativelib.initMapFile(target.getAbsolutePath(), false);
		}
		LOGGER.info("Init new obf file " + target.getName() + " " + (System.currentTimeMillis() - val) + " ms");
	}

	public List<BinaryMapIndexReaderReference> getObfReaders(QuadRect quadRect, List<LatLon> bbox, String reason) throws IOException {
		initObfReaders();
		List<BinaryMapIndexReaderReference> files = new ArrayList<>();
		List<File> filesToUse = getMaps(quadRect, bbox);
		if (!filesToUse.isEmpty()) {
			for (File f : filesToUse) {
				BinaryMapIndexReaderReference ref = obfFiles.get(f.getAbsolutePath());
				files.add(ref);
			}
		}
		LOGGER.info(String.format("Preparing %d files for %s", files.size(), reason));
		return files;
	}

	private List<File> getMaps(QuadRect quadRect, List<LatLon> bbox) throws IOException {
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
		return filterMap(files);
	}

	public BinaryMapIndexReaderReference getBaseMap() throws IOException {
		initObfReaders();
		for (BinaryMapIndexReaderReference ref : obfFiles.values()) {
			if (ref.file.getName().contains("basemap")) {
				LOGGER.info("Base map successfully found " + ref.file.getName());
				return ref;
			}
		}
		throw new IOException("Base map not found! Add it in OBF dir.");
	}

	private List<File> filterMap(List<File> files) throws IOException {
		List<File> res = new ArrayList<>();
		if (osmandRegions == null) {
			osmandRegions = new OsmandRegions();
			osmandRegions.prepareFile();
		}
		
		for (File file : files) {
			String dwName = getDownloadNameByFileName(file.getName());
			WorldRegion wr = osmandRegions.getRegionDataByDownloadName(dwName);
			if (wr != null && (wr.isRegionJoinMapDownload() || wr.isRegionJoinMapDownload())) {
				// skip joint maps
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


	public synchronized void initObfReaders() throws IOException {
		if (cacheFiles != null) {
			return;
		}
		File mapsFolder = new File(tileConfig.obfLocation);
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
		return ResponseEntity.badRequest()
				.body("Tile service is not initialized: " + (tileConfig == null ? "" : tileConfig.initErrorMessage));
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
}
