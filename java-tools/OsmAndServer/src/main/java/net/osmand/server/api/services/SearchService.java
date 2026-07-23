package net.osmand.server.api.services;

import static net.osmand.binary.BinaryMapIndexReader.SearchRequest.ZOOM_TO_SEARCH_POI;
import static net.osmand.data.Amenity.OPENING_HOURS;
import static net.osmand.data.MapObject.unzipContent;
import static net.osmand.gpx.GPXUtilities.AMENITY_PREFIX;
import static net.osmand.search.SearchUICore.createAddressString;
import static net.osmand.search.SearchUICore.getDominatedCity;
import static net.osmand.search.SearchUICore.getMainCityName;
import static net.osmand.shared.gpx.GpxUtilities.OSM_PREFIX;
import static net.osmand.util.LocationParser.parseOpenLocationCode;
import static net.osmand.util.OpeningHoursParser.parseOpenedHours;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import net.osmand.search.core.spatial.SpatialPoiSearch.SpatialPoiType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.Nullable;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchPoiTypeFilter;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapIndexReaderStats;
import net.osmand.binary.BinaryMapPoiReaderAdapter;
import net.osmand.binary.GeocodingUtilities;
import net.osmand.binary.NameIndexReader;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.City.CityType;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.QuadRect;
import net.osmand.data.Street;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.osm.PoiFilter;
import net.osmand.osm.PoiType;
import net.osmand.osm.edit.Entity;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchExportSettings;
import net.osmand.search.core.SearchPhrase;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.search.core.TopIndexFilter;
import net.osmand.search.core.spatial.SpatialPoiSearch;
import net.osmand.search.core.spatial.SpatialSearchContext;
import net.osmand.search.core.spatial.SpatialSearchResult;
import net.osmand.search.core.spatial.SpatialTextSearch;
import net.osmand.search.core.spatial.SpatialTextSearch.SpatialSearchResults;
import net.osmand.search.core.spatial.SpatialTextSearch.SpatialTextSearchSettings;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.server.controllers.pub.GeojsonClasses.FeatureCollection;
import net.osmand.server.controllers.pub.GeojsonClasses.Geometry;
import net.osmand.server.utils.MapPoiTypesTranslator;
import net.osmand.util.Algorithms;
import net.osmand.util.LocationParser;
import net.osmand.util.MapUtils;
import net.osmand.util.OpeningHoursParser.OpeningHours;
import net.osmand.util.TextDirectionUtil;

@Service
public class SearchService {

	private static final Log LOGGER = LogFactory.getLog(SearchService.class);
	public static final String IS_OPENED_PREFIX = "open:";
	public static final String OPENING_HOURS_INFO_SUFFIX = "_info";

	private static final ObjectType[] CLASSIC_SEARCH_TYPES = {
			// Exclude PARTIAL_LOCATION and app-only types (fav, tracks, markers, etc.)
			ObjectType.CITY, ObjectType.VILLAGE, ObjectType.BOUNDARY, ObjectType.POSTCODE,
			ObjectType.STREET, ObjectType.HOUSE, ObjectType.STREET_INTERSECTION,
			ObjectType.POI_TYPE, ObjectType.POI, ObjectType.LOCATION, ObjectType.REGION
	};

	@Autowired
	OsmAndMapsService osmAndMapsService;

	@Autowired
	WikiService wikiService;

	OsmandRegions osmandRegions;

	private final ConcurrentHashMap<String, Map<String, String>> translationsCache = new ConcurrentHashMap<>();

	private static final int SEARCH_RADIUS_LEVEL = 1;
	private static final int TOTAL_LIMIT_POI = 2000;
	private static final int SPATIAL_POI_CATEGORY_VIEW_ZOOM_SHIFT = 2;
	// the engine cache is shared by all requests (engine default 1000 assumes per-client caches)
	private static final int SPATIAL_PREFIX_CACHE_LIMIT = 4_000;
	private static final int SPATIAL_POI_CATEGORY_MIN_ZOOM = 4;
	private static final int SPATIAL_POI_CATEGORY_MAX_ZOOM = 18;
	// viewport is ~3 tiles wide: zoom ~= log2(360 * 3 / bboxLonWidth)
	private static final int SPATIAL_POI_CATEGORY_VIEW_TILES = 3;
	private static final int TOTAL_LIMIT_SEARCH_RESULTS = 15000;
	private static final int TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB = 1000;
	private static final double SEARCH_POI_RADIUS_DEGREE = 0.0007;

	private static final String DEFAULT_SEARCH_LANG = "en";
	private static final String AND_RES = "/androidResources/";

	private static final String DELIMITER = " ";
	private static final String WIKI_POI_TYPE = "osmwiki";
	// For test increase default limit to cache more
	private static final int TEST_CACHE_PREFIX_LIMIT = 1_000; // 8_000 too much

	private final ConcurrentHashMap<String, MapPoiTypes> poiTypesByLocale = new ConcurrentHashMap<>();

	private final AtomicInteger spatialSearchThreadNum = new AtomicInteger();
	private final ThreadPoolExecutor spatialSearchThreadPool = new ThreadPoolExecutor(
			1, 4, 10, TimeUnit.MINUTES, new SynchronousQueue<>(),
			r -> {
				Thread t = new Thread(r, "spatial-search-" + spatialSearchThreadNum.incrementAndGet());
				t.setDaemon(true);
				return t;
			});
	// shared between pool threads: SpatialSearchGlobalCache is concurrent (per-file locking),
	// so all threads reuse one prefix cache instead of 4 independent ones
	private final SpatialTextSearch spatialTextSearch = new SpatialTextSearch();
	private final ThreadLocal<RegionsReaderHolder> osmandRegionsLocal = ThreadLocal
			.withInitial(() -> new RegionsReaderHolder());
	// reused for cache
	private SpatialPoiSearch poiSearch;
	private volatile Map<String, PoiType> poiAdditionalsByKey;

	public static class PoiSearchResult {

		public PoiSearchResult(boolean useLimit, boolean mapLimitExceeded, boolean alreadyFound,
		                       FeatureCollection features) {
			this.useLimit = useLimit;
			this.mapLimitExceeded = mapLimitExceeded;
			this.alreadyFound = alreadyFound;
			this.features = features;
		}

		public boolean useLimit;
		// spatial search was requested but found nothing / category unsupported - results are from the old scan
		public boolean oldSearch;
		public boolean mapLimitExceeded;
		public boolean alreadyFound;
		public FeatureCollection features;
	}

	private static class PoiSearchLimit {
		int limit;
		int leftoverLimit;
		boolean useLimit;

		public PoiSearchLimit(int limit, int leftoverLimit, boolean useLimit) {
			this.limit = limit;
			this.leftoverLimit = leftoverLimit;
			this.useLimit = useLimit;
		}

		public int getRemainingForSave(int categoryStartSize, int currentSize) {
			int remainingForCategory = getRemainingForCategory(categoryStartSize, currentSize);
			int remainingTotal = getRemainingTotal();
			return Math.min(remainingForCategory, remainingTotal);
		}

		public boolean shouldStopCategory(int categoryStartSize, int currentSize) {
			return getRemainingForCategory(categoryStartSize, currentSize) == 0;
		}

		private int getMaxForCategory() {
			return Math.min(limit, leftoverLimit);
		}

		private int getRemainingForCategory(int categoryStartSize, int currentSize) {
			int used = currentSize - categoryStartSize;
			return Math.max(0, getMaxForCategory() - used);
		}

		private int getRemainingTotal() {
			return Math.max(0, leftoverLimit);
		}

		public void updateAfterCategory(int categoryStartSize, int categoryEndSize) {
			int used = categoryEndSize - categoryStartSize;
			leftoverLimit -= used;
			if (leftoverLimit <= 0) {
				useLimit = true;
			}
		}

		public boolean isLimitReached() {
			return leftoverLimit <= 0;
		}
	}

	public static class PoiSearchData {

		public PoiSearchData(List<PoiSearchCategory> categories, String northWest, String southEast,
		                     String savedNorthWest, String savedSouthEast, int prevCategoriesCount, String prevSearchRes,
		                     String prevSearchCategory) {
			this.categories = categories;
			this.bbox = getBboxCoords(Arrays.asList(northWest, southEast));
			if (savedNorthWest != null && savedSouthEast != null) {
				this.savedBbox = getBboxCoords(Arrays.asList(savedNorthWest, savedSouthEast));
			}
			this.prevCategoriesCount = prevCategoriesCount;
			if (prevSearchRes != null && prevSearchCategory != null) {
				this.prevSearchRes = prevSearchRes;
				this.prevSearchCategory = prevSearchCategory;
			}
		}

		public List<PoiSearchCategory> categories;
		public List<LatLon> bbox;
		public List<LatLon> savedBbox;
		public int prevCategoriesCount;
		public String prevSearchRes;
		public String prevSearchCategory;

		private static List<LatLon> getBboxCoords(List<String> coords) {
			List<LatLon> bbox = new ArrayList<>();
			for (String coord : coords) {
				String[] lanLonArr = coord.split(",");
				bbox.add(new LatLon(Double.parseDouble(lanLonArr[0]), Double.parseDouble(lanLonArr[1])));
			}
			return bbox;
		}
	}

	public record PoiSearchCategory(String category, String lang) {
	}

	public SearchService() {
		try {
			osmandRegions = PlatformUtil.getOsmandRegions();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public List<LatLon> getBboxCoords(List<String> coords) {
		List<LatLon> bbox = new ArrayList<>();
		for (String coord : coords) {
			String[] lanLonArr = coord.split(",");
			bbox.add(new LatLon(Double.parseDouble(lanLonArr[0]), Double.parseDouble(lanLonArr[1])));
		}
		return bbox;
	}

	public record SearchContext(double lat, double lon, String text, String locale, boolean baseSearch,
	                            String northWest, String southEast) {
	}

	public record SearchOption(boolean unlimited, SearchExportSettings exportedSettings, Double radiusToLoadMaps,
	                           boolean queryIsCompleted, boolean isBatch, ObjectType... searchTypes) {
		private static final double SEARCH_RADIUS_DEGREE = 1.5;

		public double getRadius() {
			return radiusToLoadMaps == null ? SEARCH_RADIUS_DEGREE : radiusToLoadMaps;
		}
	}

	public record SearchResults(List<SearchResult> results, SearchSettings settings, JSONObject unitTestJson,
	                            SearchPhrase phrase) {
		public SearchResults(List<SearchResult> results) {
			this(results, null, null, null);
		}
	}

	public List<Feature> search(SearchContext ctx, String timeZone) throws IOException {
		long tm = System.currentTimeMillis();
		SearchResults searchResults = getImmediateSearchResults(ctx,
				new SearchOption(false, null, null, true, false, CLASSIC_SEARCH_TYPES), null);
		List<SearchResult> res = searchResults.results();
		if (System.currentTimeMillis() - tm > 1000) {
			BinaryMapIndexReaderStats.SearchStat stat = searchResults.settings != null
					? searchResults.settings.getStat()
					: null;
			LOGGER.info(String.format("Search %s results %d took %.2f sec - %s", ctx.text,
					searchResults.results() == null ? 0 : searchResults.results().size(),
					(System.currentTimeMillis() - tm) / 1000.0, stat));
		}
		List<Feature> features = new ArrayList<>();
		if (res != null && !res.isEmpty()) {
			saveSearchResult(res, features, timeZone);
		}

		return !features.isEmpty() ? features : Collections.emptyList();
	}

	public static class SpatialResponse {
		public List<Feature> features = new ArrayList<>();
		public Map<String, Object> info = new LinkedHashMap<>();
	}

	public record SpatialResults(SpatialSearchResults results, SpatialSearchContext.SpatialSearchStats stats) {
	}

	// dev-only: new prototype search using SpatialTextSearch.
	public SpatialResults searchTestSpatial(SearchContext ctx, SearchService.SearchOption options, List<BinaryMapIndexReader> readers, boolean printLogs)
			throws IOException {
		SpatialResults res = null;
		try {
			if (readers == null) {
				QuadRect points = osmAndMapsService.points(null,
						new LatLon(ctx.lat + options.getRadius(), ctx.lon - options.getRadius()),
						new LatLon(ctx.lat - options.getRadius(), ctx.lon + options.getRadius()));
				List<OsmAndMapsService.BinaryMapIndexReaderReference> maps = getMapsForSearch(points, false);
				if (maps.isEmpty()) {
					return null;
				}
				readers = osmAndMapsService.getReaders(maps, null, true);
			}

			res = searchTestSpatial(ctx, readers, printLogs);
		} catch (RuntimeException e) {
			LOGGER.error(String.format("Spatial search failed for '%s': %s", ctx.text, e), e);
			StringWriter stackTrace = new StringWriter();
			e.printStackTrace(new PrintWriter(stackTrace));
			LOGGER.error("RuntimeException stacktrace:\n" + stackTrace);
		} finally {
			osmAndMapsService.unlockReaders(readers);
		}
		return res;
	}

	@PostConstruct
	public void warmupSpatialPoiTypeSearch() {
		Thread t = new Thread(this::getSpatialPoiTypeSearch, "spatial-search-warmup");
		t.setDaemon(true);
		t.start();
	}

	public synchronized SpatialPoiSearch getSpatialPoiTypeSearch() {
		if (poiSearch == null) {
			MapPoiTypes poiTypes = MapPoiTypes.getDefault();
			poiTypes.setPoiTranslator(parseGlobalTranslations());
			poiSearch = new SpatialPoiSearch(poiTypes);
		}
		return poiSearch;
	}

	private SpatialResults searchTestSpatial(SearchContext ctx, List<BinaryMapIndexReader> readers, boolean printLogs)
			throws IOException {
		if (readers == null || readers.isEmpty()) {
			return null;
		}
		SpatialTextSearchSettings settings = SpatialTextSearchSettings.defaultSettings();
		settings.AUTO_CLEAR_PREFIX_CACHE_LIMIT = TEST_CACHE_PREFIX_LIMIT;
		SpatialSearchContext sscontext = new SpatialSearchContext(settings, readers, getSpatialPoiTypeSearch(), new LatLon(ctx.lat, ctx.lon));
		SpatialSearchContext.SpatialSearchStats stats = sscontext.getStats();
		stats.printLogs = printLogs;

		SpatialSearchResults results = spatialTextSearch.searchAPI(ctx.text, sscontext);
		return new SpatialResults(results, stats);
	}

	public List<OsmAndMapsService.BinaryMapIndexReaderReference> getMapRefs(String northWest,
	                                                                        String southEast, double radius,
	                                                                        boolean baseSearch) throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return Collections.emptyList();
		}
		if (northWest == null || southEast == null) {
			return Collections.emptyList();
		}
		QuadRect points = getSearchBbox(getBboxCoords(Arrays.asList(northWest, southEast)), radius);
		return getMapsForSearch(points, baseSearch);
	}

	public List<BinaryMapIndexReader> openReaders(
			List<OsmAndMapsService.BinaryMapIndexReaderReference> maps) throws IOException {
		List<BinaryMapIndexReader> readers = new ArrayList<>();
		if (maps == null) {
			return readers;
		}
		try {
			for (OsmAndMapsService.BinaryMapIndexReaderReference ref : maps) {
				if (ref.file.getName().startsWith("World_")) {
					continue;
				}
				BinaryMapIndexReader reader = new BinaryMapIndexReader(new RandomAccessFile(ref.file, "r"), ref.file, true);
				if (reader.containsAddressData() && reader.containsRouteData()) {
					readers.add(reader);
				} else {
					reader.close();
				}
			}
			BinaryMapIndexReader regionsReader = openRegionsReader();
			if (regionsReader != null) {
				readers.add(regionsReader);
			}
		} catch (IOException | RuntimeException e) {
			closeReaders(readers);
			throw e;
		}
		return readers;
	}

	private BinaryMapIndexReader openRegionsReader() {
		BinaryMapIndexReader regionsReader = osmandRegions.getFile();
		try {
			if (regionsReader == null || regionsReader.getFile() == null) {
				return null;
			}
			return new BinaryMapIndexReader(new RandomAccessFile(regionsReader.getFile(), "r"), regionsReader);
		} catch (Exception e) {
			LOGGER.warn("Failed to open regions reader for spatial search.", e);
		}
		return null;
	}

	private static class RegionsReaderHolder {
		BinaryMapIndexReader reader;
		long fileTimestamp;
	}

	/**
	 * Regions reader owned by the current pool thread: reopened if the underlying
	 * obf file was updated, retried if the previous attempt to open it failed.
	 */
	private BinaryMapIndexReader regionsReaderForThread() {
		RegionsReaderHolder holder = osmandRegionsLocal.get();
		BinaryMapIndexReader shared = osmandRegions == null ? null : osmandRegions.getFile();
		File regionsFile = shared == null ? null : shared.getFile();
		if (regionsFile == null) {
			return holder.reader;
		}
		long ts = regionsFile.lastModified();
		if (holder.reader != null && holder.fileTimestamp != ts) {
			try {
				holder.reader.close();
			} catch (IOException e) {
				LOGGER.warn("Failed to close outdated regions reader.", e);
			}
			holder.reader = null;
		}
		if (holder.reader == null) {
			holder.reader = openRegionsReader();
			holder.fileTimestamp = ts;
		}
		return holder.reader;
	}

	@PreDestroy
	public void shutdownSpatialSearchPool() {
		spatialSearchThreadPool.shutdownNow();
	}

	public void closeReaders(List<BinaryMapIndexReader> readers) {
		if (readers == null) {
			return;
		}
		for (BinaryMapIndexReader reader : readers) {
			try {
				reader.close();
			} catch (IOException e) {
				LOGGER.warn("Failed to close spatial test reader.", e);
			}
		}
	}

	public SpatialResponse searchSpatial(SearchContext ctx, String timeZone, boolean autocomplete) throws IOException {
		long sTime = System.currentTimeMillis();
		SpatialResponse response = new SpatialResponse();
		if (!osmAndMapsService.validateAndInitConfig()) {
			return response;
		}
		final AtomicBoolean cancelled = new AtomicBoolean();
		// suggestions must fail fast, full search gets the long budget
		final long timeoutMs = autocomplete ? 3_000 : 15_000;
		boolean readersOwnedByWorker = false;
//		double radius = SearchOption.SEARCH_RADIUS_DEGREE;
//		QuadRect points = osmAndMapsService.points(null,
//				new LatLon(ctx.lat + radius, ctx.lon - radius),
//				new LatLon(ctx.lat - radius, ctx.lon + radius));
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		try {
//			List<OsmAndMapsService.BinaryMapIndexReaderReference> list = getMapsForSearch(points, ctx.baseSearch);
			// OPTION B
			List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService
					.getObfReadersForSpatialSearch(ctx.lat, ctx.lon, autocomplete);
			if (list.isEmpty()) {
				return response;
			}
			usedMapList = osmAndMapsService.getReaders(list, null);
			if (usedMapList.isEmpty()) {
				return response;
			}
			LatLon coord = parseLocation(ctx.text, new LatLon(ctx.lat, ctx.lon));
			if (coord != null) {
				SearchResult sr = new SearchResult();
				sr.object = coord;
				sr.location = coord;
				sr.objectType = ObjectType.LOCATION;
				sr.localeName = ((float) coord.getLatitude()) + ", " + ((float) coord.getLongitude());
				response.features.add(getFeature(sr, timeZone));
			}
			// In future multiple spatialTextSearch & multiple osmand regions
			SpatialTextSearchSettings settings = autocomplete
					? SpatialTextSearchSettings.suggestionSettings()
					: SpatialTextSearchSettings.defaultSettings();
			settings.AUTO_CLEAR_PREFIX_CACHE_LIMIT = SPATIAL_PREFIX_CACHE_LIMIT;
			final List<BinaryMapIndexReader> lockedReaders = usedMapList;
			final ResultMatcher<SpatialSearchResult> matcher = new ResultMatcher<SpatialSearchResult>() {

				@Override
				public boolean publish(SpatialSearchResult object) {
					return false; // used only as a cancellation hook, results are collected by searchAPI
				}

				@Override
				public boolean isCancelled() {
					return cancelled.get();
				}
			};
			Future<SpatialSearchResults> task = spatialSearchThreadPool.submit(new Callable<SpatialSearchResults>() {

				@Override
				public SpatialSearchResults call() throws Exception {
					try {
						// worker owns a private copy: the caller never sees concurrent mutation
						List<BinaryMapIndexReader> readers = new ArrayList<>(lockedReaders);
						BinaryMapIndexReader regionsReader = regionsReaderForThread();
						if (regionsReader != null) {
							readers.add(regionsReader);
						}
						SpatialSearchContext sscontext =
								new SpatialSearchContext(settings, readers, getSpatialPoiTypeSearch(), new LatLon(ctx.lat, ctx.lon));
						sscontext.resultMatcher = matcher;
						return spatialTextSearch.searchAPI(ctx.text, sscontext);
					} finally {
						// unlock strictly after the search has really finished (incl. timeout/cancel path)
						osmAndMapsService.unlockReaders(lockedReaders);
					}
				}
			});
			readersOwnedByWorker = true;
			SpatialSearchResults res = task.get(timeoutMs, TimeUnit.MILLISECONDS);
			if (res.mainResults != null) {
				String dominatedCity = calculateSpatialDominatedCity(res.mainResults, ctx.locale);
				SpatialPoiSearch poiTypeSearch = getSpatialPoiTypeSearch();
				Map<MapObject, Feature> amenityFeatureCache = new IdentityHashMap<>();
				for (SpatialSearchResult r : res.mainResults) {
					List<MapObject> objs = r.getObjects();
					if (r.isPoiCategory()) {
						SpatialPoiType type = r.getPoiCategory(poiTypeSearch);
						if (type != null) {
							Feature f = getSpatialPoiTypeFeature(type);
							f.prop(PoiTypeField.MATCHED_OBJECTS.getFieldName(),
									matchedObjects(objs, ctx.locale, timeZone, dominatedCity, r.getViewBBox31(),
											amenityFeatureCache));
							f.prop(PoiTypeField.VISIBLE_LEVEL.getFieldName(), r.visibleLevel());
							f.prop(PoiTypeField.COMPARE_KEY.getFieldName(), SpatialSearchResult.compareKeyString(r));
							response.features.add(f);
						}
					} else if (!objs.isEmpty()) {
						LatLon l = r.getLatLon() == null ? new LatLon(ctx.lat, ctx.lon) : r.getLatLon();
						Feature f = getSpatialFeature(l, objs, ctx.locale, timeZone, dominatedCity, r.getExtraNameMatch());
						if (f != null) {
							f.prop(PoiTypeField.MATCHED_OBJECTS.getFieldName(),
									matchedObjects(objs, ctx.locale, timeZone, dominatedCity, r.getViewBBox31(),
											amenityFeatureCache));
							f.prop(PoiTypeField.VISIBLE_LEVEL.getFieldName(), r.visibleLevel());
							f.prop(PoiTypeField.COMPARE_KEY.getFieldName(), SpatialSearchResult.compareKeyString(r));
							response.features.add(f);
						}
					}
				}
			}
			// extra info shown in the UI
			response.info.put("timeAll", String.format("%.1f", (System.currentTimeMillis() - sTime) / 1e3));
			response.info.put("atoms", String.format("%.2f, %,d", res.stats.step1Atoms.ms() / 1000.0,
					res.stats.tokenObjs));
			response.info.put("compute", String.format("%.2f, %,d", res.stats.step2Compute.ms() / 1000.0,
					res.stats.maxCombinations));
			response.info.put("results", response.features.size());
			response.info.put("words-matched", res.combinations == null || res.combinations.size() == 0 ? 0
					: res.combinations.get(0).getTokenCount());
//			response.info.put("x", res.combinations == null ? 0 : res.combinations.size());
		} catch (TimeoutException e) {
			LOGGER.warn(String.format("Spatial search timeout %d ms for '%s'", timeoutMs, ctx.text));
			response.info.put("timeout", true);
		} catch (RejectedExecutionException e) {
			LOGGER.warn(String.format("Spatial search rejected, all pool threads are busy: '%s'", ctx.text));
			response.info.put("busy", true);
		} catch (ExecutionException e) {
			LOGGER.error(String.format("Spatial search failed for '%s': %s", ctx.text, e.getCause()), e.getCause());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.warn(String.format("Spatial search interrupted for '%s'", ctx.text));
			response.info.put("interrupted", true);
		} catch (RuntimeException e) {
			LOGGER.error(String.format("Spatial search failed for '%s': %s", ctx.text, e), e);
		} finally {
			cancelled.set(true);
			if (!readersOwnedByWorker) {
				// worker never started (early return / rejection / pre-submit failure)
				osmAndMapsService.unlockReaders(usedMapList);
			}
		}
		return response;
	}

	private Feature getSpatialPoiTypeFeature(SpatialPoiType type) {
		Feature feature = new Feature(Geometry.point(new LatLon(0, 0)))
				.prop(PoiTypeField.TYPE.getFieldName(), ObjectType.POI_TYPE);
		Map<String, String> tags = getSpatialPoiTypeFields(type);
		for (Map.Entry<String, String> entry : tags.entrySet()) {
			feature.prop(entry.getKey(), entry.getValue());
		}
		return feature;
	}

	private Map<String, String> getSpatialPoiTypeFields(SpatialPoiType type) {
		if (type.singleType != null) {
			if (type.singleType instanceof PoiType poiType && poiType.isAdditional()
					&& type.getParentTypes() != null && type.getParentTypes().size() > 1) {
				SearchCoreFactory.PoiAdditionalCustomFilter filter =
						new SearchCoreFactory.PoiAdditionalCustomFilter(MapPoiTypes.getDefault(), poiType);
				Map<String, String> tags = getPoiTypeFields(filter);
				tags.put(PoiTypeField.NAME.getFieldName(), filter.getTranslation());
				return tags;
			}
			Map<String, String> tags = getPoiTypeFields(type.singleType);
			tags.put(PoiTypeField.NAME.getFieldName(), type.singleType.getTranslation());
			if (type.getWikidataId() != null) {
				tags.put(PoiTypeField.WIKIDATA_ID.getFieldName(), type.getWikidataId());
			}
			return tags;
		}
		Map<String, String> tags = new HashMap<>();
		if (type.poiAdditional != null) {
			String valueKey = TopIndexFilter.getValueKey(type.poiAdditional);
			String tag = type.getKey();
			if (tag.endsWith("_" + valueKey)) {
				tag = tag.substring(0, tag.length() - valueKey.length() - 1);
			}
			if (tag.startsWith(MapPoiTypes.TOP_INDEX_ADDITIONAL_PREFIX)) {
				tag = tag.substring(MapPoiTypes.TOP_INDEX_ADDITIONAL_PREFIX.length());
			}
			tags.put(PoiTypeField.CATEGORY_KEY_NAME.getFieldName(), tag);
			tags.put(PoiTypeField.CATEGORY_ICON.getFieldName(), tag);
			// canonical key ("top_index_brand_<valueKey>"): web uses it as the search type in the URL
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getKey());
			tags.put(PoiTypeField.NAME.getFieldName(), type.poiAdditional);
			if (type.getWikidataId() != null) {
				tags.put(PoiTypeField.WIKIDATA_ID.getFieldName(), type.getWikidataId());
			}
		}
		return tags;
	}

	private List<Map<String, Object>> matchedObjects(List<MapObject> objs, String locale, String timeZone,
	                                                 String dominatedCity, int[] bbox31,
	                                                 Map<MapObject, Feature> amenityFeatureCache) {
		List<Map<String, Object>> matched = new ArrayList<>();
		for (MapObject o : objs) {
			if (o.getLocation() == null) {
				continue;
			}
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("name", o.getName(locale));
			m.put("type", o.getClass().getSimpleName());
			m.put("lat", roundCoord(o.getLocation().getLatitude()));
			m.put("lon", roundCoord(o.getLocation().getLongitude()));
			if (o instanceof Amenity amenity) {
				Feature feature = amenityFeatureCache.computeIfAbsent(amenity,
						k -> getPoiFeature(buildPoiSearchResult((Amenity) k, locale, dominatedCity), timeZone));
				m.putAll(feature.properties);
			} else if (o instanceof City city) {
				m.put(PoiTypeField.CITY_TYPE.getFieldName(), city.getType().name());
			}
			if (bbox31 != null && bbox31.length >= 4) {
				Map<String, Object> bbox = new LinkedHashMap<>();
				bbox.put("top", roundCoord(MapUtils.get31LatitudeY(bbox31[1])));
				bbox.put("left", roundCoord(MapUtils.get31LongitudeX(bbox31[0])));
				bbox.put("bottom", roundCoord(MapUtils.get31LatitudeY(bbox31[3])));
				bbox.put("right", roundCoord(MapUtils.get31LongitudeX(bbox31[2])));
				m.put(PoiTypeField.BBOX_LAT_LON.getFieldName(), bbox);
			}
			matched.add(m);
		}
		return matched;
	}

	private double roundCoord(double value) {
		return Math.round(value * 1000000d) / 1000000d;
	}

	private Feature getSpatialFeature(LatLon loc, List<MapObject> objs, String locale, String timeZone,
	                                  String dominatedCity, String extraNameMatch) {
		MapObject obj = objs.isEmpty() ? null : objs.get(0);
		if (obj == null || loc == null) {
			return null;
		}
		if (obj instanceof Amenity amenity) {
			SearchResult result = buildPoiSearchResult(amenity, locale, dominatedCity);
			result.localeName = amenity.getName(locale);
			if (Algorithms.isNotEmpty(extraNameMatch)) {
				result.localeName += " (" + extraNameMatch + ")"; // ref
			}
			return getFeature(result, timeZone);
		}
		SearchResult result = new SearchResult();
		result.object = obj;
		result.location = loc;
		if (obj instanceof Building b && b.isInterpolation() && Algorithms.isNotEmpty(extraNameMatch)) {
			result.localeName = extraNameMatch; // interpolated house number
		} else {
			result.localeName = obj.getName(locale);
			if (Algorithms.isNotEmpty(extraNameMatch)) {
				result.localeName += " [" + extraNameMatch + "]"; // ref for non-Amenity objects
			}
		}
		if (obj instanceof Street) {
			result.objectType = ObjectType.STREET;
			City city = getSpatialCity(objs);
			if (city != null) {
				result.localeRelatedObjectName = city.getName(locale);
			}
		} else if (obj instanceof Building) {
			result.objectType = ObjectType.HOUSE;
			Street street = getSpatialStreet(objs);
			if (street != null) {
				result.localeRelatedObjectName = street.getName(locale);
			}
			City city = getSpatialCity(objs);
			if (city != null) {
				SearchResult parent = new SearchResult();
				parent.localeRelatedObjectName = city.getName(locale);
				result.parentSearchResult = parent;
			}
		} else if (obj instanceof City) {
			result.objectType = ObjectType.CITY;
		} else {
			result.objectType = ObjectType.LOCATION;
		}
		return getFeature(result, timeZone);
	}

	private Street getSpatialStreet(List<MapObject> objs) {
		for (MapObject obj : objs) {
			if (obj instanceof Street street) {
				return street;
			}
		}
		return null;
	}

	private City getSpatialCity(List<MapObject> objs) {
		for (MapObject obj : objs) {
			if (obj instanceof City city) {
				return city;
			}
			if (obj instanceof Street street && street.getCity() != null) {
				return street.getCity();
			}
		}
		return null;
	}

	public SearchResults getImmediateSearchResults(SearchContext ctx, SearchOption option,
	                                               Consumer<List<SearchResult>> consumerInContext) throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return new SearchResults(Collections.emptyList());
		}
		SearchUICore searchUICore = new SearchUICore(getMapPoiTypes(ctx.locale), ctx.locale, false);
		if (option.searchTypes != null) {
			searchUICore.getPhrase().getSettings().updateSearchTypes(option.searchTypes);
		}
		if (!option.unlimited) {
			searchUICore.setTotalLimit(TOTAL_LIMIT_SEARCH_RESULTS);
		}
		searchUICore.getSearchSettings().setRegions(osmandRegions);

		QuadRect points = osmAndMapsService.points(null,
				new LatLon(ctx.lat + option.getRadius(), ctx.lon - option.getRadius()),
				new LatLon(ctx.lat - option.getRadius(), ctx.lon + option.getRadius()));
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> list = getMapsForSearch(points, ctx.baseSearch);
			if (list.isEmpty()) {
				return new SearchResults(Collections.emptyList());
			}
			usedMapList = osmAndMapsService.getReaders(list, null);
			if (usedMapList.isEmpty()) {
				return new SearchResults(Collections.emptyList());
			}
			SearchSettings settings = searchUICore.getPhrase().getSettings();
			BinaryMapIndexReaderStats.SearchStat stat = new BinaryMapIndexReaderStats.SearchStat();
			stat.isBatch = option.isBatch;
			settings.setStat(stat);

			settings.setOfflineIndexes(usedMapList);
			settings.setRadiusLevel(SEARCH_RADIUS_LEVEL);
			settings.setExportSettings(option.exportedSettings);
			searchUICore.updateSettings(settings);

			searchUICore.init();
			searchUICore.registerAPI(new SearchCoreFactory.SearchRegionByNameAPI());

			SearchUICore.SearchResultCollection resultCollection = searchUICore.immediateSearch(
					ctx.text + (option.queryIsCompleted ? DELIMITER : ""), new LatLon(ctx.lat, ctx.lon));
			resultCollection = addPoiCategoriesToSearchResult(resultCollection, ctx.text, ctx.locale, searchUICore);

			List<SearchResult> res = resultCollection != null ? resultCollection.getCurrentSearchResults()
					: Collections.emptyList();
			res = filterBrandsOutsideBBox(res, ctx.northWest, ctx.southEast, ctx.locale, ctx.lat, ctx.lon,
					ctx.baseSearch);
			res = res.size() > TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB ? res.subList(0, TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB)
					: res;
			if (consumerInContext != null) {
				consumerInContext.accept(res);
			}

			JSONObject unitTestJson = null;
			if (option.exportedSettings != null) {
				unitTestJson = SearchUICore.createTestJSON(resultCollection, settings.getExportedObjects(),
						settings.getExportedCities());
			}
			return new SearchResults(res, settings, unitTestJson,
					resultCollection == null ? null : resultCollection.getPhrase());
		} finally {
			osmAndMapsService.unlockReaders(usedMapList);
		}
	}

	public Feature getPoi(String type, String name, LatLon loc, Long osmId, String timeZone) throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return null;
		}
		QuadRect searchBbox = osmAndMapsService.points(null,
				new LatLon(loc.getLatitude() + SEARCH_POI_RADIUS_DEGREE, loc.getLongitude() - SEARCH_POI_RADIUS_DEGREE),
				new LatLon(loc.getLatitude() - SEARCH_POI_RADIUS_DEGREE,
						loc.getLongitude() + SEARCH_POI_RADIUS_DEGREE));
		List<BinaryMapIndexReader> readers = new ArrayList<>();
		Feature feature = null;

		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> mapRefs = getMapsForSearch(searchBbox, false);
			if (mapRefs.isEmpty()) {
				return null;
			}
			readers = osmAndMapsService.getReaders(mapRefs, null);
			if (readers.isEmpty()) {
				return null;
			}
			SearchUICore searchUICore = prepareSearchUICoreForSearchByPoiType(readers, searchBbox, DEFAULT_SEARCH_LANG,
					loc.getLatitude(), loc.getLongitude());

			// Find POIs by type
			SearchUICore.SearchResultCollection rc = searchPoiByCategory(searchUICore, type,
					TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB);
			if (rc == null) {
				return null;
			}

			if (name != null) {
				feature = getPoiFeatureByName(rc, name, timeZone);
			} else if (osmId != null) {
				feature = getPoiFeatureByOsmId(rc, osmId, timeZone);
			}

		} finally {
			osmAndMapsService.unlockReaders(readers);
		}
		return feature;
	}

	private Feature getPoiFeatureByName(SearchUICore.SearchResultCollection rc, String name, String timeZone) {
		for (SearchResult r : rc.getCurrentSearchResults()) {
			if (r.objectType != ObjectType.POI || !(r.object instanceof Amenity a)) {
				continue;
			}
			if (matchesName(a, name)) {
				Feature f = getPoiFeature(r, timeZone);
				if (f != null) {
					return f;
				}
			}
		}
		return null;
	}

	private Feature getPoiFeatureByOsmId(SearchUICore.SearchResultCollection rc, long osmId, String timeZone) {
		for (SearchResult r : rc.getCurrentSearchResults()) {
			if (r.objectType != ObjectType.POI || !(r.object instanceof Amenity a)) {
				continue;
			}
			if (ObfConstants.getOsmObjectId(a) == osmId) {
				Feature f = getPoiFeature(r, timeZone);
				if (f != null) {
					return f;
				}
			}
		}
		return null;
	}

	public Feature getPoiResultByShareLink(String type, LatLon loc, String name, Long osmId, Long wikidataId,
	                                       String timeZone) throws IOException {
		Feature poiFeature = getPoi(type, name, loc, osmId, timeZone);

		if (poiFeature != null && wikidataId == null) {
			Object wikiIdFromOsm = poiFeature.properties.get("osm_tag_wikidata");
			if (wikiIdFromOsm != null) {
				wikidataId = Long.parseLong(wikiIdFromOsm.toString().replace("Q", ""));
			}
		}

		Feature wikiFeature = getWikiPoiById(wikidataId);
		return mergeFeatures(wikiFeature, poiFeature);
	}

	public static Feature mergeFeatures(Feature f1, Feature f2) {
		if (f1 == null)
			return f2;
		if (f2 == null)
			return f1;

		Feature merged = new Feature(f1.geometry != null ? f1.geometry : f2.geometry);
		merged.properties.putAll(f1.properties);
		merged.prop("poiTags", f2.properties);
		return merged;
	}

	private Feature getWikiPoiById(Long wikidataId) {
		if (wikidataId == null) {
			return null;
		}
		List<String> langs = List.of(DEFAULT_SEARCH_LANG);

		String langListQuery = wikiService.getLangListQuery(langs);

		String query = "SELECT w.id, w.photoId, w.wikiTitle, w.wikiLang, w.wikiDesc, w.photoTitle, "
				+ "w.osmid, w.osmtype, w.poitype, w.poisubtype, " + "w.search_lat AS lat, w.search_lon AS lon, "
				+ "arrayFirst(x -> has(w.wikiArticleLangs, x), " + langListQuery + ") AS lang, "
				+ "indexOf(w.wikiArticleLangs, lang) AS ind, " + "w.wikiArticleContents[ind] AS content, "
				+ "w.wvLinks, w.elo AS elo, w.topic AS topic, w.categories AS categories, w.qrank, w.labelsJson "
				+ "FROM wiki.wikidata w " + "WHERE w.id = " + wikidataId + " " + "ORDER BY w.elo DESC, w.qrank DESC";
		FeatureCollection res = wikiService.getPoiData(null, null, query, "lat", "lon", langs);
		return res.features.get(0);
	}

	private boolean matchesName(Amenity a, String name) {
		if (name == null || name.isBlank())
			return true;
		String target = name.trim();

		if (equalsIgnoreCaseSafe(a.getName(), target))
			return true;
		if (equalsIgnoreCaseSafe(a.getEnName(false), target))
			return true;

		Map<String, String> names = a.getNamesMap(true);
		for (String v : names.values()) {
			if (equalsIgnoreCaseSafe(v, target))
				return true;
		}
		return false;
	}

	private boolean equalsIgnoreCaseSafe(String s, String t) {
		return s != null && s.equalsIgnoreCase(t);
	}

	private SearchUICore.SearchResultCollection addPoiCategoriesToSearchResult(
			SearchUICore.SearchResultCollection resultCollection, String text, String locale,
			SearchUICore searchUICore) {
		List<SearchResult> poiCategories = searchPoiCategoriesByName(text, locale);
		List<SearchResult> uniquePoiCategories = new ArrayList<>(poiCategories.stream()
				.collect(Collectors.toMap(sr -> sr.localeName, Function.identity(), (first, second) -> first))
				.values());
		if (!uniquePoiCategories.isEmpty()) {
			if (resultCollection != null) {
				resultCollection.addSearchResults(uniquePoiCategories, true, true);
			} else {
				resultCollection = searchUICore.getCurrentSearchResult();
				if (resultCollection != null) {
					resultCollection.addSearchResults(uniquePoiCategories, true, true);
				}
			}
		}
		return resultCollection;
	}

	private List<SearchResult> filterBrandsOutsideBBox(List<SearchResult> res, String northWest, String southEast,
	                                                   String locale, double lat, double lon, boolean baseSearch) throws IOException {
		if (northWest != null && southEast != null) {
			List<LatLon> bbox = getBboxCoords(Arrays.asList(northWest, southEast));
			QuadRect searchBbox = getSearchBbox(bbox);
			List<BinaryMapIndexReader> readers = new ArrayList<>();
			try {
				List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = getMapsForSearch(searchBbox,
						baseSearch);
				readers = osmAndMapsService.getReaders(mapList, null);
				if (readers.isEmpty()) {
					return res.stream().filter(r -> r.objectType != ObjectType.POI_TYPE || r.file == null).toList();
				}
				SearchUICore searchUICore = prepareSearchUICoreForSearchByPoiType(readers, searchBbox, locale, lat,
						lon);
				return res.stream().filter(r -> {
					if (r.objectType != ObjectType.POI_TYPE || r.file == null) {
						return true;
					}
					Map<String, String> tags = getPoiTypeFields(r.object);
					if (tags.isEmpty()) {
						return true;
					}
					if (tags.get(PoiTypeField.CATEGORY_KEY_NAME.getFieldName()).startsWith("brand")) {
						SearchUICore.SearchResultCollection resultCollection;
						try {
							String brand = tags.get(PoiTypeField.NAME.getFieldName());
							SearchResult prevResult = new SearchResult();
							prevResult.object = r.object;
							prevResult.localeName = brand;
							prevResult.objectType = ObjectType.POI_TYPE;
							searchUICore.resetPhrase(prevResult);
							resultCollection = searchPoiByCategory(searchUICore, brand, 2);
						} catch (IOException e) {
							return true;
						}
						return resultCollection != null && !resultCollection.getCurrentSearchResults().isEmpty();
					}
					return true;
				}).toList();
			} finally {
				osmAndMapsService.unlockReaders(readers);
			}
		}
		return res;
	}

	private SearchUICore prepareSearchUICoreForSearchByPoiType(List<BinaryMapIndexReader> readers, QuadRect searchBbox,
	                                                           String locale, double lat, double lon) {
		MapPoiTypes mapPoiTypes = getMapPoiTypes(locale);

		SearchUICore searchUICore = new SearchUICore(mapPoiTypes, locale, false);

		SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(
				searchUICore.getPoiTypes());
		SearchCoreFactory.SearchAmenityByTypeAPI searchAmenityByTypesAPI = new SearchCoreFactory.SearchAmenityByTypeAPI(
				searchUICore.getPoiTypes(), searchAmenityTypesAPI);
		searchUICore.registerAPI(searchAmenityByTypesAPI);
		SearchSettings settings = searchUICore.getSearchSettings().setSearchTypes(ObjectType.POI);
		settings = settings.setOriginalLocation(new LatLon(lat, lon));
		settings.setRegions(osmandRegions);

		settings.setOfflineIndexes(readers);
		searchUICore.updateSettings(settings.setSearchBBox31(searchBbox));

		searchUICore.init();

		return searchUICore;
	}

	public PoiSearchResult searchPoi(SearchService.PoiSearchData data, String locale, LatLon center, boolean baseSearch,
	                                 boolean spatial, int zoom, String timeZone) throws IOException {
		if (data.savedBbox != null && isContainsBbox(data) && data.prevCategoriesCount == data.categories.size()) {
			return new PoiSearchResult(false, false, true, null);
		}

		Map<Long, Feature> foundFeatures = new HashMap<>();
		if (data.categories.isEmpty()) {
			return new PoiSearchResult(false, false, false, null);
		}
		QuadRect searchBbox = getSearchBbox(data.bbox);
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		boolean useLimit = false;
		boolean[] spatialFallback = new boolean[1];
		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = getMapsForSearch(searchBbox, baseSearch);
			if (mapList.isEmpty()) {
				return new PoiSearchResult(false, true, false, null);
			}

			usedMapList = osmAndMapsService.getReaders(mapList, null);

			if (data.categories.size() == 1) {
				searchPoiByTypeCategory(data.categories.get(0), locale, searchBbox, usedMapList, foundFeatures,
						spatial, zoom, spatialFallback, timeZone);
				useLimit = foundFeatures.size() >= TOTAL_LIMIT_POI;
			} else {
				PoiSearchLimit poiSearchLimit = new PoiSearchLimit(TOTAL_LIMIT_POI / data.categories.size(),
						TOTAL_LIMIT_POI, false);
				for (PoiSearchCategory categoryObj : data.categories) {
					if (poiSearchLimit.isLimitReached()) {
						useLimit = true;
						break;
					}
					int categoryStartSize = foundFeatures.size();
					searchPoiByTypeCategory(categoryObj, locale, searchBbox, usedMapList, foundFeatures, poiSearchLimit,
							spatial, zoom, spatialFallback, timeZone);
					int categoryEndSize = foundFeatures.size();
					poiSearchLimit.updateAfterCategory(categoryStartSize, categoryEndSize);
					if (poiSearchLimit.useLimit) {
						useLimit = true;
						break;
					}
				}
			}
		} finally {
			osmAndMapsService.unlockReaders(usedMapList);
		}
		List<Feature> features = new ArrayList<>(foundFeatures.values());
		if (!features.isEmpty()) {
			sortPoiResultsByDistance(features, center);
			PoiSearchResult result = new PoiSearchResult(useLimit, false, false,
					new FeatureCollection(features.toArray(new Feature[0])));
			result.oldSearch = spatial && spatialFallback[0];
			return result;
		} else {
			return new PoiSearchResult(false, false, false, null);
		}
	}

	private void searchPoiByTypeCategory(PoiSearchCategory categoryObj, String locale, QuadRect searchBbox,
	                                     List<BinaryMapIndexReader> readers, Map<Long, Feature> foundFeatures, boolean spatial, int zoom,
	                                     boolean[] spatialFallback, String timeZone) throws IOException {
		searchPoiByTypeCategory(categoryObj, locale, searchBbox, readers, foundFeatures, null, spatial, zoom,
				spatialFallback, timeZone);
	}

	private void searchPoiByTypeCategory(PoiSearchCategory categoryObj, String locale, QuadRect searchBbox,
	                                     List<BinaryMapIndexReader> readers, Map<Long, Feature> foundFeatures, PoiSearchLimit poiSearchLimit,
	                                     boolean spatial, int zoom, boolean[] spatialFallback, String timeZone) throws IOException {
		if (searchBbox == null) {
			return;
		}
		if (spatial) {
			if (searchPoiByCategorySpatial(categoryObj, locale, searchBbox, readers, foundFeatures,
					poiSearchLimit, zoom, timeZone)) {
				return;
			}
			if (spatialFallback != null) {
				spatialFallback[0] = true;
			}
		}

		MapPoiTypes mapPoiTypes = getMapPoiTypes(locale);
		AbstractPoiType poiType = mapPoiTypes.getAnyPoiTypeByKey(categoryObj.category, false);
		SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(
				mapPoiTypes);
		SearchCoreFactory.SearchAmenityByTypeAPI searchAmenityByTypesAPI = new SearchCoreFactory.SearchAmenityByTypeAPI(
				mapPoiTypes, searchAmenityTypesAPI);
		SearchPoiTypeFilter filter = null;

		Set<String> poiAdditionals = new LinkedHashSet<>();
		if (poiType != null) {
			filter = searchAmenityByTypesAPI.getPoiTypeFilter(poiType, poiAdditionals);
		}

		int left31 = (int) searchBbox.left;
		int right31 = (int) searchBbox.right;
		int top31 = (int) searchBbox.top;
		int bottom31 = (int) searchBbox.bottom;

		ResultMatcher<Amenity> additionalsMatcher = null;
		if (filter != null && !filter.isEmpty() && !poiAdditionals.isEmpty()) {
			Set<String> addSet = new LinkedHashSet<>(poiAdditionals);
			additionalsMatcher = new ResultMatcher<>() {
				@Override
				public boolean publish(Amenity object) {
					for (String add : addSet) {
						if (object.getAdditionalInfoKeys().contains(add))
							return true;
					}
					return false;
				}

				@Override
				public boolean isCancelled() {
					return false;
				}
			};
		}

		int categoryStartSize = foundFeatures.size();

		for (BinaryMapIndexReader reader : readers) {
			if (poiSearchLimit != null ? poiSearchLimit.isLimitReached() : foundFeatures.size() >= TOTAL_LIMIT_POI) {
				break;
			}
			BinaryMapIndexReader.SearchRequest<Amenity> request;
			if (filter == null || filter.isEmpty()) {
				request = createSearchRequestByBrand(reader, categoryObj, mapPoiTypes, left31, right31, top31,
						bottom31);
				if (request == null) {
					LOGGER.debug(String.format("Brand '%s' not found in '%s'", categoryObj.category,
							reader.getFile().getName()));
					continue;
				}
			} else {
				request = BinaryMapIndexReader.buildSearchPoiRequest(left31, right31, top31, bottom31,
						ZOOM_TO_SEARCH_POI, filter, additionalsMatcher);
			}
			if (request == null) {
				continue;
			}
			List<Amenity> amenities = reader.searchPoi(request);
			int remaining = poiSearchLimit != null
					? poiSearchLimit.getRemainingForSave(categoryStartSize, foundFeatures.size())
					: TOTAL_LIMIT_POI - foundFeatures.size();
			saveAmenityResults(amenities, foundFeatures, remaining, locale, timeZone);
			if (poiSearchLimit != null && poiSearchLimit.shouldStopCategory(categoryStartSize, foundFeatures.size())) {
				break;
			}
		}
	}

	// POI-by-category via spatial name index; false = category unsupported, caller uses the old path
	private boolean searchPoiByCategorySpatial(PoiSearchCategory categoryObj, String locale, QuadRect searchBbox,
	                                           List<BinaryMapIndexReader> readers, Map<Long, Feature> foundFeatures, PoiSearchLimit poiSearchLimit,
	                                           int zoom, String timeZone) throws IOException {
		SpatialPoiSearch poiTypeSearch = getSpatialPoiTypeSearch();
		SpatialPoiType spatialType = poiTypeSearch.getByKey(categoryObj.category);
		if (spatialType == null) {
			return false;
		}
		int remaining = poiSearchLimit != null
				? poiSearchLimit.getRemainingForSave(foundFeatures.size(), foundFeatures.size())
				: TOTAL_LIMIT_POI - foundFeatures.size();
		if (remaining <= 0) {
			return true;
		}
		QuadRect bboxLatLon = new QuadRect(
				MapUtils.get31LongitudeX((int) searchBbox.left), MapUtils.get31LatitudeY((int) searchBbox.top),
				MapUtils.get31LongitudeX((int) searchBbox.right), MapUtils.get31LatitudeY((int) searchBbox.bottom));
		if (zoom < 0) {
			// no zoom from client - estimate from bbox width
			double lonWidth = Math.abs(bboxLatLon.right - bboxLatLon.left);
			zoom = (int) Math.round(Math.log(360 * SPATIAL_POI_CATEGORY_VIEW_TILES / Math.max(lonWidth, 0.001))
					/ Math.log(2));
		}
		zoom = Math.max(SPATIAL_POI_CATEGORY_MIN_ZOOM, Math.min(SPATIAL_POI_CATEGORY_MAX_ZOOM, zoom));
		SpatialTextSearchSettings settings = SpatialTextSearchSettings
				.searchPoiByCategorySettings(zoom + SPATIAL_POI_CATEGORY_VIEW_ZOOM_SHIFT, bboxLatLon);
		settings.AUTO_CLEAR_PREFIX_CACHE_LIMIT = SPATIAL_PREFIX_CACHE_LIMIT;
		SpatialSearchContext sscontext = new SpatialSearchContext(settings, readers, poiTypeSearch, null);
		sscontext.getStats().printLogs = false;
		SpatialSearchResults res = spatialTextSearch
				.searchAPI(NameIndexReader.POI_CATEGORY_PREFIX + spatialType.getKey(), sscontext);
		List<Amenity> amenities = new ArrayList<>();
		if (res.mainResults != null) {
			for (SpatialSearchResult r : res.mainResults) {
				for (MapObject o : r.getObjects()) {
					if (o instanceof Amenity amenity) {
						amenities.add(amenity);
					}
				}
			}
		}
		if (amenities.isEmpty()) {
			// empty may mean maps without "#^" name-index entries - let the old scan double-check
			return false;
		}
		saveAmenityResults(amenities, foundFeatures, remaining, locale, timeZone);
		return true;
	}

	private BinaryMapIndexReader.SearchRequest<Amenity> createSearchRequestByBrand(BinaryMapIndexReader reader,
	                                                                               PoiSearchCategory categoryObj, MapPoiTypes mapPoiTypes, int left31, int right31, int top31, int bottom31)
			throws IOException {
		List<BinaryMapPoiReaderAdapter.PoiSubType> brands = reader.getTopIndexSubTypes();
		// canonical key ("top_index_<subtype>_<valueKey>") is resolved against this reader's own
		// top-index subtypes: values differ per map and the filter needs the raw value
		BinaryMapPoiReaderAdapter.PoiSubType selectedBrand = null;
		String brandValueToSearch = null;
		for (BinaryMapPoiReaderAdapter.PoiSubType subType : brands) {
			String subTypePrefix = subType.name + "_";
			if (!categoryObj.category.startsWith(subTypePrefix) || subType.possibleValues == null) {
				continue;
			}
			String valueKey = categoryObj.category.substring(subTypePrefix.length());
			for (String value : subType.possibleValues) {
				if (TopIndexFilter.getValueKey(value).equals(valueKey)) {
					selectedBrand = subType;
					brandValueToSearch = value;
					break;
				}
			}
			if (selectedBrand != null) {
				break;
			}
		}
		if (selectedBrand == null) {
			return null;
		}
		TopIndexFilter brandFilter = new TopIndexFilter(selectedBrand, mapPoiTypes, brandValueToSearch);
		SearchPoiTypeFilter filter = BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER;
		return BinaryMapIndexReader.buildSearchPoiRequest(left31, right31, top31, bottom31, ZOOM_TO_SEARCH_POI, filter,
				brandFilter, null);
	}

	private void sortPoiResultsByDistance(List<Feature> features, LatLon center) {
		Map<Feature, Double> distances = new IdentityHashMap<>(features.size());
		for (Feature f : features) {
			float[] coords = (float[]) f.geometry.coordinates;
			distances.put(f, MapUtils.getDistance(new LatLon(coords[1], coords[0]), center.getLatitude(),
					center.getLongitude()));
		}
		features.sort(Comparator.comparingDouble(distances::get));
	}

	public Feature searchPoiByOsmId(LatLon loc, long osmid, String type, String timeZone) throws IOException {
		final String RELATION_TYPE = "3";
		final double RELATION_SEARCH_RADIUS = 0.0055; // ~600 meters
		final double OTHER_POI_SEARCH_RADIUS = 0.0001; // ~11 meters
		final double SEARCH_POI_BY_OSMID_RADIUS_DEGREE = type.equals(RELATION_TYPE) ? RELATION_SEARCH_RADIUS
				: OTHER_POI_SEARCH_RADIUS;
		final int mapZoom = 15;
		LatLon p1 = new LatLon(loc.getLatitude() + SEARCH_POI_BY_OSMID_RADIUS_DEGREE,
				loc.getLongitude() - SEARCH_POI_BY_OSMID_RADIUS_DEGREE);
		LatLon p2 = new LatLon(loc.getLatitude() - SEARCH_POI_BY_OSMID_RADIUS_DEGREE,
				loc.getLongitude() + SEARCH_POI_BY_OSMID_RADIUS_DEGREE);
		BinaryMapIndexReader.SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(
				MapUtils.get31TileNumberX(p1.getLongitude()), MapUtils.get31TileNumberX(p2.getLongitude()),
				MapUtils.get31TileNumberY(p1.getLatitude()), MapUtils.get31TileNumberY(p2.getLatitude()), mapZoom,
				BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER, new ResultMatcher<>() {
					@Override
					public boolean publish(Amenity amenity) {
						return ObfConstants.getOsmObjectId(amenity) == osmid;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});

		SearchResult res = searchPoiByReq(req, p1, p2, false);
		if (res != null) {
			return getPoiFeature(res, timeZone);
		}
		return null;
	}

	public Feature searchPoiByEnName(LatLon loc, String enName) throws IOException {
		final double SEARCH_POI_RADIUS_DEGREE = 0.0001;
		final int mapZoom = 15;
		LatLon p1 = new LatLon(loc.getLatitude() + SEARCH_POI_RADIUS_DEGREE,
				loc.getLongitude() - SEARCH_POI_RADIUS_DEGREE);
		LatLon p2 = new LatLon(loc.getLatitude() - SEARCH_POI_RADIUS_DEGREE,
				loc.getLongitude() + SEARCH_POI_RADIUS_DEGREE);
		BinaryMapIndexReader.SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(
				MapUtils.get31TileNumberX(p1.getLongitude()), MapUtils.get31TileNumberX(p2.getLongitude()),
				MapUtils.get31TileNumberY(p1.getLatitude()), MapUtils.get31TileNumberY(p2.getLatitude()), mapZoom,
				BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER, new ResultMatcher<>() {
					@Override
					public boolean publish(Amenity amenity) {
						return amenity.getEnName(false).equals(enName);
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});

		SearchResult res = searchPoiByReq(req, p1, p2, false);
		if (res != null) {
			return getPoiFeature(res, null);
		}
		return null;
	}

	private SearchResult searchPoiByReq(BinaryMapIndexReader.SearchRequest<Amenity> req, LatLon p1, LatLon p2,
	                                    boolean baseSearch) throws IOException {
		List<LatLon> bbox = Arrays.asList(p1, p2);
		QuadRect searchBbox = getSearchBbox(bbox);

		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		SearchResult res = null;
		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = getMapsForSearch(searchBbox, baseSearch);
			if (!mapList.isEmpty()) {
				usedMapList = osmAndMapsService.getReaders(mapList, null);
				for (BinaryMapIndexReader map : usedMapList) {
					if (res != null) {
						break;
					}
					for (BinaryIndexPart indexPart : map.getIndexes()) {
						if (indexPart instanceof BinaryMapPoiReaderAdapter.PoiRegion p) {
							map.initCategories(p);
							List<Amenity> poiRes = map.searchPoi(req, p);
							if (!poiRes.isEmpty()) {
								SearchResult wikiRes = null;
								SearchResult nonWikiRes = null;

								for (Amenity poi : poiRes) {
									if (WIKI_POI_TYPE.equals(poi.getType().getKeyName())) {
										if (wikiRes == null) {
											wikiRes = new SearchResult();
											wikiRes.object = poi;
										}
									} else {
										nonWikiRes = new SearchResult();
										nonWikiRes.object = poi;
										break;
									}
								}

								res = nonWikiRes != null ? nonWikiRes : wikiRes;
							}
						}
					}
				}
			}
		} finally {
			osmAndMapsService.unlockReaders(usedMapList);
		}
		return res;
	}

	private boolean isContainsBbox(SearchService.PoiSearchData data) {
		QuadRect searchBbox = getSearchBbox(data.bbox);
		QuadRect oldSearchBbox = getSearchBbox(data.savedBbox);
		return oldSearchBbox.contains(searchBbox.left, searchBbox.top, searchBbox.right, searchBbox.bottom);
	}

	List<OsmAndMapsService.BinaryMapIndexReaderReference> getMapsForSearch(QuadRect searchBbox, boolean baseSearch)
			throws IOException {
		OsmAndMapsService.BinaryMapIndexReaderReference basemap = osmAndMapsService.getBaseMap();
		if (baseSearch) {
			return List.of(basemap);
		} else {
			if (searchBbox != null) {
				List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService.getObfReaders(searchBbox,
						OsmAndMapsService.ObfReason.SEARCH.value());
				list.add(basemap);
				return list;
			}
		}
		return Collections.emptyList();
	}

	public SearchUICore.SearchResultCollection searchPoiByCategory(SearchUICore searchUICore, String text, int limit)
			throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return null;
		}
		searchUICore.setTotalLimit(limit);
		return searchUICore.immediateSearch(text + DELIMITER, null);
	}

	private double getRating(Amenity sr, double lat, double lon) {
		String populationS = sr.getAdditionalInfo("population");
		City.CityType type = CityType.valueFromString(sr.getSubType());
		long population = type.getPopulation();
		if (populationS != null) {
			populationS = populationS.replaceAll("\\D", "");
			if (populationS.matches("\\d+")) {
				population = Long.parseLong(populationS);
			}
		}
		double distance = MapUtils.getDistance(sr.getLocation(), lat, lon) / 1000.0;
		return Math.log10(population + 1.0) - distance;
	}

	public Amenity searchCitiesByBbox(QuadRect searchBbox, double lat, double lon, List<BinaryMapIndexReader> mapList)
			throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return null;
		}
		List<Amenity> modifiableFoundedPlaces = new ArrayList<>();

		// searchBbox comes from OsmAndMapsService.points(): left/right/top/bottom are
		// already 31-bit tile coords.
		SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest((int) searchBbox.left,
				(int) searchBbox.right, (int) searchBbox.top, (int) searchBbox.bottom, 15, new SearchPoiTypeFilter() {

					@Override
					public boolean isEmpty() {
						return false;
					}

					@Override
					public boolean accept(PoiCategory type, String subcategory) {
						if (type.getKeyName().equals("administrative")
								&& CityType.valueFromString(subcategory) != null) {
							return true;
						}
						return false;
					}
				}, new ResultMatcher<Amenity>() {

					@Override
					public boolean publish(Amenity object) {
						modifiableFoundedPlaces.add(object);
						return false;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});
		for (BinaryMapIndexReader r : mapList) {
			r.searchPoi(req);
		}
		modifiableFoundedPlaces.sort((o1, o2) -> {
			if (o1 instanceof Amenity && o2 instanceof Amenity) {
				double rating1 = getRating(o1, lat, lon);
				double rating2 = getRating(o2, lat, lon);
				return Double.compare(rating2, rating1);
			}
			return 0;
		});
		if (modifiableFoundedPlaces.size() > 0) {
			return modifiableFoundedPlaces.get(0);
		}

		return null;
	}

	public QuadRect getSearchBbox(List<LatLon> bbox) {
		if (bbox.size() == 2) {
			return osmAndMapsService.points(null, bbox.get(0), bbox.get(1));
		}
		return null;
	}

	public QuadRect getSearchBbox(List<LatLon> bbox, double radius) {
		if (bbox.size() == 2) {
			LatLon northWest = bbox.get(0);
			LatLon southEast = bbox.get(1);
			return osmAndMapsService.points(null,
					new LatLon(northWest.getLatitude() + radius, northWest.getLongitude() - radius),
					new LatLon(southEast.getLatitude() - radius, southEast.getLongitude() + radius));
		}
		return null;
	}

	public List<String> getTopFilters() {
		List<String> filters = new ArrayList<>();
		SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), DEFAULT_SEARCH_LANG, true);
		searchUICore.getPoiTypes().getTopVisibleFilters().forEach(f -> filters.add(f.getKeyName()));
		return filters;
	}

	public Map<String, Map<String, String>> searchPoiCategories(String search, String locale) {
		Map<String, Map<String, String>> results = new HashMap<>();
		List<SearchResult> categories = searchPoiCategoriesByName(search, locale);
		if (categories != null && !categories.isEmpty()) {
			results = preparePoiCategoriesResult(categories);
		}
		return results;
	}

	private List<SearchResult> searchPoiCategoriesByName(String search, String locale) {
		MapPoiTypes mapPoiTypes = getMapPoiTypes(locale);
		SearchUICore searchUICore = new SearchUICore(mapPoiTypes, locale, true);
		SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(
				mapPoiTypes);
		List<AbstractPoiType> topFilters = searchUICore.getPoiTypes().getTopVisibleFilters();
		List<String> filterOrder = topFilters.stream().map(AbstractPoiType::getKeyName).toList();
		searchAmenityTypesAPI.setActivePoiFiltersByOrder(filterOrder);
		searchUICore.registerAPI(searchAmenityTypesAPI);

		return searchUICore.immediateSearch(search, null).getCurrentSearchResults();
	}

	private Map<String, Map<String, String>> preparePoiCategoriesResult(List<SearchResult> results) {
		Map<String, Map<String, String>> searchRes = new HashMap<>();
		results.forEach(res -> searchRes.put(res.localeName, getPoiTypeFields(res.object)));
		return searchRes;
	}

	public Map<String, List<String>> searchPoiCategories(String locale) {
		SearchUICore searchUICore = new SearchUICore(getMapPoiTypes(locale), locale, false);
		List<PoiCategory> categoriesList = searchUICore.getPoiTypes().getCategories(false);
		Map<String, List<String>> res = new HashMap<>();
		categoriesList.forEach(poiCategory -> {
			String category = poiCategory.getKeyName();
			List<PoiType> poiTypes = poiCategory.getPoiTypes();
			List<String> typesNames = new ArrayList<>();
			poiTypes.forEach(type -> typesNames.add(type.getOsmValue()));
			res.put(category, typesNames);

		});
		return res;
	}

	private Map<String, String> getTranslations(String locale) {
		return translationsCache.computeIfAbsent(locale, loc -> {
			try {
				String validLoc = validateLocale(loc);
				String localPath = validLoc.equals("en") ? "values" : "values-" + validLoc;

				InputStream phrasesStream = this.getClass().getResourceAsStream(AND_RES + localPath + "/phrases.xml");
				if (phrasesStream == null) {
					throw new IllegalArgumentException("Locale not found: " + loc);
				}

				return parseStringsXml(phrasesStream);
			} catch (XmlPullParserException | IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	private MapPoiTypesTranslator parseGlobalTranslations() {
		Map<String, String> enTranslations = getTranslations(DEFAULT_SEARCH_LANG);
		MapPoiTypesTranslator translations = new MapPoiTypesTranslator(enTranslations, enTranslations);
		for (String l : MapRenderingTypes.langs) {
			InputStream phrasesStream = this.getClass().getResourceAsStream(AND_RES + "values-" + l + "/phrases.xml");
			if (phrasesStream != null) {
				try {
					Map<String, String> stringsXml = parseStringsXml(phrasesStream);
					translations.appendTranslations(l, stringsXml);
				} catch (XmlPullParserException | IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return translations;
	}

	private Map<String, PoiType> getPoiAdditionalsByKey() {
		Map<String, PoiType> byKey = poiAdditionalsByKey;
		if (byKey == null) {
			synchronized (this) {
				byKey = poiAdditionalsByKey;
				if (byKey == null) {
					byKey = new HashMap<>();
					for (PoiCategory pc : MapPoiTypes.getDefault().getCategories()) {
						collectPoiAdditionals(pc, byKey);
						for (PoiFilter pf : pc.getPoiFilters()) {
							collectPoiAdditionals(pf, byKey);
						}
						for (PoiType p : pc.getPoiTypes()) {
							collectPoiAdditionals(p, byKey);
						}
					}
					poiAdditionalsByKey = byKey;
				}
			}
		}
		return byKey;
	}

	private void collectPoiAdditionals(AbstractPoiType p, Map<String, PoiType> byKey) {
		List<PoiType> additionals = p.getPoiAdditionals();
		if (additionals != null) {
			for (PoiType pt : additionals) {
				byKey.putIfAbsent(pt.getKeyName(), pt);
			}
		}
	}

	private MapPoiTypes getMapPoiTypes(String locale) {
		locale = locale == null ? DEFAULT_SEARCH_LANG : locale;

		return poiTypesByLocale.computeIfAbsent(locale, loc -> {
			MapPoiTypes mapPoiTypes = new MapPoiTypes(null);
			mapPoiTypes.init();
			Map<String, String> translations = getTranslations(loc);
			Map<String, String> enTranslations = getTranslations(DEFAULT_SEARCH_LANG);
			mapPoiTypes.setPoiTranslator(new MapPoiTypesTranslator(translations, enTranslations));
			return mapPoiTypes;
		});
	}

	private Map<String, String> parseStringsXml(InputStream inputStream) throws XmlPullParserException, IOException {
		Map<String, String> resultMap = new HashMap<>();

		XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
		XmlPullParser parser = factory.newPullParser();
		parser.setInput(inputStream, "UTF-8");

		int eventType = parser.getEventType();
		String key = null;
		String value = null;

		while (eventType != XmlPullParser.END_DOCUMENT) {
			String tagName = parser.getName();
			switch (eventType) {
				case XmlPullParser.START_TAG:
					if (tagName.equals("string")) {
						key = parser.getAttributeValue(null, "name");
					}
					break;
				case XmlPullParser.TEXT:
					value = parser.getText();
					break;
				case XmlPullParser.END_TAG:
					if (tagName.equals("string") && key != null && value != null) {
						resultMap.put(key, value);
						key = null;
						value = null;
					}
					break;
			}
			eventType = parser.next();
		}
		inputStream.close();
		return resultMap;
	}

	private String validateLocale(String locale) {
		if (locale == null || locale.isEmpty()) {
			throw new IllegalArgumentException("Locale cannot be null or empty");
		}
		// Remove potentially dangerous characters such as '/'
		return locale.replaceAll("[/\\\\]", "");
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

	private Map<String, String> getPoiTypeFields(Object obj) {
		Map<String, String> tags = new HashMap<>();
		if (obj instanceof PoiType type) {
			if (type.isHidden()) {
				return tags;
			}
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getKeyName());
			tags.put(PoiTypeField.OSM_TAG.getFieldName(), type.getOsmTag());
			tags.put(PoiTypeField.OSM_VALUE.getFieldName(), type.getOsmValue());
			tags.put(PoiTypeField.ICON_NAME.getFieldName(), type.getIconKeyName());
			PoiCategory category = type.getCategory();
			if (category != null) {
				tags.put(PoiTypeField.CATEGORY_ICON.getFieldName(), category.getIconKeyName());
				tags.put(PoiTypeField.CATEGORY_KEY_NAME.getFieldName(), category.getKeyName());
			}
			tags.put(PoiTypeField.POI_ADD_CATEGORY_NAME.getFieldName(), type.getPoiAdditionalCategory());
		} else if (obj instanceof PoiFilter type) {
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getKeyName());
			PoiCategory category = type.getPoiCategory();
			if (category != null) {
				tags.put(PoiTypeField.CATEGORY_ICON.getFieldName(), category.getIconKeyName());
				tags.put(PoiTypeField.CATEGORY_KEY_NAME.getFieldName(), category.getKeyName());
			} else if (obj instanceof PoiCategory cat) {
				tags.put(PoiTypeField.CATEGORY_ICON.getFieldName(), cat.getIconKeyName());
				tags.put(PoiTypeField.CATEGORY_KEY_NAME.getFieldName(), cat.getKeyName());
			}
		} else if (obj instanceof SearchCoreFactory.PoiAdditionalCustomFilter type) {
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getKeyName());
			tags.put(PoiTypeField.ICON_NAME.getFieldName(), type.getIconKeyName());
			type.additionalPoiTypes.stream().findFirst().ifPresent(poiType -> {
				tags.put(PoiTypeField.ICON_NAME.getFieldName(), getIconName(poiType));
				PoiFilter poiFilter = poiType.getFilter();
				if (poiFilter != null) {
					tags.put(PoiTypeField.POI_FILTER_NAME.getFieldName(), poiFilter.getKeyName());
				}
				String additionalCategory = poiType.getPoiAdditionalCategory();
				if (additionalCategory != null) {
					tags.put(PoiTypeField.POI_ADD_CATEGORY_NAME.getFieldName(), additionalCategory);
				}
			});
		} else if (obj instanceof TopIndexFilter type) {
			tags.put(PoiTypeField.CATEGORY_KEY_NAME.getFieldName(), type.getTag());
			tags.put(PoiTypeField.CATEGORY_ICON.getFieldName(), type.getTag());
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getFilterId());
			tags.put(PoiTypeField.NAME.getFieldName(), type.getValue());
		} else if (obj instanceof AbstractPoiType type) {
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getKeyName());
			tags.put(PoiTypeField.ICON_NAME.getFieldName(), type.getIconKeyName());
		} else if (obj instanceof MapObject type) {
			String enName = type.getEnName(false);
			if (Algorithms.isNotEmpty(enName)) {
				tags.put(PoiTypeField.EN_NAME.getFieldName(), enName);
			}
		}
		return tags;
	}

	public enum PoiTypeField {
		NAME("web_name"), EN_NAME("web_en_name"), TYPE("web_type"), ADDRESS_1("web_address1"),
		ADDRESS_2("web_address2"), KEY_NAME("web_keyName"), OSM_TAG("web_typeOsmTag"), OSM_VALUE("web_typeOsmValue"),
		ICON_NAME("web_iconKeyName"), CATEGORY_ICON("web_categoryIcon"), CATEGORY_KEY_NAME("web_categoryKeyName"),
		POI_ADD_CATEGORY_NAME("web_poiAdditionalCategory"), POI_FILTER_NAME("web_poiFilterName"), POI_ID("web_poi_id"),
		POI_NAME("web_poi_name"), POI_COLOR("web_poi_color"), POI_ICON_NAME("web_poi_iconName"),
		POI_TYPE("web_poi_type"), POI_SUBTYPE("web_poi_subType"), POI_OSM_URL("web_poi_osmUrl"), CITY("web_city"),
		// names of all objects matched in a spatial-search result (street, city, ...)
		MATCHED_OBJECTS("web_matched_objects"), VISIBLE_LEVEL("web_visible_level"),
		COMPARE_KEY("web_compare_key"), BBOX_LAT_LON("web_bbox_lat_lon"),
		WIKIDATA_ID("web_wikidata_id"), CITY_TYPE("web_city_type");

		private final String fieldName;

		PoiTypeField(String fieldName) {
			this.fieldName = fieldName;
		}

		public String getFieldName() {
			return fieldName;
		}
	}

	private void saveSearchResult(List<SearchResult> res, List<Feature> features, String timeZone) {
		for (SearchResult result : res) {
			features.add(getFeature(result, timeZone));
		}
	}

	private void saveAmenityResults(List<Amenity> amenities, Map<Long, Feature> foundFeatures, int remainingLimit,
	                                String locale, String timeZone) {
		String dominatedCity = "";
		Map<String, Integer> cityCounter = new TreeMap<>();
		for (Amenity amenity : amenities) {
			String cityName = amenity.getCityFromTagGroups(locale);
			if (!Algorithms.isEmpty(cityName)) {
				String mainCity = getMainCityName(cityName);
				String domCity = getDominatedCity(cityCounter, mainCity);
				if (domCity != null) {
					dominatedCity = domCity;
					break;
				}
			}
		}
		for (Amenity amenity : amenities) {
			if (remainingLimit <= 0) {
				break;
			}
			long osmId = amenity.getId();
			if (!foundFeatures.containsKey(osmId)) {
				foundFeatures.put(osmId, getPoiFeature(buildPoiSearchResult(amenity, locale, dominatedCity), timeZone));
				remainingLimit--;
			}
		}
	}

	private SearchResult buildPoiSearchResult(Amenity amenity, String locale, String dominatedCity) {
		String cityName = amenity.getCityFromTagGroups(locale);
		String city = cityName == null ? "" : cityName;
		SearchResult result = new SearchResult();
		result.object = amenity;
		result.objectType = ObjectType.POI;
		result.location = amenity.getLocation();
		result.addressName = calculateAddressString(amenity, city, getMainCityName(city), dominatedCity);
		return result;
	}

	private String calculateAddressString(Amenity amenity, String cityName, String mainCity, String dominatedCity) {
		String streetName = amenity.getStreetName();
		if (Algorithms.isEmpty(streetName)) {
			return cityName.isEmpty() ? null : cityName;
		}
		String houseNumber = amenity.getAdditionalInfo(Amenity.ADDR_HOUSENUMBER);
		String addr = streetName + (Algorithms.isEmpty(houseNumber) ? "" : " " + houseNumber);

		return createAddressString(cityName, mainCity, dominatedCity, addr);
	}

	private String calculateSpatialDominatedCity(List<SpatialSearchResult> results, String locale) {
		Map<String, Integer> cityCounter = new TreeMap<>();
		for (SpatialSearchResult r : results) {
			for (MapObject obj : r.getObjects()) {
				if (obj instanceof Amenity amenity) {
					String cityName = amenity.getCityFromTagGroups(locale);
					if (!Algorithms.isEmpty(cityName)) {
						String domCity = getDominatedCity(cityCounter, getMainCityName(cityName));
						if (domCity != null) {
							return domCity;
						}
					}
				}
			}
		}
		return "";
	}

	public Feature getFeature(SearchResult result, String timeZone) {
		Feature feature;
		if (result.objectType == ObjectType.POI) {
			feature = getPoiFeature(result, timeZone);
		} else {
			Geometry geometry = Geometry.point(result.location != null ? result.location : new LatLon(0, 0));
			feature = new Feature(geometry).prop(PoiTypeField.TYPE.getFieldName(), result.objectType)
					.prop(PoiTypeField.NAME.getFieldName(), result.localeName);
			if (result.objectType == ObjectType.STREET || result.objectType == ObjectType.HOUSE) {
				if (result.localeRelatedObjectName != null) {
					feature.prop(PoiTypeField.ADDRESS_1.getFieldName(), result.localeRelatedObjectName);
				}
				SearchResult parentResult = result.parentSearchResult;
				if (parentResult != null && parentResult.localeRelatedObjectName != null) {
					feature.prop(PoiTypeField.ADDRESS_2.getFieldName(), parentResult.localeRelatedObjectName);
				}
			} else if (result.objectType == ObjectType.STREET_INTERSECTION) {
				feature.prop(PoiTypeField.NAME.getFieldName(),
						result.localeName + " - " + result.localeRelatedObjectName);
			}
			Map<String, String> tags = getPoiTypeFields(result.object);
			for (Map.Entry<String, String> entry : tags.entrySet()) {
				feature.prop(entry.getKey(), entry.getValue());
			}
		}
		return feature;
	}

	private Feature getPoiFeature(SearchResult result, String timeZone) {
		Amenity amenity = (Amenity) result.object;
		Feature feature = null;

		String poiNameWithAlternateName = result.localeName != null ? result.localeName : amenity.getName();
		if (!Algorithms.isEmpty(result.alternateName)
				&& !Algorithms.objectEquals(result.localeName, result.alternateName)) {
			poiNameWithAlternateName += " (" + result.alternateName + ")";
		}

		feature = new Feature(Geometry.point(amenity.getLocation()))
				.prop(PoiTypeField.TYPE.getFieldName(), result.objectType)
				.prop(PoiTypeField.POI_ID.getFieldName(), amenity.getId())
				.prop(PoiTypeField.POI_NAME.getFieldName(), poiNameWithAlternateName)
				.prop(PoiTypeField.POI_COLOR.getFieldName(), amenity.getColor())

				.prop(PoiTypeField.POI_TYPE.getFieldName(), amenity.getType().getKeyName())
				.prop(PoiTypeField.POI_SUBTYPE.getFieldName(), amenity.getSubType())
				.prop(PoiTypeField.POI_OSM_URL.getFieldName(), getOsmUrl(result));

		Map<String, String> tags = amenity.getAmenityExtensions();
		filterWikiTags(tags);
		for (Map.Entry<String, String> entry : tags.entrySet()) {
			String key = entry.getKey().startsWith(OSM_PREFIX) ? entry.getKey().substring(OSM_PREFIX.length())
					: entry.getKey();
			PoiType additionalType = getPoiAdditionalsByKey().get(key);
			if (additionalType != null && additionalType.isHidden()) {
				continue;
			}
			String value = unzipContent(entry.getValue());
			feature.prop(entry.getKey(), value);
		}
		String openingHoursValue = tags.get(AMENITY_PREFIX + OPENING_HOURS);
		Calendar clientTime = getClientTime(timeZone);
		if (clientTime != null && openingHoursValue != null) {
			String openingHoursInfo = getOpeningHoursInfo(openingHoursValue, clientTime);
			if (openingHoursInfo != null) {
				feature.prop(AMENITY_PREFIX + OPENING_HOURS + OPENING_HOURS_INFO_SUFFIX, openingHoursInfo);
			}
		}
		Map<String, String> names = amenity.getNamesMap(true);
		for (Map.Entry<String, String> entry : names.entrySet()) {
			feature.prop(PoiTypeField.POI_NAME.getFieldName() + ":" + entry.getKey(), entry.getValue());
		}
		feature.prop(PoiTypeField.CITY.getFieldName(), result.addressName);
		String subType = amenity.getSubType();
		if (subType != null && subType.indexOf(';') != -1) {
			subType = subType.substring(0, subType.indexOf(';'));
		}
		PoiType poiType = amenity.getType().getPoiTypeByKeyName(subType);
		if (poiType != null) {
			feature.prop(PoiTypeField.POI_ICON_NAME.getFieldName(), getIconName(poiType));
			Map<String, String> typeTags = getPoiTypeFields(poiType);
			for (Map.Entry<String, String> entry : typeTags.entrySet()) {
				feature.prop(entry.getKey(), entry.getValue());
			}

		}
		return feature;
	}

	private String getOpeningHoursInfo(String openingHoursValue, Calendar calendar) {
		OpeningHours openingHours = parseOpenedHours(openingHoursValue);
		if (openingHours != null) {
			List<OpeningHours.Info> openingHoursInfo = openingHours.getInfo(calendar);
			if (!Algorithms.isEmpty(openingHoursInfo)) {
				StringJoiner openHoursInfos = new StringJoiner(";");
				for (OpeningHours.Info info : openingHoursInfo) {
					String infoString = info.getInfo();
					if (!Algorithms.isEmpty(infoString)) {
						String s = (info.isOpened() ? IS_OPENED_PREFIX : "") + infoString;
						openHoursInfos.add(s);
					}
				}
				return openHoursInfos.toString();
			}
		}
		return null;
	}

	private void filterWikiTags(Map<String, String> tags) {
		tags.entrySet().removeIf(entry -> entry.getKey().startsWith("osm_tag_travel_elo")
				|| entry.getKey().startsWith("osm_tag_travel_topic") || entry.getKey().startsWith("osm_tag_qrank")
				|| entry.getKey().startsWith("osm_tag_wiki_place") || entry.getKey().startsWith("osm_tag_wiki_photo"));
	}

	public String getPoiAddress(LatLon location) throws IOException, InterruptedException {
		if (location != null) {
			List<GeocodingUtilities.GeocodingResult> list = osmAndMapsService.geocoding(location.getLatitude(),
					location.getLongitude());
			Optional<GeocodingUtilities.GeocodingResult> nearestResult = list.stream()
					.min(Comparator.comparingDouble(GeocodingUtilities.GeocodingResult::getDistance));
			if (nearestResult.isPresent()) {
				return nearestResult.get().toString();
			}
		}
		return null;
	}

	private String getOsmUrl(SearchResult result) {
		MapObject mapObject = (MapObject) result.object;
		Entity.EntityType type = ObfConstants.getOsmEntityType(mapObject);
		if (type != null) {
			long osmId = ObfConstants.getOsmObjectId(mapObject);
			return "https://www.openstreetmap.org/" + type.name().toLowerCase(Locale.US) + "/" + osmId;
		}
		return null;
	}

	public static boolean isOsmUrlAvailable(MapObject object) {
		Long id = object.getId();
		return id != null && id > 0;
	}

	public LatLon parseLocation(String locationString, LatLon bboxCentre) throws IOException {
		if (locationString == null || locationString.trim().isEmpty()) {
			return null;
		}
		locationString = TextDirectionUtil.clearDirectionMarks(locationString);
		LocationParser.ParsedOpenLocationCode olcParsed = parseOpenLocationCode(locationString);
		if (olcParsed != null) {
			if (olcParsed.isFull()) {
				return olcParsed.getLatLon();
			}
			LatLon location = searchOlcOnBasemap(locationString, bboxCentre);
			if (location != null) {
				return location;
			}
		}
		return LocationParser.parseLocation(locationString);
	}

	private LatLon searchOlcOnBasemap(String locationString, LatLon bboxCentre) throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return null;
		}
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();

		try {
			QuadRect world = new QuadRect(0, 0, Integer.MAX_VALUE, Integer.MAX_VALUE);
			List<OsmAndMapsService.BinaryMapIndexReaderReference> refs = getMapsForSearch(world, true);
			if (refs.isEmpty()) {
				return null;
			}
			usedMapList = osmAndMapsService.getReaders(refs, null);

			SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), DEFAULT_SEARCH_LANG, false);
			SearchSettings settings = searchUICore.getSearchSettings();
			settings.setOfflineIndexes(usedMapList);
			settings.setSearchBBox31(world);
			searchUICore.updateSettings(settings);
			SearchCoreFactory.SearchAmenityByNameAPI amenitiesApi = new SearchCoreFactory.SearchAmenityByNameAPI();
			searchUICore.registerAPI(amenitiesApi);
			searchUICore.registerAPI(new SearchCoreFactory.SearchLocationAndUrlAPI(amenitiesApi));

			SearchUICore.SearchResultCollection resultCollection = searchUICore.immediateSearch(locationString,
					bboxCentre);
			if (resultCollection != null && !resultCollection.getCurrentSearchResults().isEmpty()) {
				SearchResult result = resultCollection.getCurrentSearchResults().get(0);
				if (result.object instanceof LatLon location) {
					return location;
				}
			}
		} finally {
			osmAndMapsService.unlockReaders(usedMapList);
		}
		return null;
	}

	@Nullable
	private static Calendar getClientTime(String timeZone) {
		if (Algorithms.isBlank(timeZone)) {
			return null;
		}
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
		calendar.setTimeInMillis(System.currentTimeMillis());
		return calendar;
	}

}
