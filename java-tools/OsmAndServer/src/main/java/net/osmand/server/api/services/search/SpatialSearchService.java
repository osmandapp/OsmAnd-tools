package net.osmand.server.api.services.search;

import static net.osmand.search.SearchUICore.getDominatedCity;
import static net.osmand.search.SearchUICore.getMainCityName;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import net.osmand.search.core.spatial.SpatialPoiSearch.SpatialPoiType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.Amenity;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.QuadRect;
import net.osmand.data.Street;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiType;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.TopIndexFilter;
import net.osmand.search.core.spatial.SpatialPoiSearch;
import net.osmand.search.core.spatial.SpatialSearchContext;
import net.osmand.search.core.spatial.SpatialSearchResult;
import net.osmand.search.core.spatial.SpatialTextSearch;
import net.osmand.search.core.spatial.SpatialTextSearch.SpatialSearchResults;
import net.osmand.search.core.spatial.SpatialTextSearch.SpatialTextSearchSettings;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.server.controllers.pub.GeojsonClasses.Geometry;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

@Service
public class SpatialSearchService {

	private static final Log LOGGER = LogFactory.getLog(SpatialSearchService.class);

	@Autowired
	OsmAndMapsService osmAndMapsService;

	@Autowired
	private MapReadersService mapReadersService;

	@Autowired
	private PoiTypesService poiTypesService;

	@Autowired
	private SearchResultConverter searchResultConverter;

	// the engine cache is shared by all requests (engine default 1000 assumes per-client caches)
	public static final int SPATIAL_PREFIX_CACHE_LIMIT = 4_000;

	private final AtomicInteger spatialSearchThreadNum = new AtomicInteger();

	private final ThreadPoolExecutor spatialSearchThreadPool = new ThreadPoolExecutor(
			1, 4, 10, TimeUnit.MINUTES, new SynchronousQueue<>(),
			r -> {
				Thread t = new Thread(r, "spatial-search-" + spatialSearchThreadNum.incrementAndGet());
				t.setDaemon(true);
				return t;
			});

	private final SpatialTextSearch spatialTextSearch = new SpatialTextSearch();

	private final ThreadLocal<RegionsReaderHolder> osmandRegionsLocal = ThreadLocal
			.withInitial(RegionsReaderHolder::new);

	// reused for cache
	private SpatialPoiSearch poiSearch;

	public static class SpatialResponse {
		public List<Feature> features = new ArrayList<>();
		public Map<String, Object> info = new LinkedHashMap<>();
	}

	public record SpatialResults(SpatialSearchResults results, SpatialSearchContext.SpatialSearchStats stats) {
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
			poiTypes.setPoiTranslator(poiTypesService.parseGlobalTranslations());
			poiSearch = new SpatialPoiSearch(poiTypes);
		}
		return poiSearch;
	}

	private static class RegionsReaderHolder {
		BinaryMapIndexReader reader;
		long fileTimestamp;
	}

	private BinaryMapIndexReader regionsReaderForThread() {
		RegionsReaderHolder holder = osmandRegionsLocal.get();
		BinaryMapIndexReader shared = mapReadersService.getOsmandRegions() == null ? null : mapReadersService.getOsmandRegions().getFile();
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
			holder.reader = mapReadersService.openRegionsReader();
			holder.fileTimestamp = ts;
		}
		return holder.reader;
	}

	@PreDestroy
	public void shutdownSpatialSearchPool() {
		spatialSearchThreadPool.shutdownNow();
	}

	public SpatialResponse searchSpatial(ClassicSearchService.SearchContext ctx, String timeZone, boolean autocomplete) throws IOException {
		long sTime = System.currentTimeMillis();
		SpatialResponse response = new SpatialResponse();
		if (!osmAndMapsService.validateAndInitConfig()) {
			return response;
		}
		final AtomicBoolean cancelled = new AtomicBoolean();
		// suggestions must fail fast, full search gets the long budget
		final long timeoutMs = autocomplete ? 3_000 : 15_000;
		boolean readersOwnedByWorker = false;
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService
					.getObfReadersForSpatialSearch(ctx.lat(), ctx.lon(), autocomplete);
			if (list.isEmpty()) {
				return response;
			}
			usedMapList = osmAndMapsService.getReaders(list, null);
			if (usedMapList.isEmpty()) {
				return response;
			}
			LatLon coord = mapReadersService.parseLocation(ctx.text(), new LatLon(ctx.lat(), ctx.lon()));
			if (coord != null) {
				SearchResult sr = new SearchResult();
				sr.object = coord;
				sr.location = coord;
				sr.objectType = ObjectType.LOCATION;
				sr.localeName = ((float) coord.getLatitude()) + ", " + ((float) coord.getLongitude());
				response.features.add(searchResultConverter.getFeature(sr, timeZone));
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
								new SpatialSearchContext(settings, readers, getSpatialPoiTypeSearch(), new LatLon(ctx.lat(), ctx.lon()));
						sscontext.resultMatcher = matcher;
						return spatialTextSearch.searchAPI(ctx.text(), sscontext);
					} finally {
						// unlock strictly after the search has really finished (incl. timeout/cancel path)
						osmAndMapsService.unlockReaders(lockedReaders);
					}
				}
			});
			readersOwnedByWorker = true;
			SpatialSearchResults res = task.get(timeoutMs, TimeUnit.MILLISECONDS);
			if (res.mainResults != null) {
				String dominatedCity = calculateSpatialDominatedCity(res.mainResults, ctx.locale());
				SpatialPoiSearch poiTypeSearch = getSpatialPoiTypeSearch();
				Map<MapObject, Feature> amenityFeatureCache = new IdentityHashMap<>();
				for (SpatialSearchResult r : res.mainResults) {
					List<MapObject> objs = r.getObjects();
					if (r.isPoiCategory()) {
						SpatialPoiType type = r.getPoiCategory(poiTypeSearch);
						if (type != null) {
							Feature f = getSpatialPoiTypeFeature(type);
							f.prop(SearchResultConverter.PoiTypeField.MATCHED_OBJECTS.getFieldName(),
									matchedObjects(objs, ctx.locale(), timeZone, dominatedCity, r.getViewBBox31(),
											amenityFeatureCache));
							f.prop(SearchResultConverter.PoiTypeField.VISIBLE_LEVEL.getFieldName(), r.visibleLevel());
							f.prop(SearchResultConverter.PoiTypeField.COMPARE_KEY.getFieldName(), SpatialSearchResult.compareKeyString(r));
							response.features.add(f);
						}
					} else if (!objs.isEmpty()) {
						LatLon l = r.getLatLon() == null ? new LatLon(ctx.lat(), ctx.lon()) : r.getLatLon();
						Feature f = getSpatialFeature(l, objs, ctx.locale(), timeZone, dominatedCity, r.getExtraNameMatch());
						if (f != null) {
							f.prop(SearchResultConverter.PoiTypeField.MATCHED_OBJECTS.getFieldName(),
									matchedObjects(objs, ctx.locale(), timeZone, dominatedCity, r.getViewBBox31(),
											amenityFeatureCache));
							f.prop(SearchResultConverter.PoiTypeField.VISIBLE_LEVEL.getFieldName(), r.visibleLevel());
							f.prop(SearchResultConverter.PoiTypeField.COMPARE_KEY.getFieldName(), SpatialSearchResult.compareKeyString(r));
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
		} catch (TimeoutException e) {
			LOGGER.warn(String.format("Spatial search timeout %d ms for '%s'", timeoutMs, ctx.text()));
			response.info.put("timeout", true);
		} catch (RejectedExecutionException e) {
			LOGGER.warn(String.format("Spatial search rejected, all pool threads are busy: '%s'", ctx.text()));
			response.info.put("busy", true);
		} catch (ExecutionException e) {
			LOGGER.error(String.format("Spatial search failed for '%s': %s", ctx.text(), e.getCause()), e.getCause());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.warn(String.format("Spatial search interrupted for '%s'", ctx.text()));
			response.info.put("interrupted", true);
		} catch (RuntimeException e) {
			LOGGER.error(String.format("Spatial search failed for '%s': %s", ctx.text(), e), e);
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
				.prop(SearchResultConverter.PoiTypeField.TYPE.getFieldName(), ObjectType.POI_TYPE);
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
				Map<String, String> tags = searchResultConverter.getPoiTypeFields(filter);
				tags.put(SearchResultConverter.PoiTypeField.NAME.getFieldName(), filter.getTranslation());
				return tags;
			}
			Map<String, String> tags = searchResultConverter.getPoiTypeFields(type.singleType);
			tags.put(SearchResultConverter.PoiTypeField.NAME.getFieldName(), type.singleType.getTranslation());
			if (type.getWikidataId() != null) {
				tags.put(SearchResultConverter.PoiTypeField.WIKIDATA_ID.getFieldName(), type.getWikidataId());
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
			tags.put(SearchResultConverter.PoiTypeField.CATEGORY_KEY_NAME.getFieldName(), tag);
			tags.put(SearchResultConverter.PoiTypeField.CATEGORY_ICON.getFieldName(), tag);
			// canonical key ("top_index_brand_<valueKey>"): web uses it as the search type in the URL
			tags.put(SearchResultConverter.PoiTypeField.KEY_NAME.getFieldName(), type.getKey());
			tags.put(SearchResultConverter.PoiTypeField.NAME.getFieldName(), type.poiAdditional);
			if (type.getWikidataId() != null) {
				tags.put(SearchResultConverter.PoiTypeField.WIKIDATA_ID.getFieldName(), type.getWikidataId());
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
						k -> searchResultConverter.getPoiFeature(searchResultConverter.buildPoiSearchResult((Amenity) k, locale, dominatedCity), timeZone));
				m.putAll(feature.properties);
			} else if (o instanceof City city) {
				m.put(SearchResultConverter.PoiTypeField.CITY_TYPE.getFieldName(), city.getType().name());
			}
			if (bbox31 != null && bbox31.length >= 4) {
				Map<String, Object> bbox = new LinkedHashMap<>();
				bbox.put("top", roundCoord(MapUtils.get31LatitudeY(bbox31[1])));
				bbox.put("left", roundCoord(MapUtils.get31LongitudeX(bbox31[0])));
				bbox.put("bottom", roundCoord(MapUtils.get31LatitudeY(bbox31[3])));
				bbox.put("right", roundCoord(MapUtils.get31LongitudeX(bbox31[2])));
				m.put(SearchResultConverter.PoiTypeField.BBOX_LAT_LON.getFieldName(), bbox);
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
			SearchResult result = searchResultConverter.buildPoiSearchResult(amenity, locale, dominatedCity);
			result.localeName = amenity.getName(locale);
			if (Algorithms.isNotEmpty(extraNameMatch)) {
				result.localeName += " (" + extraNameMatch + ")"; // ref
			}
			return searchResultConverter.getFeature(result, timeZone);
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
		return searchResultConverter.getFeature(result, timeZone);
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

	public SpatialTextSearch getSpatialTextSearch() {
		return spatialTextSearch;
	}
}
