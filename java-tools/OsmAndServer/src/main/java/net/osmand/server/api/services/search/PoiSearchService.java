package net.osmand.server.api.services.search;

import static net.osmand.binary.BinaryMapIndexReader.SearchRequest.ZOOM_TO_SEARCH_POI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.osmand.search.core.spatial.SpatialPoiSearch.SpatialPoiType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchPoiTypeFilter;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapPoiReaderAdapter;
import net.osmand.binary.NameIndexReader;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.data.City;
import net.osmand.data.City.CityType;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.QuadRect;
import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.search.core.TopIndexFilter;
import net.osmand.search.core.spatial.SpatialPoiSearch;
import net.osmand.search.core.spatial.SpatialSearchContext;
import net.osmand.search.core.spatial.SpatialSearchResult;
import net.osmand.search.core.spatial.SpatialTextSearch.SpatialSearchResults;
import net.osmand.search.core.spatial.SpatialTextSearch.SpatialTextSearchSettings;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.WikiService;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.server.controllers.pub.GeojsonClasses.FeatureCollection;
import net.osmand.util.MapUtils;

@Service
public class PoiSearchService {

	private static final Log LOGGER = LogFactory.getLog(PoiSearchService.class);

	private static final int TOTAL_LIMIT_POI = 2000;
	private static final int SPATIAL_POI_CATEGORY_VIEW_ZOOM_SHIFT = 2;
	private static final int SPATIAL_POI_CATEGORY_MIN_ZOOM = 4;
	private static final int SPATIAL_POI_CATEGORY_MAX_ZOOM = 18;
	// viewport is ~3 tiles wide: zoom ~= log2(360 * 3 / bboxLonWidth)
	private static final int SPATIAL_POI_CATEGORY_VIEW_TILES = 3;
	private static final double SEARCH_POI_RADIUS_DEGREE = 0.0007;
	private static final String WIKI_POI_TYPE = "osmwiki";

	@Autowired
	OsmAndMapsService osmAndMapsService;

	@Autowired
	WikiService wikiService;

	@Autowired
	private SpatialSearchService spatialSearchService;

	@Autowired
	private MapReadersService mapReadersService;

	@Autowired
	private PoiTypesService poiTypesService;

	@Autowired
	private SearchResultConverter searchResultConverter;

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

	public PoiSearchResult searchPoi(PoiSearchData data, String locale, LatLon center, boolean baseSearch,
	                                 boolean spatial, int zoom, String timeZone) throws IOException {
		if (data.savedBbox != null && isContainsBbox(data) && data.prevCategoriesCount == data.categories.size()) {
			return new PoiSearchResult(false, false, true, null);
		}

		Map<Long, Feature> foundFeatures = new HashMap<>();
		if (data.categories.isEmpty()) {
			return new PoiSearchResult(false, false, false, null);
		}
		QuadRect searchBbox = mapReadersService.getSearchBbox(data.bbox);
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		boolean useLimit = false;
		boolean[] spatialFallback = new boolean[1];
		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = mapReadersService.getMapsForSearch(searchBbox, baseSearch);
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

		MapPoiTypes mapPoiTypes = poiTypesService.getMapPoiTypes(locale);
		AbstractPoiType poiType = mapPoiTypes.getAnyPoiTypeByKey(categoryObj.category(), false);
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
					LOGGER.debug(String.format("Brand '%s' not found in '%s'", categoryObj.category(),
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
			searchResultConverter.saveAmenityResults(amenities, foundFeatures, remaining, locale, timeZone);
			if (poiSearchLimit != null && poiSearchLimit.shouldStopCategory(categoryStartSize, foundFeatures.size())) {
				break;
			}
		}
	}

	// POI-by-category via spatial name index; false = category unsupported, caller uses the old path
	private boolean searchPoiByCategorySpatial(PoiSearchCategory categoryObj, String locale, QuadRect searchBbox,
	                                           List<BinaryMapIndexReader> readers, Map<Long, Feature> foundFeatures, PoiSearchLimit poiSearchLimit,
	                                           int zoom, String timeZone) throws IOException {
		SpatialPoiSearch poiTypeSearch = spatialSearchService.getSpatialPoiTypeSearch();
		SpatialPoiType spatialType = poiTypeSearch.getByKey(categoryObj.category());
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
		settings.AUTO_CLEAR_PREFIX_CACHE_LIMIT = SpatialSearchService.SPATIAL_PREFIX_CACHE_LIMIT;
		SpatialSearchContext sscontext = new SpatialSearchContext(settings, readers, poiTypeSearch, null);
		sscontext.getStats().printLogs = false;
		SpatialSearchResults res = spatialSearchService.getSpatialTextSearch()
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
		searchResultConverter.saveAmenityResults(amenities, foundFeatures, remaining, locale, timeZone);
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
			if (!categoryObj.category().startsWith(subTypePrefix) || subType.possibleValues == null) {
				continue;
			}
			String valueKey = categoryObj.category().substring(subTypePrefix.length());
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

	private Feature getPoi(String type, String name, LatLon loc, Long osmId, String timeZone) throws IOException {
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
			List<OsmAndMapsService.BinaryMapIndexReaderReference> mapRefs = mapReadersService.getMapsForSearch(searchBbox, false);
			if (mapRefs.isEmpty()) {
				return null;
			}
			readers = osmAndMapsService.getReaders(mapRefs, null);
			if (readers.isEmpty()) {
				return null;
			}
			SearchUICore searchUICore = prepareSearchUICoreForSearchByPoiType(readers, searchBbox, PoiTypesService.DEFAULT_SEARCH_LANG,
					loc.getLatitude(), loc.getLongitude());

			// Find POIs by type
			SearchUICore.SearchResultCollection rc = searchPoiByCategory(searchUICore, type,
					ClassicSearchService.TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB);
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
		return SearchResultConverter.mergeFeatures(wikiFeature, poiFeature);
	}

	private Feature getPoiFeatureByName(SearchUICore.SearchResultCollection rc, String name, String timeZone) {
		for (SearchResult r : rc.getCurrentSearchResults()) {
			if (r.objectType != ObjectType.POI || !(r.object instanceof Amenity a)) {
				continue;
			}
			if (matchesName(a, name)) {
				Feature f = searchResultConverter.getPoiFeature(r, timeZone);
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
				Feature f = searchResultConverter.getPoiFeature(r, timeZone);
				if (f != null) {
					return f;
				}
			}
		}
		return null;
	}

	private Feature getWikiPoiById(Long wikidataId) {
		if (wikidataId == null) {
			return null;
		}
		List<String> langs = List.of(PoiTypesService.DEFAULT_SEARCH_LANG);

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
			return searchResultConverter.getPoiFeature(res, timeZone);
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
			return searchResultConverter.getPoiFeature(res, null);
		}
		return null;
	}

	private SearchResult searchPoiByReq(BinaryMapIndexReader.SearchRequest<Amenity> req, LatLon p1, LatLon p2,
	                                    boolean baseSearch) throws IOException {
		List<LatLon> bbox = Arrays.asList(p1, p2);
		QuadRect searchBbox = mapReadersService.getSearchBbox(bbox);

		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		SearchResult res = null;
		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = mapReadersService.getMapsForSearch(searchBbox, baseSearch);
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

	public SearchUICore.SearchResultCollection searchPoiByCategory(SearchUICore searchUICore, String text, int limit)
			throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return null;
		}
		searchUICore.setTotalLimit(limit);
		return searchUICore.immediateSearch(text + ClassicSearchService.DELIMITER, null);
	}

	public List<SearchResult> filterBrandsOutsideBBox(List<SearchResult> res, String northWest, String southEast,
	                                                  String locale, double lat, double lon, boolean baseSearch) throws IOException {
		if (northWest != null && southEast != null) {
			List<LatLon> bbox = mapReadersService.getBboxCoords(Arrays.asList(northWest, southEast));
			QuadRect searchBbox = mapReadersService.getSearchBbox(bbox);
			List<BinaryMapIndexReader> readers = new ArrayList<>();
			try {
				List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = mapReadersService.getMapsForSearch(searchBbox,
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
					Map<String, String> tags = searchResultConverter.getPoiTypeFields(r.object);
					if (tags.isEmpty()) {
						return true;
					}
					if (tags.get(SearchResultConverter.PoiTypeField.CATEGORY_KEY_NAME.getFieldName()).startsWith("brand")) {
						SearchUICore.SearchResultCollection resultCollection;
						try {
							String brand = tags.get(SearchResultConverter.PoiTypeField.NAME.getFieldName());
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
		MapPoiTypes mapPoiTypes = poiTypesService.getMapPoiTypes(locale);

		SearchUICore searchUICore = new SearchUICore(mapPoiTypes, locale, false);

		SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(
				searchUICore.getPoiTypes());
		SearchCoreFactory.SearchAmenityByTypeAPI searchAmenityByTypesAPI = new SearchCoreFactory.SearchAmenityByTypeAPI(
				searchUICore.getPoiTypes(), searchAmenityTypesAPI);
		searchUICore.registerAPI(searchAmenityByTypesAPI);
		SearchSettings settings = searchUICore.getSearchSettings().setSearchTypes(ObjectType.POI);
		settings = settings.setOriginalLocation(new LatLon(lat, lon));
		settings.setRegions(mapReadersService.getOsmandRegions());

		settings.setOfflineIndexes(readers);
		searchUICore.updateSettings(settings.setSearchBBox31(searchBbox));

		searchUICore.init();

		return searchUICore;
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

	private void sortPoiResultsByDistance(List<Feature> features, LatLon center) {
		Map<Feature, Double> distances = new IdentityHashMap<>(features.size());
		for (Feature f : features) {
			float[] coords = (float[]) f.geometry.coordinates;
			distances.put(f, MapUtils.getDistance(new LatLon(coords[1], coords[0]), center.getLatitude(),
					center.getLongitude()));
		}
		features.sort(Comparator.comparingDouble(distances::get));
	}

	private boolean isContainsBbox(PoiSearchData data) {
		QuadRect searchBbox = mapReadersService.getSearchBbox(data.bbox);
		QuadRect oldSearchBbox = mapReadersService.getSearchBbox(data.savedBbox);
		return oldSearchBbox.contains(searchBbox.left, searchBbox.top, searchBbox.right, searchBbox.bottom);
	}
}
