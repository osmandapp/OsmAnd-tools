package net.osmand.server.api.services.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import net.osmand.search.core.TopIndexFilter;
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
import net.osmand.osm.PoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchResult;
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
	                                 int zoom, String timeZone) throws IOException {
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
		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = mapReadersService.getMapsForSearch(searchBbox, baseSearch);
			if (mapList.isEmpty()) {
				return new PoiSearchResult(false, true, false, null);
			}

			usedMapList = osmAndMapsService.getReaders(mapList, null);

			if (data.categories.size() == 1) {
				searchPoiByTypeCategory(data.categories.get(0), locale, searchBbox, usedMapList, foundFeatures,
						null, zoom, timeZone);
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
							zoom, timeZone);
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
			return new PoiSearchResult(useLimit, false, false,
					new FeatureCollection(features.toArray(new Feature[0])));
		} else {
			return new PoiSearchResult(false, false, false, null);
		}
	}

	private List<Amenity> searchPoiAmenities(String categoryKey, QuadRect bboxLatLon, int poiZoom,
	                                         List<BinaryMapIndexReader> readers, int limit) throws IOException {
		SpatialPoiSearch poiTypeSearch = spatialSearchService.getSpatialPoiTypeSearch();
		SpatialPoiType spatialType = null;
		if (!categoryKey.startsWith(MapPoiTypes.TOP_INDEX_ADDITIONAL_PREFIX)) {
			spatialType = poiTypeSearch.getByKey(categoryKey);
			if (spatialType == null) {
				LOGGER.debug(String.format("Unknown poi category '%s'", categoryKey));
				return Collections.emptyList();
			}
			categoryKey = spatialType.getKey();
		}
		SpatialTextSearchSettings settings = SpatialTextSearchSettings.searchPoiByCategorySettings(poiZoom, bboxLatLon);
		settings.AUTO_CLEAR_PREFIX_CACHE_LIMIT = SpatialSearchService.SPATIAL_PREFIX_CACHE_LIMIT;
		SpatialSearchContext sscontext = new SpatialSearchContext(settings, readers, poiTypeSearch, null);
		sscontext.getStats().printLogs = false;

		boolean indexed = spatialType == null
				|| (spatialType.singleType instanceof PoiType poiType && !poiType.isNonIndx());
		if (!indexed) {
			return poiTypeSearch.loadPOIObjects(sscontext, spatialType, bboxLatLon, poiZoom, limit);
		}

		SpatialSearchResults res = spatialSearchService.getSpatialTextSearch()
				.searchAPI(NameIndexReader.POI_CATEGORY_PREFIX + categoryKey, sscontext);

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
		return amenities;
	}

	private QuadRect toLatLonBbox(QuadRect searchBbox31) {
		return new QuadRect(
				MapUtils.get31LongitudeX((int) searchBbox31.left), MapUtils.get31LatitudeY((int) searchBbox31.top),
				MapUtils.get31LongitudeX((int) searchBbox31.right), MapUtils.get31LatitudeY((int) searchBbox31.bottom));
	}

	private int estimatePoiZoom(QuadRect bboxLatLon, int zoom) {
		if (zoom < 0) {
			double lonWidth = Math.abs(bboxLatLon.right - bboxLatLon.left);
			zoom = (int) Math.round(Math.log(360 * SPATIAL_POI_CATEGORY_VIEW_TILES / Math.max(lonWidth, 0.001))
					/ Math.log(2));
		}
		zoom = Math.max(SPATIAL_POI_CATEGORY_MIN_ZOOM, Math.min(SPATIAL_POI_CATEGORY_MAX_ZOOM, zoom));
		return zoom + SPATIAL_POI_CATEGORY_VIEW_ZOOM_SHIFT;
	}

	private void searchPoiByTypeCategory(PoiSearchCategory categoryObj, String locale, QuadRect searchBbox,
	                                     List<BinaryMapIndexReader> readers, Map<Long, Feature> foundFeatures, PoiSearchLimit poiSearchLimit,
	                                     int zoom, String timeZone) throws IOException {
		if (searchBbox == null) {
			return;
		}

		int remaining = poiSearchLimit != null
				? poiSearchLimit.getRemainingForSave(foundFeatures.size(), foundFeatures.size())
				: TOTAL_LIMIT_POI - foundFeatures.size();

		if (remaining <= 0) {
			return;
		}

		QuadRect bboxLatLon = toLatLonBbox(searchBbox);
		int poiZoom = estimatePoiZoom(bboxLatLon, zoom);
		List<Amenity> amenities = searchPoiAmenities(categoryObj.category(), bboxLatLon, poiZoom, readers, remaining);

		searchResultConverter.saveAmenityResults(amenities, foundFeatures, remaining, locale, timeZone);
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
		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> mapRefs = mapReadersService.getMapsForSearch(searchBbox, false);
			if (mapRefs.isEmpty()) {
				return null;
			}
			readers = osmAndMapsService.getReaders(mapRefs, null);
			if (readers.isEmpty()) {
				return null;
			}

			QuadRect bboxLatLon = toLatLonBbox(searchBbox);
			int poiZoom = SPATIAL_POI_CATEGORY_MAX_ZOOM + SPATIAL_POI_CATEGORY_VIEW_ZOOM_SHIFT;
			List<Amenity> amenities = searchPoiAmenities(type, bboxLatLon, poiZoom, readers,
					ClassicSearchService.TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB);

			for (Amenity a : amenities) {
				boolean match = name != null ? matchesName(a, name)
						: osmId != null && ObfConstants.getOsmObjectId(a) == osmId;
				if (match) {
					Feature f = searchResultConverter.getPoiFeature(
							searchResultConverter.buildPoiSearchResult(a, PoiTypesService.DEFAULT_SEARCH_LANG, ""), timeZone);
					if (f != null) {
						return f;
					}
				}
			}
			return null;
		} finally {
			osmAndMapsService.unlockReaders(readers);
		}
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
		double radiusDegree = type.equals(RELATION_TYPE) ? RELATION_SEARCH_RADIUS : OTHER_POI_SEARCH_RADIUS;
		return searchSinglePoi(loc, radiusDegree, new ResultMatcher<>() {
			@Override
			public boolean publish(Amenity amenity) {
				return ObfConstants.getOsmObjectId(amenity) == osmid;
			}

			@Override
			public boolean isCancelled() {
				return false;
			}
		}, timeZone);
	}

	public Feature searchPoiByEnName(LatLon loc, String enName) throws IOException {
		final double SEARCH_RADIUS_DEGREE = 0.0001;
		return searchSinglePoi(loc, SEARCH_RADIUS_DEGREE, new ResultMatcher<>() {
			@Override
			public boolean publish(Amenity amenity) {
				return amenity.getEnName(false).equals(enName);
			}

			@Override
			public boolean isCancelled() {
				return false;
			}
		}, null);
	}

	private Feature searchSinglePoi(LatLon loc, double radiusDegree, ResultMatcher<Amenity> matcher, String timeZone)
			throws IOException {
		final int mapZoom = 15;
		LatLon p1 = new LatLon(loc.getLatitude() + radiusDegree, loc.getLongitude() - radiusDegree);
		LatLon p2 = new LatLon(loc.getLatitude() - radiusDegree, loc.getLongitude() + radiusDegree);
		BinaryMapIndexReader.SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(
				MapUtils.get31TileNumberX(p1.getLongitude()), MapUtils.get31TileNumberX(p2.getLongitude()),
				MapUtils.get31TileNumberY(p1.getLatitude()), MapUtils.get31TileNumberY(p2.getLatitude()), mapZoom,
				BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER, matcher);
		SearchResult res = searchPoiByReq(req, p1, p2, false);

		return res != null ? searchResultConverter.getPoiFeature(res, timeZone) : null;
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

	public List<SearchResult> filterBrandsOutsideBBox(List<SearchResult> res, String northWest, String southEast,
	                                                  boolean baseSearch) throws IOException {
		if (northWest == null || southEast == null) {
			return res;
		}
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
			QuadRect bboxLatLon = toLatLonBbox(searchBbox);
			int poiZoom = estimatePoiZoom(bboxLatLon, -1);
			List<BinaryMapIndexReader> lockedReaders = readers;
			return res.stream().filter(r -> {
				if (r.objectType != ObjectType.POI_TYPE || r.file == null) {
					return true;
				}
				if (!(r.object instanceof TopIndexFilter brandFilter) || !brandFilter.getTag().startsWith("brand")) {
					return true;
				}
				try {
					return !searchPoiAmenities(brandFilter.getFilterId(), bboxLatLon, poiZoom, lockedReaders, 1).isEmpty();
				} catch (IOException e) {
					return true;
				}
			}).toList();
		} finally {
			osmAndMapsService.unlockReaders(readers);
		}
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
