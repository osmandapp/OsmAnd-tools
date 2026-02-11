package net.osmand.server.api.services;

import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.*;
import net.osmand.binary.BinaryMapIndexReader.SearchPoiTypeFilter;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.data.*;
import net.osmand.data.City.CityType;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.*;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.router.TransportStopsRouteReader;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.*;
import net.osmand.server.utils.MapPoiTypesTranslator;
import net.osmand.util.Algorithms;
import net.osmand.util.LocationParser;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static net.osmand.binary.BinaryMapIndexReader.SearchRequest.ZOOM_TO_SEARCH_POI;
import static net.osmand.data.Amenity.OPENING_HOURS;
import static net.osmand.data.MapObject.unzipContent;
import static net.osmand.gpx.GPXUtilities.AMENITY_PREFIX;
import static net.osmand.search.SearchUICore.*;
import static net.osmand.server.controllers.pub.GeojsonClasses.*;
import static net.osmand.shared.gpx.GpxUtilities.OSM_PREFIX;
import static net.osmand.util.OpeningHoursParser.*;
import static net.osmand.util.OpeningHoursParser.parseOpenedHours;

@Service
public class SearchService {

	private static final Log LOGGER = LogFactory.getLog(SearchService.class);
    public static final String IS_OPENED_PREFIX = "open:";
	public static final String OPENING_HOURS_INFO_SUFFIX = "_info";

    @Autowired
    OsmAndMapsService osmAndMapsService;

    @Autowired
    WikiService wikiService;
    
    OsmandRegions osmandRegions;
    
    private ConcurrentHashMap<String, Map<String, String>> translationsCache;
    
    private static final int SEARCH_RADIUS_LEVEL = 1;
    public static final double SEARCH_RADIUS_DEGREE = 1.5;
    private static final int TOTAL_LIMIT_POI = 2000;
    private static final int TOTAL_LIMIT_SEARCH_RESULTS = 15000;
    private static final int TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB = 1000;
    private static final int TOTAL_LIMIT_TRANSPORT_STOPS = 1000;
    private static final double SEARCH_POI_RADIUS_DEGREE = 0.0007;

    private static final String DEFAULT_SEARCH_LANG = "en";
    private static final String AND_RES = "/androidResources/";
    
    private static final String DELIMITER = " ";

    private static final String WIKI_POI_TYPE = "osmwiki";

    private final ConcurrentHashMap<String, MapPoiTypes> poiTypesByLocale = new ConcurrentHashMap<>();

    public static class PoiSearchResult {
        
        public PoiSearchResult(boolean useLimit, boolean mapLimitExceeded, boolean alreadyFound, FeatureCollection features) {
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
        
        public PoiSearchData(List<PoiSearchCategory> categories,
                             String northWest,
                             String southEast,
                             String savedNorthWest,
                             String savedSouthEast,
                             int prevCategoriesCount,
                             String prevSearchRes,
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

    public record PoiSearchCategory(String category, String lang) {}

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

	public List<Feature> search(SearchContext ctx, Calendar clientTime) throws IOException {
		long tm = System.currentTimeMillis();
		SearchResultWrapper searchResults = searchResults(ctx, new SearchOption(false, null), null);
		List<SearchResult> res = searchResults.results();
		if (System.currentTimeMillis() - tm > 1000) {
			LOGGER.info(String.format("Search %s results %d took %.2f sec - %s",ctx. text,
					searchResults.results() == null ? 0 : searchResults.results().size(),
					(System.currentTimeMillis() - tm) / 1000.0, searchResults.stat));
		}
		List<Feature> features = new ArrayList<>();
		if (res != null && !res.isEmpty()) {
			saveSearchResult(res, features, clientTime);
		}

		return !features.isEmpty() ? features : Collections.emptyList();
	}

	public record SearchContext(double lat, double lon, String text, String locale, boolean baseSearch,
	                            Double radiusToLoadMaps, String northWest, String southEast) {
		public double getRadius() {
			return radiusToLoadMaps == null ? SEARCH_RADIUS_DEGREE : radiusToLoadMaps;
		}
	}
	public record SearchOption(boolean unlimited, SearchExportSettings exportedSettings) {}
	public record SearchResultWrapper(List<SearchResult> results, BinaryMapIndexReaderStats.SearchStat stat,
	                                  SearchSettings settings, String unitTestJson) {}

    public SearchResultWrapper searchResults(SearchContext ctx, SearchOption option,
                                             Consumer<List<SearchResult>> consumerInContext) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return new SearchResultWrapper(Collections.emptyList(), null, null, null);
        }
        SearchUICore searchUICore = new SearchUICore(getMapPoiTypes(ctx.locale), ctx.locale, false);
        if (!option.unlimited) {
            searchUICore.setTotalLimit(TOTAL_LIMIT_SEARCH_RESULTS);
        }
        searchUICore.getSearchSettings().setRegions(osmandRegions);

        QuadRect points = osmAndMapsService.points(null,
		        new LatLon(ctx.lat + ctx.getRadius(), ctx.lon - ctx.getRadius()),
                new LatLon(ctx.lat - ctx.getRadius(), ctx.lon + ctx.getRadius()));
        List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
        try {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> list = getMapsForSearch(points, ctx.baseSearch);
            if (list.isEmpty()) {
                return new SearchResultWrapper(Collections.emptyList(), null, null, null);
            }
            usedMapList = osmAndMapsService.getReaders(list,null);
            if (usedMapList.isEmpty()) {
                return new SearchResultWrapper(Collections.emptyList(), null, null, null);
            }
            SearchSettings settings = searchUICore.getPhrase().getSettings();
	        BinaryMapIndexReaderStats.SearchStat stat = new BinaryMapIndexReaderStats.SearchStat();
	        settings.setStat(stat);

	        settings.setOfflineIndexes(usedMapList);
            settings.setRadiusLevel(SEARCH_RADIUS_LEVEL);
			settings.setExportSettings(option.exportedSettings);
            searchUICore.updateSettings(settings);
            
            searchUICore.init();
            searchUICore.registerAPI(new SearchCoreFactory.SearchRegionByNameAPI());
            
            SearchUICore.SearchResultCollection resultCollection = searchUICore.immediateSearch(ctx.text + DELIMITER,
		            new LatLon(ctx.lat, ctx.lon));
            resultCollection = addPoiCategoriesToSearchResult(resultCollection, ctx.text, ctx.locale, searchUICore);

	        List<SearchResult> res = resultCollection != null ? resultCollection.getCurrentSearchResults() : Collections.emptyList();
	        res = filterBrandsOutsideBBox(res, ctx.northWest, ctx.southEast, ctx.locale, ctx.lat, ctx.lon, ctx.baseSearch);
	        res = res.size() > TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB ? res.subList(0, TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB) : res;
			if (consumerInContext != null) {
				consumerInContext.accept(res);
			}

	        String unitTestJson = null;
			if (option.exportedSettings != null) {
				JSONObject json = SearchUICore.createTestJSON(resultCollection, settings.getExportedObjects(), settings.getExportedCities());
				unitTestJson = json == null ? null : json.toString();
			}
			return new SearchResultWrapper(res, stat, settings, unitTestJson);
        } finally {
            osmAndMapsService.unlockReaders(usedMapList);
        }
    }

    public Feature getPoi(String type, String name, LatLon loc, Long osmId, Calendar clientTime) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return null;
        }
        QuadRect searchBbox = osmAndMapsService.points(null,
		        new LatLon(loc.getLatitude() + SEARCH_POI_RADIUS_DEGREE, loc.getLongitude() - SEARCH_POI_RADIUS_DEGREE),
                new LatLon(loc.getLatitude() - SEARCH_POI_RADIUS_DEGREE, loc.getLongitude() + SEARCH_POI_RADIUS_DEGREE));
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
            SearchUICore searchUICore = prepareSearchUICoreForSearchByPoiType(
                    readers, searchBbox, DEFAULT_SEARCH_LANG, loc.getLatitude(), loc.getLongitude());

            // Find POIs by type
            SearchUICore.SearchResultCollection rc =
                    searchPoiByCategory(searchUICore, type, TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB);
            if (rc == null) {
                return null;
            }

            if (name != null) {
                feature = getPoiFeatureByName(rc, name, clientTime);
            } else if (osmId != null) {
                feature = getPoiFeatureByOsmId(rc, osmId, clientTime);
            }

        } finally {
            osmAndMapsService.unlockReaders(readers);
        }
        return feature;
    }

    private Feature getPoiFeatureByName(SearchUICore.SearchResultCollection rc, String name, Calendar clientTime) {
        for (SearchResult r : rc.getCurrentSearchResults()) {
            if (r.objectType != ObjectType.POI || !(r.object instanceof Amenity a)) {
                continue;
            }
            if (matchesName(a, name)) {
                Feature f = getPoiFeature(r, clientTime);
                if (f != null) {
                    return f;
                }
            }
        }
        return null;
    }

    private Feature getPoiFeatureByOsmId(SearchUICore.SearchResultCollection rc, long osmId, Calendar clientTime) {
        for (SearchResult r : rc.getCurrentSearchResults()) {
            if (r.objectType != ObjectType.POI || !(r.object instanceof Amenity a)) {
                continue;
            }
            if (ObfConstants.getOsmObjectId(a) == osmId) {
                Feature f = getPoiFeature(r, clientTime);
                if (f != null) {
                    return f;
                }
            }
        }
        return null;
    }

    public Feature getPoiResultByShareLink(String type, LatLon loc, String name, Long osmId, Long wikidataId, Calendar clientTime) throws IOException {
        Feature poiFeature = getPoi(type, name, loc, osmId, clientTime);

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
        if (f1 == null) return f2;
        if (f2 == null) return f1;

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

        String query =
                "SELECT w.id, w.photoId, w.wikiTitle, w.wikiLang, w.wikiDesc, w.photoTitle, " +
                        "w.osmid, w.osmtype, w.poitype, w.poisubtype, " +
                        "w.search_lat AS lat, w.search_lon AS lon, " +
                        "arrayFirst(x -> has(w.wikiArticleLangs, x), " + langListQuery + ") AS lang, " +
                        "indexOf(w.wikiArticleLangs, lang) AS ind, " +
                        "w.wikiArticleContents[ind] AS content, " +
                        "w.wvLinks, w.elo AS elo, w.topic AS topic, w.categories AS categories, w.qrank, w.labelsJson " +
                        "FROM wiki.wikidata w " +
                        "WHERE w.id = " + wikidataId + " " +
                        "ORDER BY w.elo DESC, w.qrank DESC";
        FeatureCollection res = wikiService.getPoiData(null, null, query, "lat", "lon", langs);
        return res.features.get(0);
    }

    private boolean matchesName(Amenity a, String name) {
        if (name == null || name.isBlank()) return true;
        String target = name.trim();

        if (equalsIgnoreCaseSafe(a.getName(), target)) return true;
        if (equalsIgnoreCaseSafe(a.getEnName(false), target)) return true;

        Map<String, String> names = a.getNamesMap(true);
        for (String v : names.values()) {
            if (equalsIgnoreCaseSafe(v, target)) return true;
        }
        return false;
    }

    private boolean equalsIgnoreCaseSafe(String s, String t) {
        return s != null && s.equalsIgnoreCase(t);
    }

    private SearchUICore.SearchResultCollection addPoiCategoriesToSearchResult(SearchUICore.SearchResultCollection resultCollection, String text, String locale, SearchUICore searchUICore) {
        List<SearchResult> poiCategories = searchPoiCategoriesByName(text, locale);
        List<SearchResult> uniquePoiCategories = new ArrayList<>(
                poiCategories.stream()
                        .collect(Collectors.toMap(
                                sr -> sr.localeName,
                                Function.identity(),
                                (first, second) -> first
                        ))
                        .values()
        );
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

    private List<SearchResult> filterBrandsOutsideBBox(List<SearchResult> res, String northWest, String southEast, String locale, double lat, double lon, boolean baseSearch) throws IOException {
        if (northWest != null && southEast != null) {
            List<LatLon> bbox = getBboxCoords(Arrays.asList(northWest, southEast));
            QuadRect searchBbox = getSearchBbox(bbox);
            List<BinaryMapIndexReader> readers = new ArrayList<>();
            try {
                List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = getMapsForSearch(bbox, searchBbox, baseSearch);
                readers = osmAndMapsService.getReaders(mapList, null);
                if (readers.isEmpty()) {
                    return res.stream().filter(r-> r.objectType != ObjectType.POI_TYPE || r.file == null).toList();
                }
                SearchUICore searchUICore = prepareSearchUICoreForSearchByPoiType(readers, searchBbox, locale, lat, lon);
                return res.stream()
                        .filter(r -> {
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
                        })
                        .toList();
            } finally {
                osmAndMapsService.unlockReaders(readers);
            }
        }
        return res;
    }

    private SearchUICore prepareSearchUICoreForSearchByPoiType(List<BinaryMapIndexReader> readers, QuadRect searchBbox, String locale, double lat, double lon)  {
        MapPoiTypes mapPoiTypes = getMapPoiTypes(locale);

        SearchUICore searchUICore = new SearchUICore(mapPoiTypes, locale, false);

        SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(searchUICore.getPoiTypes());
        SearchCoreFactory.SearchAmenityByTypeAPI searchAmenityByTypesAPI = new SearchCoreFactory.SearchAmenityByTypeAPI(searchUICore.getPoiTypes(), searchAmenityTypesAPI);
        searchUICore.registerAPI(searchAmenityByTypesAPI);
        SearchSettings settings = searchUICore.getSearchSettings().setSearchTypes(ObjectType.POI);
        settings = settings.setOriginalLocation(new LatLon(lat, lon));
        settings.setRegions(osmandRegions);

        settings.setOfflineIndexes(readers);
        searchUICore.updateSettings(settings.setSearchBBox31(searchBbox));
        
        searchUICore.init();

        return searchUICore;
    }

    public PoiSearchResult searchPoi(SearchService.PoiSearchData data, String locale, LatLon center, boolean baseSearch, Calendar clientTime) throws IOException {
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
        try {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = getMapsForSearch(data.bbox, searchBbox, baseSearch);
            if (mapList.isEmpty()) {
                return new PoiSearchResult(false, true, false, null);
            }

            usedMapList = osmAndMapsService.getReaders(mapList, null);

            if (data.categories.size() == 1) {
                searchPoiByTypeCategory(data.categories.get(0), locale, searchBbox, usedMapList, foundFeatures, clientTime);
                useLimit = foundFeatures.size() >= TOTAL_LIMIT_POI;
            } else {
                PoiSearchLimit poiSearchLimit = new PoiSearchLimit(TOTAL_LIMIT_POI / data.categories.size(), TOTAL_LIMIT_POI, false);
                for (PoiSearchCategory categoryObj : data.categories) {
                    if (poiSearchLimit.isLimitReached()) {
                        useLimit = true;
                        break;
                    }
                    int categoryStartSize = foundFeatures.size();
                    searchPoiByTypeCategory(categoryObj, locale, searchBbox, usedMapList, foundFeatures, poiSearchLimit,
                            clientTime);
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
            return new PoiSearchResult(useLimit, false, false, new FeatureCollection(features.toArray(new Feature[0])));
        } else {
            return new PoiSearchResult(false, false, false, null);
        }
    }

    private void searchPoiByTypeCategory(PoiSearchCategory categoryObj, String locale, QuadRect searchBbox,
                                         List<BinaryMapIndexReader> readers, Map<Long, Feature> foundFeatures,
                                         Calendar clientTime) throws IOException {
        searchPoiByTypeCategory(categoryObj, locale, searchBbox, readers, foundFeatures, null, clientTime);
    }

    private void searchPoiByTypeCategory(PoiSearchCategory categoryObj, String locale, QuadRect searchBbox,
                                         List<BinaryMapIndexReader> readers, Map<Long, Feature> foundFeatures,
                                         PoiSearchLimit poiSearchLimit, Calendar clientTime) throws IOException {
        if (searchBbox == null) {
            return;
        }

        MapPoiTypes mapPoiTypes = getMapPoiTypes(locale);
        AbstractPoiType poiType = mapPoiTypes.getAnyPoiTypeByKey(categoryObj.category, false);
        SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(mapPoiTypes);
        SearchCoreFactory.SearchAmenityByTypeAPI searchAmenityByTypesAPI = new SearchCoreFactory.SearchAmenityByTypeAPI(mapPoiTypes, searchAmenityTypesAPI);
        SearchPoiTypeFilter filter = null;

        if (poiType != null) {
            filter = searchAmenityByTypesAPI.getPoiTypeFilter(poiType, new LinkedHashSet<>());
        }

        int left31 = (int) searchBbox.left;
        int right31 = (int) searchBbox.right;
        int top31 = (int) searchBbox.top;
        int bottom31 = (int) searchBbox.bottom;

        int categoryStartSize = foundFeatures.size();

        for (BinaryMapIndexReader reader : readers) {
            if (poiSearchLimit != null ? poiSearchLimit.isLimitReached() : foundFeatures.size() >= TOTAL_LIMIT_POI) {
                break;
            }
            BinaryMapIndexReader.SearchRequest<Amenity> request;
            if (filter == null || filter.isEmpty()) {
                request = createSearchRequestByBrand(reader, categoryObj, mapPoiTypes, left31, right31, top31, bottom31);
                if (request == null) {
                    if (categoryObj.lang == null || categoryObj.lang.isEmpty()) {
                        LOGGER.warn(String.format("Brand search skipped for category '%s': language is null or empty", categoryObj.category));
                    } else {
                        LOGGER.warn(String.format("Brand search skipped for category '%s' with language '%s': no matching brand found", categoryObj.category, categoryObj.lang));
                    }
                    continue;
                }
            } else {
                request = BinaryMapIndexReader.buildSearchPoiRequest(
                        left31, right31, top31, bottom31, ZOOM_TO_SEARCH_POI, filter, null);
            }
            if (request == null) {
                continue;
            }
            List<Amenity> amenities = reader.searchPoi(request);
            int remaining = poiSearchLimit != null 
                    ? poiSearchLimit.getRemainingForSave(categoryStartSize, foundFeatures.size())
                    : TOTAL_LIMIT_POI - foundFeatures.size();
            saveAmenityResults(amenities, foundFeatures, remaining, locale, clientTime);
            if (poiSearchLimit != null && poiSearchLimit.shouldStopCategory(categoryStartSize, foundFeatures.size())) {
                break;
            }
        }
    }

    private BinaryMapIndexReader.SearchRequest<Amenity> createSearchRequestByBrand(BinaryMapIndexReader reader,
                                                                                   PoiSearchCategory categoryObj,
                                                                                   MapPoiTypes mapPoiTypes,
                                                                                   int left31, int right31,
                                                                                   int top31, int bottom31) throws IOException {
        List<BinaryMapPoiReaderAdapter.PoiSubType> brands = reader.getTopIndexSubTypes();
        String lang = categoryObj.lang;
        BinaryMapPoiReaderAdapter.PoiSubType selectedBrand = brands.stream()
                .filter(brand -> brand.name.contains(":") && brand.name.split(":")[1].equalsIgnoreCase(lang))
                .findFirst()
                .orElseGet(() -> brands.stream()
                        .filter(brand -> !brand.name.contains(":"))
                        .findFirst()
                        .orElse(null)
                );
        if (selectedBrand == null) {
            return null;
        }
        String brandValueToSearch = categoryObj.category;
        TopIndexFilter brandFilter = new TopIndexFilter(selectedBrand, mapPoiTypes, brandValueToSearch);
        SearchPoiTypeFilter filter = BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER;
        return BinaryMapIndexReader.buildSearchPoiRequest(
                left31, right31, top31, bottom31, ZOOM_TO_SEARCH_POI, filter, brandFilter, null);
    }

    private void sortPoiResultsByDistance(List<Feature> features, LatLon center) {
        features.sort(Comparator.comparingDouble(f -> {
            float[] coords = (float[]) f.geometry.coordinates;
            LatLon loc = new LatLon(coords[1], coords[0]);
            return MapUtils.getDistance(loc, center.getLatitude(), center.getLongitude());
        }));
    }

    public Feature searchPoiByOsmId(LatLon loc, long osmid, String type, Calendar clientTime) throws IOException {
        final String RELATION_TYPE = "3";
        final double RELATION_SEARCH_RADIUS = 0.0055; // ~600 meters
        final double OTHER_POI_SEARCH_RADIUS = 0.0001; // ~11 meters
        final double SEARCH_POI_BY_OSMID_RADIUS_DEGREE = type.equals(RELATION_TYPE) ? RELATION_SEARCH_RADIUS : OTHER_POI_SEARCH_RADIUS;
        final int mapZoom = 15;
        LatLon p1 = new LatLon(loc.getLatitude() + SEARCH_POI_BY_OSMID_RADIUS_DEGREE, loc.getLongitude() - SEARCH_POI_BY_OSMID_RADIUS_DEGREE);
        LatLon p2 = new LatLon(loc.getLatitude() - SEARCH_POI_BY_OSMID_RADIUS_DEGREE, loc.getLongitude() + SEARCH_POI_BY_OSMID_RADIUS_DEGREE);
        BinaryMapIndexReader.SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(
                MapUtils.get31TileNumberX(p1.getLongitude()),
                MapUtils.get31TileNumberX(p2.getLongitude()),
                MapUtils.get31TileNumberY(p1.getLatitude()),
                MapUtils.get31TileNumberY(p2.getLatitude()),
                mapZoom,
                BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER,
                new ResultMatcher<>() {
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
            return getPoiFeature(res, clientTime);
        }
        return null;
    }

    public Feature searchPoiByEnName(LatLon loc, String enName) throws IOException {
        final double SEARCH_POI_RADIUS_DEGREE = 0.0001;
        final int mapZoom = 15;
        LatLon p1 = new LatLon(loc.getLatitude() + SEARCH_POI_RADIUS_DEGREE, loc.getLongitude() - SEARCH_POI_RADIUS_DEGREE);
        LatLon p2 = new LatLon(loc.getLatitude() - SEARCH_POI_RADIUS_DEGREE, loc.getLongitude() + SEARCH_POI_RADIUS_DEGREE);
        BinaryMapIndexReader.SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(
                MapUtils.get31TileNumberX(p1.getLongitude()),
                MapUtils.get31TileNumberX(p2.getLongitude()),
                MapUtils.get31TileNumberY(p1.getLatitude()),
                MapUtils.get31TileNumberY(p2.getLatitude()),
                mapZoom,
                BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER,
                new ResultMatcher<>() {
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

    private SearchResult searchPoiByReq(BinaryMapIndexReader.SearchRequest<Amenity> req, LatLon p1, LatLon p2, boolean baseSearch) throws IOException {
        List<LatLon> bbox = Arrays.asList(p1, p2);
        QuadRect searchBbox = getSearchBbox(bbox);

        List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
        SearchResult res = null;
        try {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = getMapsForSearch(bbox, searchBbox, baseSearch);
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
    
    private List<OsmAndMapsService.BinaryMapIndexReaderReference> getMapsForSearch(List<LatLon> bbox, QuadRect searchBbox, boolean baseSearch) throws IOException {
        OsmAndMapsService.BinaryMapIndexReaderReference basemap = osmAndMapsService.getBaseMap();
        if (baseSearch) {
            return List.of(basemap);
        } else {
            if (searchBbox != null) {
                List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService.getObfReaders(searchBbox, bbox, "search");
                list.add(basemap);
                return list;
            }
        }
        return Collections.emptyList();
    }

    private List<OsmAndMapsService.BinaryMapIndexReaderReference> getMapsForSearch(QuadRect points, boolean baseSearch) throws IOException {
        OsmAndMapsService.BinaryMapIndexReaderReference basemap = osmAndMapsService.getBaseMap();
        if (baseSearch) {
            return List.of(basemap);
        } else {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService.getObfReaders(points, null, "search");
            list.add(basemap);
            return list;
        }
    }
    
    public SearchUICore.SearchResultCollection searchPoiByCategory(SearchUICore searchUICore, String text, int limit) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return null;
        }
        searchUICore.setTotalLimit(limit);
        return searchUICore.immediateSearch(text  + DELIMITER, null);
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

		SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(
				MapUtils.get31TileNumberX(searchBbox.left), MapUtils.get31TileNumberX(searchBbox.right),
				MapUtils.get31TileNumberY(searchBbox.top), MapUtils.get31TileNumberY(searchBbox.bottom), 15,
				new SearchPoiTypeFilter() {

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
        SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(mapPoiTypes);
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
        if (translationsCache == null) {
            translationsCache = new ConcurrentHashMap<>();
        }
        
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
            tags.put(PoiTypeField.NAME.getFieldName(), type.getValue());
        } else if (obj instanceof AbstractPoiType type) {
            tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getKeyName());
            tags.put(PoiTypeField.ICON_NAME.getFieldName(), type.getIconKeyName());
        } else if (obj instanceof MapObject type) {
            tags.put(PoiTypeField.EN_NAME.getFieldName(), type.getEnName(false));
        }
        return tags;
    }

    public enum PoiTypeField {
        NAME("web_name"),
        EN_NAME("web_en_name"),
        TYPE("web_type"),
        ADDRESS_1("web_address1"),
        ADDRESS_2("web_address2"),
        KEY_NAME("web_keyName"),
        OSM_TAG("web_typeOsmTag"),
        OSM_VALUE("web_typeOsmValue"),
        ICON_NAME("web_iconKeyName"),
        CATEGORY_ICON("web_categoryIcon"),
        CATEGORY_KEY_NAME("web_categoryKeyName"),
        POI_ADD_CATEGORY_NAME("web_poiAdditionalCategory"),
        POI_FILTER_NAME("web_poiFilterName"),
        POI_ID("web_poi_id"),
        POI_NAME("web_poi_name"),
        POI_COLOR("web_poi_color"),
        POI_ICON_NAME("web_poi_iconName"),
        POI_TYPE("web_poi_type"),
        POI_SUBTYPE("web_poi_subType"),
        POI_OSM_URL("web_poi_osmUrl"),
        CITY("web_city");

        private final String fieldName;

        PoiTypeField(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    private void saveSearchResult(List<SearchResult> res, List<Feature> features, Calendar clientTime) {
        for (SearchResult result : res) {
            features.add(getFeature(result, clientTime));
        }
    }

    private void saveAmenityResults(List<Amenity> amenities, Map<Long, Feature> foundFeatures, int remainingLimit,
                                    String locale, Calendar clientTime) {
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
                String cityName = amenity.getCityFromTagGroups(locale);
                String city = cityName == null ? "" : cityName;
                String mainCity = getMainCityName(city);
                SearchResult result = new SearchResult();
                result.object = amenity;
                result.objectType = ObjectType.POI;
                result.location = amenity.getLocation();
                result.addressName = calculateAddressString(amenity, cityName, mainCity, dominatedCity);
                foundFeatures.put(osmId, getPoiFeature(result, clientTime));
                remainingLimit--;
            }
        }
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

	public Feature getFeature(SearchResult result, Calendar clientTime) {
		Feature feature;
		if (result.objectType == ObjectType.POI) {
			feature = getPoiFeature(result, clientTime);
		} else {
			Geometry geometry = Geometry.point(result.location != null ? result.location : new LatLon(0, 0));
			feature = new Feature(geometry)
					.prop(PoiTypeField.TYPE.getFieldName(), result.objectType)
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
				feature.prop(PoiTypeField.NAME.getFieldName(), result.localeName + " - " + result.localeRelatedObjectName);
			}
			Map<String, String> tags = getPoiTypeFields(result.object);
			for (Map.Entry<String, String> entry : tags.entrySet()) {
				feature.prop(entry.getKey(), entry.getValue());
			}
		}
		return feature;
	}

	private Feature getPoiFeature(SearchResult result, Calendar clientTime) {
		Amenity amenity = (Amenity) result.object;
		Feature feature = null;

		feature = new Feature(Geometry.point(amenity.getLocation()))
				.prop(PoiTypeField.TYPE.getFieldName(), result.objectType)
				.prop(PoiTypeField.POI_ID.getFieldName(), amenity.getId())
				.prop(PoiTypeField.POI_NAME.getFieldName(), amenity.getName())
				.prop(PoiTypeField.POI_COLOR.getFieldName(), amenity.getColor())

				.prop(PoiTypeField.POI_TYPE.getFieldName(), amenity.getType().getKeyName())
				.prop(PoiTypeField.POI_SUBTYPE.getFieldName(), amenity.getSubType())
				.prop(PoiTypeField.POI_OSM_URL.getFieldName(), getOsmUrl(result));
		

		Map<String, String> tags = amenity.getAmenityExtensions();
		filterWikiTags(tags);
		for (Map.Entry<String, String> entry : tags.entrySet()) {
			String key = entry.getKey().startsWith(OSM_PREFIX) ? entry.getKey().substring(OSM_PREFIX.length())
					: entry.getKey();
			if (MapPoiTypes.getDefault().getAnyPoiAdditionalTypeByKey(key) instanceof PoiType type && type.isHidden()) {
				continue;
			}
			String value = unzipContent(entry.getValue());
			feature.prop(entry.getKey(), value);
		}
		String openingHoursValue = tags.get(AMENITY_PREFIX + OPENING_HOURS);
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
				return openingHoursInfo.stream()
						.map(info -> (info.isOpened() ? IS_OPENED_PREFIX : "") + info.getInfo())
						.collect(Collectors.joining(";"));
			}
		}
		return null;
	}

    private void filterWikiTags(Map<String, String> tags) {
        tags.entrySet().removeIf(entry -> entry.getKey().startsWith("osm_tag_travel_elo")
                || entry.getKey().startsWith("osm_tag_travel_topic")
                || entry.getKey().startsWith("osm_tag_qrank")
                || entry.getKey().startsWith("osm_tag_wiki_place")
                || entry.getKey().startsWith("osm_tag_wiki_photo"));
    }
    
    public String getPoiAddress(LatLon location) throws IOException, InterruptedException {
        if (location != null) {
            List<GeocodingUtilities.GeocodingResult> list = osmAndMapsService.geocoding(location.getLatitude(), location.getLongitude());
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

    public TransportStopsSearchResult searchTransportStops(String northWest, String southEast) throws IOException {
        List<LatLon> bbox = getBboxCoords(Arrays.asList(northWest, southEast));
        if (bbox.size() != 2) {
            return new TransportStopsSearchResult(false, new FeatureCollection());
        }

        TransportStopsReaderResult readerResult = getTransportStopsReader(bbox);
        if (readerResult == null) {
            return new TransportStopsSearchResult(false, new FeatureCollection());
        }

        List<Feature> features = new ArrayList<>();
        boolean useLimit = false;
        
        try {
            for (TransportStop s : readerResult.transportReaders.readMergedTransportStops(readerResult.request)) {
                if (features.size() >= TOTAL_LIMIT_TRANSPORT_STOPS) {
                    useLimit = true;
                    break;
                }
                if (!s.isDeleted() && !s.isMissingStop()) {
                    Feature feature = convertTransportStopToFeature(s);
                    if (feature != null) {
                        features.add(feature);
                    }
                }
            }
        } finally {
            osmAndMapsService.unlockReaders(readerResult.readers);
        }

        if (features.isEmpty()) {
            return new TransportStopsSearchResult(false, new FeatureCollection());
        }
        return new TransportStopsSearchResult(useLimit, new FeatureCollection(features.toArray(new Feature[0])));
    }

    public TransportRouteFeature getTransportRoute(LatLon transportStopCoords, long stopId, long routeId) throws IOException {
        List<LatLon> bbox = Arrays.asList(
                new LatLon(transportStopCoords.getLatitude() + SEARCH_POI_RADIUS_DEGREE, transportStopCoords.getLongitude() - SEARCH_POI_RADIUS_DEGREE),
                new LatLon(transportStopCoords.getLatitude() - SEARCH_POI_RADIUS_DEGREE, transportStopCoords.getLongitude() + SEARCH_POI_RADIUS_DEGREE)
        );

        TransportStopsReaderResult readerResult = getTransportStopsReader(bbox);
        if (readerResult == null) {
            return null;
        }

        try {
            TransportStop foundStop = null;
            for (TransportStop s : readerResult.transportReaders.readMergedTransportStops(readerResult.request)) {
                if (s.getId() == stopId) {
                    foundStop = s;
                    break;
                }
            }
            if (foundStop != null) {
                List<TransportRoute> routes = foundStop.getRoutes();
                if (routes == null || routes.isEmpty()) {
                    return null;
                }
                for (TransportRoute route : routes) {
                    if (route.getId() == routeId) {
                        List<Long> stops = route.getForwardStops()
                                .stream()
                                .map(TransportStop::getId)
                                .toList();
                        List<List<LatLon>> nodes = route.getForwardWays()
                                .stream()
                                .map(way -> way.getNodes()
                                        .stream()
                                        .map(Node::getLatLon)
                                        .toList())
                                .toList();
                        return new TransportRouteFeature(route.getId(), stops, nodes);
                    }
                }
            }
        } finally {
            osmAndMapsService.unlockReaders(readerResult.readers);
        }
        return null;
    }

    public Feature getTransportStop(LatLon transportStopCoords, long stopId) throws IOException {
        List<LatLon> bbox = Arrays.asList(
                new LatLon(transportStopCoords.getLatitude() + SEARCH_POI_RADIUS_DEGREE, transportStopCoords.getLongitude() - SEARCH_POI_RADIUS_DEGREE),
                new LatLon(transportStopCoords.getLatitude() - SEARCH_POI_RADIUS_DEGREE, transportStopCoords.getLongitude() + SEARCH_POI_RADIUS_DEGREE)
        );
        TransportStopsReaderResult readerResult = getTransportStopsReader(bbox);
        if (readerResult == null) {
            return null;
        }
        try {
            for (TransportStop s : readerResult.transportReaders.readMergedTransportStops(readerResult.request)) {
                if (s.getId() == stopId) {
                    return convertTransportStopToFeature(s);
                }
            }
        } finally {
            osmAndMapsService.unlockReaders(readerResult.readers);
        }
        return null;
    }

    private TransportStopsReaderResult getTransportStopsReader(List<LatLon> bbox) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return null;
        }

        QuadRect searchBbox = getSearchBbox(bbox);
        if (searchBbox == null) {
            return null;
        }

        int left31 = (int) searchBbox.left;
        int right31 = (int) searchBbox.right;
        int top31 = (int) searchBbox.top;
        int bottom31 = (int) searchBbox.bottom;

        List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = getMapsForSearch(bbox, searchBbox, false);
        if (mapList.isEmpty()) {
            return null;
        }
        List<BinaryMapIndexReader> readers = osmAndMapsService.getReaders(mapList, null);
        if (readers.isEmpty()) {
            return null;
        }
        TransportStopsRouteReader transportReaders = new TransportStopsRouteReader(readers);
        SearchRequest<TransportStop> request = BinaryMapIndexReader.buildSearchTransportRequest(
                left31, right31, top31, bottom31, -1, new ArrayList<>());

        return new TransportStopsReaderResult(transportReaders, readers, request);
    }

    private Feature convertTransportStopToFeature(TransportStop stop) {
        if (stop == null || stop.getLocation() == null) {
            return null;
        }

        LatLon location = stop.getLocation();
        Feature feature = new Feature(Geometry.point(location));

        feature.prop("id", stop.getId());
        feature.prop("name", stop.getName());

        List<TransportRoute> routes = stop.getRoutes();
        if (routes != null && !routes.isEmpty()) {
            List<TransportStopFeature> stopFeatures = new ArrayList<>();
            routes.forEach(route -> {
                stopFeatures.add(new TransportStopFeature(route.getId(), route.getName(), route.getType(), route.getRef(), route.getColor()));
            });
            feature.prop("routes", stopFeatures);
        }

        return feature;
    }

    private record TransportStopsReaderResult(TransportStopsRouteReader transportReaders,
                                              List<BinaryMapIndexReader> readers,
                                              SearchRequest<TransportStop> request) {
    }

    public record TransportStopsSearchResult(boolean useLimit, FeatureCollection features) {
    }

    public record TransportStopFeature(long id, String name, String type, String ref, String color) {
    }

    public record TransportRouteFeature(long id, List<Long> stops, List<List<LatLon>> nodes) {
    }

    public LatLon parseLocation(String locationString) {
        if (locationString == null || locationString.trim().isEmpty()) {
            return null;
        }
        return LocationParser.parseLocation(locationString);
    }

}