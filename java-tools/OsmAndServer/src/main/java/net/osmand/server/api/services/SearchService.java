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
import net.osmand.search.SearchUICore;
import net.osmand.search.core.*;
import net.osmand.server.utils.MapPoiTypesTranslator;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import static net.osmand.data.MapObject.unzipContent;
import static net.osmand.server.controllers.pub.GeojsonClasses.*;
import static net.osmand.shared.gpx.GpxUtilities.OSM_PREFIX;

@Service
public class SearchService {
    
	private static final Log LOGGER = LogFactory.getLog(SearchService.class);

    @Autowired
    OsmAndMapsService osmAndMapsService;

    @Autowired
    WikiService wikiService;
    
    OsmandRegions osmandRegions;
    
    private ConcurrentHashMap<String, Map<String, String>> translationsCache;
    
    private static final int SEARCH_RADIUS_LEVEL = 1;
    private static final double SEARCH_RADIUS_DEGREE = 1.5;
    private static final int TOTAL_LIMIT_POI = 2000;
    private static final int TOTAL_LIMIT_SEARCH_RESULTS = 15000;
    private static final int TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB = 1000;
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

    public static class PoiSearchCategory {
        public String category;
        public String key;
        public String lang;
        public String mode;

        public PoiSearchCategory(String category, String lang, String key, String mode) {
            this.category = category;
            this.key = key;
            this.lang = lang;
            this.mode = mode;
        }
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

	public List<Feature> search(double lat, double lon, String text, String locale, boolean baseSearch, String northWest, String southEast) throws IOException {
		long tm = System.currentTimeMillis();
		SearchResultWrapper searchResults = searchResults(lat, lon, text, locale, baseSearch, northWest, southEast, false, null);
		List<SearchResult> res = searchResults.results();
		if (System.currentTimeMillis() - tm > 1000) {
			LOGGER.info(String.format("Search %s results %d took %.2f sec - %s", text,
					searchResults.results() == null ? 0 : searchResults.results().size(),
					(System.currentTimeMillis() - tm) / 1000.0, searchResults.stat));
		}
		List<Feature> features = new ArrayList<>();
		if (res != null && !res.isEmpty()) {
			saveSearchResult(res, features);
		}

		return !features.isEmpty() ? features : Collections.emptyList();
	}

	public record SearchResultWrapper(List<SearchResult> results, BinaryMapIndexReaderStats.SearchStat stat) {}

    public SearchResultWrapper searchResults(double lat, double lon, String text, String locale, boolean baseSearch,
                                             String northWest, String southEast,
                                             boolean unlimited, Consumer<List<SearchResult>> consumerInContext) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return new SearchResultWrapper(Collections.emptyList(), null);
        }
        SearchUICore searchUICore = new SearchUICore(getMapPoiTypes(locale), locale, false);
        if (!unlimited) {
            searchUICore.setTotalLimit(TOTAL_LIMIT_SEARCH_RESULTS);
        }
        searchUICore.getSearchSettings().setRegions(osmandRegions);

        QuadRect points = osmAndMapsService.points(null, new LatLon(lat + SEARCH_RADIUS_DEGREE, lon - SEARCH_RADIUS_DEGREE),
                new LatLon(lat - SEARCH_RADIUS_DEGREE, lon + SEARCH_RADIUS_DEGREE));
        List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
        try {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> list = getMapsForSearch(points, baseSearch);
            if (list.isEmpty()) {
                return new SearchResultWrapper(Collections.emptyList(), null);
            }
            usedMapList = osmAndMapsService.getReaders(list,null);
            if (usedMapList.isEmpty()) {
                return new SearchResultWrapper(Collections.emptyList(), null);
            }
            SearchSettings settings = searchUICore.getPhrase().getSettings();
	        BinaryMapIndexReaderStats.SearchStat stat = new BinaryMapIndexReaderStats.SearchStat();
	        settings.setStat(stat);

	        settings.setOfflineIndexes(usedMapList);
            settings.setRadiusLevel(SEARCH_RADIUS_LEVEL);
            searchUICore.updateSettings(settings);
            
            searchUICore.init();
            searchUICore.registerAPI(new SearchCoreFactory.SearchRegionByNameAPI());
            
            SearchUICore.SearchResultCollection resultCollection = searchUICore.immediateSearch(text + DELIMITER, new LatLon(lat, lon));
            resultCollection = addPoiCategoriesToSearchResult(resultCollection, text, locale, searchUICore);

	        List<SearchResult> res = resultCollection != null ? resultCollection.getCurrentSearchResults() : Collections.emptyList();
	        res = filterBrandsOutsideBBox(res, northWest, southEast, locale, lat, lon, baseSearch);
	        res = res.size() > TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB ? res.subList(0, TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB) : res;
			if (consumerInContext != null) {
				consumerInContext.accept(res);
			}
			return new SearchResultWrapper(res, stat);
        } finally {
            osmAndMapsService.unlockReaders(usedMapList);
        }
    }

    public Feature getPoi(String type, String name, LatLon loc, Long osmId) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return null;
        }
        QuadRect searchBbox = osmAndMapsService.points(null, new LatLon(loc.getLatitude() + SEARCH_POI_RADIUS_DEGREE, loc.getLongitude() - SEARCH_POI_RADIUS_DEGREE),
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
                feature = getPoiFeatureByName(rc, name);
            } else if (osmId != null) {
                feature = getPoiFeatureByOsmId(rc, osmId);
            }

        } finally {
            osmAndMapsService.unlockReaders(readers);
        }
        return feature;
    }

    private Feature getPoiFeatureByName(SearchUICore.SearchResultCollection rc, String name) {
        for (SearchResult r : rc.getCurrentSearchResults()) {
            if (r.objectType != ObjectType.POI || !(r.object instanceof Amenity a)) {
                continue;
            }
            if (matchesName(a, name)) {
                Feature f = getPoiFeature(r);
                if (f != null) {
                    return f;
                }
            }
        }
        return null;
    }

    private Feature getPoiFeatureByOsmId(SearchUICore.SearchResultCollection rc, long osmId) {
        for (SearchResult r : rc.getCurrentSearchResults()) {
            if (r.objectType != ObjectType.POI || !(r.object instanceof Amenity a)) {
                continue;
            }
            if (ObfConstants.getOsmObjectId(a) == osmId) {
                Feature f = getPoiFeature(r);
                if (f != null) {
                    return f;
                }
            }
        }
        return null;
    }

    public Feature getPoiResultByShareLink(String type, LatLon loc, String name, Long osmId, Long wikidataId) throws IOException {
        Feature poiFeature = getPoi(type, name, loc, osmId);

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
    
    public PoiSearchResult searchPoi(SearchService.PoiSearchData data, String locale, LatLon loc, boolean baseSearch) throws IOException {
        if (data.savedBbox != null && isContainsBbox(data) && data.prevCategoriesCount == data.categories.size()) {
            return new PoiSearchResult(false, false, true, null);
        }
        
        MapPoiTypes mapPoiTypes = getMapPoiTypes(locale);
        
        SearchUICore searchUICore = new SearchUICore(mapPoiTypes, locale, false);
        SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(searchUICore.getPoiTypes());
        SearchCoreFactory.SearchAmenityByTypeAPI searchAmenityByTypesAPI = new SearchCoreFactory.SearchAmenityByTypeAPI(searchUICore.getPoiTypes(), searchAmenityTypesAPI);
        searchUICore.registerAPI(searchAmenityByTypesAPI);
        
        List<Feature> features = new ArrayList<>();
        PoiSearchLimit poiSearchLimit = new PoiSearchLimit(TOTAL_LIMIT_POI / data.categories.size(), 0, false);
        QuadRect searchBbox = getSearchBbox(data.bbox);
        List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
        try {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = getMapsForSearch(data.bbox, searchBbox,baseSearch);
            if (mapList.isEmpty()) {
                return new PoiSearchResult(false, true, false, null);
            }
            
            usedMapList = osmAndMapsService.getReaders(mapList, null);
            
            SearchSettings settings = searchUICore.getSearchSettings().setSearchTypes(ObjectType.POI);
            settings = settings.setOriginalLocation(loc);
            settings.setRegions(osmandRegions);
            settings.setOfflineIndexes(usedMapList);
            searchUICore.updateSettings(settings.setSearchBBox31(searchBbox));
            List<BinaryMapPoiReaderAdapter.PoiSubType> brands = new ArrayList<>();
            for (BinaryMapIndexReader map : usedMapList) {
                brands.addAll(map.getTopIndexSubTypes());
            }
            for (PoiSearchCategory categoryObj : data.categories) {
                if (categoryObj.key != null && categoryObj.mode != null && categoryObj.mode.equals("all")) {
                    searchPoiByTypeCategory(categoryObj, mapPoiTypes, searchBbox, usedMapList, features);
                } else {
                    searchPoiByNameCategory(categoryObj, data, mapPoiTypes, searchUICore, features, brands, poiSearchLimit);
                }
            }
        } finally {
            osmAndMapsService.unlockReaders(usedMapList);
        }
        if (!features.isEmpty()) {
            return new PoiSearchResult(poiSearchLimit.useLimit, false, false, new FeatureCollection(features.toArray(new Feature[0])));
        } else {
            return null;
        }
    }

    private void searchPoiByNameCategory(PoiSearchCategory categoryObj, SearchService.PoiSearchData data, MapPoiTypes mapPoiTypes, SearchUICore searchUICore, List<Feature> features,
                                         List<BinaryMapPoiReaderAdapter.PoiSubType> brands, PoiSearchLimit poiSearchLimit) throws IOException {
        String lang = categoryObj.lang;
        if (data.prevSearchRes != null && data.prevSearchCategory.equals(categoryObj.category)) {
            SearchResult prevResult = new SearchResult();
            prevResult.object = mapPoiTypes.getAnyPoiTypeByKey(data.prevSearchRes, false);
            if (prevResult.object == null) {
                // try to find in brands
                BinaryMapPoiReaderAdapter.PoiSubType selectedBrand = brands.stream()
                        .filter(brand -> brand.name.contains(":") && brand.name.split(":")[1].equalsIgnoreCase(lang))
                        .findFirst()
                        .orElseGet(() -> brands.stream()
                                .filter(brand -> !brand.name.contains(":"))
                                .findFirst()
                                .orElse(null)
                        );

                if (selectedBrand != null) {
                    prevResult.object = new TopIndexFilter(selectedBrand, mapPoiTypes, categoryObj.category);
                }
            }
            if (prevResult.object == null) {
                searchUICore.resetPhrase();
            }
            prevResult.localeName = categoryObj.category;
            prevResult.objectType = ObjectType.POI_TYPE;
            searchUICore.resetPhrase(prevResult);
        } else {
            searchUICore.resetPhrase();
        }
        int sumLimit = poiSearchLimit.limit + poiSearchLimit.leftoverLimit;
        SearchUICore.SearchResultCollection resultCollection = searchPoiByCategory(searchUICore, categoryObj.category, sumLimit);
        List<SearchResult> res = new ArrayList<>();
        if (resultCollection != null) {
            res = resultCollection.getCurrentSearchResults();
            if (!res.isEmpty()) {
                if (resultCollection.getUseLimit()) {
                    poiSearchLimit.useLimit = true;
                }
                saveSearchResult(res, features);
            }
        }
        poiSearchLimit.leftoverLimit = poiSearchLimit.limit - res.size();
    }
    
    private void searchPoiByTypeCategory(PoiSearchCategory categoryObj, MapPoiTypes mapPoiTypes, QuadRect searchBbox,
                                         List<BinaryMapIndexReader> readers, List<Feature> features) throws IOException {
        AbstractPoiType poiType = mapPoiTypes.getAnyPoiTypeByKey(categoryObj.key, false);
        if (poiType == null || searchBbox == null) {
            return;
        }
        
        SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(mapPoiTypes);
        SearchCoreFactory.SearchAmenityByTypeAPI searchAmenityByTypesAPI = new SearchCoreFactory.SearchAmenityByTypeAPI(mapPoiTypes, searchAmenityTypesAPI);
        SearchPoiTypeFilter filter = searchAmenityByTypesAPI.getPoiTypeFilter(poiType, new LinkedHashSet<>());
        if (filter == null || filter.isEmpty()) {
            return;
        }
        
        int left31 = (int) searchBbox.left;
        int right31 = (int) searchBbox.right;
        int top31 = (int) searchBbox.top;
        int bottom31 = (int) searchBbox.bottom;
        
        for (BinaryMapIndexReader reader : readers) {
            BinaryMapIndexReader.SearchRequest<Amenity> request = BinaryMapIndexReader.buildSearchPoiRequest(
                    left31, right31, top31, bottom31, ZOOM_TO_SEARCH_POI, filter, null);
            List<Amenity> amenities = reader.searchPoi(request);
            saveAmenityResults(amenities, features);
        }
    }
    
    public Feature searchPoiByOsmId(LatLon loc, long osmid, String type) throws IOException {
        final double SEARCH_POI_RADIUS_DEGREE = type.equals("3") ? 0.0055 : 0.0001; // 3-relation,0.0055-600m,0.0001-11m
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
                        return ObfConstants.getOsmObjectId(amenity) == osmid;
                    }
                    
                    @Override
                    public boolean isCancelled() {
                        return false;
                    }
                });

        SearchResult res = searchPoiByReq(req, p1, p2, false);
        if (res != null) {
            return getPoiFeature(res);
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
            return getPoiFeature(res);
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
    
    private void saveSearchResult(List<SearchResult> res, List<Feature> features) {
        for (SearchResult result : res) {
            features.add(getFeature(result));
        }
    }

    private void saveAmenityResults(List<Amenity> amenities, List<Feature> features) {
        for (Amenity amenity : amenities) {
            SearchResult result = new SearchResult();
            result.object = amenity;
            result.objectType = ObjectType.POI;
            result.location = amenity.getLocation();
            features.add(getPoiFeature(result));
        }
    }

	public Feature getFeature(SearchResult result) {
		Feature feature;
		if (result.objectType == ObjectType.POI) {
			feature = getPoiFeature(result);
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
    
	private Feature getPoiFeature(SearchResult result) {
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
    
}