package net.osmand.server.api.services;

import net.osmand.NativeLibrary;
import net.osmand.ResultMatcher;
import net.osmand.binary.*;
import net.osmand.data.*;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.*;
import net.osmand.osm.edit.Entity;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.*;
import net.osmand.server.utils.MapPoiTypesTranslator;
import net.osmand.util.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static net.osmand.data.City.CityType.getAllCityTypeStrings;
import static net.osmand.data.MapObject.AMENITY_ID_RIGHT_SHIFT;
import static net.osmand.data.MapObject.unzipContent;
import static net.osmand.router.RouteResultPreparation.SHIFT_ID;
import static net.osmand.server.controllers.pub.GeojsonClasses.*;
@Service
public class SearchService {
    
    @Autowired
    OsmAndMapsService osmAndMapsService;
    
    OsmandRegions osmandRegions;
    
    private ConcurrentHashMap<String, Map<String, String>> translationsCache;
    
    private static final int SEARCH_RADIUS_LEVEL = 1;
    private static final double SEARCH_RADIUS_DEGREE = 1.5;
    private static final int TOTAL_LIMIT_POI = 2000;
    private static final int TOTAL_LIMIT_SEARCH_RESULTS = 10000;
    private static final int TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB = 1000;
    private static final double SEARCH_POI_RADIUS_DEGREE = 0.0007;

    private static final String SEARCH_LOCALE = "en";
    private static final String AND_RES = "/androidResources/";
    
    private static final int DUPLICATE_SPLIT = 5;
    public static final long RELATION_BIT = 1L << ObfConstants.SHIFT_MULTIPOLYGON_IDS - 1; //According IndexPoiCreator SHIFT_MULTIPOLYGON_IDS
    public static final long SPLIT_BIT = 1L << ObfConstants.SHIFT_NON_SPLIT_EXISTING_IDS - 1; //According IndexVectorMapCreator
    
    private static final String DELIMITER = " ";

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
    
    public static class PoiSearchData {
        
        public PoiSearchData(List<String> categories,
                             Map<String, String> categoriesLang,
                             String northWest,
                             String southEast,
                             String savedNorthWest,
                             String savedSouthEast,
                             int prevCategoriesCount,
                             String prevSearchRes,
                             String prevSearchCategory) {
            this.categories = categories;
            this.categoriesLang = categoriesLang;
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
        
        public List<String> categories;
        public Map<String, String> categoriesLang;
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
    
    public List<LatLon> getBboxCoords(List<String> coords) {
        List<LatLon> bbox = new ArrayList<>();
        for (String coord : coords) {
            String[] lanLonArr = coord.split(",");
            bbox.add(new LatLon(Double.parseDouble(lanLonArr[0]), Double.parseDouble(lanLonArr[1])));
        }
        return bbox;
    }

	public List<Feature> search(double lat, double lon, String text, String locale, boolean baseSearch, String northWest, String southEast) throws IOException {
		List<SearchResult> res = searchResults(lat, lon, text, locale, baseSearch, northWest, southEast);

		List<Feature> features = new ArrayList<>();
		if (res != null && !res.isEmpty()) {
			saveSearchResult(res, features);
		}

		return !features.isEmpty() ? features : Collections.emptyList();
	}

    public List<SearchResult> searchResults(double lat, double lon, String text, String locale, boolean baseSearch, String northWest, String southEast) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return Collections.emptyList();
        }
        SearchUICore searchUICore = new SearchUICore(getMapPoiTypes(locale), locale, false);
        searchUICore.setTotalLimit(TOTAL_LIMIT_SEARCH_RESULTS);
        searchUICore.getSearchSettings().setRegions(osmandRegions);
        
        QuadRect points = osmAndMapsService.points(null, new LatLon(lat + SEARCH_RADIUS_DEGREE, lon - SEARCH_RADIUS_DEGREE),
                new LatLon(lat - SEARCH_RADIUS_DEGREE, lon + SEARCH_RADIUS_DEGREE));
        List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
        try {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> list = getMapsForSearch(points, baseSearch);
            if (list.isEmpty()) {
                return Collections.emptyList();
            }
            usedMapList = osmAndMapsService.getReaders(list,null);
            if (usedMapList.isEmpty()) {
                return Collections.emptyList();
            }
            SearchSettings settings = searchUICore.getPhrase().getSettings();
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
			return res;
        } finally {
            osmAndMapsService.unlockReaders(usedMapList);
        }
    }

    public Feature getPoi(String name, String type, LatLon loc) throws IOException {
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
                    readers, searchBbox, SEARCH_LOCALE, loc.getLatitude(), loc.getLongitude());

            // Find POIs by type
            SearchUICore.SearchResultCollection rc =
                    searchPoiByCategory(searchUICore, type, TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB);
            if (rc == null) {
                return null;
            }

            // Filter by name
            for (SearchResult r : rc.getCurrentSearchResults()) {
                if (r.objectType != ObjectType.POI || !(r.object instanceof Amenity a)) {
                    continue;
                }
                if (!matchesName(a, name)) {
                    continue;
                }
                Feature f = getPoiFeature(r);
                if (f != null) {
                    feature = f;
                    break;
                }
            }
        } finally {
            osmAndMapsService.unlockReaders(readers);
        }
        return feature;
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
        int leftoverLimit = 0;
        int limit = TOTAL_LIMIT_POI / data.categories.size();
        boolean useLimit = false;
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
            for (String category : data.categories) {
                String lang = data.categoriesLang != null ? data.categoriesLang.get(category) : null;
	            if (data.prevSearchRes != null && data.prevSearchCategory.equals(category)) {
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
                            prevResult.object = new TopIndexFilter(selectedBrand, mapPoiTypes, category);
                        }
                    }
                    if (prevResult.object == null) {
                        searchUICore.resetPhrase();
                    }
                    prevResult.localeName = category;
                    prevResult.objectType = ObjectType.POI_TYPE;
                    searchUICore.resetPhrase(prevResult);
                } else {
                    searchUICore.resetPhrase();
                }
                int sumLimit = limit + leftoverLimit;
                SearchUICore.SearchResultCollection resultCollection = searchPoiByCategory(searchUICore, category, sumLimit);
                List<SearchResult> res = new ArrayList<>();
                if (resultCollection != null) {
                    res = resultCollection.getCurrentSearchResults();
                    if (!res.isEmpty()) {
                        if (resultCollection.getUseLimit()) {
                            useLimit = true;
                        }
                        saveSearchResult(res, features);
                    }
                }
                leftoverLimit = limit - res.size();
            }
        } finally {
            osmAndMapsService.unlockReaders(usedMapList);
        }
        if (!features.isEmpty()) {
            return new PoiSearchResult(useLimit, false, false, new FeatureCollection(features.toArray(new Feature[0])));
        } else {
            return null;
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
                        return getOsmObjectId(amenity) == osmid;
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
                            List<Amenity> poiRes = map.searchPoi(p, req);
                            if (!poiRes.isEmpty()) {
                                res = new SearchResult();
                                res.object = poiRes.get(0);
                                break;
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
                List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService.getObfReaders(searchBbox, bbox, 0, "search");
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
            List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService.getObfReaders(points, null, 0, "search");
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
    
    public SearchUICore.SearchResultCollection searchCitiesByBbox(QuadRect searchBbox, List<BinaryMapIndexReader> mapList) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return null;
        }
        SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), SEARCH_LOCALE, false);
        MapPoiTypes mapPoiTypes = searchUICore.getPoiTypes();
        SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(mapPoiTypes);
        searchUICore.registerAPI(new SearchCoreFactory.SearchAmenityByTypeAPI(mapPoiTypes, searchAmenityTypesAPI));
        
        SearchSettings settings = searchUICore.getPhrase().getSettings();
        settings.setRegions(osmandRegions);
        settings.setOfflineIndexes(mapList);
        
        Set<String> types = getAllCityTypeStrings();
        SearchUICore.SearchResultCollection res = searchWithBbox(searchUICore, settings, searchBbox, types);
        
        int attempts = 0;
        while ((res == null || res.getCurrentSearchResults().isEmpty()) && attempts < 10) {
            searchBbox = doubleBboxSize(searchBbox);
            res = searchWithBbox(searchUICore, settings, searchBbox, types);
            attempts++;
        }
        
        return res;
    }
    
    private SearchUICore.SearchResultCollection searchWithBbox(SearchUICore searchUICore, SearchSettings settings, QuadRect searchBbox, Set<String> types) {
        searchUICore.updateSettings(settings.setSearchBBox31(searchBbox));
        SearchUICore.SearchResultCollection res = null;
        for (String type : types) {
            type = type.replace("_", " ");
            if (res == null) {
                res = searchUICore.immediateSearch(type, null);
            } else {
                SearchUICore.SearchResultCollection searchResults = searchUICore.immediateSearch(type, null);
                res.addSearchResults(searchResults.getCurrentSearchResults(), false, true);
            }
        }
        return res;
    }
    
    private QuadRect doubleBboxSize(QuadRect bbox) {
        double centerX = (bbox.left + bbox.right) / 2;
        double centerY = (bbox.top + bbox.bottom) / 2;
        double width = bbox.right - bbox.left;
        double height = bbox.bottom - bbox.top;
        
        double newWidth = width * 2;
        double newHeight = height * 2;
        
        return new QuadRect(
                centerX - newWidth / 2,
                centerY + newHeight / 2,
                centerX + newWidth / 2,
                centerY - newHeight / 2
        );
    }
    
    public QuadRect getSearchBbox(List<LatLon> bbox) {
        if (bbox.size() == 2) {
            return osmAndMapsService.points(null, bbox.get(0), bbox.get(1));
        }
        return null;
    }
    
    public List<String> getTopFilters() {
        List<String> filters = new ArrayList<>();
        SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), SEARCH_LOCALE, true);
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
        locale = locale == null ? SEARCH_LOCALE : locale;

        return poiTypesByLocale.computeIfAbsent(locale, loc -> {
            MapPoiTypes mapPoiTypes = new MapPoiTypes(null);
            mapPoiTypes.init();
            Map<String, String> translations = getTranslations(loc);
            Map<String, String> enTranslations = getTranslations(SEARCH_LOCALE);
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
        PoiType poiType = amenity.getType().getPoiTypeByKeyName(amenity.getSubType());
        Feature feature = null;
        if (poiType != null) {
            feature = new Feature(Geometry.point(amenity.getLocation()))
                    .prop(PoiTypeField.TYPE.getFieldName(), result.objectType)
                    .prop(PoiTypeField.POI_ID.getFieldName(), amenity.getId())
                    .prop(PoiTypeField.POI_NAME.getFieldName(), amenity.getName())
                    .prop(PoiTypeField.POI_COLOR.getFieldName(), amenity.getColor())
                    .prop(PoiTypeField.POI_ICON_NAME.getFieldName(), getIconName(poiType))
                    .prop(PoiTypeField.POI_TYPE.getFieldName(), amenity.getType().getKeyName())
                    .prop(PoiTypeField.POI_SUBTYPE.getFieldName(), amenity.getSubType())
                    .prop(PoiTypeField.POI_OSM_URL.getFieldName(), getOsmUrl(result));
            Map<String, String> tags = amenity.getAmenityExtensions();
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String value = unzipContent(entry.getValue());
                feature.prop(entry.getKey(), value);
            }
            Map<String, String> names = amenity.getNamesMap(true);
            for (Map.Entry<String, String> entry : names.entrySet()) {
                feature.prop(PoiTypeField.POI_NAME.getFieldName() + ":" + entry.getKey(), entry.getValue());
            }
            Map<String, String> typeTags = getPoiTypeFields(poiType);
            for (Map.Entry<String, String> entry : typeTags.entrySet()) {
                feature.prop(entry.getKey(), entry.getValue());
            }
            feature.prop(PoiTypeField.CITY.getFieldName(), result.alternateName);
        }
        return feature;
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
        Entity.EntityType type = getOsmEntityType(mapObject);
        if (type != null) {
            long osmId = getOsmObjectId(mapObject);
            return "https://www.openstreetmap.org/" + type.name().toLowerCase(Locale.US) + "/" + osmId;
        }
        return null;
    }
    
    public static long getOsmObjectId(MapObject object) {
        long originalId = -1;
        Long id = object.getId();
        if (id != null) {
            if (object instanceof NativeLibrary.RenderedObject) {
                id >>= 1;
            }
            if (isShiftedID(id)) {
                originalId = getOsmId(id);
            } else {
                int shift = object instanceof Amenity ? AMENITY_ID_RIGHT_SHIFT : SHIFT_ID;
                originalId = id >> shift;
            }
        }
        return originalId;
    }
    
    public Entity.EntityType getOsmEntityType(MapObject object) {
        if (isOsmUrlAvailable(object)) {
            Long id = object.getId();
            long originalId = id >> 1;
            long relationShift = 1L << 41;
            if (originalId > relationShift) {
                return Entity.EntityType.RELATION;
            } else {
                return id % 2 == MapObject.WAY_MODULO_REMAINDER ? Entity.EntityType.WAY : Entity.EntityType.NODE;
            }
        }
        return null;
    }
    
    public static boolean isOsmUrlAvailable(MapObject object) {
        Long id = object.getId();
        return id != null && id > 0;
    }
    
    public static long getOsmId(long id) {
        long clearBits = RELATION_BIT | SPLIT_BIT;
        id = isShiftedID(id) ? (id & ~clearBits) >> DUPLICATE_SPLIT : id;
        return id >> SHIFT_ID;
    }
    
    public static boolean isShiftedID(long id) {
        return isIdFromRelation(id) || isIdFromSplit(id);
    }
    
    public static boolean isIdFromRelation(long id) {
        return id > 0 && (id & RELATION_BIT) == RELATION_BIT;
    }
    
    public static boolean isIdFromSplit(long id) {
        return id > 0 && (id & SPLIT_BIT) == SPLIT_BIT;
    }
}