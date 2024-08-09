package net.osmand.server.api.services;

import net.osmand.NativeLibrary;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapPoiReaderAdapter;
import net.osmand.binary.GeocodingUtilities;
import net.osmand.data.*;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.*;
import net.osmand.osm.edit.Entity;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.server.utils.MapPoiTypesTranslator;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static net.osmand.data.City.CityType.getAllCityTypeStrings;
import static net.osmand.data.MapObject.AMENITY_ID_RIGHT_SHIFT;
import static net.osmand.data.MapObject.unzipContent;
import static net.osmand.router.RouteResultPreparation.SHIFT_ID;
import static net.osmand.server.controllers.pub.GeojsonClasses.*;
@Service
public class SearchService {
    
    @Autowired
    OsmAndMapsService osmAndMapsService;
    
    @Value("${osmand.android.translations.location}")
    String andTranslationsLocation;
    
    OsmandRegions osmandRegions;
    
    private static final int SEARCH_RADIUS_LEVEL = 1;
    private static final double SEARCH_RADIUS_DEGREE = 1.5;
    private static final int TOTAL_LIMIT_POI = 2000;
    
    private static final int MAX_NUMBER_OF_MAP_SEARCH_POI = 5;
    private static final String SEARCH_LOCALE = "en";
    
    private static final int SHIFT_MULTIPOLYGON_IDS = 43;
    private static final int SHIFT_NON_SPLIT_EXISTING_IDS = 41;
    private static final int DUPLICATE_SPLIT = 5;
    public static final long RELATION_BIT = 1L << SHIFT_MULTIPOLYGON_IDS - 1; //According IndexPoiCreator SHIFT_MULTIPOLYGON_IDS
    public static final long SPLIT_BIT = 1L << SHIFT_NON_SPLIT_EXISTING_IDS - 1; //According IndexVectorMapCreator
    
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
        
        public PoiSearchData(List<String> categories, String northWest, String southEast, String savedNorthWest, String savedSouthEast, int prevCategoriesCount) {
            this.categories = categories;
            this.bbox = getBboxCoords(Arrays.asList(northWest, southEast));
            if (savedNorthWest != null && savedSouthEast != null) {
                this.savedBbox = getBboxCoords(Arrays.asList(savedNorthWest, savedSouthEast));
            }
            this.prevCategoriesCount = prevCategoriesCount;
        }
        
        public List<String> categories;
        public List<LatLon> bbox;
        public List<LatLon> savedBbox;
        public int prevCategoriesCount;
        
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
    
    public List<Feature> search(double lat, double lon, String text, String locale) throws IOException, XmlPullParserException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return Collections.emptyList();
        }
        SearchUICore searchUICore = new SearchUICore(getMapPoiTypes(locale), locale, false);
        searchUICore.setTotalLimit(1000);
        searchUICore.getSearchSettings().setRegions(osmandRegions);
        
        QuadRect points = osmAndMapsService.points(null, new LatLon(lat + SEARCH_RADIUS_DEGREE, lon - SEARCH_RADIUS_DEGREE),
                new LatLon(lat - SEARCH_RADIUS_DEGREE, lon + SEARCH_RADIUS_DEGREE));
        List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
        List<Feature> features = new ArrayList<>();
        try {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService.getObfReaders(points, null, 0, "search");
            usedMapList = osmAndMapsService.getReaders(list, null);
            searchUICore.getSearchSettings().setOfflineIndexes(usedMapList);
            searchUICore.init();
            searchUICore.registerAPI(new SearchCoreFactory.SearchRegionByNameAPI());
            
            SearchSettings settings = searchUICore.getPhrase().getSettings();
            searchUICore.updateSettings(settings.setRadiusLevel(SEARCH_RADIUS_LEVEL));
            SearchUICore.SearchResultCollection resultCollection = searchUICore.immediateSearch(text, new LatLon(lat, lon));
            List<SearchResult> res;
            if (resultCollection != null) {
                res = resultCollection.getCurrentSearchResults();
                if (!res.isEmpty()) {
                    saveSearchResult(res, features);
                }
            }
        } finally {
            osmAndMapsService.unlockReaders(usedMapList);
        }
        
        if (!features.isEmpty()) {
            return features;
        } else {
            return Collections.emptyList();
        }
    }
    
    public PoiSearchResult searchPoi(SearchService.PoiSearchData data, String locale) throws IOException, XmlPullParserException {
        if (data.savedBbox != null && isContainsBbox(data) && data.prevCategoriesCount == data.categories.size()) {
            return new PoiSearchResult(false, false, true, null);
        }
        
        List<Feature> features = new ArrayList<>();
        int leftoverLimit = 0;
        int limit = TOTAL_LIMIT_POI / data.categories.size();
        boolean useLimit = false;
        QuadRect searchBbox = getSearchBbox(data.bbox);
        List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
        try {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = getMapsForSearch(data.bbox, searchBbox);
            if (mapList.isEmpty()) {
                return new PoiSearchResult(false, true, false, null);
            }
            usedMapList = osmAndMapsService.getReaders(mapList, null);
            for (String category : data.categories) {
                int sumLimit = limit + leftoverLimit;
                SearchUICore.SearchResultCollection resultCollection = searchPoiByCategory(category, searchBbox, sumLimit, usedMapList, locale);
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
        List<LatLon> bbox = Arrays.asList(p1, p2);
        QuadRect searchBbox = getSearchBbox(bbox);
        
        List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
        SearchResult res = null;
        
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
        
        try {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = getMapsForSearch(bbox, searchBbox);
            if (!mapList.isEmpty()) {
                usedMapList = osmAndMapsService.getReaders(mapList, null);
                for (BinaryMapIndexReader map : usedMapList) {
                    if (res != null) {
                        break;
                    }
                    for (BinaryIndexPart indexPart : map.getIndexes()) {
                        if (indexPart instanceof BinaryMapPoiReaderAdapter.PoiRegion) {
                            BinaryMapPoiReaderAdapter.PoiRegion p = (BinaryMapPoiReaderAdapter.PoiRegion) indexPart;
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
        if (res != null) {
            return getPoiFeature(res);
        }
        return null;
    }
    
    private boolean isContainsBbox(SearchService.PoiSearchData data) {
        QuadRect searchBbox = getSearchBbox(data.bbox);
        QuadRect oldSearchBbox = getSearchBbox(data.savedBbox);
        return oldSearchBbox.contains(searchBbox.left, searchBbox.top, searchBbox.right, searchBbox.bottom);
    }
    
    private List<OsmAndMapsService.BinaryMapIndexReaderReference> getMapsForSearch(List<LatLon> bbox, QuadRect searchBbox) throws IOException {
        if (searchBbox != null) {
            SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), SEARCH_LOCALE, false);
            searchUICore.getSearchSettings().setRegions(osmandRegions);
            List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService.getObfReaders(searchBbox, bbox, MAX_NUMBER_OF_MAP_SEARCH_POI, "search");
            if (list.size() < MAX_NUMBER_OF_MAP_SEARCH_POI) {
                return list;
            }
        }
        return List.of();
    }
    
    public SearchUICore.SearchResultCollection searchPoiByCategory(String text, QuadRect searchBbox, int limit, List<BinaryMapIndexReader> mapList, String locale) throws IOException, XmlPullParserException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return null;
        }
        SearchUICore searchUICore = new SearchUICore(getMapPoiTypes(locale), locale, false);
        MapPoiTypes mapPoiTypes = searchUICore.getPoiTypes();
        SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(mapPoiTypes);
        searchUICore.registerAPI(new SearchCoreFactory.SearchAmenityByTypeAPI(mapPoiTypes, searchAmenityTypesAPI));
        searchUICore.setTotalLimit(limit);
        
        SearchSettings settings = searchUICore.getPhrase().getSettings();
        settings.setRegions(osmandRegions);
        settings.setOfflineIndexes(mapList);
        searchUICore.updateSettings(settings.setSearchBBox31(searchBbox));
        
        return searchUICore.immediateSearch(text, null);
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
    
    
    public Map<String, Map<String, String>> searchPoiCategories(String search, String locale) throws IOException, XmlPullParserException {
        Map<String, Map<String, String>> searchRes = new HashMap<>();
        SearchUICore searchUICore = new SearchUICore(getMapPoiTypes(locale), locale, true);
        searchUICore.init();
        List<SearchResult> results = searchUICore.shallowSearch(SearchCoreFactory.SearchAmenityTypesAPI.class, search, null)
                .getCurrentSearchResults();
        results.forEach(res -> searchRes.put(res.localeName, getPoiTypeFields(res.object)));
        return searchRes;
    }
    
    public Map<String, List<String>> searchPoiCategories(String locale) throws XmlPullParserException, IOException {
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
    
    private MapPoiTypes getMapPoiTypes(String locale) throws XmlPullParserException, IOException {
        MapPoiTypes mapPoiTypes = MapPoiTypes.getDefault();
        String localPath = locale.equals("en") ? "values" : "values-" + locale;
        Map<String, String> phrases = Algorithms.parseStringsXml(new File(andTranslationsLocation + localPath + "/phrases.xml"));
        Map<String, String> enPhrases = Algorithms.parseStringsXml(new File(andTranslationsLocation + "values/phrases.xml"));
        mapPoiTypes.setPoiTranslator(new MapPoiTypesTranslator(phrases, enPhrases));
        
        return mapPoiTypes;
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
        final String KEY_NAME = "web_keyName";
        final String OSM_TAG = "web_typeOsmTag";
        final String OSM_VALUE = "web_typeOsmValue";
        final String ICON_NAME = "web_iconKeyName";
        final String CATEGORY_ICON = "web_categoryIcon";
        final String CATEGORY_KEY_NAME = "web_categoryKeyName";
        Map<String, String> tags = new HashMap<>();
        if (obj instanceof PoiType type) {
            tags.put(KEY_NAME, type.getKeyName());
            tags.put(OSM_TAG, type.getOsmTag());
            tags.put(OSM_VALUE, type.getOsmValue());
            tags.put(ICON_NAME, type.getIconKeyName());
            PoiCategory category = type.getCategory();
            if (category != null) {
                tags.put(CATEGORY_ICON, category.getIconKeyName());
                tags.put(CATEGORY_KEY_NAME, category.getKeyName());
            }
        } else if (obj instanceof PoiCategory type) {
            tags.put(KEY_NAME, type.getKeyName());
            tags.put(ICON_NAME, type.getIconKeyName());
        } else if (obj instanceof PoiFilter type) {
            tags.put(KEY_NAME, type.getKeyName());
            PoiCategory category = type.getPoiCategory();
            if (category != null) {
                tags.put(CATEGORY_ICON, category.getIconKeyName());
                tags.put(CATEGORY_KEY_NAME, category.getKeyName());
            }
        }
        return tags;
    }
    
    private void saveSearchResult(List<SearchResult> res, List<Feature> features) {
        for (SearchResult result : res) {
            Feature feature;
            if (result.objectType == ObjectType.POI) {
                feature = getPoiFeature(result);
            } else {
                Geometry geometry = Geometry.point(result.location != null ? result.location : new LatLon(0, 0));
                feature = new Feature(geometry)
                        .prop("web_type", result.objectType)
                        .prop("web_name", result.localeName);
                Map<String, String> tags = getPoiTypeFields(result.object);
                for (Map.Entry<String, String> entry : tags.entrySet()) {
                    feature.prop(entry.getKey(), entry.getValue());
                }
            }
            features.add(feature);
        }
    }
    
    private Feature getPoiFeature(SearchResult result) {
        Amenity amenity = (Amenity) result.object;
        PoiType poiType = amenity.getType().getPoiTypeByKeyName(amenity.getSubType());
        Feature feature = null;
        if (poiType != null) {
            feature = new Feature(Geometry.point(amenity.getLocation()))
                    .prop("web_type", result.objectType)
                    .prop("web_poi_id", amenity.getId())
                    .prop("web_poi_name", amenity.getName())
                    .prop("web_poi_color", amenity.getColor())
                    .prop("web_poi_iconName", getIconName(poiType))
                    .prop("web_poi_type", amenity.getType().getKeyName())
                    .prop("web_poi_subType", amenity.getSubType())
                    .prop("web_poi_osmUrl", getOsmUrl(result));
            Map<String, String> tags = amenity.getAmenityExtensions();
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String value = unzipContent(entry.getValue());
                feature.prop(entry.getKey(), value);
            }
            Map<String, String> typeTags = getPoiTypeFields(poiType);
            for (Map.Entry<String, String> entry : typeTags.entrySet()) {
                feature.prop(entry.getKey(), entry.getValue());
            }
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

