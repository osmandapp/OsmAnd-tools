package net.osmand.server.api.services;

import net.osmand.NativeLibrary;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.*;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.osm.PoiFilter;
import net.osmand.osm.PoiType;
import net.osmand.osm.edit.Entity;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.server.controllers.pub.RoutingController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

import static net.osmand.data.Amenity.*;
import static net.osmand.data.City.CityType.getAllCityTypeStrings;
import static net.osmand.data.MapObject.AMENITY_ID_RIGHT_SHIFT;
import static net.osmand.router.RouteResultPreparation.SHIFT_ID;

@Service
public class SearchService {
    
    @Autowired
    OsmAndMapsService osmAndMapsService;
    
    OsmandRegions osmandRegions;
    
    private static final int SEARCH_RADIUS_LEVEL = 1;
    private static final double SEARCH_RADIUS_DEGREE = 1.5;
    private static final int TOTAL_LIMIT_POI = 400;
    
    private static final int MAX_NUMBER_OF_MAP_SEARCH_POI = 5;
    private static final String SEARCH_LOCALE = "en";
    
    private static final int SHIFT_MULTIPOLYGON_IDS = 43;
    private static final int SHIFT_NON_SPLIT_EXISTING_IDS = 41;
    private static final int DUPLICATE_SPLIT = 5;
    public static final long RELATION_BIT = 1L << SHIFT_MULTIPOLYGON_IDS - 1; //According IndexPoiCreator SHIFT_MULTIPOLYGON_IDS
    public static final long SPLIT_BIT = 1L << SHIFT_NON_SPLIT_EXISTING_IDS - 1; //According IndexVectorMapCreator
    
    public static class PoiSearchResult {
        
        public PoiSearchResult(boolean useLimit, boolean mapLimitExceeded, boolean alreadyFound, RoutingController.FeatureCollection features) {
            this.useLimit = useLimit;
            this.mapLimitExceeded = mapLimitExceeded;
            this.alreadyFound = alreadyFound;
            this.features = features;
        }
        
        public boolean useLimit;
        public boolean mapLimitExceeded;
        public boolean alreadyFound;
        public RoutingController.FeatureCollection features;
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
    
    public List<SearchResult> search(double lat, double lon, String text) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return Collections.emptyList();
        }
        SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), SEARCH_LOCALE, false);
        searchUICore.getSearchSettings().setRegions(osmandRegions);
        SearchUICore.SearchResultCollection res;
        QuadRect points = osmAndMapsService.points(null, new LatLon(lat + SEARCH_RADIUS_DEGREE, lon - SEARCH_RADIUS_DEGREE),
                new LatLon(lat - SEARCH_RADIUS_DEGREE, lon + SEARCH_RADIUS_DEGREE));
        List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
        try {
            List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService.getObfReaders(points, null, 0, "search");
            usedMapList = osmAndMapsService.getReaders(list, null);
            searchUICore.getSearchSettings().setOfflineIndexes(usedMapList);
            searchUICore.init();
            searchUICore.registerAPI(new SearchCoreFactory.SearchRegionByNameAPI());
            
            SearchSettings settings = searchUICore.getPhrase().getSettings();
            searchUICore.updateSettings(settings.setRadiusLevel(SEARCH_RADIUS_LEVEL));
            res = searchUICore.immediateSearch(text, new LatLon(lat, lon));
        } finally {
            osmAndMapsService.unlockReaders(usedMapList);
        }
        return res != null ? res.getCurrentSearchResults() : Collections.emptyList();
    }
    
    public PoiSearchResult searchPoi(SearchService.PoiSearchData data) throws IOException {
        if (data.savedBbox != null && isContainsBbox(data) && data.prevCategoriesCount == data.categories.size()) {
            return new PoiSearchResult(false, false, true, null);
        }
        
        List<RoutingController.Feature> features = new ArrayList<>();
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
                SearchUICore.SearchResultCollection resultCollection = searchPoiByCategory(category, searchBbox, sumLimit, usedMapList);
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
            return new PoiSearchResult(useLimit, false, false, new RoutingController.FeatureCollection(features.toArray(new RoutingController.Feature[0])));
        } else {
            return null;
        }
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
    
    public SearchUICore.SearchResultCollection searchPoiByCategory(String text, QuadRect searchBbox, int limit, List<BinaryMapIndexReader> mapList) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return null;
        }
        SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), SEARCH_LOCALE, false);
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
    
    
    public Map<String, Map<String, String>> searchPoiCategories(String search) throws IOException {
        Map<String, Map<String, String>> searchRes = new HashMap<>();
        SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), SEARCH_LOCALE, true);
        searchUICore.init();
        List<SearchResult> results = searchUICore.shallowSearch(SearchCoreFactory.SearchAmenityTypesAPI.class, search, null)
                .getCurrentSearchResults();
        results.forEach(res -> searchRes.put(res.localeName, getTags(res.object)));
        return searchRes;
    }
    
    public Map<String, List<String>> searchPoiCategories() {
        SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), SEARCH_LOCALE, false);
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
    
    private Map<String, String> getTags(Object obj) {
        final String KEY_NAME = "keyName";
        final String OSM_TAG = "osmTag";
        final String OSM_VALUE = "osmValue";
        final String ICON_NAME = "iconName";
        Map<String, String> tags = new HashMap<>();
        if (obj instanceof PoiType) {
            PoiType type = (PoiType) obj;
            tags.put(KEY_NAME, type.getKeyName());
            tags.put(OSM_TAG, type.getOsmTag());
            tags.put(OSM_VALUE, type.getOsmValue());
            tags.put(ICON_NAME, type.getIconKeyName());
        } else if (obj instanceof PoiCategory) {
            PoiCategory type = (PoiCategory) obj;
            tags.put(KEY_NAME, type.getKeyName());
            tags.put(ICON_NAME, type.getIconKeyName());
        } else if (obj instanceof PoiFilter) {
            PoiFilter type = (PoiFilter) obj;
            tags.put(KEY_NAME, type.getKeyName());
        }
        return tags;
    }
    
    private void saveSearchResult(List<SearchResult> res, List<RoutingController.Feature> features) {
        for (SearchResult result : res) {
            if (result.objectType == ObjectType.POI) {
                Amenity amenity = (Amenity) result.object;
                PoiType poiType = amenity.getType().getPoiTypeByKeyName(amenity.getSubType());
                RoutingController.Feature feature;
                if (poiType != null) {
                    feature = new RoutingController.Feature(RoutingController.Geometry.point(amenity.getLocation()))
                            .prop("poi_name", amenity.getName())
                            .prop("poi_color", amenity.getColor())
                            .prop("poi_iconKeyName", poiType.getIconKeyName())
                            .prop("poi_typeOsmTag", poiType.getOsmTag())
                            .prop("poi_typeOsmValue", poiType.getOsmValue())
                            .prop("poi_iconName", getIconName(poiType))
                            .prop("poi_type", amenity.getType().getKeyName())
                            .prop("poi_subType", amenity.getSubType())
                            .prop("poi_osmUrl", getOsmUrl(result));
                    Map<String, String> tags = amenity.getAmenityExtensions();
                    for (Map.Entry<String, String> entry : tags.entrySet()) {
                        feature.prop(entry.getKey(), entry.getValue());
                    }
                    features.add(feature);
                }
            }
        }
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
