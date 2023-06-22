package net.osmand.server.api.services;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.osm.PoiFilter;
import net.osmand.osm.PoiType;
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

@Service
public class SearchService {
    
    @Autowired
    OsmAndMapsService osmAndMapsService;
    
    OsmandRegions osmandRegions;
    
    private static final int SEARCH_RADIUS_LEVEL = 1;
    private static final double SEARCH_RADIUS_DEGREE = 1.5;
    private static final int TOTAL_LIMIT_POI = 400;
    
    private static final int MAX_NUMBER_OF_MAP_SEARCH_POI = 4;
    private static final int MIN_ZOOM_FOR_DETAILED_BBOX_SEARCH_POI = 14;
    private static final String SEARCH_LOCALE = "en";
    
    public static class PoiSearchResult {
        public PoiSearchResult(boolean useLimit, RoutingController.FeatureCollection features) {
            this.useLimit = useLimit;
            this.features = features;
        }
        
        public boolean useLimit;
        public RoutingController.FeatureCollection features;
    }
    
    public synchronized List<SearchResult> search(double lat, double lon, String text) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return Collections.emptyList();
        }
        SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), SEARCH_LOCALE, false);
        searchUICore.getSearchSettings().setRegions(osmandRegions);
        QuadRect points = osmAndMapsService.points(null, new LatLon(lat + SEARCH_RADIUS_DEGREE, lon - SEARCH_RADIUS_DEGREE),
                new LatLon(lat - SEARCH_RADIUS_DEGREE, lon + SEARCH_RADIUS_DEGREE));
        List<BinaryMapIndexReader> list = Arrays.asList(osmAndMapsService.getObfReaders(points, null));
        searchUICore.getSearchSettings().setOfflineIndexes(list);
        searchUICore.init();
        searchUICore.registerAPI(new SearchCoreFactory.SearchRegionByNameAPI());
        
        SearchSettings settings = searchUICore.getPhrase().getSettings();
        searchUICore.updateSettings(settings.setRadiusLevel(SEARCH_RADIUS_LEVEL));
        SearchUICore.SearchResultCollection r = searchUICore.immediateSearch(text, new LatLon(lat, lon));
        
        return r.getCurrentSearchResults();
    }
    
    public synchronized PoiSearchResult searchPoi(double lat, double lon, List<String> categories, Map<String, Object> data, int zoom) throws IOException {
        List<RoutingController.Feature> features = new ArrayList<>();
        int leftoverLimit = 0;
        int limit = TOTAL_LIMIT_POI / categories.size();
        boolean useLimit = false;
        for (String category : categories) {
            int sumLimit = limit + leftoverLimit;
            List<SearchResult> res = searchPoiByCategory(lat, lon, category, data, limit + leftoverLimit, zoom);
            if (!res.isEmpty()) {
                if (res.size() == sumLimit) {
                    useLimit = true;
                }
                leftoverLimit = limit - res.size();
                prepareSearchResult(res, features);
            }
        }
        if (!features.isEmpty()) {
            return new PoiSearchResult(useLimit, new RoutingController.FeatureCollection(features.toArray(new RoutingController.Feature[0])));
        } else {
            return null;
        }
    }
    
    public synchronized List<SearchResult> searchPoiByCategory(double lat, double lon, String text, Map<String, Object> data, int limit, int zoom) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return List.of();
        }
        QuadRect searchBbox = getSearchBbox(data, zoom);
        if (searchBbox != null) {
            SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), SEARCH_LOCALE, false);
            searchUICore.getSearchSettings().setRegions(osmandRegions);
            List<BinaryMapIndexReader> list = Arrays.asList(osmAndMapsService.getObfReaders(searchBbox, data));
            if (list.size() < MAX_NUMBER_OF_MAP_SEARCH_POI) {
                searchUICore.getSearchSettings().setOfflineIndexes(list);
                searchUICore.init();
                searchUICore.registerAPI(new SearchCoreFactory.SearchRegionByNameAPI());
                SearchSettings settings = searchUICore.getPhrase().getSettings();
                if (zoom >= MIN_ZOOM_FOR_DETAILED_BBOX_SEARCH_POI) {
                    searchUICore.updateSettings(settings.setSearchBBox31(searchBbox));
                    searchUICore.setTotalLimit(limit);
                    SearchUICore.SearchResultCollection r = searchUICore.immediateSearch(text, new LatLon(lat, lon));
                    return r.getCurrentSearchResults();
                }
            }
        }
        return List.of();
    }
    
    private void prepareSearchResult(List<SearchResult> res, List<RoutingController.Feature> features) {
        for (SearchResult result : res) {
            if (result.objectType == ObjectType.POI) {
                Amenity amenity = (Amenity) result.object;
                PoiType poiType = amenity.getType().getPoiTypeByKeyName(amenity.getSubType());
                RoutingController.Feature feature;
                if (poiType != null) {
                    feature = new RoutingController.Feature(RoutingController.Geometry.point(amenity.getLocation()))
                            .prop("name", amenity.getName())
                            .prop("color", amenity.getColor())
                            .prop("iconKeyName", poiType.getIconKeyName())
                            .prop("typeOsmTag", poiType.getOsmTag())
                            .prop("typeOsmValue", poiType.getOsmValue())
                            .prop("iconName", getIconName(poiType))
                            .prop("type", amenity.getType().getKeyName())
                            .prop("subType", amenity.getSubType());
                    
                    for (String e : amenity.getAdditionalInfoKeys()) {
                        feature.prop(e, amenity.getAdditionalInfo(e));
                    }
                    features.add(feature);
                }
            }
        }
    }
    
    public QuadRect getSearchBbox(Map<String, Object> data, int zoom) {
        if (zoom >= MIN_ZOOM_FOR_DETAILED_BBOX_SEARCH_POI) {
            LatLon point1 = new LatLon((double) data.get("latBboxPoint1"), (double) data.get("lngBboxPoint1"));
            LatLon point2 = new LatLon((double) data.get("latBboxPoint2"), (double) data.get("lngBboxPoint2"));
            return osmAndMapsService.points(null, point1, point2);
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
}
