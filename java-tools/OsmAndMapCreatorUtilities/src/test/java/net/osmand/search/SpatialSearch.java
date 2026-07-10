package net.osmand.search;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.LatLon;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.spatial.SpatialPoiSearch;
import net.osmand.search.core.spatial.SpatialSearchContext;
import net.osmand.search.core.spatial.SpatialSearchResult;
import net.osmand.search.core.spatial.SpatialTextSearch;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class SpatialSearch implements SearchEngine {
    private final SpatialTextSearch spatialSearch;
    private final SpatialSearchContext searchContext;
    
    public SpatialSearch(JSONObject settingsJson, List<BinaryMapIndexReader> readers) {
        spatialSearch = new SpatialTextSearch();
        SpatialPoiSearch poiSearch = new SpatialPoiSearch(MapPoiTypes.getDefault());
        SpatialTextSearch.SpatialTextSearchSettings spatialSettings = parseSpatialSettings(settingsJson);
        LatLon location = parseLocation(settingsJson);
        searchContext = new SpatialSearchContext(spatialSettings, readers, poiSearch, location);
    }
    
    @Override
    public List<String> apply(String phrase, List<String> expectedResults) throws IOException {
        SpatialTextSearch.SpatialSearchResults searchResults = spatialSearch.searchAPI(phrase, searchContext);
        List<SpatialSearchResult> mainResults = searchResults.mainResults == null ? Collections.emptyList()
                : searchResults.mainResults;
        
        List<String> result = new ArrayList<>();
        for(SpatialSearchResult res : mainResults) {
            if (res.getObjects().isEmpty()) {
                result.add("");
            } else {
                result.add(res.getObjects().get(0).toString());
            }
        }
        return result;
    }

    @Override
    public void close() {
        
    }

    private LatLon parseLocation(JSONObject settingsJson) {
        JSONObject locationJson = settingsJson.optJSONObject("location");
        if (locationJson != null) {
            return new LatLon(locationJson.getDouble("lat"), locationJson.getDouble("lon"));
        }
        if (settingsJson.has("lat") && settingsJson.has("lon")) {
            return new LatLon(settingsJson.getDouble("lat"), settingsJson.getDouble("lon"));
        }
        return null;
    }

    private SpatialTextSearch.SpatialTextSearchSettings parseSpatialSettings(JSONObject settingsJson) {
        SpatialTextSearch.SpatialTextSearchSettings settings = new SpatialTextSearch.SpatialTextSearchSettings();
        settings.SEARCH_ADDR = settingsJson.optBoolean("SEARCH_ADDR", settings.SEARCH_ADDR);
        settings.SEARCH_POI = settingsJson.optBoolean("SEARCH_POI", settings.SEARCH_POI);
        settings.SEARCH_BUILDINGS = settingsJson.optBoolean("SEARCH_BUILDINGS", settings.SEARCH_BUILDINGS);
        settings.SEARCH_STREET_INTERSECTIONS = settingsJson.optBoolean("SEARCH_STREET_INTERSECTIONS", settings.SEARCH_STREET_INTERSECTIONS);
        settings.SEARCH_POI_INTERSECTIONS = settingsJson.optBoolean("SEARCH_POI_INTERSECTIONS", settings.SEARCH_POI_INTERSECTIONS);
        settings.SEARCH_POI_CATEGORIES = settingsJson.optBoolean("SEARCH_POI_CATEGORIES", settings.SEARCH_POI_CATEGORIES);
        settings.ALLOW_VIRTUAL_STREET_INTERSECTIONS = settingsJson.optBoolean("ALLOW_VIRTUAL_STREET_INTERSECTIONS",
                settings.ALLOW_VIRTUAL_STREET_INTERSECTIONS);
        settings.OPTIM_DELETE_EMBEDDED_BOUNDARIES = settingsJson.optBoolean("OPTIM_DELETE_EMBEDDED_BOUNDARIES",
                settings.OPTIM_DELETE_EMBEDDED_BOUNDARIES);
        settings.OPTIM_FLAG_POI_SAME_AS_CITY_STREET = settingsJson.optBoolean("OPTIM_FLAG_POI_SAME_AS_CITY_STREET",
                settings.OPTIM_FLAG_POI_SAME_AS_CITY_STREET);
        settings.OPTIM_DELETE_POI_SAME_AS_CITY_STREET = settingsJson.optBoolean("OPTIM_DELETE_POI_SAME_AS_CITY_STREET",
                settings.OPTIM_DELETE_POI_SAME_AS_CITY_STREET);
        settings.DEDUPLICATE_RES = settingsJson.optBoolean("DEDUPLICATE_RES", settings.DEDUPLICATE_RES);
        settings.TEST_ALLOW_HOUSE_POI_TYPE_INTERSECTION = settingsJson.optBoolean("TEST_ALLOW_HOUSE_POI_TYPE_INTERSECTION",
                settings.TEST_ALLOW_HOUSE_POI_TYPE_INTERSECTION);
        settings.ALWAYS_READ_COMMON_WORDS_ATOMS = settingsJson.optBoolean("ALWAYS_READ_COMMON_WORDS_ATOMS",
                settings.ALWAYS_READ_COMMON_WORDS_ATOMS);
        settings.ALWAYS_READ_FREQ_WORDS_ATOMS = settingsJson.optBoolean("ALWAYS_READ_FREQ_WORDS_ATOMS",
                settings.ALWAYS_READ_FREQ_WORDS_ATOMS);
        settings.LANG_DEDUPLICATE = settingsJson.optString("LANG_DEDUPLICATE", settings.LANG_DEDUPLICATE);
        settings.MIN_ELO_RATING = settingsJson.optInt("MIN_ELO_RATING", settings.MIN_ELO_RATING);
        settings.MIN_CHARACTERS_INCOMPLETE = settingsJson.optInt("MIN_CHARACTERS_INCOMPLETE", settings.MIN_CHARACTERS_INCOMPLETE);
        settings.LIMIT_ATOMIC_OBJECTS = settingsJson.optInt("LIMIT_ATOMIC_OBJECTS", settings.LIMIT_ATOMIC_OBJECTS);
        settings.LIMIT_ALL_GOALS_MAX_UNIQUE_OBJECTS = settingsJson.optInt("LIMIT_ALL_GOALS_MAX_UNIQUE_OBJECTS",
                settings.LIMIT_ALL_GOALS_MAX_UNIQUE_OBJECTS);
        settings.LIMIT_GOAL_NEXT_LEVEL_MAX_UNIQUE_OBJECTS = settingsJson.optInt("LIMIT_GOAL_NEXT_LEVEL_MAX_UNIQUE_OBJECTS",
                settings.LIMIT_GOAL_NEXT_LEVEL_MAX_UNIQUE_OBJECTS);
        settings.LIMIT_GOAL_LEVEL_2 = settingsJson.optInt("LIMIT_GOAL_LEVEL_2", settings.LIMIT_GOAL_LEVEL_2);
        return settings;
    }


}
