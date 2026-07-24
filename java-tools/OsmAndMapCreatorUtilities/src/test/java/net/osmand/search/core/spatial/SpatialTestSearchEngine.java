package net.osmand.search.core.spatial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.json.JSONObject;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.Amenity;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.SearchUICoreGenOBFTest.SearchTestEngine;
import net.osmand.search.core.spatial.SpatialPoiSearch;
import net.osmand.search.core.spatial.SpatialSearchContext;
import net.osmand.search.core.spatial.SpatialSearchResult;
import net.osmand.search.core.spatial.SpatialSearchToken;
import net.osmand.search.core.spatial.SpatialTextSearch;
import net.osmand.search.core.spatial.SpatialTextSearch.SpatialTextSearchSettings;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class SpatialTestSearchEngine implements SearchTestEngine {
    private final SpatialTextSearch spatialSearch;
    private final SpatialSearchContext searchContext;
    private final LatLon location;

    public SpatialTestSearchEngine(JSONObject settingsJson, List<BinaryMapIndexReader> readers) {
        spatialSearch = new SpatialTextSearch();
        SpatialPoiSearch poiSearch = new SpatialPoiSearch(MapPoiTypes.getDefault());
        SpatialTextSearch.SpatialTextSearchSettings spatialSettings = parseSpatialSettings(settingsJson);
        location = parseLocation(settingsJson);
        searchContext = new SpatialSearchContext(spatialSettings, readers, poiSearch, location);
    }

    @Override
    public List<String> search(String phrase, boolean print) throws IOException {
        searchContext.stats.printLogs = print;
        
        SpatialTextSearch.SpatialSearchResults searchResults = spatialSearch.searchAPI(phrase, searchContext);
        List<SpatialSearchResult> mainResults = searchResults.mainResults == null ? Collections.emptyList()
                : searchResults.mainResults;

        List<String> result = new ArrayList<>();
        for(SpatialSearchResult res : mainResults) {
        	if(print) {
        		System.out.println(SpatialSearchResult.compareKeyString(res) + " " +  res);
        	}
            result.add(formatResult(res));
        }
        return result;
    }

    public String formatResult(SpatialSearchResult r) {
        double dist = 0.0;
        if (location != null && r.getLatLon() != null) {
            dist = MapUtils.getDistance(location, r.getLatLon());
        }
        StringBuilder b = new StringBuilder();
        SpatialSearchToken.NameIndexAtom atom = r.getFirstRef().getNameIndexAtom();
        Building building = atom.getBuilding();
        boolean poiCategory = atom.isPoiCategory();
        if (building != null && building.isInterpolation() && r.getExtraNameMatch() != null) {
            b.append(r.getExtraNameMatch()); // interpolated house number
        } else {
            appendName(b, r.getExtraNameMatch(), atom.getBuilding());
        }
        appendName(b, r.getExtraNameMatch(), atom.getObject());
        if (atom.getBuilding() == null && atom.getObject() == null) {
            b.append(atom.getName());
        }
        List<MapObject> allObjs = r.getObjects();
        String subtype = "";
        for (MapObject o : allObjs) {
            if (o instanceof Street street) {
                appendName(b, r.getExtraNameMatch(), street.getCity());
                break;
            }
            if (o instanceof City city) {
                appendName(b, r.getExtraNameMatch(), city);
                break;
            }
            if (o instanceof Amenity am && subtype.isEmpty()) {
				if (poiCategory) {
            		appendName(b, am.getName(), o);
            	} else {
            		subtype = " " + am.getSubType();
            	}
            }
        }
        String sorting = SpatialSearchResult.compareKeyString(r);
        return String.format(Locale.US, "%s [[%s, %s, %.2f km, %s]]", b,
                testTypeStr(atom) + subtype, sorting, dist / 1000, r.toString(searchContext));
    }

	private void appendName(StringBuilder b, String extraMatch, MapObject object) {
		if (object == null) {
			return;
		}
		String name = object instanceof Building building ? building.getName() : object.getName();
		if ((Algorithms.isEmpty(name) || extraMatch != null) && object instanceof Amenity a
				&& a.getAdditionalInfo("ref") != null) {
			name = (Algorithms.isEmpty(name) ? "" : (name + " ")) + a.getAdditionalInfo("ref");
		}
		if (Algorithms.isEmpty(name)) {
			return;
		}
		if (!b.isEmpty()) {
			b.append(", ");
		}
		b.append(name);
	}

    private String testTypeStr(SpatialSearchToken.NameIndexAtom atom) {
        if (atom.isBuilding()) {
            return "HOUSE";
        } else if (atom.isPOI()) {
            return "POI";
        } else if (atom.isStreet()) {
            return "STREET";
        } else if (atom.isPoiCategory()) {
            return "POI_TYPE";
        } else if (atom.getObject() instanceof City) {
            return "CITY";
        }
        return atom.typeStr().toUpperCase(Locale.US);
    }

    @Override
    public void close() {}

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
        SpatialTextSearch.SpatialTextSearchSettings settings = SpatialTextSearchSettings.defaultSettings();
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
        settings.OPTIM_READ_COMMON_WORDS_LIMIT = settingsJson.optInt("OPTIM_READ_COMMON_WORDS_LIMIT",
                settings.OPTIM_READ_COMMON_WORDS_LIMIT);
//        settings.ALWAYS_READ_COMMON_WORDS_ATOMS = settingsJson.optBoolean("ALWAYS_READ_COMMON_WORDS_ATOMS",
//                settings.ALWAYS_READ_COMMON_WORDS_ATOMS);
//        settings.ALWAYS_READ_FREQ_WORDS_ATOMS = settingsJson.optBoolean("ALWAYS_READ_FREQ_WORDS_ATOMS",
//                settings.ALWAYS_READ_FREQ_WORDS_ATOMS);
        settings.LANG_DEDUPLICATE = settingsJson.optString("LANG_DEDUPLICATE", settings.LANG_DEDUPLICATE);
        settings.MIN_ELO_RATING = settingsJson.optInt("MIN_ELO_RATING", settings.MIN_ELO_RATING);
        settings.MIN_CHARACTERS_INCOMPLETE = settingsJson.optInt("MIN_CHARACTERS_INCOMPLETE", settings.MIN_CHARACTERS_INCOMPLETE);
        settings.LIMIT_ATOMIC_OBJECTS = settingsJson.optInt("LIMIT_ATOMIC_OBJECTS", settings.LIMIT_ATOMIC_OBJECTS);
        settings.LIMIT_STOP_GOALS_ANY_LEVEL_WHEN_REACHED_RES = settingsJson.optInt("LIMIT_ALL_GOALS_MAX_UNIQUE_OBJECTS",
                settings.LIMIT_STOP_GOALS_ANY_LEVEL_WHEN_REACHED_RES);
        settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES = settingsJson.optInt("LIMIT_STOP_OTHER_GOALS_WHEN_REACHED_UNIQUE_OBJECTS",
                settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES);
        settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES = settingsJson.optInt("LIMIT_GOAL_LEVEL_2", settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES);
        return settings;
    }


}
