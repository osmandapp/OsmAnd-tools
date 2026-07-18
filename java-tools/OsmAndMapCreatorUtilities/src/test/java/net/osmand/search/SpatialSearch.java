package net.osmand.search;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.*;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.core.spatial.*;
import net.osmand.search.core.spatial.SpatialSearchResult.SpatialSearchResultRef;
import net.osmand.search.core.spatial.SpatialTextSearch.SpatialTextSearchSettings;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

public class SpatialSearch implements SearchEngine {
    private final SpatialTextSearch spatialSearch;
    private final SpatialSearchContext searchContext;
    private final LatLon location;

    public SpatialSearch(JSONObject settingsJson, List<BinaryMapIndexReader> readers) {
        spatialSearch = new SpatialTextSearch();
        SpatialPoiSearch poiSearch = new SpatialPoiSearch(MapPoiTypes.getDefault());
        SpatialTextSearch.SpatialTextSearchSettings spatialSettings = parseSpatialSettings(settingsJson);
        location = parseLocation(settingsJson);
        searchContext = new SpatialSearchContext(spatialSettings, readers, poiSearch, location);
    }

    @Override
    public List<String> apply(String phrase, List<String> expectedResults) throws IOException {
        SpatialTextSearch.SpatialSearchResults searchResults = spatialSearch.searchAPI(phrase, searchContext);
        List<SpatialSearchResult> mainResults = searchResults.mainResults == null ? Collections.emptyList()
                : searchResults.mainResults;

        List<String> result = new ArrayList<>();
        for(SpatialSearchResult res : mainResults) {
//        	System.out.println(SpatialSearchResult.compareKeyString(res) + " " +  res);
            result.add(formatResult(res));
        }
        return result;
    }

    public String formatResult(SpatialSearchResult r) {
        int tCount = r.getParent().getTokenCount();
        double dist = 0.0;
        if (location != null && r.getLatLon() != null) {
            dist = MapUtils.getDistance(location, r.getLatLon());
        }
        StringBuilder b = new StringBuilder();
        SpatialSearchToken.NameIndexAtom atom = r.getFirstRef().getNameIndexAtom();
        Building building = atom.getBuilding();
        if (building != null && building.isInterpolation() && r.getExtraNameMatch() != null) {
            b.append(r.getExtraNameMatch()); // interpolated house number
        } else {
            appendName(b, r.getExtraNameMatch(), atom.getBuilding());
        }
        appendName(b, r.getExtraNameMatch(), atom.getObject());
        if (atom.getBuilding() == null && atom.getObject() == null) {
            b.append(atom.getName());
        }
        List<MapObject> allObjs = r.getAllObjects();
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
            if (o instanceof Amenity am && subtype.length() == 0) {
                subtype = " " + am.getSubType();
            }
        }
        String sorting = SpatialSearchResult.compareKeyString(r);
        return String.format(Locale.US, "%s [[%d, %s, %s, %.2f km]]", b,
                tCount, testTypeStr(atom) + subtype, sorting, dist / 1000);
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
//        settings.TEST_ALLOW_HOUSE_POI_TYPE_INTERSECTION = settingsJson.optBoolean("TEST_ALLOW_HOUSE_POI_TYPE_INTERSECTION",
//                settings.TEST_ALLOW_HOUSE_POI_TYPE_INTERSECTION);
//        settings.ALWAYS_READ_COMMON_WORDS_ATOMS = settingsJson.optBoolean("ALWAYS_READ_COMMON_WORDS_ATOMS",
//                settings.ALWAYS_READ_COMMON_WORDS_ATOMS);
//        settings.ALWAYS_READ_FREQ_WORDS_ATOMS = settingsJson.optBoolean("ALWAYS_READ_FREQ_WORDS_ATOMS",
//                settings.ALWAYS_READ_FREQ_WORDS_ATOMS);
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
