package net.osmand.search.core.spatial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.Amenity;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.SpatialSearchPipelineTest.SearchTestEngine;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class SpatialTestSearchEngine implements SearchTestEngine {
    private final SpatialTextSearch spatialSearch;
    private final SpatialSearchContext searchContext;
    private final LatLon location;

    public SpatialTestSearchEngine(SpatialTextSearch.SpatialTextSearchSettings spatialSettings, LatLon location, List<BinaryMapIndexReader> readers) {
        this.location = location;
        spatialSearch = new SpatialTextSearch();
        SpatialPoiSearch poiSearch = new SpatialPoiSearch(MapPoiTypes.getDefault());
        searchContext = new SpatialSearchContext(spatialSettings, readers, poiSearch, location);
    }

    @Override
    public List<String> search(String phrase, boolean print) throws IOException {
        searchContext.stats.printLogs = print;
        
        SpatialTextSearch.SpatialSearchResults searchResults = spatialSearch.searchAPI(phrase, searchContext);
        List<SpatialSearchResult> mainResults = searchResults.mainResults == null ? Collections.emptyList()
                : searchResults.mainResults;

        List<String> result = new ArrayList<>();
		for (SpatialSearchResult res : mainResults) {
			if (print) {
//				System.out.println(SpatialSearchResult.compareKeyString(res) + " " + res);
			}
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
        return String.format(Locale.US, "%s [[%d, %s, %s, %.2f km, %s]]", b,
                tCount, testTypeStr(atom) + subtype, sorting, dist / 1000, r.toString(searchContext).replace("\"", "'"));
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
}
