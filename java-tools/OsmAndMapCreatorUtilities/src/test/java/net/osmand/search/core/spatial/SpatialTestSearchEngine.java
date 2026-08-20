package net.osmand.search.core.spatial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import net.osmand.osm.AbstractPoiType;

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
    private static MapPoiTypes.PoiTranslator defaultPoiTranslator;

    private final SpatialTextSearch spatialSearch;
    private final SpatialSearchContext searchContext;
    private final LatLon location;
    private final MapPoiTypes poiTypes;

    public SpatialTestSearchEngine(SpatialTextSearch.SpatialTextSearchSettings spatialSettings, LatLon location, 
                                   List<BinaryMapIndexReader> readers, boolean translation) {
        this.location = location;
        spatialSearch = new SpatialTextSearch();
        MapPoiTypes.PoiTranslator currentPoiTranslator = MapPoiTypes.getDefault().getPoiTranslator();
        if (!(currentPoiTranslator instanceof TestPoiTranslator)) {
            defaultPoiTranslator = currentPoiTranslator;
        }
        poiTypes = new MapPoiTypes(null);
        poiTypes.setPoiTranslator(translation ? new TestPoiTranslator() : defaultPoiTranslator);
        MapPoiTypes.setDefault(poiTypes);
        
        SpatialPoiSearch poiSearch = new SpatialPoiSearch(poiTypes);
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
        Amenity poi = null;
        for (MapObject object : allObjs) {
            if (object instanceof Amenity amenity && amenity.getType() != null) {
                poi = amenity;
                amenity.setType(poiTypes.getPoiCategoryByName(amenity.getType().getKeyName()));
            }
        }
        List<Street> intersectionStreets = getIntersectionStreets(r);
        String subtype = "", resultType = null;
        for (MapObject o : allObjs) {
            if (o instanceof Street street) {
                if (intersectionStreets.size() == 2) {
                    if (poi == null && building == null) {
                        b.setLength(0);
                    }
                    appendIntersection(b, intersectionStreets);
                    resultType = "STREET_INTERSECTION";
                } else {
                    appendName(b, r.getExtraNameMatch(), street.getCity());
                }
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
                tCount, resultType != null ? resultType : (testTypeStr(atom) + subtype), sorting, dist / 1000, r.toString(searchContext).replace("\"", "'"));
    }

    private List<Street> getIntersectionStreets(SpatialSearchResult result) {
        SpatialSearchToken.NameIndexAtom first = null, second = null;
        SpatialSearchResultsList parent = result.getParent();
        for (int i = 0; i < parent.tCount; i++) {
            SpatialSearchToken.NameIndexAtom atom = parent.linearResults.get(result.parentInd * parent.tCount + i);
            if (atom.getObject() instanceof Street && !atom.isCityStreetName()) {
                if (first == null || Objects.equals(first.getObject().getId(), atom.getObject().getId())) {
                    first = atom;
                } else {
                    second = atom;
                }
            }
        }

        if (first == null || second == null
                || !(first.getObject() instanceof Street firstStreet)
                || !(second.getObject() instanceof Street secondStreet)) {
            return Collections.emptyList();
        }

        List<Street> streets = new ArrayList<>(List.of(firstStreet, secondStreet));
        streets.sort(Comparator.comparing((Street street) -> normalizedName(street.getName()))
                .thenComparing(Street::getId, Comparator.nullsLast(Long::compareTo)));
        return streets;
    }

    private void appendIntersection(StringBuilder b, List<Street> streets) {
        for (Street street : streets) {
            if (!b.isEmpty()) {
                b.append(" - ");
            }
            b.append(street.getName());
        }
    }

    private String normalizedName(String name) {
        return name == null ? "" : name.trim().toLowerCase(Locale.ROOT);
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

    private static class TestPoiTranslator implements MapPoiTypes.PoiTranslator {

        @Override
        public String getTranslation(String keyName) {
            if (keyName.equals("hotel")) {
                return "отель";
            }
            if (keyName.equals("island")) {
                return "остров";
            }
            return null;
        }

        @Override
        public String getTranslation(AbstractPoiType type) {
            return getTranslation(type.getKeyName());
        }

        @Override
        public String getSynonyms(String keyName) {
            if (keyName.equals("hotel")) {
                return "отель;готель;гатэль";
            }
            if (keyName.equals("island")) {
                return "остров";
            }
            return null;
        }

        @Override
        public String getSynonyms(AbstractPoiType type) {
            return getSynonyms(type.getKeyName());
        }

        @Override
        public String getEnTranslation(String keyName) {
            return null;
        }

        @Override
        public String getEnTranslation(AbstractPoiType type) {
            return null;
        }

        @Override
        public String getAllLanguagesTranslationSuffix() {
            return "";
        }
    }
}
