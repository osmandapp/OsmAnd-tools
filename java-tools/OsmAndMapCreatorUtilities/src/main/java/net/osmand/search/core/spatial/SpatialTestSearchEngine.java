package net.osmand.search.core.spatial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.osmand.osm.AbstractPoiType;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.LatLon;
import net.osmand.osm.MapPoiTypes;

public class SpatialTestSearchEngine {
    private final SpatialTextSearch spatialSearch;
    private final SpatialSearchContext searchContext;
	private final SpatialResultFormatter resultFormatter;

    public SpatialTestSearchEngine(SpatialTextSearch.SpatialTextSearchSettings spatialSettings, LatLon location,
                                   List<BinaryMapIndexReader> readers, MapPoiTypes.PoiTranslator poiTranslator,
                                   boolean translation) {
        spatialSearch = new SpatialTextSearch();
        MapPoiTypes poiTypes = new MapPoiTypes(null);
        poiTypes.setPoiTranslator(translation ? new TestPoiTranslator() : poiTranslator);
        // Binary readers and map objects still resolve POI metadata through MapPoiTypes.getDefault().
        MapPoiTypes.setDefault(poiTypes);

        SpatialPoiSearch poiSearch = new SpatialPoiSearch(poiTypes);
        searchContext = new SpatialSearchContext(spatialSettings, readers, poiSearch, location);
        resultFormatter = new SpatialResultFormatter(searchContext, location, poiTypes);
    }

    public List<String> search(String phrase, boolean print) throws IOException {
        List<SpatialSearchResult> mainResults = searchResults(phrase, print);
        List<String> result = new ArrayList<>();
		for (SpatialSearchResult res : mainResults) {
			result.add(formatResult(res));
		}
		return result;
	}

    public List<SpatialSearchResult> searchResults(String phrase, boolean print) throws IOException {
        searchContext.stats.printLogs = print;

        SpatialTextSearch.SpatialSearchResults searchResults = spatialSearch.searchAPI(phrase, searchContext);
        return searchResults.mainResults == null ? Collections.emptyList()
                : searchResults.mainResults;
	}

    public String formatResult(SpatialSearchResult r) {
		return resultFormatter.format(r);
    }

    public void close() {}

    private static class TestPoiTranslator implements MapPoiTypes.PoiTranslator {

        @Override
        public String getTranslation(String keyName) {
            return switch (keyName) {
                case "hotel" -> "отель";
                case "school" -> "школа";
                case "island" -> "остров";
                default -> null;
            };
        }

        @Override
        public String getTranslation(AbstractPoiType type) {
            return getTranslation(type.getKeyName());
        }

        @Override
        public String getSynonyms(String keyName) {
            return switch (keyName) {
                case "hotel" -> "отель;готель;гатэль";
                case "school" -> "школа";
                case "island" -> "остров";
                case "kindergarten" -> "Kindergarten;Дитячий садок";
                default -> null;
            };
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
