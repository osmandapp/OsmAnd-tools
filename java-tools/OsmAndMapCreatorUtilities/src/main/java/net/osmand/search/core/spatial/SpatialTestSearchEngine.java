package net.osmand.search.core.spatial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.LatLon;
import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;

/** Runs the phrases of a spatial search unit test and formats rows the way the test JSON stores them */
public class SpatialTestSearchEngine {
	private final SpatialTextSearch spatialSearch = new SpatialTextSearch();
	private final SpatialSearchContext searchContext;
	private final SpatialResultFormatter resultFormatter;

	/**
	 * @param poiTypes initialised POI types the search takes categories and translations from; the engine does not make
	 *                 them the process default: the tests do that themselves, the server keeps its own
	 */
	public SpatialTestSearchEngine(SpatialTextSearch.SpatialTextSearchSettings spatialSettings, LatLon location,
			List<BinaryMapIndexReader> readers, MapPoiTypes poiTypes) {
		searchContext = new SpatialSearchContext(spatialSettings, readers, new SpatialPoiSearch(poiTypes), location);
		resultFormatter = new SpatialResultFormatter(searchContext, location, poiTypes);
	}

	public List<String> search(String phrase, boolean print) throws IOException {
		List<String> result = new ArrayList<>();
		for (SpatialSearchResult res : searchResults(phrase, print)) {
			result.add(formatResult(res));
		}
		return result;
	}

	public List<SpatialSearchResult> searchResults(String phrase, boolean print) throws IOException {
		searchContext.stats.printLogs = print;
		SpatialTextSearch.SpatialSearchResults searchResults = spatialSearch.searchAPI(phrase, searchContext);
		return searchResults.mainResults == null ? Collections.emptyList() : searchResults.mainResults;
	}

	public String formatResult(SpatialSearchResult r) {
		return resultFormatter.format(r);
	}

	/** the translations the translation tests were written with */
	public static class TestPoiTranslator implements MapPoiTypes.PoiTranslator {

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
