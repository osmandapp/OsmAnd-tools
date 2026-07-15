package net.osmand.search;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.GeocodingUtilities;
import net.osmand.data.Building;
import net.osmand.data.Street;
import net.osmand.osm.MapPoiTypes;
import net.osmand.router.RoutingContext;
import net.osmand.search.core.*;
import org.json.JSONObject;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

public class LegacySearch implements SearchEngine {
    private final SearchUICore core;  
    private final SearchSettings settings;
    private final boolean multiSearch;
    private RoutingContext geoCtx = null;
    private final GeocodingUtilities geoUtils = new GeocodingUtilities();
    private final BinaryMapIndexReader firstReader;
    private final ResultMatcher<SearchResult> matcher = new ResultMatcher<>() {
        @Override
        public boolean publish(SearchResult object) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    };

    public LegacySearch(JSONObject settingsJson, List<BinaryMapIndexReader> readers) {
        settings = SearchSettings.parseJSON(settingsJson);
        multiSearch = readers.size() > 1;
        if (!readers.isEmpty()) {
            settings.setOfflineIndexes(readers);
            firstReader = readers.get(0);
        } else {
            firstReader = null;
        }

        core = new SearchUICore(MapPoiTypes.getDefault(), "en", false);
        core.init();
    }
    
    @Override
    public List<String> apply(String text, List<String> expectedResults) throws IOException {
        SearchPhrase emptyPhrase = SearchPhrase.emptyPhrase(settings);

        String[] arr = text.split("[\\\\{}]");
        List<SearchResult> searchResults;
        SearchPhrase phrase;
        if (arr.length > 0 && arr[0].equals("POI_TYPE:")) {
            SearchCoreFactory.DISPLAY_DEFAULT_POI_TYPES = true;
            phrase = emptyPhrase.generateNewPhrase("", settings);
            searchResults = getSearchResult(phrase);
            for (SearchResult searchResult : searchResults) {
                if (arr.length > 1 && arr[1].equals(searchResult.localeName)) {
                    String fullText = arr.length > 2 ? arr[2] : "";
                    phrase = emptyPhrase.generateNewPhrase(fullText, settings);
                    phrase.getWords().add(new SearchWord(searchResult.localeName, searchResult));
                    searchResults = getSearchResult(phrase);
                    break;
                }
            }
        } else {
            phrase = emptyPhrase.generateNewPhrase(text, settings);
            searchResults = getSearchResult(phrase);
        }

        List<String> result = new ArrayList<>();
        for(int i = 0; i < expectedResults.size(); i++) {
            SearchResult res = searchResults.get(i);
            String expected = expectedResults.get(i);
            if (expected != null && expected.startsWith("@")) {
                Assert.assertNotNull("Reverse geocoding requested but no OBF readers are available", firstReader);
                testReverseGeocoding(res);
            }
            result.add(multiSearch ? 
                    formatResultMultiSearch(res, phrase) :
                    formatResult(false, res, phrase));
        }
        return result;
    }

    @Override
    public void close() {
        if (geoCtx != null) {
            geoCtx = null;
        }       
    }

    private void testReverseGeocoding(SearchResult searchResult) throws IOException {
        Assert.assertNotNull(searchResult);
        Assert.assertNotNull(searchResult.location);
        if (geoCtx == null) {
            geoCtx = GeocodingUtilities.buildDefaultContextForPOI(firstReader);
        }

        List<GeocodingUtilities.GeocodingResult> geoResult = geoUtils.reverseGeocodingSearch(
                geoCtx, searchResult.location.getLatitude(), searchResult.location.getLongitude(), false);

        geoResult = geoUtils.sortGeocodingResults(Collections.singletonList(firstReader), geoResult);

        Assert.assertFalse(geoResult.isEmpty());

        if (searchResult.object instanceof Building b1 && searchResult.relatedObject instanceof Street s1) {
            Assert.assertEquals(s1.getCity(), geoResult.get(0).city);
            Assert.assertEquals(s1.getName(), geoResult.get(0).street.getName());
            Assert.assertEquals(b1.getName(), geoResult.get(0).building.getName());
        } else {
            Assert.fail("Unsupported searchResult object / relatedObject");
        }
    }

    private static String formatResult(boolean simpleTest, SearchResult r, SearchPhrase phrase) {
        return SearchUICore.formatSearchResultForTest(simpleTest, r, phrase);
    }

    private static String formatResultMultiSearch(SearchResult r, SearchPhrase phrase) {
        String format = formatResult(false, r, phrase);
        String reg = r.file == null ? "-" : r.file.getFile().getName();
        return String.format(Locale.US, "%s [%s]", format, reg);
    }

    private List<SearchResult> getSearchResult(SearchPhrase phrase){
        SearchUICore.SearchResultMatcher matcher = new SearchUICore.SearchResultMatcher(this.matcher, phrase, 1, new AtomicInteger(1), -1);
        core.searchInternal(phrase, matcher);
        SearchUICore.SearchResultCollection collection = new SearchUICore.SearchResultCollection(phrase);
        collection.addSearchResults(matcher.getRequestResults(), true, true);
        if (matcher.totalLimit != -1 && matcher.count > matcher.totalLimit) {
            collection.setUseLimit(true);
        }

        return collection.getCurrentSearchResults();
    }
}
