package net.osmand.search;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.search.core.spatial.SpatialTestSearchEngine;
import org.json.JSONObject;

import java.io.File;
import java.util.List;

public class SearchUICoreGenOBFPrePipelineTest extends SearchUICoreGenOBFTest {
    public SearchUICoreGenOBFPrePipelineTest(String name, File file) {
        super(name, file);
    }

    @Override
    protected List<List<String>> getExpectedResults(JSONObject sourceJson, int phrasesSize) {
        List<List<String>> mainResults = parseExpectedResults(sourceJson, "results", phrasesSize);
        if (sourceJson.has("pre-pipeline-results")) {
            List<List<String>> overriddenResults = parseExpectedResults(sourceJson, "pre-pipeline-results", phrasesSize);
            for (int i = 0; i < overriddenResults.size(); i++) {
                List<String> overriddenResult = overriddenResults.get(i);
                if (overriddenResult != null && !overriddenResult.isEmpty()) {
                    mainResults.set(i, overriddenResult);
                }
            }
        }
        return mainResults;
    }

    @Override
    protected SearchTestEngine createSearchEngine(JSONObject settingsJson, List<BinaryMapIndexReader> readers) {
        settingsJson.put("DEV_USE_PIPELINE", false);
        return new SpatialTestSearchEngine(settingsJson, readers);
    }
}
