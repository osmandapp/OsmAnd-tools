package net.osmand.search;

import java.io.File;
import java.util.List;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.LatLon;
import net.osmand.search.core.spatial.SpatialTestSearchEngine;
import net.osmand.search.core.spatial.SpatialTextSearch;

public class SpatialSearchScoreTest extends SpatialSearchPipelineTest {
	
    public SpatialSearchScoreTest(String name, File file) {
        super(name, new File(new File(file.getParentFile(), "score"), file.getName()));
    }

    @Override
    protected SearchTestEngine createSearchEngine(SpatialTextSearch.SpatialTextSearchSettings spatialSettings, 
                                                  LatLon point, List<BinaryMapIndexReader> readers, boolean translation) {
        spatialSettings.SCORE_RANKING = true;
        return new SpatialTestSearchEngine(spatialSettings, point, readers, translation);
    }
}
