package net.osmand.search;

import java.io.File;
import java.util.List;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.LatLon;
import net.osmand.search.core.spatial.test.SpatialTestSearchEngine;
import net.osmand.search.core.spatial.SpatialTextSearch;

public class SpatialSearchScoreTest extends SpatialSearchPipelineTest {
	
    public SpatialSearchScoreTest(String name, File file) {
        super(name, scoreFileOrDefault(file));
    }

    private static File scoreFileOrDefault(File file) {
        File scoreFile = new File(new File(file.getParentFile(), "score"), file.getName());
        return scoreFile.isFile() ? scoreFile : file;
    }

    @Override
    protected SpatialTestSearchEngine createSearchEngine(SpatialTextSearch.SpatialTextSearchSettings spatialSettings, 
                                                  LatLon point, List<BinaryMapIndexReader> readers) {
        spatialSettings.SCORE_RANKING = true;
        return super.createSearchEngine(spatialSettings, point, readers);
    }
}
