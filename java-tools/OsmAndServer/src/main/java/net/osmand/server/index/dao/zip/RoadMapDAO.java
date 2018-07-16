package net.osmand.server.index.dao.zip;

import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

@Component("RoadMaps")
public class RoadMapDAO extends ZipFilesDAO {
    private static final String ROADS_FILE_EXTENSION = "*.road.obf.zip";
    private static final String ROADS_PATH = "road-indexes/";

    public RoadMapDAO() {
        super(ROADS_PATH, ROADS_FILE_EXTENSION);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newRoadmapType();
    }
}
