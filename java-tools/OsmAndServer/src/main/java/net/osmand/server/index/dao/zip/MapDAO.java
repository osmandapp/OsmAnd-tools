package net.osmand.server.index.dao.zip;

import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

@Component("Maps")
public class MapDAO extends ZipFilesDAO {

    private static final String MAPS_FILE_EXTENSION = "*.obf.zip";
    private static final String MAPS_PATH = "indexes/";

    public MapDAO() {
        super(MAPS_PATH, MAPS_FILE_EXTENSION);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newMapType();
    }
}