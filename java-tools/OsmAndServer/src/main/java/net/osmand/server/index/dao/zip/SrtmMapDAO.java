package net.osmand.server.index.dao.zip;

import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

@Component("SrtmMaps")
public class SrtmMapDAO extends ZipFilesDAO {
    private static final String SRTM_MAP_FILE_EXTENSION = "*.srtm.obf.zip";
    private static final String SRTM_MAP_PATH = "srtm-countries/";


    public SrtmMapDAO() {
        super(SRTM_MAP_PATH, SRTM_MAP_FILE_EXTENSION);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newSrtmMapType();
    }
}
