package net.osmand.server.index.dao.zip;

import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

@Component("Depths")
public class DepthDAO extends ZipFilesDAO {
    private static final String DEPTH_FILE_EXTENSION = "*.obf.zip";
    private static final String DEPTH_PATH = "indexes/inapp/depth/";

    public DepthDAO() {
        super(DEPTH_PATH, DEPTH_FILE_EXTENSION);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newDepthType();
    }
}
