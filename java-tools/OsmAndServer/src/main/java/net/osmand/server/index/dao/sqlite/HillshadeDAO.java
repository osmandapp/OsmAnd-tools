package net.osmand.server.index.dao.sqlite;

import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

@Component("Hillshades")
public class HillshadeDAO extends SqliteFilesDAO {
    private static final String HILLSHADE_FILE_EXTENSION = "*.sqlitedb";
    private static final String HILLSHADE_PATH = "hillshade/";

    public HillshadeDAO() {
        super(HILLSHADE_PATH, HILLSHADE_FILE_EXTENSION);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newHillshadeType();
    }
}
