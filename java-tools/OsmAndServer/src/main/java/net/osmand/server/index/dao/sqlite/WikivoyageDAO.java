package net.osmand.server.index.dao.sqlite;

import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

@Component("Wikivoyage")
public class WikivoyageDAO extends SqliteFilesDAO {
    private static final String WIKIVOYAGE_FILE_EXTENSION = "*.sqlite";
    private static final String WIKIVOYAGE_PATH = "wikivoyage/";

    public WikivoyageDAO() {
        super(WIKIVOYAGE_PATH, WIKIVOYAGE_FILE_EXTENSION);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newWikivoyageType();
    }
}
