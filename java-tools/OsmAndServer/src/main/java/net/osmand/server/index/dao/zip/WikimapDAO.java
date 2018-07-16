package net.osmand.server.index.dao.zip;

import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

@Component("Wikimaps")
public class WikimapDAO extends ZipFilesDAO {

    private static final String WIKIMAP_FILE_EXTENSION = "*.wiki.obf.zip";
    private static final String WIKIMAP_PATH = "wiki/";

    public WikimapDAO() {
        super(WIKIMAP_PATH, WIKIMAP_FILE_EXTENSION);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newWikimapType();
    }
}
