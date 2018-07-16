package net.osmand.server.index.dao.zip;

import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

@Component("Fonts")
public class FontDAO extends ZipFilesDAO {

    private static final String FONTS_FILE_EXTENSION = "*.otf.zip";
    private static final String FONTS_PATH = "indexes/fonts/";

    public FontDAO() {
        super(FONTS_PATH, FONTS_FILE_EXTENSION);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newFontsType();
    }
}
