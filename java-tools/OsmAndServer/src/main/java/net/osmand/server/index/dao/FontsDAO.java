package net.osmand.server.index.dao;


import net.osmand.server.index.reader.DirectoryReader;
import net.osmand.server.index.reader.ZipFileDirectoryReader;
import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;

@Component
public class FontsDAO extends AbstractDAO {

    private static final String FONTS_FILE_EXTENSION = "*.otf.zip";
    private static final String FONTS_PATH = "indexes/fonts";

    public FontsDAO() {
        super(FONTS_PATH, FONTS_FILE_EXTENSION);
    }

    @Override
    public DirectoryReader getDirectoryReader(Path path, String glob) throws IOException {
        return new ZipFileDirectoryReader(path, glob);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newFontsType();
    }
}
