package net.osmand.server.index.dao;

import net.osmand.server.index.reader.DirectoryReader;
import net.osmand.server.index.reader.ZipFileDirectoryReader;
import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;

@Component
public class MapsDAO extends AbstractDAO {

    private static final String MAPS_FILE_EXTENSION = "*.obf.zip";
    private static final String MAPS_PATH = "indexes";

    public MapsDAO() {
        super(MAPS_PATH, MAPS_FILE_EXTENSION);
    }

    @Override
    public DirectoryReader getDirectoryReader(Path path, String glob) throws IOException {
        return new ZipFileDirectoryReader(path, glob);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newMapType();
    }
}