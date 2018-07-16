package net.osmand.server.index.dao;

import net.osmand.server.index.reader.DirectoryReader;
import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDAO {

    private static final Path BASE_PATH = Paths.get("/var/www-download/");

    private final TypeFactory typeFactory = new TypeFactory();
    private final Path path;
    private final String glob;

    public AbstractDAO(String relativePath, String glob) {
        this.path = BASE_PATH.resolve(relativePath);
        this.glob = glob;
    }

    private Type handleType(Path path) throws IOException {
        Type type = getType(typeFactory);
        processAttributes(type, path);
        return type;
    }

    private List<Type> readDirectory() throws IOException {
        DirectoryReader dirReader = getDirectoryReader(path, glob);
        List<Path> files = dirReader.readFiles();
        List<Type> types = new ArrayList<>();
        for (Path map : files) {
            types.add(handleType(map));
        }
        return types;
    }

    public List<Type> getAll() throws IOException {
        return readDirectory();
    }

    public abstract DirectoryReader getDirectoryReader(Path path, String glob) throws IOException;

    public abstract Type getType(TypeFactory typeFactory);

    public abstract void processAttributes(Type type, Path path) throws IOException;
}
