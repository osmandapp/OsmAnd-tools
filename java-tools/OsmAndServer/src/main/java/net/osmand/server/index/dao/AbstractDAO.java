package net.osmand.server.index.dao;

import net.osmand.server.index.reader.DirectoryReader;
import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public abstract class AbstractDAO {

    private static final Path BASE_PATH = Paths.get("/var/www-download/");

    private final TypeFactory typeFactory = new TypeFactory();
    private final Path path;
    private final String glob;

    public AbstractDAO(String relativePath, String glob) {
        this.path = BASE_PATH.resolve(relativePath);
        this.glob = glob;
    }

    private long sizeInBytes(BasicFileAttributes bfa) {
        return bfa.size();
    }

    private long getTimestamp(BasicFileAttributes bfa) {
        return bfa.creationTime().to(TimeUnit.MILLISECONDS);
    }

    private long calculateContentSizeForZipFile(Path path) throws IOException {
        ZipFile zipFile = new ZipFile(path.toFile());
        return zipFile.stream().mapToLong(ZipEntry::getSize).sum();
    }

    private Type handleType(Path path) throws IOException {
        BasicFileAttributes bfa = Files.readAttributes(path, BasicFileAttributes.class);
        Type mapType = getType(typeFactory);
        processAttrsAndSetType(mapType, bfa, path);
        return mapType;
    }

    private void processAttrsAndSetType(Type ia, BasicFileAttributes bfa, Path filePath)
            throws IOException {
        String fileName = filePath.getFileName().toString();
        long sizeInBytes = sizeInBytes(bfa);
        long contentSize = calculateContentSizeForZipFile(filePath);
        ia.setContainerSize(sizeInBytes);
        ia.setContentSize(contentSize);
        ia.setTimestamp(getTimestamp(bfa));
        ia.setSize(sizeInBytes);
        ia.setTargetSize(sizeInBytes);
        ia.setName(fileName);
    }

    private List<Type> readDirectory() throws IOException {
        DirectoryReader zipDirReader = getDirectoryReader(path, glob);
        List<Path> files = zipDirReader.readFiles();
        List<Type> mapTypes = new ArrayList<>();
        for (Path map : files) {
            mapTypes.add(handleType(map));
        }
        return mapTypes;
    }

    public List<Type> getAll() throws IOException {
        return readDirectory();
    }

    public abstract DirectoryReader getDirectoryReader(Path path, String glob) throws IOException;

    public abstract Type getType(TypeFactory typeFactory);
}
