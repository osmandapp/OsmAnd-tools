package net.osmand.server.index.dao.sqlite;

import net.osmand.server.index.dao.AbstractDAO;
import net.osmand.server.index.reader.DirectoryReader;
import net.osmand.server.index.reader.FileDirectoryReader;
import net.osmand.server.index.type.Type;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

public abstract class SqliteFilesDAO extends AbstractDAO {

    public SqliteFilesDAO(String relativePath, String glob) {
        super(relativePath, glob);
    }

    @Override
    public void processAttributes(Type type, Path path) throws IOException {
        BasicFileAttributes fileAttributes = Files.readAttributes(path, BasicFileAttributes.class);

        String fileName = path.getFileName().toString();
        long sizeInBytes = fileAttributes.size();
        long timestamp = fileAttributes.creationTime().to(TimeUnit.MILLISECONDS);

        type.setContainerSize(sizeInBytes);
        type.setContentSize(sizeInBytes);
        type.setTimestamp(timestamp);
        type.setSize(sizeInBytes);
        type.setTargetSize(sizeInBytes);
        type.setName(fileName);
    }

    @Override
    public DirectoryReader getDirectoryReader(Path path, String glob) throws IOException {
        return new FileDirectoryReader(path, glob);
    }
}
