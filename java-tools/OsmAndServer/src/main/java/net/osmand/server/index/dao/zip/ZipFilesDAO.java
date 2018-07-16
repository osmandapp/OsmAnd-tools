package net.osmand.server.index.dao.zip;

import net.osmand.server.index.dao.AbstractDAO;
import net.osmand.server.index.reader.DirectoryReader;
import net.osmand.server.index.reader.ZipFileDirectoryReader;
import net.osmand.server.index.type.Type;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public abstract class ZipFilesDAO extends AbstractDAO {


    public ZipFilesDAO(String relativePath, String glob) {
        super(relativePath, glob);
    }

    private long calculateContentSizeForZipFile(Path path) throws IOException {
        ZipFile zipFile = new ZipFile(path.toFile());
        return zipFile.stream().mapToLong(ZipEntry::getSize).sum();
    }

    @Override
    public void processAttributes(Type type, Path path) throws IOException {
        BasicFileAttributes fileAttributes = Files.readAttributes(path, BasicFileAttributes.class);

        String fileName = path.getFileName().toString();
        long sizeInBytes = fileAttributes.size();
        long contentSize = calculateContentSizeForZipFile(path);
        long timestamp = fileAttributes.creationTime().to(TimeUnit.MILLISECONDS);

        type.setContainerSize(sizeInBytes);
        type.setContentSize(contentSize);
        type.setTimestamp(timestamp);
        type.setSize(sizeInBytes);
        type.setTargetSize(contentSize);
        type.setName(fileName);
    }

    @Override
    public DirectoryReader getDirectoryReader(Path path, String glob) throws IOException {
        return new ZipFileDirectoryReader(path, glob);
    }
}
