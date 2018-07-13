package net.osmand.server.index.reader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipFile;

public class ZipFileDirectoryReader extends AbstractDirectoryReader {

    private static final Log LOGGER = LogFactory.getLog(ZipFileDirectoryReader.class);

    public ZipFileDirectoryReader(Path dir, String glob) throws IOException {
        super(dir, glob);
    }

    private boolean validateZipFile(Path zipFile) {
        boolean isValid = true;
        try {
            new ZipFile(zipFile.toFile());
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
            isValid = false;
        }
        return isValid;
    }

    @Override
    public List<Path> readFiles() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
            List<Path> zipFiles = new ArrayList<>();
            for (Path zipFile : stream) {
                if (validateZipFile(zipFile)) {
                    zipFiles.add(zipFile);
                }
            }
            return zipFiles;
        }
    }
}
