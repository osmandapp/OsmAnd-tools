package net.osmand.server.index.reader;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class FileDirectoryReader extends AbstractDirectoryReader {

    public FileDirectoryReader(Path dir, String glob) throws IOException {
        super(dir, glob);
    }

    @Override
    public List<Path> readFiles() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
            List<Path> files = new ArrayList<>();
            for (Path file : stream) {
                files.add(file);
            }
            return files;
        }
    }
}
