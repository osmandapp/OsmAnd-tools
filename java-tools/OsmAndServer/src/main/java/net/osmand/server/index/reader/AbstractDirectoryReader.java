package net.osmand.server.index.reader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class AbstractDirectoryReader implements DirectoryReader {

    protected final Path dir;
    protected final String glob;

    public AbstractDirectoryReader(Path dir, String glob) throws IOException {
        this.dir = getDirPathOrThrow(dir);
        this.glob = glob;
    }

    private Path getDirPathOrThrow(Path dir) throws IOException {
        if (Files.isDirectory(dir)) {
            return dir;
        }
        throw new IOException("It's not a directory");
    }


}
