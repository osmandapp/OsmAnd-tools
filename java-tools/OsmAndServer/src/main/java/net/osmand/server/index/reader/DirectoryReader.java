package net.osmand.server.index.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface DirectoryReader {

    List<Path> readFiles() throws IOException;
}
