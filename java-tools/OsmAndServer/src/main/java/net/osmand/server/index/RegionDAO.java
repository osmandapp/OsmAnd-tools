package net.osmand.server.index;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RegionDAO  extends AbstractDAO {
    private static final String REGION_FILE_EXTENSION = "*.obf.zip";

    private final Path regionsPath = BASE_PATH.resolve("indexes");

    public List<Index> process() throws IOException {
        List<Index> regions = new ArrayList<>();
        if (Files.isDirectory(regionsPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(regionsPath, REGION_FILE_EXTENSION)) {
                for (Path entry : stream) {
                    if (!validateZipFile(entry.toFile())) {
                        continue;
                    }
                    RegionIndex ri = new RegionIndex();
                    BasicFileAttributes bfa = Files.readAttributes(entry, BasicFileAttributes.class);
                    String fname = entry.getFileName().toString();
                    long creationTime = bfa.creationTime().to(TimeUnit.MILLISECONDS);
                    double size = sizeInMBs(bfa);
                    String date = formatDate(bfa);
                    String description = createDescriptionByPattern(fname, FILES_PATTERN);
                    IndexAttributes ia = ri.getAttributes();
                    ia.setType("map");
                    ia.setName(fname);
                    ia.setTimestamp(creationTime);
                    ia.setContainerSize(sizeInBytes(bfa));
                    ia.setContentSize(sizeInBytes(bfa));
                    ia.setTargetSize(size);
                    ia.setSize(size);
                    ia.setDescription(description);
                    ia.setDate(date);

                    regions.add(ri);
                }
                return regions;
            }
        }
        throw new IOException("It is not a directory.");
    }
}
