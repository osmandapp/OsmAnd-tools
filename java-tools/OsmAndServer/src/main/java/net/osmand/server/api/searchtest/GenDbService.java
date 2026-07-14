package net.osmand.server.api.searchtest;

import net.osmand.data.LatLon;
import net.osmand.util.Algorithms;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public interface GenDbService extends OBFService {
    record GenerateDbProgress(String status, String obfName, int obfIndex, int totalObfs, int processedTokens,
                              int totalTokens, long elapsedMs, long estimatedMs, String error,
                              List<GenerateDbObfProgress> obfs) {}
    record GenerateDbObfProgress(String obfName, int obfIndex, int totalTokens, int processedTokens,
                                 long elapsedMs, long estimatedMs, String status) {}
    record GenerateDbObfTokens(String obf, String obfName, int obfIndex, long startMs, List<IndexToken> tokens) {}
    record GenerateDbTokenObjects(String obf, String obfName, int obfIndex, long startMs, IndexToken token, ObjectAddressPage objectsPage) {}
    record GenerateDbTokenChunk(String obf, String obfName, int obfIndex, long startMs, List<GenerateDbTokenObjects> tokens) {}

    record Datasource(String name, long size, long lastModified, boolean valid, String error) {}
    record DbTagName(String name, long objects, boolean isSkipped) {}
    record DbTagValue(String value, long objects_count) {}
    record DbToken(long id, String name, long matched, long alone, boolean isCommon, boolean isFrequent, boolean isGenerated) {}
    record DbTokenSummary(long matchedSum, long aloneSum, long commonSum, long frequentSum, long generatedSum, long matchedMax, long aloneMax) {}
    record DbTokenPage(List<DbToken> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages, DbTokenSummary summary) {}
    record DbObjectToken(long id, String name, boolean isCommon, boolean isFrequent, boolean isGenerated, String obfName) {}
    record DbObjectTokenPage(List<DbObjectToken> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages) {}
    record DbObject(int sequenceId, String name, LatLon point, Map<String, String> commonTags,
                    String type, Long osmId, String osmType,
                    boolean isAlone, String obfName, long tokens) {}
    record TestCaseObject(String name, String type, LatLon point, long tokens,
                          long commonFrequentTokens, long commonTokens, long frequentTokens,
                          long newTokens, double proneScore, String topCommonFrequentTokens) {}
    record DbReport(long totalTokens, long totalPostings, List<DbReportDistribution> distribution,
                    List<DbReportPruneToken> pruning, List<TestCaseObject> mainWordInconsistency) {}
    record DbReportDistribution(String bucket, int ord, long tokens, long postings, long tokensNew, long postingsNew) {}
    record DbReportTagHit(String tag, long hits, double sharePct) {}
    record DbReportPruneToken(String name, boolean isCommon, boolean isFrequent, long matched, long alone,
                              double cumulativePct, List<DbReportTagHit> topTags) {}

    record DbObjectPage(List<DbObject> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages) {}
    
    int TAGS_DB_PAGE_SIZE = 100;

    default Path getTagsDatasourceDir() throws IOException {
        String url = getSearchTestDatasourceUrl();
        if (Algorithms.isEmpty(url)) {
            throw new IOException("spring.searchtestdatasource.url is not configured");
        }
        String path = url.startsWith("jdbc:sqlite:") ? url.substring("jdbc:sqlite:".length()) : url;
        Path parent = Path.of(path).toAbsolutePath().getParent();
        if (parent == null) {
            throw new IOException("Could not resolve datasource parent directory");
        }
        Path dir = parent.resolve("tags_db");
        Files.createDirectories(dir);
        return dir;
    }

    default Path resolveTagsDatasource(String name) throws IOException {
        if (Algorithms.isEmpty(name)) {
            throw new IOException("Datasource name is required");
        }
        String fileName = Path.of(name).getFileName().toString();
        if (!fileName.endsWith(".db") && !fileName.endsWith(".sqlite")) {
            fileName += ".db";
        }
        Path dir = getTagsDatasourceDir();
        Path file = dir.resolve(fileName).normalize();
        if (!file.startsWith(dir)) {
            throw new IOException("Invalid datasource name");
        }
        return file;
    }

    default List<Datasource> getTagsDatasources() throws IOException {
        Path dir = getTagsDatasourceDir();
        List<Datasource> result = new ArrayList<>();
        try (var stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".db") || path.getFileName().toString().endsWith(".sqlite"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString(), String.CASE_INSENSITIVE_ORDER))
                    .forEach(path -> {
                        try {
                            result.add(new Datasource(path.getFileName().toString(), Files.size(path),
                                    Files.getLastModifiedTime(path).toMillis(), true, ""));
                        } catch (IOException ignored) {
                        }
                    });
        }
        return result;
    }

    default boolean deleteTagsDatasource(String name) throws IOException {
        return Files.deleteIfExists(resolveTagsDatasource(name));
    }

    default void downloadTagsDatasource(String name, OutputStream out) throws IOException {
        Path dbFile = resolveTagsDatasource(name);
        if (!Files.exists(dbFile)) {
            throw new IOException("Datasource not found: " + name);
        }
        try (ZipOutputStream zip = new ZipOutputStream(out)) {
            zip.putNextEntry(new ZipEntry("db.sqlite"));
            Files.copy(dbFile, zip);
            zip.closeEntry();
            zip.finish();
        }
    }

}
