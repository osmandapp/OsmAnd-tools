package net.osmand.server.api.searchtest;

import net.osmand.CollatorStringMatcher;
import net.osmand.binary.*;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.data.*;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static net.osmand.binary.ObfConstants.*;

public interface AnalystService extends InspectorService, GenDbService {

    String GENERATE_DB_SCHEMA_DDL = "/analyst-db/schema.ddl";
    String GENERATE_DB_INDEX_DDL = "/analyst-db/index.ddl";
    
    default void generateDb(List<String> obfs, OutputStream out) throws IOException, SQLException {
        generateDb(obfs, out, null);
    }

    default void generateDb(List<String> obfs, OutputStream out, GenerateDbProgressListener progressListener) throws IOException, SQLException {
        generateDb(obfs, out, progressListener, GenerateDbOptions.DEFAULT);
    }

    default void generateDb(List<String> obfs, OutputStream out, GenerateDbProgressListener progressListener,
                            GenerateDbOptions options) throws IOException, SQLException {
        if (obfs == null || obfs.isEmpty()) {
            throw new IllegalArgumentException("OBF file list is required");
        }
        Path dbFile = Files.createTempFile("search-test-db-", ".sqlite");
        try {
            generateDbFile(obfs, dbFile, progressListener, options);
            try (ZipOutputStream zip = new ZipOutputStream(out)) {
                zip.putNextEntry(new ZipEntry("db.sqlite"));
                Files.copy(dbFile, zip);
                zip.closeEntry();
                zip.finish();
            }
        } finally {
            try {
                Files.deleteIfExists(dbFile);
            } catch (IOException e) {
                getLogger().warn("Failed to delete temporary generated DB {}", dbFile, e);
            }
        }
    }

    default void generateDbFile(List<String> obfs, Path dbFile, GenerateDbProgressListener progressListener,
                                GenerateDbOptions options) throws IOException, SQLException {
        if (obfs == null || obfs.isEmpty()) {
            throw new IllegalArgumentException("OBF file list is required");
        }
        GenerateDbOptions safeOptions = options == null ? GenerateDbOptions.DEFAULT : options;
        Files.createDirectories(dbFile.toAbsolutePath().getParent());
        Files.deleteIfExists(dbFile);
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath())) {
            configureGenerateDbBulkLoad(conn);
            createDbSchema(conn);
            try {
                populateDb(conn, obfs, progressListener, safeOptions);
                finalizeGenerateDb(conn);
            } catch (Exception e) {
                rollbackGenerateDb(conn);
                if (e instanceof CancellationException cancellationException) {
                    throw cancellationException;
                }
                if (e instanceof SQLException sqlException) {
                    throw sqlException;
                }
                if (e instanceof IOException ioException) {
                    throw ioException;
                }
                throw new IOException("Failed to generate SQLite DB: " + e.getMessage(), e);
            }
        }
    }

    private void configureGenerateDbBulkLoad(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA page_size = 32768");
            stmt.execute("PRAGMA journal_mode = OFF");
            stmt.execute("PRAGMA synchronous = OFF");
            stmt.execute("PRAGMA locking_mode = EXCLUSIVE");
            stmt.execute("PRAGMA temp_store = MEMORY");
            stmt.execute("PRAGMA cache_size = -524288");
            stmt.execute("PRAGMA foreign_keys = OFF");
            stmt.execute("PRAGMA automatic_index = OFF");
        }
        conn.setAutoCommit(true);
    }

    private void finalizeGenerateDb(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long start = System.currentTimeMillis();
            stmt.execute("ANALYZE");
            stmt.execute("PRAGMA optimize");
            getLogger().info("generateDb: post-load ANALYZE/optimize completed in {} ms", System.currentTimeMillis() - start);
        }
    }

    private void rollbackGenerateDb(Connection conn) {
        try {
            if (!conn.getAutoCommit()) {
                conn.rollback();
                conn.setAutoCommit(true);
            }
        } catch (SQLException rollbackError) {
            getLogger().warn("generateDb: failed to rollback current SQLite transaction", rollbackError);
        }
    }

    private void createDbSchema(Connection conn) throws SQLException, IOException {
        executeGenerateDbSqlResource(conn, GENERATE_DB_SCHEMA_DDL);
    }

    private void buildGenerateDbPostLoadIndexes(Connection conn) throws SQLException, IOException {
        long start = System.currentTimeMillis();
        executeGenerateDbSqlResource(conn, GENERATE_DB_INDEX_DDL);
        getLogger().info("generateDb: post-load posting indexes completed in {} ms", System.currentTimeMillis() - start);
    }

    private void executeGenerateDbSqlResource(Connection conn, String resourcePath) throws SQLException, IOException {
        try (InputStream inputStream = AnalystService.class.getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IOException("Missing SQL resource: " + resourcePath);
            }
            executeGenerateDbSqlScript(conn, Algorithms.readFromInputStream(inputStream).toString());
        }
    }

    private void executeGenerateDbSqlScript(Connection conn, String sqlScript) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            for (String statement : sqlScript.split(";")) {
                String sql = statement.trim();
                if (!sql.isEmpty()) {
                    stmt.execute(sql);
                }
            }
        }
    }

    private void populateDb(Connection conn, List<String> obfs, GenerateDbProgressListener progressListener,
                            GenerateDbOptions options) throws SQLException, IOException {
        Map<String, Long> tokenIds = new HashMap<>();
        try (PreparedStatement insertObf = conn.prepareStatement("INSERT INTO OBF(name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
             PreparedStatement insertToken = conn.prepareStatement("""
                     INSERT INTO token(name, isCommon, isFrequent, isGenerated)
                     VALUES (?, ?, ?, ?)
                     ON CONFLICT(name) DO UPDATE SET
                     	isCommon = CASE WHEN excluded.isCommon = 1 THEN 1 ELSE Token.isCommon END,
                        isFrequent = CASE WHEN excluded.isFrequent = 1 THEN 1 ELSE Token.isFrequent END,
                        isGenerated = CASE WHEN excluded.isGenerated = 0 THEN 0 ELSE Token.isGenerated END
                     """);
             PreparedStatement selectTokenId = conn.prepareStatement("SELECT id FROM token WHERE name = ?");
             PreparedStatement insertObject = conn.prepareStatement("""
                     INSERT OR IGNORE INTO "object"(id, name, lat, lon, commonTags, type, osmType)
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                     """);
             PreparedStatement insertTag = conn.prepareStatement("""
                     INSERT INTO tag(name, type, isSkipped)
                     VALUES (?, ?, ?)
                     ON CONFLICT(name, type) DO UPDATE SET
                        isSkipped = CASE WHEN excluded.isSkipped = 0 THEN 0 ELSE tag.isSkipped END
                     """);
             PreparedStatement selectTagId = conn.prepareStatement("SELECT id FROM tag WHERE name = ? AND type = ?");
             PreparedStatement insertValue = conn.prepareStatement("""
                     INSERT OR IGNORE INTO value(tag_id, value)
                     VALUES (?, ?)
                     """);
             PreparedStatement selectValueId = conn.prepareStatement("SELECT id FROM value WHERE tag_id = ? AND value = ?");
             PreparedStatement insertObjectTagValue = conn.prepareStatement("""
                     INSERT OR IGNORE INTO object_tag_value(object_id, tag_id, token_id, value_id, value)
                     VALUES (?, ?, ?, ?, ?)
                     """);
             PreparedStatement insertPosting = conn.prepareStatement("""
                     INSERT OR IGNORE INTO posting(obf_id, token_id, object_id, sequenceId, isAlone)
                     VALUES (?, ?, ?, ?, ?)
                     """)) {
            int totalObfs = obfs.size();
            Map<Integer, GenerateDbObfState> progressStates = new LinkedHashMap<>();
            Map<Integer, Long> obfIds = new HashMap<>();
            Map<Integer, AtomicInteger> objectCounts = new HashMap<>();
            Map<Integer, AtomicInteger> skippedWithoutOsmIds = new HashMap<>();
            Map<String, Long> tagIds = new HashMap<>();
            Map<String, Long> valueIds = new HashMap<>();
            GenerateDbMetrics metrics = new GenerateDbMetrics();
            GenerateDbWriterBatch writerBatch = new GenerateDbWriterBatch(conn, getLogger(), metrics);
            GenerateDbSqlBatcher sqlBatcher = new GenerateDbSqlBatcher();
            for (int obfIndex = 0; obfIndex < obfs.size(); obfIndex++) {
                String obf = obfs.get(obfIndex);
                if (Algorithms.isEmpty(obf)) {
                    continue;
                }
                String obfName = OBFService.getObfFileName(obf);
                long obfId = insertObfRow(insertObf, obfName);
                int displayIndex = obfIndex + 1;
                obfIds.put(displayIndex, obfId);
                objectCounts.put(displayIndex, new AtomicInteger());
                skippedWithoutOsmIds.put(displayIndex, new AtomicInteger());
                progressStates.put(displayIndex, new GenerateDbObfState(obfName, displayIndex, System.currentTimeMillis()));
            }
            ExecutorService executor = createGenerateDbExecutor();
            try {
                for (int obfIndex = 0; obfIndex < obfs.size(); obfIndex++) {
                    String obf = obfs.get(obfIndex);
                    if (Algorithms.isEmpty(obf)) {
                        continue;
                    }
                    int displayIndex = obfIndex + 1;
                    String obfName = OBFService.getObfFileName(obf);
                    long startMs = System.currentTimeMillis();
                    long startNs = System.nanoTime();
                    List<IndexToken> tokens = loadAllGenerateDbTokens(obf);
                    metrics.tokenLoadNs.addAndGet(System.nanoTime() - startNs);
                    getLogger().info("generateDb: loaded {} tokens for OBF {}", tokens.size(), obfName);
                    GenerateDbObfTokens obfTokens = new GenerateDbObfTokens(obf, obfName, displayIndex, startMs, tokens);
                    CompletionService<GenerateDbTokenChunk> chunkService = new ExecutorCompletionService<>(executor);
                    GenerateDbObfState state = progressStates.get(obfTokens.obfIndex());
                    if (state != null) {
                        state.startMs = obfTokens.startMs();
                        state.totalTokens = obfTokens.tokens().size();
                        state.status = "RUNNING";
                    }
                    notifyGenerateDbProgress(progressListener, "RUNNING", obfTokens.obfName(), obfTokens.obfIndex(), totalObfs,
                            0, obfTokens.tokens().size(), obfTokens.startMs(), null, progressStates);
                    if (obfTokens.tokens().isEmpty()) {
                        if (state != null) {
                            state.markDone();
                        }
                        notifyGenerateDbProgress(progressListener, "RUNNING", obfTokens.obfName(), obfTokens.obfIndex(), totalObfs,
                                0, 0, obfTokens.startMs(), null, progressStates);
                    }
                    int totalChunks = (obfTokens.tokens().size() + GenerateDbWriterBatch.TOKEN_CHUNK_SIZE - 1)
                            / GenerateDbWriterBatch.TOKEN_CHUNK_SIZE;
                    int nextChunkStart = 0;
                    int submittedChunks = 0;
                    int completedChunks = 0;
                    while (submittedChunks < totalChunks && submittedChunks < GenerateDbWriterBatch.MAX_IN_FLIGHT_CHUNKS) {
                        nextChunkStart = submitGenerateDbTokenChunk(chunkService, obfTokens, nextChunkStart, metrics);
                        submittedChunks++;
                    }
                    while (completedChunks < totalChunks) {
                        long waitStartNs = System.nanoTime();
                        GenerateDbTokenChunk chunk = getGenerateDbFuture(chunkService.take());
                        metrics.readerWaitNs.addAndGet(System.nanoTime() - waitStartNs);
                        completedChunks++;
                        while (submittedChunks < totalChunks
                                && submittedChunks - completedChunks < GenerateDbWriterBatch.MAX_IN_FLIGHT_CHUNKS) {
                            nextChunkStart = submitGenerateDbTokenChunk(chunkService, obfTokens, nextChunkStart, metrics);
                            submittedChunks++;
                        }
                        long obfId = obfIds.get(chunk.obfIndex());
                        GenerateDbObfState chunkState = progressStates.get(chunk.obfIndex());
                        for (GenerateDbTokenObjects tokenObjects : chunk.tokens()) {
                            IndexToken token = tokenObjects.token();
                            if (token == null || Algorithms.isEmpty(token.name())) {
                                incrementGenerateDbProgress(chunkState, progressListener, chunk, totalObfs, progressStates);
                                continue;
                            }
                            long insertStartNs = System.nanoTime();
                            long tokenId = upsertGenerateDbToken(insertToken, selectTokenId, tokenIds, token);
                            metrics.tokenDbNs.addAndGet(System.nanoTime() - insertStartNs);
                            writerBatch.recordRows(1, estimateGenerateDbTokenBytes(token));
                            for (ObjectAddress objectAddress : tokenObjects.objectsPage().content()) {
                                if (objectAddress == null || !objectAddress.isMatched()) {
                                    continue;
                                }
                                if (objectAddress.osmId() == null) {
                                    skippedWithoutOsmIds.get(chunk.obfIndex()).incrementAndGet();
                                    continue;
                                }
                                long objectStartNs = System.nanoTime();
                                boolean insertedObject = insertGenerateDbObject(insertObject, objectAddress, options.skipObjectTags());
                                metrics.objectDbNs.addAndGet(System.nanoTime() - objectStartNs);
                                long tagStartNs = System.nanoTime();
                                List<GenerateDbObjectTagValue> addressTagValues = insertGenerateDbTags(insertTag, selectTagId, insertValue, selectValueId, tagIds, valueIds, sqlBatcher, objectAddress);
                                metrics.tagDbNs.addAndGet(System.nanoTime() - tagStartNs);
                                if (insertedObject) {
                                    writerBatch.recordRows(1 + addressTagValues.size(), estimateGenerateDbObjectBytes(objectAddress));
                                }
                                long generatedRows = insertGenerateDbGeneratedTokenRows(insertToken, selectTokenId, insertObjectTagValue,
                                        insertPosting, tokenIds, sqlBatcher, obfId, tokenId, token.name(), objectAddress, addressTagValues,
                                        options.skipNewTokens(), options.skipTokenInTagValue());
                                writerBatch.recordRows(generatedRows, generatedRows * 64L);
                                objectCounts.get(chunk.obfIndex()).incrementAndGet();
                                long postingStartNs = System.nanoTime();
                                insertGenerateDbObfPosting(insertPosting, sqlBatcher, obfId, tokenId, objectAddress);
                                flushGenerateDbSqlBatchesIfNeeded(insertObjectTagValue, insertPosting, sqlBatcher, metrics);
                                metrics.postingDbNs.addAndGet(System.nanoTime() - postingStartNs);
                                writerBatch.recordRows(1, 64L);
                            }
                            incrementGenerateDbProgress(chunkState, progressListener, chunk, totalObfs, progressStates);
                            flushGenerateDbSqlBatchesIfNeeded(insertObjectTagValue, insertPosting, sqlBatcher, metrics);
                            writerBatch.commitIfNeeded(Math.max(0, submittedChunks - completedChunks));
                            if (chunkState != null && (chunkState.processedTokens % 10000 == 0 || chunkState.processedTokens == chunkState.totalTokens)) {
                                writerBatch.reportProgress();
                            }
                        }
                    }
                }
                flushGenerateDbSqlBatches(insertObjectTagValue, insertPosting, sqlBatcher, metrics);
                writerBatch.commitFinal();
                buildGenerateDbPostLoadIndexes(conn);
                for (GenerateDbObfState state : progressStates.values()) {
                    state.markDone();
                    notifyGenerateDbProgress(progressListener, "RUNNING", state.obfName, state.obfIndex, totalObfs,
                            state.processedTokens, state.totalTokens, state.startMs, null, progressStates);
                    getLogger().info("generateDb: completed {} objects for OBF {}", objectCounts.get(state.obfIndex).get(), state.obfName);
                    int skipped = skippedWithoutOsmIds.get(state.obfIndex).get();
                    if (skipped > 0) {
                        getLogger().info("generateDb: skipped {} matched objects without OSM ID for OBF {}", skipped, state.obfName);
                    }
                }
                getLogger().info("generateDb: final counters total_time_ms={} token_load_ms={} object_chunk_load_ms={} address_chunk_load_ms={} poi_chunk_load_ms={} reader_wait_ms={} token_db_ms={} object_db_json_ms={} tag_flatten_db_ms={} posting_db_ms={} batch_db_ms={} value_rows={} object_tag_value_rows={} posting_rows={} poi_boxes={} poi_raw_objects={} poi_candidate_checks={} poi_matches={} commit_ms={} commits={} rows={} queue_depth_avg={} queue_depth_max={} commit_latency_p50={} ms commit_latency_p95={} ms max_memory_usage={} MB",
                        metrics.elapsedMs(), metrics.nsToMs(metrics.tokenLoadNs.get()), metrics.nsToMs(metrics.objectChunkLoadNs.get()),
                        metrics.nsToMs(metrics.addressChunkLoadNs.get()), metrics.nsToMs(metrics.poiChunkLoadNs.get()),
                        metrics.nsToMs(metrics.readerWaitNs.get()), metrics.nsToMs(metrics.tokenDbNs.get()),
                        metrics.nsToMs(metrics.objectDbNs.get()), metrics.nsToMs(metrics.tagDbNs.get()),
                        metrics.nsToMs(metrics.postingDbNs.get()), metrics.nsToMs(metrics.batchDbNs.get()),
                        sqlBatcher.totalValueRows, sqlBatcher.totalObjectTagValueRows, sqlBatcher.totalPostingRows,
                        metrics.poiBoxes.get(), metrics.poiRawObjects.get(), metrics.poiCandidateChecks.get(), metrics.poiMatches.get(),
                        metrics.nsToMs(metrics.commitNs.get()),
                        writerBatch.commitCount(), writerBatch.totalRows(), writerBatch.averageQueueDepth(), writerBatch.queueDepthMax(),
                        writerBatch.percentileLatency(50), writerBatch.percentileLatency(95), writerBatch.usedMemoryMb());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while generating SQLite DB", e);
            } finally {
                executor.shutdownNow();
            }
        }
    }

    private long estimateGenerateDbTokenBytes(IndexToken token) {
        return 64L + safeString(token.name()).length() * 2L;
    }

    private long estimateGenerateDbObjectBytes(ObjectAddress objectAddress) {
        long bytes = 192L + safeString(objectAddress.name()).length() * 2L
                + safeString(objectAddress.type()).length() * 2L
                + safeString(objectAddress.osmType()).length() * 2L;
        bytes += estimateTagMapBytes(objectAddress.commonTags());
        return bytes;
    }

    private long estimateTagMapBytes(Map<String, ?> tags) {
        if (tags == null || tags.isEmpty()) {
            return 0;
        }
        long bytes = 0;
        for (Map.Entry<String, ?> entry : tags.entrySet()) {
            bytes += safeString(entry.getKey()).length() * 2L + estimateObjectBytes(entry.getValue());
        }
        return bytes;
    }

    private long estimateObjectBytes(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Map<?, ?> map) {
            long bytes = 64;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                bytes += estimateObjectBytes(entry.getKey()) + estimateObjectBytes(entry.getValue());
            }
            return bytes;
        }
        if (value instanceof Iterable<?> values) {
            long bytes = 64;
            for (Object item : values) {
                bytes += estimateObjectBytes(item);
            }
            return bytes;
        }
        if (value.getClass().isArray()) {
            long bytes = 64;
            int length = java.lang.reflect.Array.getLength(value);
            for (int i = 0; i < length; i++) {
                bytes += estimateObjectBytes(java.lang.reflect.Array.get(value, i));
            }
            return bytes;
        }
        return 32L + String.valueOf(value).length() * 2L;
    }

    private ExecutorService createGenerateDbExecutor() {
        int maxCount = Algorithms.parseIntSilently(System.getenv("MAX_THREAD_NUMBER"),
                Math.max(1, Runtime.getRuntime().availableProcessors()));
        maxCount = Math.max(1, maxCount);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r);
            t.setName("search-test-generate-db-" + t.getId());
            t.setDaemon(true);
            return t;
        };
        return new ThreadPoolExecutor(maxCount, maxCount, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(GenerateDbWriterBatch.QUEUE_CAPACITY), tf, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private int submitGenerateDbTokenChunk(CompletionService<GenerateDbTokenChunk> chunkService,
                                           GenerateDbObfTokens obfTokens,
                                           int start,
                                           GenerateDbMetrics metrics) {
        int to = Math.min(start + GenerateDbWriterBatch.TOKEN_CHUNK_SIZE, obfTokens.tokens().size());
        List<IndexToken> tokenChunk = obfTokens.tokens().subList(start, to);
        chunkService.submit(() -> {
            GenerateDbMetrics.setCurrent(metrics);
            try {
                return loadGenerateDbTokenChunk(obfTokens, tokenChunk);
            } finally {
                GenerateDbMetrics.clearCurrent();
            }
        });
        return to;
    }

    private GenerateDbTokenChunk loadGenerateDbTokenChunk(GenerateDbObfTokens obfTokens, List<IndexToken> tokens) {
        long startMs = System.currentTimeMillis();
        long startNs = System.nanoTime();
        Map<IndexToken, List<ObjectAddress>> tokenObjects = new LinkedHashMap<>();
        for (IndexToken token : tokens) {
            tokenObjects.put(token, new ArrayList<>());
        }
        File file = new File(obfTokens.obf());
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsolutePath(), "r")) {
            BinaryMapIndexReaderExt index = new BinaryMapIndexReaderExt(randomAccessFile, file);
            try {
                GenerateDbObfReader reader = new GenerateDbObfReader(index);
                loadGenerateDbAddressObjects(reader, tokens, tokenObjects);
                loadGenerateDbPoiObjects(reader, tokens, tokenObjects);
            } finally {
                index.close();
            }
        } catch (Exception e) {
            getLogger().error("Failed to read OBF object chunk {}", file, e);
            throw new RuntimeException("Failed to read OBF object chunk: " + e.getMessage(), e);
        }
        long elapsedMs = System.currentTimeMillis() - startMs;
        GenerateDbMetrics.current().objectChunkLoadNs.addAndGet(System.nanoTime() - startNs);
        if (elapsedMs > 5_000) {
            getLogger().info("generateDb: loaded object chunk with {} tokens for OBF {} in {} ms", tokens.size(), obfTokens.obfName(), elapsedMs);
        }
        List<GenerateDbTokenObjects> result = new ArrayList<>(tokens.size());
        for (IndexToken token : tokens) {
            List<ObjectAddress> objects = tokenObjects.getOrDefault(token, Collections.emptyList());
            if (token == null || Algorithms.isEmpty(token.name()) || objects.isEmpty()) {
                result.add(new GenerateDbTokenObjects(obfTokens.obf(), obfTokens.obfName(), obfTokens.obfIndex(), obfTokens.startMs(), token,
                        new ObjectAddressPage(Collections.emptyList(), 0, 0, 0, 0, new int[0], new int[0], 0, 0)));
                continue;
            }
            List<ObjectAddress> numberedObjects = assignObjectSequenceIds(objects);
            numberedObjects = markAloneObjects(numberedObjects, token);
            result.add(new GenerateDbTokenObjects(obfTokens.obf(), obfTokens.obfName(), obfTokens.obfIndex(), obfTokens.startMs(), token,
                    new ObjectAddressPage(numberedObjects, 0, Integer.MAX_VALUE, numberedObjects.size(), 1, new int[0], new int[0], 0, 0)));
        }
        return new GenerateDbTokenChunk(obfTokens.obf(), obfTokens.obfName(), obfTokens.obfIndex(), obfTokens.startMs(), result);
    }

    private void loadGenerateDbAddressObjects(GenerateDbObfReader reader,
                                              List<IndexToken> tokens,
                                              Map<IndexToken, List<ObjectAddress>> tokenObjects) throws IOException {
        long startNs = System.nanoTime();
        try {
            for (BinaryMapAddressReaderAdapter.AddressRegion region : reader.addressRegions()) {
                Map<Integer, List<IndexToken>> cityTokens = new TreeMap<>();
                Map<Integer, List<AddressTokenRef>> streetTokens = new TreeMap<>();
                for (IndexToken token : tokens) {
                    if (token == null || token.addressRefs() == null) {
                        continue;
                    }
                    for (AddressRef ref : token.addressRefs()) {
                        if (ref == null || !isOffsetWithinPart(ref.objectOffset(), region)) {
                            continue;
                        }
                        if (ref.typeIndex() == BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index) {
                            streetTokens.computeIfAbsent(ref.objectOffset(), ignored -> new ArrayList<>())
                                    .add(new AddressTokenRef(token, ref));
                        } else if (ref.typeIndex() < BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index) {
                            cityTokens.computeIfAbsent(ref.objectOffset(), ignored -> new ArrayList<>()).add(token);
                        }
                    }
                }
                for (Map.Entry<Integer, List<IndexToken>> entry : cityTokens.entrySet()) {
                    ObjectAddress objectAddress = reader.addressObjectCache.computeIfAbsent(entry.getKey(), offset -> {
                        try {
                            return loadCityGenerateDbObjectAddress(reader.index(), region, offset, "en");
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
                    if (objectAddress != null) {
                        for (IndexToken token : entry.getValue()) {
                            tokenObjects.get(token).add(objectAddress);
                        }
                    }
                }
                TreeSet<Integer> cityOffsets = new TreeSet<>();
                for (List<AddressTokenRef> refs : streetTokens.values()) {
                    for (AddressTokenRef ref : refs) {
                        if (ref.ref().cityOffset() > 0) {
                            cityOffsets.add(ref.ref().cityOffset());
                        }
                    }
                }
                for (Integer cityOffset : cityOffsets) {
                    if (!reader.cityCache.containsKey(cityOffset)) {
                        reader.cityCache.put(cityOffset, loadCity(reader.index(), region, cityOffset));
                    }
                }
                for (Map.Entry<Integer, List<AddressTokenRef>> entry : streetTokens.entrySet()) {
                    AddressRef firstRef = entry.getValue().get(0).ref();
                    ObjectAddress objectAddress = reader.addressObjectCache.computeIfAbsent(entry.getKey(), offset -> {
                        try {
                            return loadStreetGenerateDbObjectAddress(reader.index(), region, offset, reader.cityCache.get(firstRef.cityOffset()), "en");
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
                    if (objectAddress != null) {
                        for (AddressTokenRef tokenRef : entry.getValue()) {
                            tokenObjects.get(tokenRef.token()).add(objectAddress);
                        }
                    }
                }
            }
        } finally {
            GenerateDbMetrics.current().addressChunkLoadNs.addAndGet(System.nanoTime() - startNs);
        }
    }

    private void loadGenerateDbPoiObjects(GenerateDbObfReader reader,
                                          List<IndexToken> tokens,
                                          Map<IndexToken, List<ObjectAddress>> tokenObjects) throws IOException {
        long startNs = System.nanoTime();
        try {
            for (BinaryMapPoiReaderAdapter.PoiRegion poiRegion : reader.poiRegions()) {
                Map<Integer, List<IndexToken>> tokensByOffset = new TreeMap<>();
                for (IndexToken token : tokens) {
                    if (token == null || token.poiRefs() == null) {
                        continue;
                    }
                    for (int poiRef : token.poiRefs()) {
                        if (poiRef >= 0 && poiRef < poiRegion.getLength()) {
                            tokensByOffset.computeIfAbsent(poiRef, ignored -> new ArrayList<>()).add(token);
                        }
                    }
                }
                if (tokensByOffset.isEmpty()) {
                    continue;
                }
                reader.index().initCategories(poiRegion);
                Map<Integer, List<TagValuePair>> tagGroups = preloadPoiTagGroups(reader.index().getInputStream(), poiRegion);
                for (Map.Entry<Integer, List<IndexToken>> entry : tokensByOffset.entrySet()) {
                    List<GenerateDbRawPoiObject> rawObjects = readGenerateDbRawPoiObjectsAtShift(reader.index(), poiRegion, tagGroups, entry.getKey());
                    GenerateDbMetrics.current().poiBoxes.incrementAndGet();
                    GenerateDbMetrics.current().poiRawObjects.addAndGet(rawObjects.size());
                    if (rawObjects.isEmpty()) {
                        continue;
                    }
                    Map<IndexToken, CollatorStringMatcher> matchers = new HashMap<>();
                    for (IndexToken token : entry.getValue()) {
                        matchers.put(token, new CollatorStringMatcher(token.name(), CollatorStringMatcher.StringMatcherMode.CHECK_EQUALS_FROM_SPACE));
                    }
                }
            }
        } finally {
            GenerateDbMetrics.current().poiChunkLoadNs.addAndGet(System.nanoTime() - startNs);
        }
    }

    private <T> T getGenerateDbFuture(Future<T> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while generating SQLite DB", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException("Failed to generate SQLite DB: " + cause.getMessage(), cause);
        }
    }

    private void incrementGenerateDbProgress(GenerateDbObfState state,
                                             GenerateDbProgressListener progressListener,
                                             GenerateDbTokenChunk chunk,
                                             int totalObfs,
                                             Map<Integer, GenerateDbObfState> progressStates) {
        if (state == null) {
            return;
        }
        state.processedTokens++;
        if (state.processedTokens >= state.totalTokens) {
            state.markDone();
        }
        notifyGenerateDbProgress(progressListener, "RUNNING", chunk.obfName(), chunk.obfIndex(), totalObfs,
                state.processedTokens, state.totalTokens, state.startMs, null, progressStates);
        if (state.processedTokens % 10000 == 0 || state.processedTokens == state.totalTokens) {
            getLogger().info("generateDb: processed {} of {} tokens for OBF {}", state.processedTokens, state.totalTokens, state.obfName);
        }
    }

    private void notifyGenerateDbProgress(GenerateDbProgressListener progressListener,
                                          String status,
                                          String obfName,
                                          int obfIndex,
                                          int totalObfs,
                                          int processedTokens,
                                          int totalTokens,
                                          long startMs,
                                          String error,
                                          Map<Integer, GenerateDbObfState> progressStates) {
        if (progressListener == null) {
            return;
        }
        long elapsedMs = Math.max(0, System.currentTimeMillis() - startMs);
        long estimatedMs = processedTokens > 0 && totalTokens > 0
                ? Math.max(0, (elapsedMs * totalTokens / processedTokens) - elapsedMs)
                : -1;
        List<GenerateDbObfProgress> obfProgress = new ArrayList<>();
        if (progressStates != null) {
            long now = System.currentTimeMillis();
            for (GenerateDbObfState state : progressStates.values()) {
                long stateElapsedMs = "DONE".equals(state.status) && state.completedElapsedMs >= 0
                        ? state.completedElapsedMs
                        : Math.max(0, now - state.startMs);
                long stateEstimatedMs = state.processedTokens > 0 && state.totalTokens > 0 && !"DONE".equals(state.status)
                        ? Math.max(0, (stateElapsedMs * state.totalTokens / state.processedTokens) - stateElapsedMs)
                        : ("DONE".equals(state.status) ? 0 : -1);
                obfProgress.add(new GenerateDbObfProgress(state.obfName, state.obfIndex, state.totalTokens,
                        state.processedTokens, stateElapsedMs, stateEstimatedMs, state.status));
            }
        }
        progressListener.onProgress(new GenerateDbProgress(status, obfName, obfIndex, totalObfs,
                processedTokens, totalTokens, elapsedMs, estimatedMs, error, obfProgress));
    }

    private long insertObfRow(PreparedStatement insertObf, String name) throws SQLException {
        insertObf.setString(1, name);
        insertObf.executeUpdate();
        try (java.sql.ResultSet rs = insertObf.getGeneratedKeys()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        throw new SQLException("Failed to insert OBF row");
    }

    private List<IndexToken> loadAllGenerateDbTokens(String obf) {
        List<IndexToken> tokens = new ArrayList<>();
        int page = 0;
        int pageSize = 100;
        while (true) {
            IndexTokenPage tokenPage = getIndex(obf, null, page, pageSize, "name", "asc");
            tokens.addAll(tokenPage.content());
            if (page + 1 >= tokenPage.totalPages()) {
                break;
            }
            page++;
        }
        return tokens;
    }

    private long upsertGenerateDbToken(PreparedStatement insertToken,
                                       PreparedStatement selectTokenId,
                                       Map<String, Long> tokenIds,
                                       IndexToken token) throws SQLException {
        return upsertGenerateDbToken(insertToken, selectTokenId, tokenIds, token.name(),
                token.isCommon(), token.isFrequent(), false);
    }

    private long upsertGenerateDbToken(PreparedStatement insertToken,
                                       PreparedStatement selectTokenId,
                                       Map<String, Long> tokenIds,
                                       String tokenName,
                                       boolean isCommon,
                                       boolean isFrequent,
                                       boolean isGenerated) throws SQLException {
        insertToken.setString(1, tokenName);
        insertToken.setInt(2, isCommon ? 1 : 0);
        insertToken.setInt(3, isFrequent ? 1 : 0);
        insertToken.setInt(4, isGenerated ? 1 : 0);
        insertToken.executeUpdate();
        Long cachedId = tokenIds.get(tokenName);
        if (cachedId != null) {
            return cachedId;
        }
        selectTokenId.setString(1, tokenName);
        try (java.sql.ResultSet rs = selectTokenId.executeQuery()) {
            if (rs.next()) {
                long id = rs.getLong(1);
                tokenIds.put(tokenName, id);
                return id;
            }
        }
        throw new SQLException("Failed to resolve token id for " + tokenName);
    }

    private boolean insertGenerateDbObject(PreparedStatement insertObject, ObjectAddress objectAddress,
                                           boolean skipObjectTags) throws SQLException, IOException {
        LatLon point = objectAddress.point();
        insertObject.setLong(1, objectAddress.osmId());
        insertObject.setString(2, objectAddress.name());
        if (point == null) {
            insertObject.setNull(3, java.sql.Types.REAL);
            insertObject.setNull(4, java.sql.Types.REAL);
        } else {
            insertObject.setDouble(3, point.getLatitude());
            insertObject.setDouble(4, point.getLongitude());
        }
        if (skipObjectTags) {
            insertObject.setNull(5, java.sql.Types.VARCHAR);
        } else {
            insertObject.setString(5, getObjectMapper().writeValueAsString(objectAddress.commonTags()));
        }
        insertObject.setString(6, objectAddress.type());
        insertObject.setString(7, objectAddress.osmType());
        return insertObject.executeUpdate() > 0;
    }

    private List<GenerateDbObjectTagValue> insertGenerateDbTags(PreparedStatement insertTag,
                                                                PreparedStatement selectTagId,
                                                                PreparedStatement insertValue,
                                                                PreparedStatement selectValueId,
                                                                Map<String, Long> tagIds,
                                                                Map<String, Long> valueIds,
                                                                GenerateDbSqlBatcher sqlBatcher,
                                                                ObjectAddress objectAddress) throws SQLException {
        Map<String, List<String>> commonTags = flattenTags(objectAddress.commonTags());
        Set<GenerateDbObjectTagValue> result = new LinkedHashSet<>();
        insertGenerateDbTags(insertTag, selectTagId, insertValue, selectValueId, tagIds, valueIds, sqlBatcher, result, commonTags,
                "common", Collections.emptySet(), objectAddress.isPoi());
        return new ArrayList<>(result);
    }

    private void insertGenerateDbTags(PreparedStatement insertTag,
                                      PreparedStatement selectTagId,
                                      PreparedStatement insertValue,
                                      PreparedStatement selectValueId,
                                      Map<String, Long> tagIds,
                                      Map<String, Long> valueIds,
                                      GenerateDbSqlBatcher sqlBatcher,
                                      Set<GenerateDbObjectTagValue> result,
                                      Map<String, List<String>> tags,
                                      String type,
                                      Set<String> duplicateNames,
                                      boolean isPoi) throws SQLException {
        if (tags == null || tags.isEmpty()) {
            return;
        }
        for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
            String tag = entry.getKey();
            if (Algorithms.isEmpty(tag) || entry.getValue() == null || entry.getValue().isEmpty()) {
                continue;
            }
            String storedTag = duplicateNames != null && duplicateNames.contains(tag) ? type + "." + tag : tag;
            Map<String, Set<String>> valueTokens = collectGenerateDbTagValueTokens(isPoi, tag, entry.getValue());
            boolean tagSkipped = valueTokens.isEmpty();
            long tagId = upsertGenerateDbTag(insertTag, selectTagId, tagIds, storedTag, type, tagSkipped);
            for (String value : entry.getValue()) {
                if (Algorithms.isEmpty(value)) {
                    continue;
                }
                long valueId = upsertGenerateDbValue(insertValue, selectValueId, valueIds, sqlBatcher, tagId, value);
                result.add(new GenerateDbObjectTagValue(tagId, valueId, value,
                        valueTokens.getOrDefault(value, Collections.emptySet())));
                break;
            }
        }
    }

    private long upsertGenerateDbValue(PreparedStatement insertValue,
                                       PreparedStatement selectValueId,
                                       Map<String, Long> valueIds,
                                       GenerateDbSqlBatcher sqlBatcher,
                                       long tagId,
                                       String value) throws SQLException {
        String key = tagId + "\u0000" + value;
        Long cached = valueIds.get(key);
        if (cached != null) {
            return cached;
        }
        insertValue.setLong(1, tagId);
        insertValue.setString(2, value);
        insertValue.executeUpdate();
        sqlBatcher.totalValueRows++;
        selectValueId.setLong(1, tagId);
        selectValueId.setString(2, value);
        try (ResultSet rs = selectValueId.executeQuery()) {
            if (rs.next()) {
                long id = rs.getLong(1);
                valueIds.put(key, id);
                return id;
            }
        }
        throw new SQLException("Failed to resolve value id for tag " + tagId + " value " + value);
    }

    private long upsertGenerateDbTag(PreparedStatement insertTag,
                                     PreparedStatement selectTagId,
                                     Map<String, Long> tagIds,
                                     String name,
                                     String type,
                                     boolean isSkipped) throws SQLException {
        String key = name + "\u0000" + type;
        Long cached = tagIds.get(key);
        insertTag.setString(1, name);
        insertTag.setString(2, type);
        insertTag.setInt(3, isSkipped ? 1 : 0);
        insertTag.executeUpdate();
        if (cached != null) {
            return cached;
        }
        selectTagId.setString(1, name);
        selectTagId.setString(2, type);
        try (ResultSet rs = selectTagId.executeQuery()) {
            if (rs.next()) {
                long id = rs.getLong(1);
                tagIds.put(key, id);
                return id;
            }
        }
        throw new SQLException("Failed to resolve tag id for " + name);
    }

    private void insertGenerateDbObjectTagValues(PreparedStatement insertObjectTagValue,
                                                 GenerateDbSqlBatcher sqlBatcher,
                                                 long objectId,
                                                 List<GenerateDbObjectTagValue> tagValues,
                                                 long tokenId,
                                                 String tokenName) throws SQLException {
        insertGenerateDbObjectTagValues(insertObjectTagValue, sqlBatcher, objectId, tagValues, tokenId, tokenName, false);
    }

    private void insertGenerateDbObjectTagValues(PreparedStatement insertObjectTagValue,
                                                 GenerateDbSqlBatcher sqlBatcher,
                                                 long objectId,
                                                 List<GenerateDbObjectTagValue> tagValues,
                                                 long tokenId,
                                                 String tokenName,
                                                 boolean includeAllForSkippedToken) throws SQLException {
        if (tagValues == null || tagValues.isEmpty()) {
            return;
        }
        for (GenerateDbObjectTagValue tagValue : tagValues) {
            if (tagValue == null) {
                continue;
            }
            if (tokenId >= 0 && !tagValue.tokens().contains(tokenName)) {
                continue;
            }
            if (tokenId < 0 && !includeAllForSkippedToken && !tagValue.tokens().isEmpty()) {
                continue;
            }
            insertObjectTagValue.setLong(1, objectId);
            insertObjectTagValue.setLong(2, tagValue.tagId());
            insertObjectTagValue.setLong(3, tokenId);
            insertObjectTagValue.setLong(4, tagValue.valueId());
            insertObjectTagValue.setString(5, tagValue.value());
            insertObjectTagValue.addBatch();
            sqlBatcher.objectTagValueRows++;
            sqlBatcher.totalObjectTagValueRows++;
        }
    }

    private long insertGenerateDbGeneratedTokenRows(PreparedStatement insertToken,
                                                    PreparedStatement selectTokenId,
                                                    PreparedStatement insertObjectTagValue,
                                                    PreparedStatement insertPosting,
                                                    Map<String, Long> tokenIds,
                                                    GenerateDbSqlBatcher sqlBatcher,
                                                    long obfId,
                                                    long currentTokenId,
                                                    String currentTokenName,
                                                    ObjectAddress objectAddress,
                                                    List<GenerateDbObjectTagValue> tagValues,
                                                    boolean skipNewTokens,
                                                    boolean skipTokenInTagValue) throws SQLException {
        if (objectAddress == null || objectAddress.osmId() == null || tagValues == null || tagValues.isEmpty()) {
            return 0;
        }
        long rows = 0;
        Set<String> objectTokens = new LinkedHashSet<>();
        for (GenerateDbObjectTagValue tagValue : tagValues) {
            objectTokens.addAll(tagValue.tokens());
        }
        for (String tokenName : objectTokens) {
            if (Algorithms.isEmpty(tokenName)) {
                continue;
            }
            boolean indexedToken = tokenName.equals(currentTokenName);
            if (!indexedToken && skipNewTokens) {
                continue;
            }
            long tokenId = indexedToken ? currentTokenId
                    : upsertGenerateDbToken(insertToken, selectTokenId, tokenIds, tokenName, false, false, true);
            if (!skipTokenInTagValue) {
                insertGenerateDbObjectTagValues(insertObjectTagValue, sqlBatcher, objectAddress.osmId(), tagValues, tokenId, tokenName);
            }
            rows++;
            if (!indexedToken) {
                insertGenerateDbObfPosting(insertPosting, sqlBatcher, obfId, tokenId, objectAddress, false);
                rows++;
            }
        }
        insertGenerateDbObjectTagValues(insertObjectTagValue, sqlBatcher, objectAddress.osmId(), tagValues, -1, null, skipTokenInTagValue);
        return rows;
    }

    private Map<String, List<String>> flattenTags(Map<String, ?> tags) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        if (tags == null || tags.isEmpty()) {
            return result;
        }
        for (Map.Entry<String, ?> entry : tags.entrySet()) {
            collectFlattenedTag(result, entry.getKey(), entry.getValue());
        }
        return result;
    }

    private void collectFlattenedTag(Map<String, List<String>> result, String tag, Object value) {
        if (Algorithms.isEmpty(tag) || value == null) {
            return;
        }
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() != null) {
                    collectFlattenedTag(result, tag + "." + entry.getKey(), entry.getValue());
                }
            }
        } else if (value instanceof Iterable<?> values) {
            for (Object item : values) {
                collectFlattenedTag(result, tag, item);
            }
        } else if (value.getClass().isArray()) {
            int length = java.lang.reflect.Array.getLength(value);
            for (int i = 0; i < length; i++) {
                collectFlattenedTag(result, tag, java.lang.reflect.Array.get(value, i));
            }
        } else {
            String stringValue = safeString(String.valueOf(value));
            if (!Algorithms.isEmpty(stringValue)) {
                result.computeIfAbsent(tag, ignored -> new ArrayList<>()).add(stringValue);
            }
        }
    }

    private Map<String, Set<String>> collectGenerateDbTagValueTokens(boolean isPoi, String tag, List<String> values) {
        Map<String, Set<String>> result = new LinkedHashMap<>();
        if (Algorithms.isEmpty(tag) || values == null || values.isEmpty() || !isGenerateDbSearchIndexedTag(isPoi, tag)) {
            return result;
        }
        for (String value : values) {
            if (Algorithms.isEmpty(value)) {
                continue;
            }
            Set<String> tokens = new LinkedHashSet<>();
            if (isPoi && Amenity.ROUTE_MEMBERS_IDS.equals(tag)) {
                for (String id : value.split(" ")) {
                    addGenerateDbSplitTokens(tokens, id, true);
                }
            } else {
                addGenerateDbSplitTokens(tokens, value, isPoi);
            }
            if (!tokens.isEmpty()) {
                result.put(value, tokens);
            }
        }
        return result;
    }

    private boolean isGenerateDbSearchIndexedTag(boolean isPoi, String tag) {
        if (Algorithms.isEmpty(tag)) {
            return false;
        }
        if (Amenity.NAME.equals(tag) || "name:en".equals(tag) || tag.startsWith("name:") || tag.startsWith("name_")) {
            return true;
        }
        if (isPoi) {
            return isTagIndexedForSearchAsName(tag) || isTagIndexedForSearchAsId(tag)
                    || isTagIndexedAsSearchRelated(tag) || Amenity.ROUTE_MEMBERS_IDS.equals(tag);
        }
        return "place".equals(tag) || isTagIndexedForSearchAsName(tag) || isTagIndexedForSearchAsId(tag);
    }

    private void addGenerateDbSplitTokens(Set<String> tokens, String value, boolean isPoi) {
        if (Algorithms.isEmpty(value)) {
            return;
        }
        String preparedValue = isPoi ? value : removeGenerateDbAddressBraces(value);
        List<String> splitValues = SearchAlgorithms.splitAndNormalize(preparedValue, true);
        SearchAlgorithms.removeCommonWords(CommonWords.getInstance(), splitValues);
        for (String token : splitValues) {
            if (!Algorithms.isEmpty(token)) {
                tokens.add(token);
            }
        }
    }

    private String removeGenerateDbAddressBraces(String name) {
        if (name == null) {
            return null;
        }
        int i = name.indexOf('(');
        String retName = name;
        if (i > -1) {
            retName = name.substring(0, i);
            int j = name.indexOf(')', i);
            if (j > -1) {
                retName = retName.trim() + ' ' + name.substring(j + 1).trim();
            }
        }
        return retName;
    }

    private void insertGenerateDbObfPosting(PreparedStatement insertObfPosting,
                                            GenerateDbSqlBatcher sqlBatcher,
                                            long obfId,
                                            long tokenId,
                                            ObjectAddress objectAddress) throws SQLException {
        insertGenerateDbObfPosting(insertObfPosting, sqlBatcher, obfId, tokenId, objectAddress,
                objectAddress != null && objectAddress.isAlone());
    }

    private void insertGenerateDbObfPosting(PreparedStatement insertObfPosting,
                                            GenerateDbSqlBatcher sqlBatcher,
                                            long obfId,
                                            long tokenId,
                                            ObjectAddress objectAddress,
                                            boolean isAlone) throws SQLException {
        insertObfPosting.setLong(1, obfId);
        insertObfPosting.setLong(2, tokenId);
        insertObfPosting.setLong(3, objectAddress.osmId());
        insertObfPosting.setInt(4, objectAddress.sequenceId());
        insertObfPosting.setInt(5, isAlone ? 1 : 0);
        insertObfPosting.addBatch();
        sqlBatcher.postingRows++;
        sqlBatcher.totalPostingRows++;
    }

    private void flushGenerateDbSqlBatchesIfNeeded(PreparedStatement insertObjectTagValue,
                                                   PreparedStatement insertPosting,
                                                   GenerateDbSqlBatcher sqlBatcher,
                                                   GenerateDbMetrics metrics) throws SQLException {
        if (sqlBatcher.pendingRows() >= GenerateDbSqlBatcher.FLUSH_ROWS) {
            flushGenerateDbSqlBatches(insertObjectTagValue, insertPosting, sqlBatcher, metrics);
        }
    }

    private void flushGenerateDbSqlBatches(PreparedStatement insertObjectTagValue,
                                           PreparedStatement insertPosting,
                                           GenerateDbSqlBatcher sqlBatcher,
                                           GenerateDbMetrics metrics) throws SQLException {
        if (sqlBatcher.pendingRows() == 0) {
            return;
        }
        long startNs = System.nanoTime();
        if (sqlBatcher.objectTagValueRows > 0) {
            insertObjectTagValue.executeBatch();
            sqlBatcher.objectTagValueRows = 0;
        }
        if (sqlBatcher.postingRows > 0) {
            insertPosting.executeBatch();
            sqlBatcher.postingRows = 0;
        }
        metrics.batchDbNs.addAndGet(System.nanoTime() - startNs);
    }

    default void createTagsDatasource(String name, List<String> obfs, boolean overwrite, GenerateDbProgressListener progressListener,
                                      GenerateDbOptions options) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(name);
        if (Files.exists(dbFile) && !overwrite) {
            throw new IOException("Datasource already exists: " + dbFile.getFileName());
        }
        generateDbFile(obfs, dbFile, progressListener, options);
    }

    record GenerateDbOptions(boolean skipObjectTags, boolean skipNewTokens, boolean skipTokenInTagValue) {
        static final GenerateDbOptions DEFAULT = new GenerateDbOptions(false, false, false);
    }




    record GenerateDbObjectTagValue(long tagId, long valueId, String value, Set<String> tokens) {
    }


    class GenerateDbObfState {
        final String obfName;
        final int obfIndex;
        long startMs;
        long completedElapsedMs = -1;
        int totalTokens;
        int processedTokens;
        String status = "PENDING";

        GenerateDbObfState(String obfName, int obfIndex, long startMs) {
            this.obfName = obfName;
            this.obfIndex = obfIndex;
            this.startMs = startMs;
        }

        void markDone() {
            if (!"DONE".equals(status)) {
                completedElapsedMs = Math.max(0, System.currentTimeMillis() - startMs);
            }
            status = "DONE";
        }
    }

    class GenerateDbObfReader {
        private final BinaryMapIndexReaderExt index;
        private final List<BinaryMapAddressReaderAdapter.AddressRegion> addressRegions = new ArrayList<>();
        private final List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions = new ArrayList<>();
        private final Map<Integer, City> cityCache = new HashMap<>();
        private final Map<Integer, ObjectAddress> addressObjectCache = new HashMap<>();

        GenerateDbObfReader(BinaryMapIndexReaderExt index) {
            this.index = index;
            for (BinaryIndexPart part : index.getIndexes()) {
                if (part instanceof BinaryMapAddressReaderAdapter.AddressRegion addressRegion) {
                    addressRegions.add(addressRegion);
                } else if (part instanceof BinaryMapPoiReaderAdapter.PoiRegion poiRegion) {
                    poiRegions.add(poiRegion);
                }
            }
        }

        BinaryMapIndexReaderExt index() {
            return index;
        }

        List<BinaryMapAddressReaderAdapter.AddressRegion> addressRegions() {
            return addressRegions;
        }

        List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions() {
            return poiRegions;
        }
    }

    class GenerateDbMetrics {
        private static final ThreadLocal<GenerateDbMetrics> CURRENT = new ThreadLocal<>();

        final long startedMs = System.currentTimeMillis();
        final AtomicLong tokenLoadNs = new AtomicLong();
        final AtomicLong objectChunkLoadNs = new AtomicLong();
        final AtomicLong addressChunkLoadNs = new AtomicLong();
        final AtomicLong poiChunkLoadNs = new AtomicLong();
        final AtomicLong readerWaitNs = new AtomicLong();
        final AtomicLong tokenDbNs = new AtomicLong();
        final AtomicLong objectDbNs = new AtomicLong();
        final AtomicLong tagDbNs = new AtomicLong();
        final AtomicLong postingDbNs = new AtomicLong();
        final AtomicLong batchDbNs = new AtomicLong();
        final AtomicLong commitNs = new AtomicLong();
        final AtomicLong poiBoxes = new AtomicLong();
        final AtomicLong poiRawObjects = new AtomicLong();
        final AtomicLong poiCandidateChecks = new AtomicLong();
        final AtomicLong poiMatches = new AtomicLong();

        static void setCurrent(GenerateDbMetrics metrics) {
            CURRENT.set(metrics);
        }

        static GenerateDbMetrics current() {
            GenerateDbMetrics metrics = CURRENT.get();
            if (metrics == null) {
                throw new IllegalStateException("Generate DB metrics are not bound to this thread");
            }
            return metrics;
        }

        static void clearCurrent() {
            CURRENT.remove();
        }

        long elapsedMs() {
            return Math.max(0, System.currentTimeMillis() - startedMs);
        }

        long nsToMs(long ns) {
            return TimeUnit.NANOSECONDS.toMillis(ns);
        }
    }

    class GenerateDbSqlBatcher {
        static final int FLUSH_ROWS = 8_192;
        int objectTagValueRows;
        int postingRows;
        long totalValueRows;
        long totalObjectTagValueRows;
        long totalPostingRows;

        int pendingRows() {
            return objectTagValueRows + postingRows;
        }
    }

    class GenerateDbWriterBatch {
        static final int TOKEN_CHUNK_SIZE = 250;
        static final int QUEUE_CAPACITY = 16;
        static final int MAX_IN_FLIGHT_CHUNKS = 16;
        private static final int INITIAL_TARGET_ROWS = 10_000;
        private static final int MAX_TARGET_ROWS = 100_000;
        private static final long TARGET_BATCH_BYTES = 32L * 1024L * 1024L;
        private static final long FLUSH_TIMEOUT_MS = 10_000L;

        private final Connection conn;
        private final Logger logger;
        private final GenerateDbMetrics metrics;
        private final List<Long> commitLatencies = new ArrayList<>();
        private final long startedMs = System.currentTimeMillis();
        private long transactionStartedMs;
        private long batchRows;
        private long batchBytes;
        private long totalRows;
        private long lastReportRows;
        private long queueDepthSamples;
        private long queueDepthTotal;
        private int queueDepthMax;
        private int targetRows = INITIAL_TARGET_ROWS;

        GenerateDbWriterBatch(Connection conn, Logger logger, GenerateDbMetrics metrics) throws SQLException {
            this.conn = conn;
            this.logger = logger;
            this.metrics = metrics;
            begin();
        }

        void recordRows(long rows, long estimatedBytes) {
            if (rows <= 0) {
                return;
            }
            batchRows += rows;
            totalRows += rows;
            batchBytes += Math.max(0, estimatedBytes);
        }

        void commitIfNeeded(int queueDepth) throws SQLException {
            recordQueueDepth(queueDepth);
            long now = System.currentTimeMillis();
            if (batchRows >= targetRows || batchBytes >= TARGET_BATCH_BYTES || now - transactionStartedMs >= FLUSH_TIMEOUT_MS) {
                commit(false);
            }
        }

        void commitFinal() throws SQLException {
            commit(true);
            reportProgress();
        }

        void reportProgress() {
            if (totalRows == lastReportRows) {
                return;
            }
            lastReportRows = totalRows;
            logger.info("generateDb: writer_records_per_sec={} queue_depth_avg={} queue_depth_max={} batch_rows={} batch_bytes_estimate={} target_rows={} max_memory_usage={} MB",
                    recordsPerSecond(totalRows, startedMs), averageQueueDepth(), queueDepthMax, batchRows, batchBytes, targetRows, usedMemoryMb());
        }

        private void begin() throws SQLException {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("BEGIN IMMEDIATE");
            }
            transactionStartedMs = System.currentTimeMillis();
            batchRows = 0;
            batchBytes = 0;
        }

        private void commit(boolean force) throws SQLException {
            if (batchRows == 0 && !force) {
                return;
            }
            long rows = batchRows;
            long started = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("COMMIT");
            }
            long latency = System.currentTimeMillis() - started;
            metrics.commitNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(latency));
            if (rows > 0) {
                commitLatencies.add(latency);
                tuneTargetRows(rows, latency);
            }
            if (!force) {
                begin();
            } else {
                logger.info("generateDb: final writer summary total_load_time={} ms writer_records_per_sec={} queue_depth_avg={} queue_depth_max={} commit_latency_p50={} ms commit_latency_p95={} ms max_memory_usage={} MB",
                        Math.max(0, System.currentTimeMillis() - startedMs), recordsPerSecond(totalRows, startedMs),
                        averageQueueDepth(), queueDepthMax, percentileLatency(50), percentileLatency(95), usedMemoryMb());
            }
        }

        private void tuneTargetRows(long rows, long latencyMs) {
            if (latencyMs < 1_000 && rows >= targetRows && targetRows < MAX_TARGET_ROWS) {
                targetRows = Math.min(MAX_TARGET_ROWS, targetRows * 2);
            } else if (latencyMs > 5_000 && targetRows > INITIAL_TARGET_ROWS) {
                targetRows = Math.max(INITIAL_TARGET_ROWS, targetRows / 2);
            }
        }

        private void recordQueueDepth(int queueDepth) {
            queueDepthSamples++;
            queueDepthTotal += Math.max(0, queueDepth);
            queueDepthMax = Math.max(queueDepthMax, queueDepth);
        }

        private long recordsPerSecond(long records, long startMs) {
            long elapsedMs = Math.max(1, System.currentTimeMillis() - startMs);
            return records * 1000L / elapsedMs;
        }

        long totalRows() {
            return totalRows;
        }

        int commitCount() {
            return commitLatencies.size();
        }

        long averageQueueDepth() {
            return queueDepthSamples == 0 ? 0 : queueDepthTotal / queueDepthSamples;
        }

        int queueDepthMax() {
            return queueDepthMax;
        }

        long percentileLatency(int percentile) {
            if (commitLatencies.isEmpty()) {
                return 0;
            }
            List<Long> sorted = new ArrayList<>(commitLatencies);
            Collections.sort(sorted);
            int index = Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(sorted.size() * percentile / 100.0) - 1));
            return sorted.get(index);
        }

        long usedMemoryMb() {
            Runtime runtime = Runtime.getRuntime();
            return (runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L);
        }
    }
}
