package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.data.LatLon;
import net.osmand.server.api.entity.Dataset;
import net.osmand.server.api.entity.EvalJob;
import net.osmand.server.api.entity.DatasetType;
import net.osmand.server.api.entity.JobStatus;
import net.osmand.server.api.repo.DatasetJobRepository;
import net.osmand.server.api.repo.DatasetRepository;
import net.osmand.server.api.dto.JobProgress;
import net.osmand.server.api.dto.EvaluationReport;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import jakarta.annotation.PostConstruct;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static net.osmand.server.api.utils.StringUtils.crop;
import static net.osmand.server.api.utils.StringUtils.sanitize;
import static net.osmand.server.api.utils.StringUtils.unquote;

import net.osmand.server.api.utils.GeometryUtils;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.util.MapUtils;

@Service
public class TestSearchService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSearchService.class);

    private final DatasetRepository datasetRepository;
    private final DatasetJobRepository datasetJobRepository;
    private final JdbcTemplate jdbcTemplate;
    private final SearchService searchService;
    private WebClient webClient;
    private final ObjectMapper objectMapper;
    private final WebClient.Builder webClientBuilder;
    private final SimpMessagingTemplate messagingTemplate;
    @Value("${test.csv.dir}")
    private String csvDownloadingDir;
    @Value("${test.overpass.url}")
    private String overpassApiUrl;

    @Autowired
    public TestSearchService(DatasetRepository datasetRepository,
                             DatasetJobRepository datasetJobRepository,
                             @Qualifier("testJdbcTemplate") JdbcTemplate jdbcTemplate,
                             SearchService searchService, WebClient.Builder webClientBuilder, ObjectMapper objectMapper,
                             SimpMessagingTemplate messagingTemplate) {
        this.datasetRepository = datasetRepository;
        this.datasetJobRepository = datasetJobRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.searchService = searchService;
        this.webClientBuilder = webClientBuilder;
        this.objectMapper = objectMapper;
        this.messagingTemplate = messagingTemplate;
    }

    @PostConstruct
    private void initWebClient() {
        this.webClient = webClientBuilder.baseUrl(overpassApiUrl).build();
    }

    private Path queryOverpass(String query) {
        Path tempFile;
        try {
            String overpassResponse = webClient.post()
                    .uri("")
                    .bodyValue("data=[out:json][timeout:25];" + query)
                    .retrieve()
                    .bodyToMono(String.class)
                    .toFuture().join();
            tempFile = Files.createTempFile(Path.of(csvDownloadingDir), "overpass_", ".csv.gz");
            int rowCount = convertJsonToSaveInCsv(overpassResponse, tempFile);
            LOGGER.info("Wrote {} rows to temporary file: {}", rowCount, tempFile);

            return tempFile;
        } catch (Exception e) {
            LOGGER.error("Failed to query data from Overpass for {}", query, e);
            throw new RuntimeException("Failed to query from Overpass", e);
        }
    }

    private int convertJsonToSaveInCsv(String jsonResponse, Path outputPath) throws IOException {
        JsonNode root = objectMapper.readTree(jsonResponse);
        JsonNode elements = root.path("elements");

        if (!elements.isArray()) {
            return 0;
        }

        Set<String> headers = new LinkedHashSet<>();
        headers.add("id");
        headers.add("lat");
        headers.add("lon");
        List<JsonNode> elementList = new ArrayList<>();
        elements.forEach(element -> {
            element.path("tags").fieldNames().forEachRemaining(headers::add);
            elementList.add(element);
        });

        int rowCount = 0;
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(Files.newOutputStream(outputPath));
             CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(gzipOutputStream), CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])))) {

            for (JsonNode element : elementList) {
                List<String> record = new ArrayList<>();
                for (String header : headers) {
                    String value = switch (header) {
                        case "id" -> element.path("id").asText();
                        case "lat" -> element.path("lat").asText();
                        case "lon" -> element.path("lon").asText();
                        default -> element.path("tags").path(header).asText(null);
                    };
                    record.add(value);
                }
                csvPrinter.printRecord(record);
                rowCount++;
            }
        }
        return rowCount;
    }

    @Async
    public CompletableFuture<Long> countCsvRows(String filePath) {
        return CompletableFuture.supplyAsync(() -> {
            Path fullPath = Path.of(csvDownloadingDir, filePath);
            try (BufferedReader reader = new BufferedReader(new FileReader(fullPath.toFile()))) {
                // Subtract 1 for the header row
                return Math.max(0, reader.lines().count() - 1);
            } catch (IOException e) {
                LOGGER.error("Failed to count rows in CSV file: {}", fullPath, e);
                throw new RuntimeException("Failed to count rows in CSV file", e);
            }
        });
    }

    @Async
    public CompletableFuture<Dataset> refreshDataset(Long datasetId, Integer sizeLimit) {
        return CompletableFuture.supplyAsync(() -> {
            Dataset dataset = datasetRepository.findById(datasetId)
                    .orElseThrow(() -> new RuntimeException("Dataset not found with id: " + datasetId));

            if (!Objects.equals(dataset.getSizeLimit(), sizeLimit)) {
                dataset.setSizeLimit(sizeLimit);
            }

            Path fullPath;
            if (dataset.getSource().equals("OVERPASS")) {
                fullPath = queryOverpass(dataset.getSource());
            } else {
                fullPath = Path.of(csvDownloadingDir, dataset.getSource());
            }

            String tableName = "dataset_" + dataset.getName();
            try {
                List<String> sample = reservoirSample(fullPath, sizeLimit + 1);
                String[] headers = sample.get(0).toLowerCase().split(",");
                String[] columns = insertSampleData(tableName, headers, sample, true);

                dataset.setColumns(objectMapper.writeValueAsString(columns));
                dataset.setSourceStatus(DatasetType.COMPLETED.name());
                datasetRepository.save(dataset);

                LOGGER.info("Stored {} rows into table: {}", sample.size(), tableName);
                return dataset;
            } catch (Exception e) {
                dataset.setSourceStatus(DatasetType.FAILED.name());
                datasetRepository.save(dataset);
                LOGGER.error("Failed to process and insert data from CSV file: {}", fullPath, e);
                throw new RuntimeException("Failed to process CSV file", e);
            } finally {
                if (dataset.getSource().equals("OVERPASS")) {
                    try {
                        if (fullPath != null && !Files.deleteIfExists(fullPath)) {
                            LOGGER.warn("Could not delete temporary file: {}", fullPath);
                        }
                    } catch (IOException e) {
                        LOGGER.error("Error deleting temporary file: {}", fullPath, e);
                    }
                }
            }
        });
    }

    @Async
    public CompletableFuture<Dataset> createDataset(String name, String type, String source) {
        return CompletableFuture.supplyAsync(() -> {
            Optional<Dataset> datasetOptional = datasetRepository.findByName(name);
            if (datasetOptional.isPresent())
                throw new RuntimeException("Dataset is already created: " + name);

            Dataset dataset = new Dataset();
            dataset.setName(name);
            dataset.setType(type);
            dataset.setSource(source);
            dataset.setSourceStatus(DatasetType.NEW.name());
            dataset = datasetRepository.save(dataset);

            return dataset;
        });
    }

    public EvalJob startEvaluation(Long datasetId, Map<String, String> payload) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new RuntimeException("Dataset not found with id: " + datasetId));

        EvalJob job = new EvalJob();
        job.setDatasetId(datasetId);
        job.setCreated(new java.sql.Timestamp(System.currentTimeMillis()));

        job.setAddressExpression(payload.get("addressExpression"));
        job.setLocale(payload.get("locale"));
        job.setNorthWest(payload.get("northWest"));
        job.setSouthEast(payload.get("southEast"));
        job.setBaseSearch(Boolean.parseBoolean(payload.get("baseSearch")));

        job.setStatus(JobStatus.RUNNING);
        EvalJob savedJob = datasetJobRepository.save(job);

        runTest(savedJob, dataset);

        return savedJob;
    }

    @Async
    protected void runTest(EvalJob job, Dataset dataset) {
        String tableName = "dataset_" + dataset.getName();
        String sql = String.format("SELECT *, %s AS address FROM %s", job.getAddressExpression(), tableName);
        long processedCount = 0;
        long errorCount = 0;
        try {
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
            long totalRows = rows.size();
            for (Map<String, Object> row : rows) {
                if (datasetJobRepository.findById(job.getId()).map(j -> j.getStatus() == JobStatus.CANCELED).orElse(false)) {
                    LOGGER.info("Job {} was cancelled. Stopping execution.", job.getId());
                    break; // Exit the loop if the job has been cancelled
                }
                long startTime = System.currentTimeMillis();
                LatLon point = null;
                String originalJson = null, address = null;
                try {
                    originalJson = objectMapper.writeValueAsString(row);
                    address = (String) row.get("address");
                    String lat = (String) row.get("lat");
                    String lon = (String) row.get("lon");
                    point = GeometryUtils.parseLatLon(lat, lon);
                    if (point == null) {
                        throw new IllegalArgumentException("Invalid or missing (lat, lon) in WKT format.");
                    }

                    List<Feature> searchResults = searchService.search(point.getLatitude(), point.getLongitude(), address, job.getLocale(), job.getBaseSearch(), job.getNorthWest(), job.getSouthEast());
                    saveResults(job, dataset, address, originalJson, searchResults, point, System.currentTimeMillis() - startTime, null);
                } catch (Exception e) {
                    errorCount++;
                    LOGGER.warn("Failed to process row for job {}: {}", job.getId(), originalJson, e);
                    saveResults(job, dataset, address, originalJson, Collections.emptyList(), point, System.currentTimeMillis() - startTime, e.getMessage() == null ? e.toString() : e.getMessage());
                }
                processedCount++;
                JobProgress progress = new JobProgress(job.getId(), dataset.getId(), job.getStatus().name(), totalRows, processedCount, errorCount);
                messagingTemplate.convertAndSend("/topic/job-progress/" + job.getId(), progress);
            }
            if (job.getStatus() != JobStatus.CANCELED) {
                job.setStatus(JobStatus.COMPLETED);
            }
        } catch (Exception e) {
            LOGGER.error("Evaluation failed for job {} on dataset {}", job.getId(), dataset.getId(), e);
            job.setStatus(JobStatus.FAILED);
            job.setError(e.getMessage());
        } finally {
            job.setUpdated(new java.sql.Timestamp(System.currentTimeMillis()));
            datasetJobRepository.save(job);
            JobProgress finalProgress = new JobProgress(job.getId(), dataset.getId(), job.getStatus().name(), processedCount, processedCount, errorCount);
            messagingTemplate.convertAndSend("/topic/job-progress/" + job.getId(), finalProgress);
        }
    }

    @Async
    public CompletableFuture<EvalJob> cancelEvaluation(Long jobId) {
        return CompletableFuture.supplyAsync(() -> {
            EvalJob job = datasetJobRepository.findById(jobId)
                    .orElseThrow(() -> new RuntimeException("Job not found with id: " + jobId));

            if (job.getStatus() == JobStatus.RUNNING) {
                job.setStatus(JobStatus.CANCELED);
                job.setUpdated(new java.sql.Timestamp(System.currentTimeMillis()));
                return datasetJobRepository.save(job);
            } else {
                // If the job is not running, just return its current state without changes.
                return job;
            }
        });
    }

    private void saveResults(EvalJob job, Dataset dataset, String address, String originalJson, List<Feature> searchResults, LatLon originalPoint, long duration, String error) {
        if (job == null || dataset == null) {
            return;
        }
        int resultsCount = searchResults.size();
        Integer minDistance = null, actualPlace = null;
        String closestResult = null;

        if (originalPoint != null && !searchResults.isEmpty()) {
            double minDistanceMeters = Double.MAX_VALUE;
            LatLon closestPoint = null;
            int place = 0;

            for (Feature feature : searchResults) {
                place++;
                if (feature == null)
                    continue;
                LatLon foundPoint = GeometryUtils.getLatLon(feature);
                double distance = MapUtils.getDistance(originalPoint.getLatitude(), originalPoint.getLongitude(), foundPoint.getLatitude(), foundPoint.getLongitude());
                if (distance < minDistanceMeters) {
                    minDistanceMeters = distance;
                    closestPoint = foundPoint;
                    actualPlace = place;
                }
            }

            if (closestPoint != null) {
                minDistance = (int) minDistanceMeters;
                closestResult = GeometryUtils.pointToString(closestPoint);
            }
        }

        String insertSql = "INSERT INTO eval_result (job_id, dataset_id, original, error, duration, results_count, " +
                "min_distance, closest_result, address, lat, lon, actual_place, timestamp) VALUES (?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.update(insertSql, job.getId(), dataset.getId(), originalJson, error, duration, resultsCount,
                minDistance, closestResult, address, originalPoint == null ? null : originalPoint.getLatitude(), originalPoint == null ? null : originalPoint.getLongitude(), actualPlace, new java.sql.Timestamp(System.currentTimeMillis()));
    }

    public Page<Dataset> getDatasets(String search, String status, Pageable pageable) {
        return datasetRepository.findAllDatasets(search, status, pageable);
    }

    public Page<EvalJob> getDatasetJobs(Long datasetId, Pageable pageable) {
        return datasetJobRepository.findByDatasetIdOrderByIdDesc(datasetId, pageable);
    }

    public Optional<EvaluationReport> getEvaluationReport(Long datasetId, Optional<Long> jobIdOpt) {
        Optional<EvalJob> jobOptional = jobIdOpt
                .flatMap(datasetJobRepository::findById)
                .or(() -> datasetJobRepository.findTopByDatasetIdOrderByIdDesc(datasetId));

        if (jobOptional.isEmpty()) {
            return Optional.empty();
        }

        EvalJob job = jobOptional.get();
        long jobId = job.getId();

        String sql = """
            SELECT
                count(*) AS total_requests,
                count(*) FILTER (WHERE error IS NOT NULL) AS failed_requests,
                avg(duration) AS average_duration,
                sum(CASE WHEN min_distance BETWEEN 0 AND 1 THEN 1 ELSE 0 END) AS "0-1m",
                sum(CASE WHEN min_distance > 0 AND min_distance <= 50 THEN 1 ELSE 0 END) AS "0-50m",
                sum(CASE WHEN min_distance > 50 AND min_distance <= 500 THEN 1 ELSE 0 END) AS "50-500m",
                sum(CASE WHEN min_distance > 500 AND min_distance <= 1000 THEN 1 ELSE 0 END) AS "500-1000m",
                sum(CASE WHEN min_distance > 1000 THEN 1 ELSE 0 END) AS "1000m+"
            FROM
                eval_result
            WHERE
                job_id = ?
            """;

        Map<String, Object> result = jdbcTemplate.queryForMap(sql, jobId);

        long totalRequests = ((Number) result.get("total_requests")).longValue();
        if (totalRequests == 0) {
            return Optional.empty(); // No data to report
        }
        long failedRequests = ((Number) result.get("failed_requests")).longValue();
        double averageDuration = result.get("average_duration") == null ? 0 : ((Number) result.get("average_duration")).doubleValue();
        double errorRate = (double) failedRequests / totalRequests;

        Map<String, Long> distanceHistogram = new LinkedHashMap<>();
        distanceHistogram.put("0-50m", ((Number) result.getOrDefault("0-50m", 0)).longValue());
        distanceHistogram.put("50-500m", ((Number) result.getOrDefault("50-500m", 0)).longValue());
        distanceHistogram.put("500-1000m", ((Number) result.getOrDefault("500-1000m", 0)).longValue());
        distanceHistogram.put("1000m+", ((Number) result.getOrDefault("1000m+", 0)).longValue());

        EvaluationReport report = new EvaluationReport(jobId, totalRequests, failedRequests, errorRate, averageDuration, distanceHistogram);
        return Optional.of(report);
    }

    public Optional<EvalJob> getEvaluationJob(Long jobId) {
        return datasetJobRepository.findById(jobId);
    }

    public void downloadRawResults(Writer writer, Long datasetId, Optional<Long> jobIdOpt, String format) throws IOException {
        Optional<EvalJob> jobOptional = jobIdOpt
                .flatMap(datasetJobRepository::findById)
                .or(() -> datasetJobRepository.findTopByDatasetIdOrderByIdDesc(datasetId));

        if (jobOptional.isEmpty()) {
            throw new RuntimeException("No evaluation job found for datasetId: " + datasetId);
        }

        long jobId = jobOptional.get().getId();
        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM eval_result WHERE job_id = ?", jobId);

        if ("csv".equalsIgnoreCase(format)) {
            writeResultsAsCsv(writer, results);
        } else if ("json".equalsIgnoreCase(format)) {
            objectMapper.writeValue(writer, results);
        } else {
            throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }

    private void writeResultsAsCsv(Writer writer, List<Map<String, Object>> results) throws IOException {
        if (results.isEmpty()) {
            writer.write("");
            return;
        }

        // Dynamically determine headers
        Set<String> headers = new LinkedHashSet<>();
        results.get(0).keySet().stream()
                .filter(k -> !k.equals("original"))
                .forEach(headers::add);

        // Add headers from the 'original' JSON object
        Set<String> originalHeaders = new LinkedHashSet<>();
        for (Map<String, Object> row : results) {
            Object originalObj = row.get("original");
            if (originalObj != null) {
                try {
                    JsonNode originalNode = objectMapper.readTree(originalObj.toString());
                    originalNode.fieldNames().forEachRemaining(originalHeaders::add);
                } catch (IOException e) {
                    // Ignore if 'original' is not valid JSON
                }
            }
        }
        headers.addAll(originalHeaders);

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(headers.toArray(new String[0]))
                .build();

        try (final CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {
            for (Map<String, Object> row : results) {
                List<String> record = new ArrayList<>();
                JsonNode originalNode = null;
                Object originalObj = row.get("original");
                if (originalObj != null) {
                    try {
                        originalNode = objectMapper.readTree(originalObj.toString());
                    } catch (IOException e) {
                        // Keep originalNode null
                    }
                }

                for (String header : headers) {
                    if (row.containsKey(header)) {
                        Object value = row.get(header);
                        record.add(value != null ? value.toString() : null);
                    } else if (originalNode != null && originalNode.has(header)) {
                        record.add(originalNode.get(header).asText());
                    } else {
                        record.add(null);
                    }
                }
                printer.printRecord(record);
            }
        }
    }

    public String[] insertSampleData(String tableName, String[] headers, List<String> sample, boolean deleteBefore) {
        if (deleteBefore) {
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);
        }
        String[] columns = new String[headers.length];
        System.arraycopy(headers, 0, columns, 1, headers.length);

        createDynamicTable(tableName, columns);

        String insertSql = "INSERT INTO " + tableName + " (" + String.join(", ", columns) + ") VALUES (" +
                String.join(", ", Collections.nCopies(columns.length, "?")) + ")";

        List<Object[]> batchArgs = new ArrayList<>();
        for (String s : sample) {
            String[] record = s.split(",");
            Object[] values = new String[columns.length];

            for (int j = 0; j < values.length; j++) {
                values[j] = crop(unquote(record[j]), 255);
            }
            batchArgs.add(values);
        }
        jdbcTemplate.batchUpdate(insertSql, batchArgs);
        LOGGER.info("Batch inserted {} records into {}.", sample.size() - 1, tableName);

        return columns;
    }

    private void createDynamicTable(String tableName, String[] headers) {
        String columnsDefinition = Stream.of(headers)
                .map(header -> "\"" + sanitize(header) + "\" VARCHAR(255)")
                .collect(Collectors.joining(", "));
        // Use a dedicated primary key column (id) with PostgreSQL-compatible identity generation
        String createTableSql = String.format("CREATE TABLE IF NOT EXISTS %s (_id BIGSERIAL PRIMARY KEY, %s)", tableName, columnsDefinition);
        jdbcTemplate.execute(createTableSql);

        LOGGER.info("Ensured table {} exists.", tableName);
    }

    private static List<String> reservoirSample(Path filePath, int n) throws IOException {
        List<String> sample = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String header = reader.readLine();
            String line;
            int i = 1; // Line index (after header)
            while ((line = reader.readLine()) != null) {
                if (i <= n) {
                    // Fill reservoir
                    if (sample.size() <= n) {
                        sample.add(line.trim());
                    }
                } else {
                    // Replace with probability n/i
                    int j = new Random().nextInt(i);
                    if (j < n) {
                        sample.set(j, line.trim());
                    }
                }
                i++;
            }
            sample.set(0, header); // Restore header
        }

        return sample;
    }

    public Map<String, Integer> browseCsvFiles() throws IOException {
        Map<String, Integer> fileRowCounts = new HashMap<>();
        try (Stream<Path> paths = Files.walk(Path.of(csvDownloadingDir))) {
            List<Path> csvFiles = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().toLowerCase().endsWith(".csv"))
                    .toList();

            for (Path csvFile : csvFiles) {
                try (BufferedReader reader = new BufferedReader(new FileReader(csvFile.toFile()))) {
                    int rowCount = (int) reader.lines().count();
                    fileRowCounts.put(csvFile.getFileName().toString(), rowCount);
                }
            }
        }
        return fileRowCounts;
    }
}
