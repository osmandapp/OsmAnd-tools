package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.server.api.entity.Dataset;
import net.osmand.server.api.entity.DatasetType;
import net.osmand.server.api.repo.DatasetRepository;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import jakarta.annotation.PostConstruct;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

@Service
public class TestSearchService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSearchService.class);

    private final DatasetRepository datasetRepository;
    private final JdbcTemplate sqliteJdbcTemplate;
    private WebClient webClient;
    private final ObjectMapper objectMapper;
    private final WebClient.Builder webClientBuilder;
    @Value("${test.csv.dir}")
    private String csvDownloadingDir;
    @Value("${test.overpass.url}")
    private String overpassApiUrl;

    @Autowired
    public TestSearchService(DatasetRepository datasetRepository,
                             @Qualifier("sqliteJdbcTemplate") JdbcTemplate sqliteJdbcTemplate,
                             WebClient.Builder webClientBuilder, ObjectMapper objectMapper) {
        this.datasetRepository = datasetRepository;
        this.sqliteJdbcTemplate = sqliteJdbcTemplate;
        this.webClientBuilder = webClientBuilder;
        this.objectMapper = objectMapper;
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
    public CompletableFuture<String> refreshDataset(Long datasetId, Integer sizeLimit) {
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
            try (Reader reader = new BufferedReader(new FileReader(fullPath.toFile()))) {
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
                List<String> headers = csvParser.getHeaderNames();

                createDynamicTable(tableName, headers);

                List<CSVRecord> records = csvParser.getRecords();
                List<CSVRecord> sample = reservoirSample(records, sizeLimit);

                insertSampleData(tableName, headers, sample);

                dataset.setSourceStatus(DatasetType.COMPLETED.name());
                datasetRepository.save(dataset);

                LOGGER.info("Stored {} rows into table: {}", sample.size(), tableName);
                return dataset.getSourceStatus();
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

    private void createDynamicTable(String tableName, List<String> headers) {
        String columns = headers.stream()
                .map(header -> "\"" + header + "\" TEXT")
                .collect(Collectors.joining(", "));
        String createTableSql = String.format("CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT, %s)", tableName, columns);
        sqliteJdbcTemplate.execute(createTableSql);

        LOGGER.info("Ensured table {} exists.", tableName);
    }

    private void insertSampleData(String tableName, List<String> headers, List<CSVRecord> sample) {
        String columns = headers.stream().map(h -> "\"" + h + "\"").collect(Collectors.joining(", "));
        String placeholders = String.join(", ", Collections.nCopies(headers.size(), "?"));
        String insertSql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);

        List<Object[]> batchArgs = new ArrayList<>();
        for (CSVRecord record : sample) {
            Object[] values = new Object[headers.size()];
            for (int i = 0; i < headers.size(); i++) {
                values[i] = record.get(i);
            }
            batchArgs.add(values);
        }
        sqliteJdbcTemplate.batchUpdate(insertSql, batchArgs);
        LOGGER.info("Batch inserted {} records into {}.", sample.size(), tableName);
    }

    private <T> List<T> reservoirSample(List<T> stream, int n) {
        if (stream.size() <= n) {
            return stream;
        }

        Random random = new Random();
        List<T> reservoir = new ArrayList<>(stream.subList(0, n));

        for (int i = n; i < stream.size(); i++) {
            int j = random.nextInt(i + 1);
            if (j < n) {
                reservoir.set(j, stream.get(i));
            }
        }
        return reservoir;
    }
}
