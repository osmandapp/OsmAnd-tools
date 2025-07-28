package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.server.api.dto.OverpassQueryRequest;
import net.osmand.server.api.dto.OverpassQueryResult;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import jakarta.annotation.PostConstruct;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
    private WebClient webClient;
    private final ObjectMapper objectMapper;
    private final WebClient.Builder webClientBuilder;
    @Value("${test.csv.dir}")
    private String csvDownloadingDir;
    @Value("${test.overpass.url}")
    private String overpassApiUrl;

    @Autowired
    public TestSearchService(DatasetRepository datasetRepository, WebClient.Builder webClientBuilder, ObjectMapper objectMapper) {
        this.datasetRepository = datasetRepository;
        this.webClientBuilder = webClientBuilder;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    private void initWebClient() {
        this.webClient = webClientBuilder.baseUrl(overpassApiUrl).build();
    }

    private Dataset createDataset(String datasetName, String query, String type) {
        Dataset dataset = new Dataset();
        dataset.setName(datasetName);
        dataset.setType(type);
        dataset.setSource(query);
        dataset.setSourceStatus(DatasetType.NEW.name());
        dataset = datasetRepository.save(dataset);

        return dataset;
    }
    @Async
    public CompletableFuture<OverpassQueryResult> queryOverpass(String query) {
        return CompletableFuture.supplyAsync(() -> {
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

                return new OverpassQueryResult(tempFile.toString(), rowCount);
            } catch (Exception e) {
                LOGGER.error("Failed to query data from Overpass for {}", query, e);
                throw new RuntimeException("Failed to query from Overpass", e);
            }
        });
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
            Optional<Dataset> datasetOption = datasetRepository.findById(datasetId);
            if (datasetOption.isEmpty()) {
                throw new RuntimeException("Dataset not found");
            }

            Dataset dataset = datasetOption.get();
            String filePath = dataset.getSource();
            int currentSizeLimit = dataset.getSizeLimit();
            if (currentSizeLimit != sizeLimit) {
                dataset.setSizeLimit(sizeLimit);
                datasetRepository.save(dataset);
            }

            Path fullPath = Path.of(csvDownloadingDir, filePath);
            try (Reader reader = new BufferedReader(new FileReader(fullPath.toFile()))) {
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

                List<CSVRecord> records = csvParser.getRecords();
                List<CSVRecord> sample = reservoirSample(records, sizeLimit);

                // Implement store sample here
                LOGGER.info("Store {} rows to table: {}", sample.size(), dataset.getName());
                return dataset.getSourceStatus();
            } catch (IOException e) {
                LOGGER.error("Failed to retrieve sample from CSV file: {}", fullPath, e);
                throw new RuntimeException("Failed to process CSV file", e);
            }
        });
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

    private void writeSampleToCsv(List<CSVRecord> sample, Set<String> headers, Path outputPath) throws IOException {
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(Files.newOutputStream(outputPath));
             CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(gzipOutputStream), CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])))) {

            for (CSVRecord record : sample) {
                csvPrinter.printRecord(record);
            }
        }
    }

    private String generateDatasetName() {
        String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yy-MM-dd"));
        return String.format("overpass_%s", date).toLowerCase(Locale.ROOT);
    }
}
