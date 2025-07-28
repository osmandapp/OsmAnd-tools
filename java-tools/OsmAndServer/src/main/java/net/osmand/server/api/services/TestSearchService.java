package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.server.api.dto.OverpassTestRequest;
import net.osmand.server.api.entity.Dataset;
import net.osmand.server.api.repo.DatasetRepository;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPOutputStream;

@Service
public class TestSearchService {
    public static enum DatasetType {
        NEW, COMPLETED, FAILED
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSearchService.class);
    private static final String OVERPASS_API_URL = "https://overpass-api.de/api/interpreter";
    private static final int DEFAULT_SIZE_LIMIT = 10_000;

    private final DatasetRepository datasetRepository;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    @Autowired
    public TestSearchService(DatasetRepository datasetRepository, WebClient.Builder webClientBuilder, ObjectMapper objectMapper) {
        this.datasetRepository = datasetRepository;
        this.webClient = webClientBuilder.baseUrl(OVERPASS_API_URL).build();
        this.objectMapper = objectMapper;
    }

    @Async
    public CompletableFuture<Dataset> ingestFromOverpass(OverpassTestRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            String datasetName = generateDatasetName();
            if (datasetRepository.findByName(datasetName).isPresent()) {
                throw new DataIntegrityViolationException("Dataset with name '" + datasetName + "' already exists.");
            }

            Dataset dataset = new Dataset();
            dataset.setName(datasetName);
            dataset.setType("Overpass");
            dataset.setSource(request.query());
            dataset.setSourceStatus(DatasetType.NEW.name());
            dataset = datasetRepository.save(dataset);

            Path tempFile = null;
            try {
                String overpassResponse = queryOverpass(request.query()).join();
                tempFile = Files.createTempFile("overpass_", ".csv");
                int rowCount = convertJsonToCsv(overpassResponse, tempFile);
                // In a real implementation, we would now perform sampling and ingest into SQLite.
                // For now, we'll just mark it as complete.
                LOGGER.info("Wrote {} rows to temporary file: {}", rowCount, tempFile);

                dataset.setSourceStatus(DatasetType.COMPLETED.name());
                return datasetRepository.save(dataset);
            } catch (Exception e) {
                LOGGER.error("Failed to ingest data from Overpass for dataset {}", dataset.getId(), e);
                dataset.setSourceStatus(DatasetType.FAILED.name());
                datasetRepository.save(dataset);
                throw new RuntimeException("Failed to ingest from Overpass", e);
            } finally {
                if (tempFile != null) {
                    try {
                        Files.delete(tempFile);
                    } catch (IOException e) {
                        LOGGER.warn("Failed to delete temporary file: {}", tempFile, e);
                    }
                }
            }
        });
    }

    private CompletableFuture<String> queryOverpass(String query) {
        return webClient.post()
                .uri("")
                .bodyValue("data=" + query)
                .retrieve()
                .bodyToMono(String.class)
                .toFuture();
    }

    private int convertJsonToCsv(String jsonResponse, Path outputPath) throws IOException {
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

    private String generateDatasetName() {
        String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yy-MM-dd"));
        return String.format("overpass_%s", date).toLowerCase(Locale.ROOT);
    }
}
