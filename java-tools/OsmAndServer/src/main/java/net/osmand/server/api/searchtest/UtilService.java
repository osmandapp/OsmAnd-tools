package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import net.osmand.data.LatLon;
import net.osmand.server.api.services.SearchTestService;
import net.osmand.server.controllers.pub.GeojsonClasses;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public abstract class UtilService {
    protected static final Logger LOGGER = LoggerFactory.getLogger(SearchTestService.class);
    protected final ObjectMapper objectMapper;
    private final WebClient.Builder webClientBuilder;
    @Value("${testsearch.tiger.csv.dir}")
    protected String csvDownloadingDir;
    private WebClient webClient;
    @Value("${testsearch.overpass.url}")
    private String overpassApiUrl;
    @Value("${osmand.web.location}")
    protected String webLocation;

    public UtilService(WebClient.Builder webClientBuilder, ObjectMapper objectMapper) {
        this.webClientBuilder = webClientBuilder;
        this.objectMapper = objectMapper;
    }

    // -------------------- Utility methods (common) --------------------

    public static String pointToString(LatLon point) {
        return String.format(Locale.US, "POINT(%f %f)", point.getLatitude(), point.getLongitude());
    }

    public static LatLon getLatLon(GeojsonClasses.Feature feature) {
        float[] point = "Point".equals(feature.geometry.type) ? (float[]) feature.geometry.coordinates :
                ((float[][]) feature.geometry.coordinates)[0];
        return new LatLon(point[1], point[0]);
    }

    public static LatLon parseLatLon(String lat, String lon) {
        if (lat == null || lon == null) {
            return null;
        }
        try {
            return new LatLon(Double.parseDouble(lat), Double.parseDouble(lon));
        } catch (Exception e) {
            return null;
        }
    }

    public static String sanitize(String input) {
        if (input == null) {
            return "";
        }
        return input.toLowerCase().replaceAll("[^a-zA-Z0-9_]", "_");
    }

    public static String unquote(String input) {
        if (input == null) {
            return "";
        }
        if (input.length() >= 2 && input.startsWith("\"") && input.endsWith("\"")) {
            return input.substring(1, input.length() - 1);
        }
        return input;
    }

    public static String crop(String input, int length) {
        if (input == null) {
            return "";
        }
        return input.substring(0, Math.min(length, input.length()));
    }

    protected static List<String> reservoirSample(Path filePath, int n) throws IOException {
        List<String> sample = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String header = reader.readLine();
            String line;
            int i = 1; // line index after header
            while ((line = reader.readLine()) != null) {
                if (i <= n) {
                    if (sample.size() <= n) {
                        sample.add(line.trim());
                    }
                } else {
                    int j = new Random().nextInt(i);
                    if (j < n) {
                        sample.set(j, line.trim());
                    }
                }
                i++;
            }
            sample.add(0, header); // restore header
        }
        return sample;
    }

    protected static String getHeader(Path filePath) throws IOException {
        String fileName = filePath.getFileName().toString();
        if (fileName.endsWith(".csv")) {
            try (BufferedReader reader = Files.newBufferedReader(filePath)) {
                return reader.readLine();
            }
        }
        if (fileName.endsWith(".gz")) {
            try (BufferedReader reader =
                            new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(filePath))))) {
                return reader.readLine();
            }
        }
        return null;
    }

    @PostConstruct
    protected void initWebClient() {
        this.webClient =
                webClientBuilder.baseUrl(overpassApiUrl).exchangeStrategies(ExchangeStrategies.builder().codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024)).build()).build();
    }

    protected Path queryOverpass(String query) {
        Path tempFile;
        try {
            String overpassResponse =
                    webClient.post().uri("").bodyValue("[out:json][timeout:25];" + query + ";out;").retrieve().bodyToMono(String.class).toFuture().join();
            tempFile = Files.createTempFile(Path.of(csvDownloadingDir), "overpass_", ".csv");
            int rowCount = convertJsonToSaveInCsv(overpassResponse, tempFile);
            LOGGER.info("Wrote {} rows to temporary file: {}", rowCount, tempFile);
            return tempFile;
        } catch (Exception e) {
            LOGGER.error("Failed to query data from Overpass for {}", query, e);
            throw new RuntimeException("Failed to query from Overpass", e);
        }
    }

    protected int convertJsonToSaveInCsv(String jsonResponse, Path outputPath) throws IOException {
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
        try (OutputStream outStream = new BufferedOutputStream(Files.newOutputStream(outputPath)); CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(outStream), CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])))) {

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

    public Map<String, Integer> browseCsvFiles() throws IOException {
        Map<String, Integer> fileRowCounts = new HashMap<>();
        try (Stream<Path> paths = Files.walk(Path.of(csvDownloadingDir))) {
            List<Path> csvFiles = paths.filter(Files::isRegularFile).filter(p -> p.toString().toLowerCase().endsWith(
                    ".csv")).toList();

            for (Path csvFile : csvFiles) {
                try (BufferedReader reader = new BufferedReader(new FileReader(csvFile.toFile()))) {
                    int rowCount = (int) reader.lines().count();
                    fileRowCounts.put(csvFile.getFileName().toString(), rowCount);
                }
            }
        }
        return fileRowCounts;
    }

    @Async
    public CompletableFuture<Long> countCsvRows(String filePath) {
        return CompletableFuture.supplyAsync(() -> {
            Path fullPath = Path.of(csvDownloadingDir, filePath);
            try (BufferedReader reader = new BufferedReader(new FileReader(fullPath.toFile()))) {
                return Math.max(0, reader.lines().count() - 1); // exclude header
            } catch (IOException e) {
                LOGGER.error("Failed to count rows in CSV file: {}", fullPath, e);
                throw new RuntimeException("Failed to count rows in CSV file", e);
            }
        });
    }

    protected String[] execute(String script, String functionName, Map<String, String> row, String[] columns,
                               String[] otherParams) {
        if (script == null || script.trim().isEmpty()) {
            throw new IllegalArgumentException("Script must not be null or empty");
        }
        if (functionName == null || functionName.trim().isEmpty()) {
            throw new IllegalArgumentException("Function name must not be null or empty");
        }
        try {
            // 1) Initialize Nashorn (JDK 8–14) or a compatible JS engine
            ScriptEngineManager manager = new ScriptEngineManager();
            ScriptEngine engine = manager.getEngineByName("nashorn");
            if (engine == null) {
                engine = manager.getEngineByName("JavaScript"); // fallback name if Nashorn is aliased
            }
            if (engine == null) {
                throw new IllegalStateException("No JavaScript engine available. Ensure Nashorn (JDK 8–14) or include 'nashorn-core' on the classpath.");
            }

            // 2) Load/evaluate the provided script source (should contain "use strict" and function definitions)
            engine.eval(script);

            // 3) Prepare JS-native arguments according to the spec
            // Convert row and columns to JSON and materialize them as JS objects/arrays inside the engine
            String rowJson = row == null ? "{}" : objectMapper.writeValueAsString(row);
            String columnsJson = columns == null ? "[]" : objectMapper.writeValueAsString(columns);
            engine.eval("var __row = " + rowJson + ";\nvar __columns = " + columnsJson + ";");
            Object jsRow = engine.get("__row");
            Object jsColumns = engine.get("__columns");

            List<Object> args = new ArrayList<>();
            args.add(jsRow);
            args.add(jsColumns);
            if (otherParams != null) {
                for (String p : otherParams) {
                    if (p == null) {
                        args.add(null);
                        continue;
                    }
                    String t = p.trim();
                    // Convert to number if it looks numeric; otherwise pass as string
                    if (t.matches("^-?\\d+$")) {
                        try {
                            args.add(Long.parseLong(t));
                        } catch (NumberFormatException ex) {
                            args.add(t);
                        }
                    } else if (t.matches("^-?\\d*\\.\\d+(?:[eE]-?\\d+)?$")) {
                        try {
                            args.add(Double.parseDouble(t));
                        } catch (NumberFormatException ex) {
                            args.add(t);
                        }
                    } else {
                        args.add(p);
                    }
                }
            }

            // 4) Invoke the target function
            Invocable inv = (Invocable) engine;
            Object result = inv.invokeFunction(functionName, args.toArray());

            // 5) Convert the returned JS array to String[] without relying on Nashorn-specific classes
            if (result == null) {
                return new String[0];
            }
            engine.put("__res", result);
            Object jsonArr = engine.eval("Array.isArray(__res) ? JSON.stringify(__res) : null");
            if (jsonArr == null) {
                throw new IllegalArgumentException("Function '" + functionName + "' did not return an array as required by the spec.");
            }
            @SuppressWarnings("unchecked")
            List<Object> list = objectMapper.readValue(jsonArr.toString(), List.class);
            String[] out = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object v = list.get(i);
                out[i] = v == null ? null : String.valueOf(v);
            }
            return out;
        } catch (ScriptException e) {
            throw new RuntimeException("JavaScript engine error: " + (e.getMessage() == null ? e.toString() : e.getMessage()), e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Function not found or wrong signature: " + functionName, e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize parameters to JSON: " + (e.getMessage() == null ? e.toString() : e.getMessage()), e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute script function '" + functionName + "': " + (e.getMessage() == null ? e.toString() : e.getMessage()), e);
        }
    }
}
