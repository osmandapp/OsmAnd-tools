package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import net.osmand.data.LatLon;
import net.osmand.server.api.services.SearchTestService;
import net.osmand.server.controllers.pub.GeojsonClasses;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

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
	@Value("${osmand.web.location}")
	protected String webLocation;
	private WebClient webClient;
	@Value("${testsearch.overpass.url}")
	private String overpassApiUrl;

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
	protected record RowAddress(Map<String, Object> row, String address) {}

	protected List<RowAddress> execute(String script, String functionName, List<Map<String, Object>> rows, String columnsJson,
							   String[] otherParams) {
		if (script == null || script.trim().isEmpty()) {
			throw new IllegalArgumentException("Script must not be null or empty");
		}
		if (functionName == null || functionName.trim().isEmpty()) {
			throw new IllegalArgumentException("Function name must not be null or empty");
		}
		try {
			// Execute with GraalVM JavaScript (Polyglot API)
			List<RowAddress> results = new ArrayList<>();
			try (Context context =
						 Context.newBuilder("js").option("js.ecmascript-version", "2022").allowAllAccess(false).build()) {

				// 1) Evaluate user script (should define the target function)
				context.eval("js", script);

				// 2) Prepare JS-native arguments
				for (Map<String, Object> row : rows) {
					String rowJson = objectMapper.writeValueAsString(row);
					org.graalvm.polyglot.Value jsonParse = context.eval("js", "JSON.parse");
					org.graalvm.polyglot.Value jsRow = jsonParse.execute(rowJson);
					org.graalvm.polyglot.Value jsColumns = jsonParse.execute(columnsJson);

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

					String[] addresses = execute(context, functionName, args);
					if (addresses.length == 0) {
						results.add(new RowAddress(row, ""));
					} else {
						for (String address : addresses) {
							results.add(new RowAddress(row, address));
						}
					}
				}
				return results;
			}
		} catch (PolyglotException e) {
			throw new RuntimeException("JavaScript execution error: " + (e.getMessage() == null ? e.toString() :
					e.getMessage()), e);
		} catch (IOException e) {
			throw new RuntimeException("Failed to serialize parameters to JSON: " + (e.getMessage() == null ?
					e.toString() : e.getMessage()), e);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException("Failed to execute script function '" + functionName + "': " + (e.getMessage() == null ? e.toString() : e.getMessage()), e);
		}
	}

	private String[] execute(Context context, String functionName, List<Object> args) throws IOException {
		// 3) Resolve and invoke the target function
		org.graalvm.polyglot.Value fn = context.getBindings("js").getMember(functionName);
		if (fn == null || !fn.canExecute()) {
			throw new IllegalArgumentException("Function not found or not executable: " + functionName);
		}
		org.graalvm.polyglot.Value result = fn.execute(args.toArray());

		// 4) Convert result array to String[]
		String[] out;
		if (result.hasArrayElements()) {
			org.graalvm.polyglot.Value stringify = context.eval("js", "JSON.stringify");
			String jsonArr = stringify.execute(result).asString();

			List<Object> list = objectMapper.readValue(jsonArr, List.class);
			out = new String[list.size()];
			for (int i = 0; i < list.size(); i++) {
				Object v = list.get(i);
				out[i] = v == null ? null : String.valueOf(v);
			}
		} else {
			out = new String[] { result.asString() };
		}
		return out;
	}
}
