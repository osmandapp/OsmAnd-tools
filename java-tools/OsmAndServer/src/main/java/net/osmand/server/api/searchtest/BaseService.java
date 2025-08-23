package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.entity.TestCase;
import net.osmand.server.api.services.SearchTestService;
import net.osmand.server.controllers.pub.GeojsonClasses;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
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

public abstract class BaseService {
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
	private Engine polyglotEngine;

	public BaseService(WebClient.Builder webClientBuilder, ObjectMapper objectMapper) {
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
	protected void init() {
		this.webClient =
				webClientBuilder.baseUrl(overpassApiUrl).exchangeStrategies(ExchangeStrategies.builder().codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024)).build()).build();

		// Initialize GraalVM Polyglot Engine and redirect logs.
		// We disable the interpreter-only warning and send Truffle logs to a temp file.
		try {
			Path logPath = Path.of(System.getProperty("java.io.tmpdir"), "osmand-polyglot.log");
			try {
				Files.createDirectories(logPath.getParent());
			} catch (IOException ignore) {
				// Directory usually exists; ignore if creation fails, Engine will still initialize.
			}
			this.polyglotEngine = Engine.newBuilder().option("engine.WarnInterpreterOnly", "false").option("log.file",
					logPath.toString()).build();
			LOGGER.info("Initialized GraalVM Polyglot Engine. Truffle logs -> {}", logPath.toAbsolutePath());
		} catch (Throwable t) {
			// Fall back to an Engine that only disables the warning, if log redirection fails.
			LOGGER.warn("Failed to initialize Polyglot Engine logging; continuing without log.file redirection", t);
			this.polyglotEngine = Engine.newBuilder().option("engine.WarnInterpreterOnly", "false").build();
		}
	}

	protected Path queryOverpass(String query) {
		Path tempFile;
		try {
			String overpassResponse =
					webClient.post().uri("").bodyValue("[out:json][timeout:25];" + query +
							";out;").retrieve().bodyToMono(String.class).toFuture().join();
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
		try (OutputStream outStream = new BufferedOutputStream(Files.newOutputStream(outputPath));
			 CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(outStream),
					 CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])))) {
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

	protected List<RowAddress> execute(String script, TestCase test, List<String> delCols,
									   List<Map<String, Object>> rows) throws Exception {
		String selectFun = test.selectFun;
		String whereFun = test.whereFun;
		String columnsJson = test.selCols;
		String[] selectParams = objectMapper.readValue(test.selectParams, String[].class);
		String[] whereParams = objectMapper.readValue(test.whereParams, String[].class);

		if (script == null || script.trim().isEmpty()) {
			throw new IllegalArgumentException("Script must not be null or empty");
		}
		if (selectFun == null || selectFun.trim().isEmpty()) {
			throw new IllegalArgumentException("Function name must not be null or empty: " + selectFun);
		}
		try {
			// Execute with GraalVM JavaScript (Polyglot API)
			List<RowAddress> results = new ArrayList<>();
			try (Context context = Context.newBuilder("js").engine(polyglotEngine).option("js.ecmascript-version",
					"2022").allowAllAccess(false).build()) {
				// 1) Evaluate user script (should define the target function)
				context.eval("js", script);

				// 2) Prepare JS-native arguments
				for (Map<String, Object> origRow : rows) {
					Map<String, Object> row = origRow;
					if (!delCols.isEmpty()) {
						row = new HashMap<>(origRow);
						for (String key : delCols) {
							row.remove(key);
						}
					}

					String rowJson = objectMapper.writeValueAsString(row);
					org.graalvm.polyglot.Value jsonParse = context.eval("js", "JSON.parse");
					org.graalvm.polyglot.Value jsRow = jsonParse.execute(rowJson);
					org.graalvm.polyglot.Value jsColumns = jsonParse.execute(columnsJson);

					List<Object> selectArgs = getArgs(selectParams);
					selectArgs.add(0, jsColumns);
					selectArgs.add(0, jsRow);

					String lat = (String) origRow.get("lat");
					String lon = (String) origRow.get("lon");
					boolean where = false;
					if (whereFun != null && !whereFun.trim().isEmpty()) {
						List<Object> whereArgs = getArgs(whereParams);
						whereArgs.add(0, jsColumns);
						whereArgs.add(0, jsRow);

						String boolJson = execute(context, whereFun, whereArgs);
						where = Boolean.parseBoolean(boolJson);
					}

					String outputJson = null;
					if (where) {
						outputJson = execute(context, selectFun, selectArgs);
					}
					results.add(new RowAddress(parseLatLon(lat, lon), origRow, outputJson));
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
			throw new RuntimeException("Failed to execute script function '" + selectFun + "': " +
					(e.getMessage() == null ? e.toString() : e.getMessage()), e);
		}
	}

	private List<Object> getArgs(String[] selectParams) {
		List<Object> args = new ArrayList<>();
		if (selectParams != null) {
			for (String p : new ArrayList<>(Arrays.asList(selectParams)).subList(Math.min(2, selectParams.length), selectParams.length)) {
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
		return args;
	}

	private String execute(Context context, String functionName, List<Object> args) throws IOException {
		// 3) Resolve and invoke the target function
		org.graalvm.polyglot.Value fn = context.getBindings("js").getMember(functionName);
		if (fn == null || !fn.canExecute()) {
			throw new IllegalArgumentException("Function not found or not executable: " + functionName);
		}
		org.graalvm.polyglot.Value result = fn.execute(args.toArray());
		if (result.isBoolean()) {
			return String.valueOf(result.asBoolean());
		}

		if (result.hasArrayElements()) {
			org.graalvm.polyglot.Value stringify = context.eval("js", "JSON.stringify");
			return stringify.execute(result).asString();
		}
		return objectMapper.writeValueAsString(new String[]{result.asString()});
	}

	protected record RowAddress(LatLon point, Map<String, Object> row, String output) {
	}
}
