package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.data.LatLon;
import net.osmand.server.controllers.pub.GeojsonClasses;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public interface BaseService {
	record Function(String name, String description, Param[] param) {
	}

	record Param(String name, String type, String description) {
	}

	record GenParam(
			String name,
			String labels,
			// New fields to explicitly support 2 functions
			String selectFun,
			String whereFun,
			String[] columns,
			String[] selectParamValues,
			String[] whereParamValues) {}

	// -------------------- Utility methods (common) --------------------
	default String pointToString(LatLon point) {
		return String.format(Locale.US, "POINT(%f %f)", point.getLatitude(), point.getLongitude());
	}

	default LatLon getLatLon(GeojsonClasses.Feature feature) {
		float[] point = "Point".equals(feature.geometry.type) ? (float[]) feature.geometry.coordinates :
				((float[][]) feature.geometry.coordinates)[0];
		return new LatLon(point[1], point[0]);
	}

	default String unquote(String input) {
		if (input == null) {
			return "";
		}
		if (input.length() >= 2 && input.startsWith("\"") && input.endsWith("\"")) {
			return input.substring(1, input.length() - 1);
		}
		return input;
	}

	default String crop(String input, int length) {
		if (input == null) {
			return "";
		}
		return input.substring(0, Math.min(length, input.length()));
	}

	default List<String> reservoirSample(Path filePath, int n) throws IOException {
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

	default String getHeader(Path filePath) throws IOException {
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

	JdbcTemplate getJdbcTemplate();

	ObjectMapper getObjectMapper();

	WebClient getWebClient();

	String getCsvDownloadingDir();

	Logger getLogger();

	default Path queryOverpass(String query) {
		Path tempFile;
		try {
			String overpassResponse =
					getWebClient().post().uri("").bodyValue("[out:json][timeout:25];" + query +
							";out;").retrieve().bodyToMono(String.class).toFuture().join();
			tempFile = Files.createTempFile(Path.of(getCsvDownloadingDir()), "overpass_", ".csv");
			int rowCount = convertJsonToSaveInCsv(overpassResponse, tempFile);
			getLogger().info("Wrote {} rows to temporary file: {}", rowCount, tempFile);
			return tempFile;
		} catch (Exception e) {
			getLogger().error("Failed to query data from Overpass for {}", query, e);
			throw new RuntimeException("Failed to query from Overpass", e);
		}
	}

	default int convertJsonToSaveInCsv(String jsonResponse, Path outputPath) throws IOException {
		JsonNode root = getObjectMapper().readTree(jsonResponse);
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

	default Map<String, Integer> browseCsvFiles() throws IOException {
		Map<String, Integer> fileRowCounts = new HashMap<>();
		try (Stream<Path> paths = Files.walk(Path.of(getCsvDownloadingDir()))) {
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
	default CompletableFuture<Long> countCsvRows(String filePath) {
		return CompletableFuture.supplyAsync(() -> {
			Path fullPath = Path.of(getCsvDownloadingDir(), filePath);
			try (BufferedReader reader = new BufferedReader(new FileReader(fullPath.toFile()))) {
				return Math.max(0, reader.lines().count() - 1); // exclude header
			} catch (IOException e) {
				getLogger().error("Failed to count rows in CSV file: {}", fullPath, e);
				throw new RuntimeException("Failed to count rows in CSV file", e);
			}
		});
	}
}
