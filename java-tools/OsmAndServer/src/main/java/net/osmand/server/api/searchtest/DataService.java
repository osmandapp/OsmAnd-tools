package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.dto.EvalJobReport;
import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.entity.EvalJob;
import net.osmand.server.api.searchtest.repo.DatasetRepository;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.util.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class DataService extends UtilService {
	protected final DatasetRepository datasetRepository;
	protected final JdbcTemplate jdbcTemplate;
	protected final EntityManager em;

	public DataService(EntityManager em, DatasetRepository datasetRepository,
					   @Qualifier("testJdbcTemplate") JdbcTemplate jdbcTemplate, WebClient.Builder webClientBuilder,
					   ObjectMapper objectMapper) {
		super(webClientBuilder, objectMapper);

		this.em = em;
		this.datasetRepository = datasetRepository;
		this.jdbcTemplate = jdbcTemplate;
	}

	@Async
	public CompletableFuture<Dataset> refreshDataset(Long datasetId, boolean reload) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset dataset = datasetRepository.findById(datasetId).orElseThrow(() ->
					new RuntimeException("Dataset not found with id: " + datasetId));
			return checkDatasetInternal(dataset, reload);
		});
	}

	private Dataset checkDatasetInternal(Dataset dataset, boolean reload) {
		reload = dataset.total == null || reload;

		Path fullPath = null;
		dataset.setSourceStatus(Dataset.ConfigStatus.UNKNOWN);
		try {
			if (dataset.type == Dataset.Source.Overpass) {
				fullPath = queryOverpass(dataset.source);
			} else {
				fullPath = Path.of(csvDownloadingDir, dataset.source);
			}
			if (!Files.exists(fullPath)) {
				dataset.setError("File is not existed.");
				datasetRepository.save(dataset);
				return dataset;
			}

			String tableName = "dataset_" + sanitize(dataset.name);
			String header = getHeader(fullPath);
			if (header == null || header.trim().isEmpty()) {
				dataset.setError("File doesn't have header.");
				datasetRepository.save(dataset);
				return dataset;
			}

			String del =
					header.chars().filter(ch -> ch == ',').count() <
							header.chars().filter(ch -> ch == ';').count() ? ";" : ",";
			String[] headers =
					Stream.of(header.toLowerCase().split(del)).map(DataService::sanitize).toArray(String[]::new);
			updateColumns(dataset, headers);
			if (!Arrays.asList(headers).contains("lat") || !Arrays.asList(headers).contains("lon")) {
				dataset.setError("Header doesn't include mandatory 'lat' or 'lon' fields.");
				datasetRepository.save(dataset);
				return dataset;
			}

			if (reload) {
				List<String> sample = reservoirSample(fullPath, dataset.sizeLimit);
				insertSampleData(tableName, headers, sample.subList(1, sample.size()), del, true);
				dataset.total = sample.size() - 1;
				LOGGER.info("Stored {} rows into table: {}", sample.size(), tableName);
			}

			dataset.setSourceStatus(dataset.total != null ? Dataset.ConfigStatus.OK : Dataset.ConfigStatus.UNKNOWN);
			return datasetRepository.save(dataset);
		} catch (Exception e) {
			dataset.setError(e.getMessage() == null ? e.toString() : e.getMessage());

			LOGGER.error("Failed to process and insert data from CSV file: {}", fullPath, e);
			return datasetRepository.save(dataset);
		} finally {
			if (dataset.type == Dataset.Source.Overpass) {
				try {
					if (fullPath != null && !Files.deleteIfExists(fullPath)) {
						LOGGER.warn("Could not delete temporary file: {}", fullPath);
					}
				} catch (IOException e) {
					LOGGER.error("Error deleting temporary file: {}", fullPath, e);
				}
			}
		}
	}

	@Async
	public CompletableFuture<Dataset> createDataset(Dataset dataset) {
		return CompletableFuture.supplyAsync(() -> {
			Optional<Dataset> datasetOptional = datasetRepository.findByName(dataset.name);
			if (datasetOptional.isPresent()) {
				throw new RuntimeException("Dataset is already created: " + dataset.name);
			}

			if (dataset.script == null) {
				Path scriptPath = Path.of(webLocation, "js", "search-test", "modules", "lib", "default.js");
				try {
					dataset.script = Files.readString(scriptPath);
				} catch (IOException e) {
					LOGGER.error("Failed to read default script from {}", scriptPath, e);
					dataset.script = null;
				}
			}
			return datasetRepository.save(dataset);
		});
	}

	@Async
	public CompletableFuture<Dataset> updateDataset(Long datasetId, Map<String, String> updates) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset dataset = datasetRepository.findById(datasetId).orElseThrow(() ->
					new RuntimeException("Dataset not found with id: " + datasetId));

			updates.forEach((key, value) -> {
				switch (key) {
					case "name" -> dataset.name = value;
					case "type" -> dataset.type = Dataset.Source.valueOf(value);
					case "source" -> dataset.source = value;
					case "sizeLimit" -> dataset.sizeLimit = Integer.valueOf(value);
					case "fileName" -> dataset.fileName = value;
					case "script" -> dataset.script = value;
				}
			});

			dataset.updated = LocalDateTime.now();
			dataset.setSourceStatus(Dataset.ConfigStatus.OK);
			datasetRepository.save(dataset);
			return checkDatasetInternal(dataset, false);
		});
	}

	private void updateColumns(Dataset dataset, String[] headers) {
		try {
			dataset.allCols = objectMapper.writeValueAsString(headers);
			headers = Arrays.stream(headers).filter(s -> s.startsWith("road") || s.startsWith("city")
					|| s.startsWith("stree") || s.startsWith("addr")).toArray(String[]::new);
			dataset.selCols = objectMapper.writeValueAsString(headers);
		} catch (JsonProcessingException e) {
			LOGGER.error("Failed to parse script: {}", dataset.script, e);
			throw new RuntimeException("Failed to parse script: " + e.getMessage(), e);
		}
	}

	protected void saveResults(EvalJob job, String address, Map<String, Object> row,
							   List<Feature> searchResults, LatLon originalPoint, long duration, String error) throws IOException {
		if (job == null) {
			return;
		}

		int resultsCount = searchResults.size();
		Feature minFeature = null;
		Integer minDistance = null, actualPlace = null;
		String closestResult = null;

		if (originalPoint != null && !searchResults.isEmpty()) {
			double minDistanceMeters = Double.MAX_VALUE;
			LatLon closestPoint = null;
			int place = 0;

			for (Feature feature : searchResults) {
				place++;
				if (feature == null) {
					continue;
				}
				LatLon foundPoint = getLatLon(feature);
				double distance = MapUtils.getDistance(originalPoint.getLatitude(), originalPoint.getLongitude(),
						foundPoint.getLatitude(), foundPoint.getLongitude());
				if (distance < minDistanceMeters) {
					minDistanceMeters = distance;
					closestPoint = foundPoint;
					actualPlace = place;
					minFeature = feature;
				}
			}

			if (closestPoint != null) {
				minDistance = (int) minDistanceMeters;
				closestResult = pointToString(closestPoint);
			}
		}

		if (minFeature != null) {
			for (Map.Entry<String, Object> e : minFeature.properties.entrySet())
				row.put(e.getKey(), e.getValue() == null ? "" : e.getValue().toString());
		}

		String insertSql =
				"INSERT INTO eval_result (job_id, dataset_id, original, error, duration, results_count, " +
						"min_distance, closest_result, address, lat, lon, actual_place, timestamp) VALUES (?, ?, ?, ?,"
						+ " ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		String rowJson = objectMapper.writeValueAsString(row);
		jdbcTemplate.update(insertSql, job.id, job.datasetId, rowJson, error, duration, resultsCount, minDistance,
				closestResult, address, originalPoint == null ? null : originalPoint.getLatitude(),
				originalPoint == null ? null : originalPoint.getLongitude(), actualPlace,
				new java.sql.Timestamp(System.currentTimeMillis()));
	}

	public Page<Dataset> getDatasets(String search, String status, Pageable pageable) {
		return datasetRepository.findAllDatasets(search, status, pageable);
	}

	public String getDatasetSample(Long datasetId) {
		Dataset dataset = datasetRepository.findById(datasetId).orElseThrow(() ->
				new RuntimeException("Dataset not found with id: " + datasetId));

		String tableName = "dataset_" + sanitize(dataset.name);
		String sql = "SELECT * FROM " + tableName;
		try {
			StringWriter stringWriter = new StringWriter();
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);

			if (rows.isEmpty()) {
				return "";
			}

			String[] headers = rows.get(0).keySet().toArray(new String[0]);
			CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader(headers).setDelimiter(';').build();
			try (final CSVPrinter printer = new CSVPrinter(stringWriter, csvFormat)) {
				for (Map<String, Object> row : rows) {
					printer.printRecord(row.values());
				}
			}

			return stringWriter.toString();
		} catch (Exception e) {
			LOGGER.error("Failed to retrieve sample for dataset {}", datasetId, e);
			throw new RuntimeException("Failed to generate dataset sample: " + e.getMessage(), e);
		}
	}

	public boolean deleteDataset(Long datasetId) {
		Optional<Dataset> dsOpt = datasetRepository.findById(datasetId);
		if (dsOpt.isEmpty()) {
			return false;
		}
		String tableName = "dataset_" + sanitize(dsOpt.get().name);
		jdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);

		String deleteSql = "DELETE FROM eval_result WHERE dataset_id = ?";
		jdbcTemplate.update(deleteSql, datasetId);

		deleteSql = "DELETE FROM eval_job WHERE dataset_id = ?";
		jdbcTemplate.update(deleteSql, datasetId);

		Dataset ds = dsOpt.get();
		datasetRepository.delete(ds);
		return true;
	}

	public void insertSampleData(String tableName, String[] columns, List<String> sample, String delimiter,
								 boolean deleteBefore) {
		if (deleteBefore) {
			jdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);
		}
		createDynamicTable(tableName, columns);

		String insertSql =
				"INSERT INTO " + tableName + " (" + String.join(", ", columns) + ") VALUES (" + String.join(", ",
						Collections.nCopies(columns.length, "?")) + ")";
		List<Object[]> batchArgs = new ArrayList<>();
		for (String s : sample) {
			String[] record = s.split(delimiter);
			String[] values = Collections.nCopies(columns.length, "").toArray(new String[0]);
			for (int j = 0; j < values.length && j < record.length; j++) {
				values[j] = crop(unquote(record[j]), 255);
			}
			batchArgs.add(values);
		}
		jdbcTemplate.batchUpdate(insertSql, batchArgs);
		LOGGER.info("Batch inserted {} records into {}.", sample.size(), tableName);
	}

	protected void createDynamicTable(String tableName, String[] columns) {
		String columnsDefinition =
				Stream.of(columns).map(header -> "\"" + header + "\" VARCHAR(255)").collect(Collectors.joining(", "));
		String createTableSql =
				String.format("CREATE TABLE IF NOT EXISTS %s (_id INTEGER PRIMARY KEY AUTOINCREMENT, " + "%s)",
						tableName, columnsDefinition);
		jdbcTemplate.execute(createTableSql);
		LOGGER.info("Ensured table {} exists.", tableName);
	}

	final String REPORT_SQL = "WITH result AS (" +
			"    SELECT" +
			"        UPPER(COALESCE(json_extract(original, '$.web_type'), ''))               AS web_type," +
			"        lat, lon, address, closest_result, min_distance, actual_place, results_count, original," +
			"        actual_place <= ?                                                       AS is_place," +
			"        min_distance <= ?                                                       AS is_dist," +
			"        CAST(COALESCE(json_extract(original, '$.id'), 0) AS INTEGER)            AS id," +
			"        COALESCE(json_extract(original, '$.web_name'), '')                      AS web_name," +
			"        COALESCE(json_extract(original, '$.web_address1'), '')                  AS web_address1," +
			"        COALESCE(json_extract(original, '$.web_address2'), '')                  AS web_address2," +
			"        CAST(COALESCE(json_extract(original, '$.web_poi_id'), 0) AS INTEGER)    AS web_poi_id," +
			"        (address LIKE '%' || COALESCE(json_extract(original, '$.web_name'), '') || '%'" +
			"            AND address LIKE '%' || COALESCE(json_extract(original, '$.web_address1'), '') || '%'" +
			"            AND address LIKE '%' || COALESCE(json_extract(original, '$.web_address2'), '') || '%')" +
			"                                                                                AS is_addr_match," +
			"        ((CAST(COALESCE(json_extract(original, '$.web_poi_id'), 0) AS INTEGER)  / 2) =" +
			"         CAST(COALESCE(json_extract(original, '$.id'), 0) AS INTEGER))          AS is_poi_match" +
			"    FROM eval_result AS r WHERE job_id = ? ORDER BY actual_place, min_distance" +
			") " +
			"SELECT" +
			"    CASE" +
			"        WHEN address IS NULL OR trim(address) = '' THEN 'Empty'" +
			"        WHEN is_place AND is_dist THEN 'Found'" +
			"        WHEN NOT is_place AND is_dist AND (web_type = 'POI' AND is_poi_match OR web_type <> 'POI' AND is_addr_match)" +
			"            THEN 'Near'" +
			"        WHEN is_place AND NOT is_dist AND (web_type = 'POI' AND is_poi_match OR web_type <> 'POI' AND is_addr_match)" +
			"            THEN 'Too Far'" +
			"        ELSE 'Not Found'" +
			"END AS \"group\", web_type, lat, lon, address, actual_place, closest_result, min_distance, results_count, original " +
			"FROM result";

	public void downloadRawResults(Writer writer, int placeLimit, int distLimit, Long jobId, String format) throws IOException {

		List<Map<String, Object>> results = jdbcTemplate.queryForList(REPORT_SQL, placeLimit, distLimit, jobId);
		if ("csv".equalsIgnoreCase(format)) {
			writeAsCsv(writer, results);
		} else if ("json".equalsIgnoreCase(format)) {
			writeAsJson(writer, results);
		} else {
			throw new IllegalArgumentException("Unsupported format: " + format);
		}
	}

	public Optional<EvalJobReport> getEvaluationReport(Long jobId, int placeLimit, int distLimit) {
		String sql = """
				SELECT
				    count(*) AS total,
				    count(*) FILTER (WHERE error IS NOT NULL) AS failed,
				    count(*) FILTER (WHERE results_count = 0) AS notFound,
				    sum(duration) AS duration,
				    avg(actual_place) AS average_place
				FROM
				    eval_result
				WHERE
				    job_id = ?
				""";

		Map<String, Object> result = jdbcTemplate.queryForMap(sql, jobId);
		long processed = ((Number) result.get("total")).longValue();
		if (processed == 0) {
			return Optional.empty();
		}
		long failed = ((Number) result.get("failed")).longValue();
		long notFound = ((Number) result.get("notFound")).longValue();
		long duration = result.get("duration") == null ? 0 : ((Number) result.get("duration")).longValue();
		double averagePlace = result.get("average_place") == null ? 0 :
				((Number) result.get("average_place")).doubleValue();

		Map<String, Number> distanceHistogram = new LinkedHashMap<>();
		distanceHistogram.put("Empty", 0);
		distanceHistogram.put("Found", 0);
		distanceHistogram.put("Near", 0);
		distanceHistogram.put("Too Far", 0);
		distanceHistogram.put("Not Found", 0);
		List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT \"group\", count(*) as cnt FROM (" + REPORT_SQL + ") GROUP BY \"group\"",
				placeLimit, distLimit, jobId);
		for (Map<String, Object> values : results) {
			distanceHistogram.put(values.get("group").toString(), ((Number) values.get("cnt")).longValue());
		}

		EvalJobReport report = new EvalJobReport(jobId, notFound, processed, failed, duration, averagePlace, distanceHistogram);
		return Optional.of(report);
	}

	protected void writeAsJson(Writer writer, List<Map<String, Object>> results) throws IOException {
		List<Map<String, Object>> expanded = results.stream().map(row -> {
			Map<String, Object> out = new LinkedHashMap<>();
			// copy base fields except 'original'
			for (Map.Entry<String, Object> e : row.entrySet()) {
				if (!"original".equals(e.getKey())) {
					out.put(e.getKey(), e.getValue());
				}
			}
			Object originalObj = row.get("original");
			if (originalObj != null) {
				try {
					JsonNode originalNode = objectMapper.readTree(originalObj.toString());
					// For consistency with CSV, serialize values as text
					originalNode.fieldNames().forEachRemaining(fn -> {
						JsonNode v = originalNode.get(fn);
						out.put(fn, v == null || v.isNull() ? null : v.asText());
					});
				} catch (IOException e) {
					// ignore invalid JSON in 'original'
				}
			}
			return out;
		}).collect(Collectors.toList());
		objectMapper.writeValue(writer, expanded);
	}

	protected void writeAsCsv(Writer writer, List<Map<String, Object>> results) throws IOException {
		if (results.isEmpty()) {
			writer.write("");
			return;
		}

		Set<String> headers = new LinkedHashSet<>();
		results.get(0).keySet().stream()
				.filter(k -> !k.equals("original"))
				.forEach(headers::add);

		Set<String> originalHeaders = new LinkedHashSet<>();
		for (Map<String, Object> row : results) {
			Object originalObj = row.get("original");
			if (originalObj != null) {
				try {
					JsonNode originalNode = objectMapper.readTree(originalObj.toString());
					originalNode.fieldNames().forEachRemaining(originalHeaders::add);
				} catch (IOException e) {
					// ignore invalid JSON in 'original'
				}
			}
		}
		headers.addAll(originalHeaders);

		CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
				.setHeader(headers.toArray(new String[0])).setDelimiter(";")
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
						// keep originalNode null
					}
				}

				for (String header : headers) {
					if (row.containsKey(header)) {
						Object value = row.get(header);
						record.add(value != null ? value.toString().trim() : null);
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
}
