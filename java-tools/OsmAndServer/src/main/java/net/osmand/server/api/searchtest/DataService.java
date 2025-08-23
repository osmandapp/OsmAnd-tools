package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.dto.RunStatus;
import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.entity.Run;
import net.osmand.server.api.searchtest.entity.TestCase;
import net.osmand.server.api.searchtest.repo.DatasetRepository;
import net.osmand.server.api.searchtest.repo.RunRepository;
import net.osmand.server.api.searchtest.repo.TestCaseRepository;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.util.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
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

public abstract class DataService extends BaseService {
	protected final DatasetRepository datasetRepo;
	protected final TestCaseRepository testCaseRepo;
	protected final RunRepository runRepo;
	protected final JdbcTemplate jdbcTemplate;
	protected final EntityManager em;

	final String REPORT_SQL = "WITH result AS (" +
			"    SELECT" +
			"        UPPER(COALESCE(json_extract(row, '$.web_type'), ''))               AS web_type," +
			"        lat, lon, address, closest_result, min_distance, actual_place, results_count, row," +
			"        actual_place <= ?                                                       AS is_place," +
			"        min_distance <= ?                                                       AS is_dist," +
			"        CAST(COALESCE(json_extract(row, '$.id'), 0) AS INTEGER)            AS id," +
			"        COALESCE(json_extract(row, '$.web_name'), '')                      AS web_name," +
			"        COALESCE(json_extract(row, '$.web_address1'), '')                  AS web_address1," +
			"        COALESCE(json_extract(row, '$.web_address2'), '')                  AS web_address2," +
			"        CAST(COALESCE(json_extract(row, '$.web_poi_id'), 0) AS INTEGER)    AS web_poi_id," +
			"        (address LIKE '%' || COALESCE(json_extract(row, '$.web_name'), '') || '%'" +
			"            AND address LIKE '%' || COALESCE(json_extract(row, '$.web_address1'), '') || '%'" +
			"            AND address LIKE '%' || COALESCE(json_extract(row, '$.web_address2'), '') || '%')" +
			"                                                                                AS is_addr_match," +
			"        ((CAST(COALESCE(json_extract(row, '$.web_poi_id'), 0) AS INTEGER)  / 2) =" +
			"         CAST(COALESCE(json_extract(row, '$.id'), 0) AS INTEGER))          AS is_poi_match" +
			"    FROM run_result AS r WHERE run_id = ? ORDER BY actual_place, min_distance" +
			") " +
			"SELECT" +
			"    CASE" +
			"        WHEN address IS NULL OR trim(address) = '' THEN 'Empty'" +
			"        WHEN is_place AND is_dist THEN 'Found'" +
			"        WHEN NOT is_place AND is_dist AND (web_type = 'POI' AND is_poi_match OR web_type <> 'POI' AND " +
			"is_addr_match)" +
			"            THEN 'Near'" +
			"        WHEN is_place AND NOT is_dist AND (web_type = 'POI' AND is_poi_match OR web_type <> 'POI' AND " +
			"is_addr_match)" +
			"            THEN 'Too Far'" +
			"        ELSE 'Not Found'" +
			"END AS \"group\", web_type, lat, lon, address, actual_place, closest_result, min_distance, results_count," +
			" row " +
			"FROM result";

	public DataService(EntityManager em, DatasetRepository datasetRepo, TestCaseRepository testCaseRepo, RunRepository runRepo,
					   @Qualifier("testJdbcTemplate") JdbcTemplate jdbcTemplate, WebClient.Builder webClientBuilder,
					   ObjectMapper objectMapper) {
		super(webClientBuilder, objectMapper);

		this.em = em;
		this.testCaseRepo = testCaseRepo;
		this.datasetRepo = datasetRepo;
		this.runRepo = runRepo;
		this.jdbcTemplate = jdbcTemplate;
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
				datasetRepo.save(dataset);
				return dataset;
			}

			String tableName = "dataset_" + sanitize(dataset.name);
			String header = getHeader(fullPath);
			if (header == null || header.trim().isEmpty()) {
				dataset.setError("File doesn't have header.");
				datasetRepo.save(dataset);
				return dataset;
			}

			String del =
					header.chars().filter(ch -> ch == ',').count() <
							header.chars().filter(ch -> ch == ';').count() ? ";" : ",";
			String[] headers =
					Stream.of(header.toLowerCase().split(del)).map(DataService::sanitize).toArray(String[]::new);
			dataset.allCols = objectMapper.writeValueAsString(headers);
			if (!Arrays.asList(headers).contains("lat") || !Arrays.asList(headers).contains("lon")) {
				String error = "Header doesn't include mandatory 'lat' or 'lon' fields.";
				LOGGER.error("{} Header: {}", error, String.join(",", headers));
				dataset.setError(error);
				datasetRepo.save(dataset);
				return dataset;
			}

			dataset.selCols = objectMapper.writeValueAsString(Arrays.stream(headers).filter(s ->
					s.startsWith("road") || s.startsWith("city") ||
							s.startsWith("stree") || s.startsWith("addr")).toArray(String[]::new));
			if (reload) {
				List<String> sample = reservoirSample(fullPath, dataset.sizeLimit);
				insertSampleData(tableName, headers, sample.subList(1, sample.size()), del, true);
				dataset.total = sample.size() - 1;
				LOGGER.info("Stored {} rows into table: {}", sample.size(), tableName);
			}

			dataset.setSourceStatus(dataset.total != null ? Dataset.ConfigStatus.OK : Dataset.ConfigStatus.UNKNOWN);
			return datasetRepo.save(dataset);
		} catch (Exception e) {
			dataset.setError(e.getMessage() == null ? e.toString() : e.getMessage());

			LOGGER.error("Failed to process and insert data from CSV file: {}", fullPath, e);
			return datasetRepo.save(dataset);
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
			Optional<Dataset> datasetOptional = datasetRepo.findByName(dataset.name);
			if (datasetOptional.isPresent()) {
				throw new RuntimeException("Dataset is already created: " + dataset.name);
			}

			dataset.created = LocalDateTime.now();
			dataset.updated = dataset.created;
			return checkDatasetInternal(datasetRepo.save(dataset), true);
		});
	}

	@Async
	public CompletableFuture<Dataset> updateDataset(Long id, Boolean reload, Map<String, String> updates) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset dataset = datasetRepo.findById(id).orElseThrow(() ->
					new RuntimeException("Dataset not found with id: " + id));

			updates.forEach((key, value) -> {
				switch (key) {
					case "name" -> dataset.name = value;
					case "type" -> dataset.type = Dataset.Source.valueOf(value);
					case "source" -> dataset.source = value;
					case "sizeLimit" -> dataset.sizeLimit = Integer.valueOf(value);
					case "labels" -> dataset.labels = value;
				}
			});

			dataset.updated = LocalDateTime.now();
			dataset.setSourceStatus(Dataset.ConfigStatus.OK);
			datasetRepo.save(dataset);
			return checkDatasetInternal(dataset, reload);
		});
	}

	protected void saveCaseResults(TestCase test, RowAddress data, long duration, String error) throws IOException {
		String sql =
				"INSERT INTO gen_result (sequence, case_id, dataset_id, row, query, error, duration, lat, lon, timestamp) " +
						"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		String rowJson = objectMapper.writeValueAsString(data.row());
		String[] outputArray = objectMapper.readValue(data.output(), String[].class);
		int sequence = 0;
		for (String query : outputArray) {
			jdbcTemplate.update(sql, sequence++, test.id, test.datasetId, rowJson, query, error, duration,
					data.point().getLatitude(), data.point().getLongitude(),
					new java.sql.Timestamp(System.currentTimeMillis()));
		}
	}

	protected void saveRunResults(long resultId, int sequence, Run run, String output, Map<String, Object> row,
								  List<Feature> searchResults, LatLon targetPoint, long duration, String error) throws IOException {
		int resultsCount = searchResults.size();
		Feature minFeature = null;
		Integer minDistance = null, actualPlace = null;
		String closestResult = null;

		if (targetPoint != null && !searchResults.isEmpty()) {
			double minDistanceMeters = Double.MAX_VALUE;
			LatLon closestPoint = null;
			int place = 0;

			for (Feature feature : searchResults) {
				place++;
				if (feature == null) {
					continue;
				}
				LatLon foundPoint = getLatLon(feature);
				double distance = MapUtils.getDistance(targetPoint.getLatitude(), targetPoint.getLongitude(),
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

		String sql = "INSERT INTO run_result (gen_id, sequence, dataset_id, run_id, case_id, query, row, error, duration, results_count, " +
						"min_distance, closest_result, actual_place, lat, lon, timestamp) "+
						"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		String rowJson = objectMapper.writeValueAsString(row);
		jdbcTemplate.update(sql, resultId, sequence, run.datasetId, run.id, run.caseId, output, rowJson, error, duration, resultsCount,
				minDistance, closestResult, actualPlace, targetPoint == null ? null : targetPoint.getLatitude(),
				targetPoint == null ? null : targetPoint.getLongitude(),
				new java.sql.Timestamp(System.currentTimeMillis()));
	}

	/**
	 * Find all datasets matching the given filters.
	 * @param name Case-insensitive search for the dataset name.
	 * @param labels Case-insensitive search for comma-separated labels associated with the dataset.
	 * @param status Limits the results to datasets with the given status.
	 * @param pageable Pageable request defining the page number and size.
	 * @return Page of matching datasets.
	 */
	public Page<Dataset> getDatasets(String name, String labels, String status, Pageable pageable) {
		return datasetRepo.findAllDatasets(name, labels, status, pageable);
	}

	public String getDatasetSample(Long datasetId) {
		Dataset dataset = datasetRepo.findById(datasetId).orElseThrow(() ->
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
		Optional<Dataset> dsOpt = datasetRepo.findById(datasetId);
		if (dsOpt.isEmpty()) {
			return false;
		}
		String tableName = "dataset_" + sanitize(dsOpt.get().name);
		jdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);

		String deleteSql = "DELETE FROM run_result WHERE dataset_id = ?";
		jdbcTemplate.update(deleteSql, datasetId);

		deleteSql = "DELETE FROM test_case WHERE dataset_id = ?";
		jdbcTemplate.update(deleteSql, datasetId);

		Dataset ds = dsOpt.get();
		datasetRepo.delete(ds);
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

	public void downloadRawResults(Writer writer, int placeLimit, int distLimit, Long caseId, String format) throws IOException {
		List<Map<String, Object>> results = jdbcTemplate.queryForList(REPORT_SQL, placeLimit, distLimit, caseId);
		if ("csv".equalsIgnoreCase(format)) {
			writeAsCsv(writer, results);
		} else if ("json".equalsIgnoreCase(format)) {
			writeAsJson(writer, results);
		} else {
			throw new IllegalArgumentException("Unsupported format: " + format);
		}
	}

	public Optional<RunStatus> getTestCaseStatus(Long cased) {
		String sql = """
				SELECT (select status from test_case where id = case_id) AS status,
				    count(*) AS total,
				    count(*) FILTER (WHERE error IS NOT NULL) AS failed,
				    sum(duration) AS duration,
				    count(*) FILTER (WHERE query IS NULL or trim(query) = '') AS empty
				FROM
				    gen_result
				WHERE
				    case_id = ?
				""";
		try {
			Map<String, Object> result = jdbcTemplate.queryForMap(sql, cased);
			String status = (String) result.get("status");
			if (status == null)
				status = TestCase.Status.NEW.name();

			long total = ((Number) result.get("total")).longValue();

			Number number = ((Number) result.get("failed"));
			long failed = number == null ? 0 : number.longValue();

			number = ((Number) result.get("duration"));
			long duration = number == null ? 0 : number.longValue();

			RunStatus report = new RunStatus(TestCase.Status.valueOf(status), 0, total, failed,	duration, 0.0, null);
			return Optional.of(report);
		} catch (EmptyResultDataAccessException ee) {
			return Optional.empty();
		}
	}

	public Optional<RunStatus> getRunStatus(Long runId) {
		String sql = """
				SELECT (select status from test_case where id = case_id) AS status,
				    count(*) AS total,
				    count(*) FILTER (WHERE error IS NOT NULL) AS failed,
				    sum(duration) AS duration,
				    avg(actual_place) FILTER (WHERE actual_place IS NOT NULL) AS average_place,
				    count(*) FILTER (WHERE query IS NULL or trim(query) = '') AS empty,
				    count(*) FILTER (WHERE min_distance IS NULL and query IS NOT NULL and trim(query) != '') AS no_result
				FROM
				    run_result
				WHERE
				    run_id = ?
				""";
		try {
			Map<String, Object> result = jdbcTemplate.queryForMap(sql, runId);
			String status = (String) result.get("status");
			if (status == null)
				status = TestCase.Status.NEW.name();

			Number number = ((Number) result.get("average_place"));
			double averagePlace = number == null ? 0.0 : number.doubleValue();

			number = ((Number) result.get("no_result"));
			long noResult = number == null ? 0 : number.longValue();

			long total = ((Number) result.get("total")).longValue();

			number = ((Number) result.get("failed"));
			long failed = number == null ? 0 : number.longValue();

			number = ((Number) result.get("duration"));
			long duration = number == null ? 0 : number.longValue();

			RunStatus report = new RunStatus(TestCase.Status.valueOf(status), noResult, total, failed,
					duration, averagePlace, null);
			return Optional.of(report);
		} catch (EmptyResultDataAccessException ee) {
			return Optional.empty();
		}
	}

	public Optional<RunStatus> getRunReport(Long caseId, int placeLimit, int distLimit) {
		Optional<RunStatus> opt = getRunStatus(caseId);
		if (opt.isEmpty()) {
			return Optional.empty();
		}

		Map<String, Number> distanceHistogram = new LinkedHashMap<>();
		distanceHistogram.put("Empty", 0);
		distanceHistogram.put("Found", 0);
		distanceHistogram.put("Near", 0);
		distanceHistogram.put("Too Far", 0);
		distanceHistogram.put("Not Found", 0);
		List<Map<String, Object>> results =
				jdbcTemplate.queryForList("SELECT \"group\", count(*) as cnt FROM (" + REPORT_SQL + ") GROUP BY " +
								"\"group\"",
				placeLimit, distLimit, caseId);
		for (Map<String, Object> values : results) {
			distanceHistogram.put(values.get("group").toString(), ((Number) values.get("cnt")).longValue());
		}

		RunStatus metric = opt.get();
		metric = new RunStatus(metric.status(), metric.noResult(), metric.processed(),
				metric.failed(), metric.duration(), metric.averagePlace(), distanceHistogram);
		return Optional.of(metric);
	}

	protected void writeAsJson(Writer writer, List<Map<String, Object>> results) throws IOException {
		List<Map<String, Object>> expanded = results.stream().map(row -> {
			Map<String, Object> out = new LinkedHashMap<>();
			// copy base fields except 'row'
			for (Map.Entry<String, Object> e : row.entrySet()) {
				if (!"row".equals(e.getKey())) {
					out.put(e.getKey(), e.getValue());
				}
			}
			Object rowObj = row.get("row");
			if (rowObj != null) {
				try {
					JsonNode rowNode = objectMapper.readTree(rowObj.toString());
					// For consistency with CSV, serialize values as text
					rowNode.fieldNames().forEachRemaining(fn -> {
						JsonNode v = rowNode.get(fn);
						out.put(fn, v == null || v.isNull() ? null : v.asText());
					});
				} catch (IOException e) {
					// ignore invalid JSON in 'row'
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
				.filter(k -> !k.equals("row"))
				.forEach(headers::add);

		Set<String> rowHeaders = new LinkedHashSet<>();
		for (Map<String, Object> row : results) {
			Object rowObj = row.get("row");
			if (rowObj != null) {
				try {
					JsonNode rowNode = objectMapper.readTree(rowObj.toString());
					rowNode.fieldNames().forEachRemaining(rowHeaders::add);
				} catch (IOException e) {
					// ignore invalid JSON in 'row'
				}
			}
		}
		headers.addAll(rowHeaders);

		CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
				.setHeader(headers.toArray(new String[0])).setDelimiter(";")
				.build();

		try (final CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {
			for (Map<String, Object> row : results) {
				List<String> record = new ArrayList<>();
				JsonNode rowNode = null;
				Object rowObj = row.get("row");
				if (rowObj != null) {
					try {
						rowNode = objectMapper.readTree(rowObj.toString());
					} catch (IOException e) {
						// keep rowNode null
					}
				}

				for (String header : headers) {
					if (row.containsKey(header)) {
						Object value = row.get(header);
						record.add(value != null ? value.toString().trim() : null);
					} else if (rowNode != null && rowNode.has(header)) {
						record.add(rowNode.get(header).asText());
					} else {
						record.add(null);
					}
				}
				printer.printRecord(record);
			}
		}
	}

	public List<String> getAllLabels() {
		try {
			String sql = "SELECT labels FROM dataset";
			List<String> rows = jdbcTemplate.queryForList(sql, String.class);
			sql = "SELECT labels FROM test_case";
			rows.addAll(jdbcTemplate.queryForList(sql, String.class));

			Set<String> set = new LinkedHashSet<>();
			for (String label : rows) {
				if (label == null) {
					continue;
				}
				for (String l : label.split("[#,]+")) { // split by '#' or ',', treat consecutive as one
					String t = l.trim();
					if (!t.isEmpty()) {
						set.add(t);
					}
				}
			}

			List<String> results = new ArrayList<>(set);
			results.sort(null);
			return results;
		} catch (Exception e) {
			LOGGER.error("Failed to retrieve labels", e);
			throw new RuntimeException("Failed to retrieve labels: " + e.getMessage(), e);
		}
	}
}
