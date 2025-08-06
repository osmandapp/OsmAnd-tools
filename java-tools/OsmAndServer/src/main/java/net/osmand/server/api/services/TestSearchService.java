package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityManager;
import net.osmand.data.LatLon;
import net.osmand.server.api.test.dto.EvaluationReport;
import net.osmand.server.api.test.dto.JobProgress;
import net.osmand.server.api.test.entity.*;
import net.osmand.server.api.test.repo.DatasetJobRepository;
import net.osmand.server.api.test.repo.DatasetRepository;
import net.osmand.server.api.utils.GeometryUtils;
import net.osmand.server.api.utils.StringUtils;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.util.MapUtils;
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
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static net.osmand.server.api.utils.StringUtils.*;

@Service
public class TestSearchService {
	private static final Logger LOGGER = LoggerFactory.getLogger(TestSearchService.class);

	private final DatasetRepository datasetRepository;
	private final DatasetJobRepository datasetJobRepository;
	private final JdbcTemplate jdbcTemplate;
	private final SearchService searchService;
	private final ObjectMapper objectMapper;
	private final WebClient.Builder webClientBuilder;
	private final SimpMessagingTemplate messagingTemplate;
	private final EntityManager em;
	private WebClient webClient;
	@Value("${test.csv.dir}")
	private String csvDownloadingDir;
	@Value("${test.overpass.url}")
	private String overpassApiUrl;

	@Autowired
	public TestSearchService(EntityManager em, DatasetRepository datasetRepository,
							 DatasetJobRepository datasetJobRepository,
							 @Qualifier("testJdbcTemplate") JdbcTemplate jdbcTemplate,
							 SearchService searchService, WebClient.Builder webClientBuilder,
							 ObjectMapper objectMapper,
							 SimpMessagingTemplate messagingTemplate) {
		this.em = em;
		this.datasetRepository = datasetRepository;
		this.datasetJobRepository = datasetJobRepository;
		this.jdbcTemplate = jdbcTemplate;
		this.searchService = searchService;
		this.webClientBuilder = webClientBuilder;
		this.objectMapper = objectMapper;
		this.messagingTemplate = messagingTemplate;
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
			sample.add(0, header); // Restore header
		}

		return sample;
	}

	@PostConstruct
	private void initWebClient() {
		this.webClient = webClientBuilder.baseUrl(overpassApiUrl)
				.exchangeStrategies(ExchangeStrategies.builder()
						.codecs(configurer -> configurer
								.defaultCodecs()
								.maxInMemorySize(16 * 1024 * 1024))
						.build())
				.build();
	}

	private Path queryOverpass(String query) {
		Path tempFile;
		try {
			String overpassResponse = webClient.post()
					.uri("")
					.bodyValue("[out:json][timeout:25];" + query + ";out;")
					.retrieve()
					.bodyToMono(String.class)
					.toFuture().join();
			tempFile = Files.createTempFile(Path.of(csvDownloadingDir), "overpass_", ".csv");
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
		try (OutputStream gzipOutputStream = new BufferedOutputStream(Files.newOutputStream(outputPath));
			 CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(gzipOutputStream),
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
	public CompletableFuture<Dataset> refreshDataset(Long datasetId, Boolean reload) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset dataset = datasetRepository.findById(datasetId)
					.orElseThrow(() -> new RuntimeException("Dataset not found with id: " + datasetId));

			Path fullPath = null;
			dataset.setSourceStatus(DatasetConfigStatus.UNKNOWN);
			try {
				if (dataset.getType() == DatasetSource.Overpass) {
					fullPath = queryOverpass(dataset.getSource());
				} else {
					fullPath = Path.of(csvDownloadingDir, dataset.getSource());
				}
				if (!Files.exists(fullPath)) {
					dataset.setError("File is not existed.");
					datasetRepository.save(dataset);
					return dataset;
				}

				String tableName = "dataset_" + sanitize(dataset.getName());
				String header = getHeader(fullPath);
				if (header == null || header.trim().isEmpty()) {
					dataset.setError("File doesn't have header.");
					datasetRepository.save(dataset);
					return dataset;
				}

				String del =
						header.chars().filter(ch -> ch == ',').count() < header.chars().filter(ch -> ch == ';').count() ? ";" : ",";
				String[] headers =
						Stream.of(header.toLowerCase().split(del)).map(StringUtils::sanitize).toArray(String[]::new);
				dataset.setColumns(objectMapper.writeValueAsString(headers));
				if (!Arrays.asList(headers).contains("lat") || !Arrays.asList(headers).contains("lon")) {
					dataset.setError("Header doesn't include mandatory 'lat' or 'lon' fields.");
					datasetRepository.save(dataset);
					return dataset;
				}

				if (reload != null && reload) {
					List<String> sample = reservoirSample(fullPath, dataset.getSizeLimit());
					insertSampleData(tableName, headers, sample.subList(1, sample.size()), del, true);
					dataset.setTotal(sample.size() - 1);
					LOGGER.info("Stored {} rows into table: {}", sample.size(), tableName);
				}

				if (dataset.getAddressExpression() == null || dataset.getAddressExpression().trim().isEmpty()) {
					String exp =
							Stream.of(headers).filter(h -> h.startsWith("city") || h.startsWith("street") || h.startsWith("road") || h.startsWith("addr_")).collect(Collectors.joining(" || ' ' || "));
					dataset.setAddressExpression(exp);
				}

				if (dataset.getTotal() != null) {
					String error = updateSQLExpression(dataset.getName(), dataset.getAddressExpression());
					if (error != null) {
						dataset.setError("Incorrect SQL expression: " + error);
						datasetRepository.save(dataset);
						return dataset;
					}
				}

				dataset.setSourceStatus(dataset.getTotal() != null ? DatasetConfigStatus.OK :
						DatasetConfigStatus.UNKNOWN);
				datasetRepository.save(dataset);

				return dataset;
			} catch (Exception e) {
				dataset.setError(e.getMessage() == null ? e.toString() : e.getMessage());
				datasetRepository.save(dataset);
				LOGGER.error("Failed to process and insert data from CSV file: {}", fullPath, e);
				return dataset;
			} finally {
				if (dataset.getType() == DatasetSource.Overpass) {
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
	public CompletableFuture<Dataset> createDataset(Dataset dataset) {
		return CompletableFuture.supplyAsync(() -> {
			Optional<Dataset> datasetOptional = datasetRepository.findByName(dataset.getName());
			if (datasetOptional.isPresent())
				throw new RuntimeException("Dataset is already created: " + dataset.getName());

			return datasetRepository.save(dataset);
		});
	}

	@Async
	public CompletableFuture<Dataset> updateDataset(Long datasetId, Map<String, String> updates) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset dataset = datasetRepository.findById(datasetId)
					.orElseThrow(() -> new RuntimeException("Dataset not found with id: " + datasetId));

			updates.forEach((key, value) -> {
				switch (key) {
					case "name":
						dataset.setName(value);
						break;
					case "type":
						dataset.setType(DatasetSource.valueOf(value));
						break;
					case "source":
						dataset.setSource(value);
						break;
					case "sizeLimit":
						dataset.setSizeLimit(Integer.valueOf(value));
						break;
					case "addressExpression":
						dataset.setAddressExpression(value);
						break;
				}
			});

			dataset.setUpdated(LocalDateTime.now());
			dataset.setSourceStatus(DatasetConfigStatus.OK);
			return datasetRepository.save(dataset);
		});
	}

	public EvalJob startEvaluation(Long datasetId, Map<String, String> payload) {
		Dataset dataset = datasetRepository.findById(datasetId)
				.orElseThrow(() -> new RuntimeException("Dataset not found with id: " + datasetId));

		if (dataset.getSourceStatus() != DatasetConfigStatus.OK) {
			throw new RuntimeException(String.format("Dataset %s is not in OK state (%s)", datasetId,
					dataset.getSourceStatus()));
		}
		EvalJob job = new EvalJob();
		job.setDatasetId(datasetId);
		job.setCreated(new java.sql.Timestamp(System.currentTimeMillis()));

		job.setAddressExpression(dataset.getAddressExpression());
		String locale = payload.get("locale");
		if (locale == null || locale.trim().isEmpty()) {
			locale = "en";
		}
		job.setLocale(locale);
		job.setNorthWest(payload.get("northWest"));
		job.setSouthEast(payload.get("southEast"));
		job.setBaseSearch(Boolean.parseBoolean(payload.get("baseSearch")));

		job.setStatus(JobStatus.RUNNING);
		return datasetJobRepository.save(job);
	}

	@Async
	public void processEvaluation(Long jobId) {
		Optional<EvalJob> jobOptional = datasetJobRepository.findById(jobId);
		EvalJob job = jobOptional.orElseThrow(() -> new RuntimeException("Job not found with id: " + jobId));

		if (job.getStatus() != JobStatus.RUNNING) {
			LOGGER.info("Job {} is not in RUNNING state ({}). Skipping processing request.", jobId, job.getStatus());
			return; // do not process cancelled/completed jobs
		}

		EvalJob finalJob = job;
		Dataset dataset = datasetRepository.findById(job.getDatasetId())
				.orElseThrow(() -> new RuntimeException("Dataset not found with id: " + finalJob.getDatasetId()));

		String tableName = "dataset_" + sanitize(dataset.getName());
		String sql = String.format("SELECT * FROM %s", tableName);
		long processedCount = 0;
		long errorCount = 0;
		long totalStartTime = System.currentTimeMillis();
		try {
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
			long totalRows = rows.size();
			for (Map<String, Object> row : rows) {
				job = datasetJobRepository.findById(jobId)
						.map(j -> {
							em.detach(j);
							return j;
						}) // detach to avoid caching
						.orElse(null);
				if (job == null || job.getStatus() != JobStatus.RUNNING) {
					LOGGER.info("Job {} was cancelled or deleted. Stopping execution.", jobId);
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
						throw new IllegalArgumentException("Invalid or missing (lat, lon) in WKT format: (" + lat + " "
								+ lon + ")");
					}

					List<Feature> searchResults = searchService.search(point.getLatitude(), point.getLongitude(),
							address, job.getLocale(), job.getBaseSearch(), job.getNorthWest(), job.getSouthEast());
					saveResults(job, dataset, address, originalJson, searchResults, point,
							System.currentTimeMillis() - startTime, null);
				} catch (Exception e) {
					errorCount++;
					LOGGER.warn("Failed to process row for job {}: {}", job.getId(), originalJson, e);
					saveResults(job, dataset, address, originalJson, Collections.emptyList(), point,
							System.currentTimeMillis() - startTime, e.getMessage() == null ? e.toString() :
									e.getMessage());
				}
				processedCount++;
				JobProgress progress = new JobProgress(job.getId(), dataset.getId(), job.getStatus(), totalRows,
						processedCount, errorCount, System.currentTimeMillis() - totalStartTime);
				messagingTemplate.convertAndSend("/topic/eval/ws", progress);
			}
			if (job != null && job.getStatus() != JobStatus.CANCELED) {
				job.setStatus(JobStatus.COMPLETED);
			}
		} catch (Exception e) {
			LOGGER.error("Evaluation failed for job {} on dataset {}", job.getId(), dataset.getId(), e);
			job.setStatus(JobStatus.FAILED);
			job.setError(e.getMessage());
		} finally {
			if (job != null) {
				job.setUpdated(new java.sql.Timestamp(System.currentTimeMillis()));
				datasetJobRepository.save(job);

				JobProgress finalProgress = new JobProgress(job.getId(), dataset.getId(), job.getStatus(),
						dataset.getTotal(), processedCount, errorCount, System.currentTimeMillis() - totalStartTime);
				messagingTemplate.convertAndSend("/topic/eval/ws", finalProgress);
			}
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

	@Async
	public CompletableFuture<Void> deleteJob(Long jobId) {
		return CompletableFuture.runAsync(() -> {
			if (!datasetJobRepository.existsById(jobId)) {
				throw new RuntimeException("Job not found with id: " + jobId);
			}

			String deleteSql = "DELETE FROM eval_result WHERE job_id = ?";
			jdbcTemplate.update(deleteSql, jobId);

			datasetJobRepository.deleteById(jobId);
			LOGGER.info("Deleted job with id: {}", jobId);
		});
	}

	private void saveResults(EvalJob job, Dataset dataset, String address, String originalJson,
							 List<Feature> searchResults, LatLon originalPoint, long duration, String error) {
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
				double distance = MapUtils.getDistance(originalPoint.getLatitude(), originalPoint.getLongitude(),
						foundPoint.getLatitude(), foundPoint.getLongitude());
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
				"min_distance, closest_result, address, lat, lon, actual_place, timestamp) VALUES (?, ?, ?, ?, ?, ?," +
				" " +
				"?, ?, ?, ?, ?, ?, ?)";
		jdbcTemplate.update(insertSql, job.getId(), dataset.getId(), originalJson, error, duration, resultsCount,
				minDistance, closestResult, address, originalPoint == null ? null : originalPoint.getLatitude(),
				originalPoint == null ? null : originalPoint.getLongitude(), actualPlace,
				new java.sql.Timestamp(System.currentTimeMillis()));
	}

	public Page<Dataset> getDatasets(String search, String status, Pageable pageable) {
		return datasetRepository.findAllDatasets(search, status, pageable);
	}

	public Page<EvalJob> getDatasetJobs(Long datasetId, Pageable pageable) {
		return datasetJobRepository.findByDatasetIdOrderByIdDesc(datasetId, pageable);
	}

	public String updateSQLExpression(String name, String exp) {
		String tableName = "dataset_" + sanitize(name);
		try {
			int rows = jdbcTemplate.update("UPDATE " + tableName + " SET address = " + exp);
			if (rows <= 0) {
				return "Dataset is empty";
			}

			Integer numRows = jdbcTemplate.queryForObject("SELECT count(*) FROM " + tableName + " WHERE address IS " +
					"NULL", Integer.class);
			if (numRows != null && numRows > 0)
				return "There are " + numRows + " rows with null address.";
			return null;
		} catch (Exception e) {
			return e.getMessage();
		}
	}

	public String getDatasetSample(Long datasetId) {
		Dataset dataset = datasetRepository.findById(datasetId)
				.orElseThrow(() -> new RuntimeException("Dataset not found with id: " + datasetId));

		String tableName = "dataset_" + sanitize(dataset.getName());
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

	public Optional<EvaluationReport> getEvaluationReport(Long jobId) {
		Optional<EvalJob> jobOptional = datasetJobRepository.findById(jobId);
		if (jobOptional.isEmpty()) {
			return Optional.empty();
		}

		String sql = """
				SELECT
				    count(*) AS total,
				    count(*) FILTER (WHERE error IS NOT NULL) AS failed,
				    sum(duration) AS duration,
				    avg(actual_place) AS average_place,
				    count(*) FILTER (WHERE min_distance IS NULL) AS not_found,
				    sum(CASE WHEN min_distance BETWEEN 0 AND 1 THEN 1 ELSE 0 END) AS "0-1m",
				    sum(CASE WHEN 0 <= min_distance AND min_distance <= 50 THEN 1 ELSE 0 END) AS "0-50m",
				    sum(CASE WHEN 50 < min_distance AND min_distance <= 500 THEN 1 ELSE 0 END) AS "50-500m",
				    sum(CASE WHEN 500 < min_distance AND min_distance <= 1000 THEN 1 ELSE 0 END) AS "500-1000m",
				    sum(CASE WHEN 1000 < min_distance THEN 1 ELSE 0 END) AS "1000m+"
				FROM
				    eval_result
				WHERE
				    job_id = ?
				""";

		Map<String, Object> result = jdbcTemplate.queryForMap(sql, jobId);
		long processed = ((Number) result.get("total")).longValue();
		if (processed == 0) {
			return Optional.empty(); // No data to report
		}
		long error = ((Number) result.get("failed")).longValue();
		long duration = result.get("duration") == null ? 0 : ((Number) result.get("duration")).longValue();
		double averagePlace = result.get("average_place") == null ? 0 :
				((Number) result.get("average_place")).doubleValue();
		Number notFound = (Number) result.get("not_found");

		Map<String, Number> distanceHistogram = new LinkedHashMap<>();
		distanceHistogram.put("Not found", notFound);
		distanceHistogram.put("0-50m", ((Number) result.getOrDefault("0-50m", 0)).longValue());
		distanceHistogram.put("50-500m", ((Number) result.getOrDefault("50-500m", 0)).longValue());
		distanceHistogram.put("500-1000m", ((Number) result.getOrDefault("500-1000m", 0)).longValue());
		distanceHistogram.put("1000m+", ((Number) result.getOrDefault("1000m+", 0)).longValue());

		EvaluationReport report = new EvaluationReport(jobId, processed, processed, error, duration, averagePlace,
				distanceHistogram);
		return Optional.of(report);
	}

	public Optional<EvalJob> getJob(Long jobId) {
		return datasetJobRepository.findById(jobId);
	}

	public boolean deleteDataset(Long datasetId) {
		Optional<Dataset> dsOpt = datasetRepository.findById(datasetId);
		if (dsOpt.isEmpty()) {
			return false;
		}
		String tableName = "dataset_" + sanitize(dsOpt.get().getName());
		jdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);

		String deleteSql = "DELETE FROM eval_result WHERE dataset_id = ?";
		jdbcTemplate.update(deleteSql, datasetId);

		deleteSql = "DELETE FROM eval_job WHERE dataset_id = ?";
		jdbcTemplate.update(deleteSql, datasetId);

		Dataset ds = dsOpt.get();
		datasetRepository.delete(ds);
		return true;
	}

	public void downloadRawResults(Writer writer, Long jobId, String format) throws IOException {
		Optional<EvalJob> jobOptional = datasetJobRepository.findById(jobId);
		if (jobOptional.isEmpty()) {
			throw new RuntimeException("No evaluation job found for jobId: " + jobId);
		}

		List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM eval_result WHERE job_id = ?",
				jobId);
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
						// Keep originalNode null
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

	public void insertSampleData(String tableName, String[] columns, List<String> sample, String delimiter,
								 boolean deleteBefore) {
		if (deleteBefore) {
			jdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);
		}
		createDynamicTable(tableName, columns);

		String insertSql = "INSERT INTO " + tableName + " (" + String.join(", ", columns) + ") VALUES (" +
				String.join(", ", Collections.nCopies(columns.length, "?")) + ")";
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

	private void createDynamicTable(String tableName, String[] columns) {
		String columnsDefinition = Stream.of(columns)
				.map(header -> "\"" + header + "\" VARCHAR(255)")
				.collect(Collectors.joining(", "));
		// Use an auto-incrementing primary key column compatible with SQLite (INTEGER PRIMARY KEY AUTOINCREMENT)
		String createTableSql = String.format("CREATE TABLE IF NOT EXISTS %s (_id INTEGER PRIMARY KEY AUTOINCREMENT," +
				" address VARCHAR(255), %s)", tableName, columnsDefinition);
		jdbcTemplate.execute(createTableSql);

		LOGGER.info("Ensured table {} exists.", tableName);
	}

	private String getHeader(Path filePath) throws IOException {
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
