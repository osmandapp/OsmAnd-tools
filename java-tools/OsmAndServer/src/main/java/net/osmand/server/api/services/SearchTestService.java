package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.DataService;
import net.osmand.server.api.searchtest.dto.EvalJobProgress;
import net.osmand.server.api.searchtest.dto.EvalJobReport;
import net.osmand.server.api.searchtest.dto.EvalStarter;
import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.entity.EvalJob;
import net.osmand.server.api.searchtest.repo.DatasetJobRepository;
import net.osmand.server.api.searchtest.repo.DatasetRepository;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Service
public class SearchTestService extends DataService {
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchTestService.class);

	private final SearchService searchService;
	private final DatasetJobRepository datasetJobRepository;

	@Autowired
	public SearchTestService(EntityManager em, DatasetRepository datasetRepository,
							 DatasetJobRepository datasetJobRepository,
							 @Qualifier("testJdbcTemplate") JdbcTemplate jdbcTemplate,
							 SearchService searchService, WebClient.Builder webClientBuilder,
							 ObjectMapper objectMapper) {
		super(em, datasetRepository, jdbcTemplate, webClientBuilder, objectMapper);
		this.datasetJobRepository = datasetJobRepository;
		this.searchService = searchService;
	}

	public Page<EvalJob> getDatasetJobs(Long datasetId, Pageable pageable) {
		return datasetJobRepository.findByDatasetIdOrderByIdDesc(datasetId, pageable);
	}

	public EvalJob startEvaluation(Long datasetId, EvalStarter payload) {
		Dataset dataset = datasetRepository.findById(datasetId)
				.orElseThrow(() -> new RuntimeException("Dataset not found with id: " + datasetId));

		if (dataset.getSourceStatus() != Dataset.ConfigStatus.OK) {
			throw new RuntimeException(String.format("Dataset %s is not in OK state (%s)", datasetId,
					dataset.getSourceStatus()));
		}
		EvalJob job = new EvalJob();
		job.datasetId = datasetId;
		job.created = new java.sql.Timestamp(System.currentTimeMillis());

		job.function = dataset.function;
		String locale = payload.locale();
		if (locale == null || locale.trim().isEmpty()) {
			locale = "en";
		}
		job.locale = locale;
		job.setNorthWest(payload.northWest());
		job.setSouthEast(payload.southEast());
		job.baseSearch = payload.baseSearch();

		job.status = EvalJob.Status.RUNNING;
		return datasetJobRepository.save(job);
	}

	@Async
	public void processEvaluation(Long jobId) {
		Optional<EvalJob> jobOptional = datasetJobRepository.findById(jobId);
		EvalJob job = jobOptional.orElseThrow(() -> new RuntimeException("Job not found with id: " + jobId));

		if (job.status != EvalJob.Status.RUNNING) {
			LOGGER.info("Job {} is not in RUNNING state ({}). Skipping processing request.", jobId, job.status);
			return; // do not process cancelled/completed jobs
		}

		EvalJob finalJob = job;
		Dataset dataset = datasetRepository.findById(job.datasetId)
				.orElseThrow(() -> new RuntimeException("Dataset not found with id: " + finalJob.datasetId));

		String tableName = "dataset_" + sanitize(dataset.name);
		String sql = String.format("SELECT * FROM %s", tableName);
		try {
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
			for (Map<String, Object> row : rows) {
				job = datasetJobRepository.findById(jobId)
						.map(j -> {
							em.detach(j);
							return j;
						}) // detach to avoid caching
						.orElse(null);
				if (job == null || job.status != EvalJob.Status.RUNNING) {
					LOGGER.info("Job {} was cancelled or deleted. Stopping execution.", jobId);
					break; // Exit the loop if the job has been cancelled
				}
				long startTime = System.currentTimeMillis();
				LatLon point = null;
				String originalJson = null, address = null;
				try {
					originalJson = objectMapper.writeValueAsString(row);
					address = (String) row.get("_address");
					String lat = (String) row.get("lat");
					String lon = (String) row.get("lon");
					point = parseLatLon(lat, lon);
					if (point == null) {
						throw new IllegalArgumentException("Invalid or missing (lat, lon) in WKT format: (" + lat + " "
								+ lon + ")");
					}

					List<Feature> searchResults = searchService.search(point.getLatitude(), point.getLongitude(),
							address, job.locale, job.baseSearch, job.getNorthWest(), job.getSouthEast());
					saveResults(job, dataset, address, originalJson, searchResults, point,
							System.currentTimeMillis() - startTime, null);
				} catch (Exception e) {
					LOGGER.warn("Failed to process row for job {}: {}", job.id, originalJson, e);
					saveResults(job, dataset, address, originalJson, Collections.emptyList(), point,
							System.currentTimeMillis() - startTime, e.getMessage() == null ? e.toString() :
									e.getMessage());
				}
			}

			if (job != null && job.status != EvalJob.Status.CANCELED) {
				job.status = EvalJob.Status.COMPLETED;
			}
		} catch (Exception e) {
			LOGGER.error("Evaluation failed for job {} on dataset {}", job.id, dataset.id, e);
			job.setError(e.getMessage());
		} finally {
			if (job != null) {
				job.updated = new java.sql.Timestamp(System.currentTimeMillis());
				datasetJobRepository.save(job);
			}
		}
	}

	@Async
	public CompletableFuture<EvalJob> cancelEvaluation(Long jobId) {
		return CompletableFuture.supplyAsync(() -> {
			EvalJob job = datasetJobRepository.findById(jobId)
					.orElseThrow(() -> new RuntimeException("Job not found with id: " + jobId));

			if (job.status == EvalJob.Status.RUNNING) {
				job.status = EvalJob.Status.CANCELED;
				job.updated = new java.sql.Timestamp(System.currentTimeMillis());
				return datasetJobRepository.save(job);
			} else {
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

	public Optional<EvalJobReport> getEvaluationReport(Long jobId) {
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
				    count(*) FILTER (WHERE address IS NULL or trim(address) = '') AS empty,
				    count(*) FILTER (WHERE min_distance IS NULL and address IS NOT NULL and trim(address) != '') AS not_found,
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
			return Optional.empty();
		}
		long failed = ((Number) result.get("failed")).longValue();
		long duration = result.get("duration") == null ? 0 : ((Number) result.get("duration")).longValue();
		double averagePlace = result.get("average_place") == null ? 0 :
				((Number) result.get("average_place")).doubleValue();
		long notFound = ((Number) result.get("not_found")).longValue();
		Number empty = (Number) result.get("empty");

		Map<String, Number> distanceHistogram = new LinkedHashMap<>();
		distanceHistogram.put("Empty", empty);
		distanceHistogram.put("Not found", notFound);
		distanceHistogram.put("0-50m", ((Number) result.getOrDefault("0-50m", 0)).longValue());
		distanceHistogram.put("50-500m", ((Number) result.getOrDefault("50-500m", 0)).longValue());
		distanceHistogram.put("500-1000m", ((Number) result.getOrDefault("500-1000m", 0)).longValue());
		distanceHistogram.put("1000m+", ((Number) result.getOrDefault("1000m+", 0)).longValue());

		EvalJobReport report = new EvalJobReport(jobId, notFound, processed, failed, duration, averagePlace,
				distanceHistogram);
		return Optional.of(report);
	}

	public Optional<EvalJob> getJob(Long jobId) {
		return datasetJobRepository.findById(jobId);
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

	public Optional<EvalJobProgress> getEvaluationProgress(Long jobId) {
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
				    count(*) FILTER (WHERE address IS NULL or trim(address) = '') AS empty,
				    count(*) FILTER (WHERE min_distance IS NULL and address IS NOT NULL and trim(address) != '') AS not_found
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
		long duration = result.get("duration") == null ? 0 : ((Number) result.get("duration")).longValue();
		double averagePlace = result.get("average_place") == null ? 0 :
				((Number) result.get("average_place")).doubleValue();
		long notFound = ((Number) result.get("not_found")).longValue();

		EvalJobProgress report = new EvalJobProgress(jobId, notFound, processed, failed, duration, averagePlace);
		return Optional.of(report);
	}
}
