package net.osmand.server.api.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.DataService;
import net.osmand.server.api.searchtest.dto.EvalJobProgress;
import net.osmand.server.api.searchtest.dto.EvalJobReport;
import net.osmand.server.api.searchtest.dto.EvalStarter;
import net.osmand.server.api.searchtest.dto.Script;
import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.entity.EvalJob;
import net.osmand.server.api.searchtest.repo.DatasetJobRepository;
import net.osmand.server.api.searchtest.repo.DatasetRepository;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
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

		String locale = payload.locale();
		if (locale == null || locale.trim().isEmpty()) {
			locale = "en";
		}
		job.locale = locale;
		job.setNorthWest(payload.northWest());
		job.setSouthEast(payload.southEast());
		job.baseSearch = payload.baseSearch();
		// Persist optional lat/lon overrides if provided
		job.lat = payload.lat();
		job.lon = payload.lon();

		job.fileName = dataset.fileName;
		job.function = payload.functionName();
		try {
			job.selCols = objectMapper.writeValueAsString(payload.columns());
			job.params = objectMapper.writeValueAsString(payload.paramValues());
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}

		dataset.selCols = job.selCols;
		dataset.params = job.params;
		dataset.function = payload.functionName();
		dataset.locale = locale;
		dataset.setNorthWest(payload.northWest());
		dataset.setSouthEast(payload.southEast());
		dataset.baseSearch = payload.baseSearch();
		dataset.lat = payload.lat();
		dataset.lon = payload.lon();

		datasetRepository.save(dataset);
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

		Dataset dataset = datasetRepository.findById(job.datasetId)
				.orElseThrow(() -> new RuntimeException("Dataset not found with id: " + job.datasetId));

		String tableName = "dataset_" + sanitize(dataset.name);
		try {
			String[] selCols = objectMapper.readValue(job.selCols, String[].class);
			List<String> columns = Arrays.asList(selCols), delCols = new ArrayList<>();
			if (Arrays.stream(selCols).noneMatch("lon"::equals)) {
				columns.add("lon");
				delCols.add("lon");
			}
			if (Arrays.stream(selCols).noneMatch("lat"::equals)) {
				columns.add("lat");
				delCols.add("lat");
			}
			if (Arrays.stream(selCols).noneMatch("id"::equals)) {
				columns.add("id");
				delCols.add("id");
			}
			String sql = String.format("SELECT %s FROM %s", String.join(",", columns), tableName);
			Script script = objectMapper.readValue(dataset.script, Script.class);

			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
			List<RowAddress> examples = execute(script.code(), job.function, delCols, rows, job.selCols,
					objectMapper.readValue(job.params, String[].class));
			for (RowAddress example : examples) {
				String status = jdbcTemplate.queryForObject("SELECT status FROM eval_job WHERE id = ?", String.class,
						jobId);
				if (!EvalJob.Status.RUNNING.name().equals(status)) {
					LOGGER.info("Job {} was cancelled or deleted. Stopping execution.", jobId);
					break; // Exit the loop if the job has been cancelled
				}

				long startTime = System.currentTimeMillis();
				Map<String, Object> row = example.row();
				String lat = (String) row.get("lat");
				String lon = (String) row.get("lon");
				LatLon expectedPoint = parseLatLon(lat, lon);
				if (expectedPoint == null) {
					LOGGER.warn("Invalid expected (lat, lon) in input format: {}, {}", lat, lon);
					continue;
				}

				LatLon point;
				if (job.lat == null || job.lon == null) {
					point = expectedPoint;
				} else {
					point = new LatLon(job.lat, job.lon);
				}

				String address = example.address();
				try {
					List<Feature> searchResults = searchService.search(point.getLatitude(), point.getLongitude(),
							address, job.locale, job.baseSearch, job.getNorthWest(), job.getSouthEast());
					saveResults(job, address, row, searchResults, expectedPoint,
							System.currentTimeMillis() - startTime, null);
				} catch (Exception e) {
					LOGGER.warn("Failed to process row for job {}.", job.id, e);
					saveResults(job, address, row, Collections.emptyList(), expectedPoint,
							System.currentTimeMillis() - startTime, e.getMessage() == null ? e.toString() :
									e.getMessage());
				}
			}

			if (job.status != EvalJob.Status.CANCELED) {
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

	public Optional<EvalJob> getJob(Long jobId) {
		return datasetJobRepository.findById(jobId);
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
