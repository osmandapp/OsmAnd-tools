package net.osmand.server.api.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.DataService;
import net.osmand.server.api.searchtest.dto.RunStarter;
import net.osmand.server.api.searchtest.dto.Script;
import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.entity.TestCase;
import net.osmand.server.api.searchtest.repo.TestCaseRepository;
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
 import org.springframework.dao.EmptyResultDataAccessException;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Service
public class SearchTestService extends DataService {
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchTestService.class);
	private final SearchService searchService;
	private final TestCaseRepository testCaseRepo;

	@Autowired
	public SearchTestService(EntityManager em, DatasetRepository datasetRepository,
							 TestCaseRepository testCaseRepo,
							 @Qualifier("testJdbcTemplate") JdbcTemplate jdbcTemplate,
							 SearchService searchService, WebClient.Builder webClientBuilder,
							 ObjectMapper objectMapper) {
		super(em, datasetRepository, jdbcTemplate, webClientBuilder, objectMapper);
		this.testCaseRepo = testCaseRepo;
		this.searchService = searchService;
	}

	public Page<TestCase> getTestCases(Long datasetId, Pageable pageable) {
		return testCaseRepo.findByDatasetIdOrderByIdDesc(datasetId, pageable);
	}

	public TestCase startTestCase(Long caseId, RunStarter payload) {
		TestCase test = testCaseRepo.findById(caseId)
				.orElseThrow(() -> new RuntimeException("Test-case not found with id: " + caseId));

		if (test.getStatus() != Dataset.ConfigStatus.OK) {
			throw new RuntimeException(String.format("Dataset %s is not in OK state (%s)", datasetId,
					dataset.getSourceStatus()));
		}

		test.datasetId = datasetId;
		test.created = new java.sql.Timestamp(System.currentTimeMillis());
		// Optional job name provided by client
		test.name = payload.name();

		String locale = payload.locale();
		if (locale == null || locale.trim().isEmpty()) {
			locale = "en";
		}
		test.locale = locale;
		test.setNorthWest(payload.northWest());
		test.setSouthEast(payload.southEast());
		test.baseSearch = payload.baseSearch();
		// Persist optional lat/lon overrides if provided
		test.lat = payload.lat();
		test.lon = payload.lon();

		test.fileName = dataset.fileName;
		test.function = payload.functionName();
		try {
			test.selCols = objectMapper.writeValueAsString(payload.columns());
			test.params = objectMapper.writeValueAsString(payload.paramValues());
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}

		dataset.selCols = test.selCols;
		dataset.params = test.params;
		dataset.function = payload.functionName();
		dataset.locale = locale;
		dataset.setNorthWest(payload.northWest());
		dataset.setSouthEast(payload.southEast());
		dataset.baseSearch = payload.baseSearch();
		dataset.lat = payload.lat();
		dataset.lon = payload.lon();

		datasetRepository.save(dataset);
		test.status = TestCase.Status.RUNNING;
		return testCaseRepo.save(test);
	}

	public void generateTestCase(Long id) {
		Optional<TestCase> testOptional = testCaseRepo.findById(id);
		TestCase test = testOptional.orElseThrow(() -> new RuntimeException("Test-case not found with id: " + id));
		Dataset dataset = datasetRepository.findById(test.datasetId)
				.orElseThrow(() -> new RuntimeException("Dataset not found for test-case id: " + id));

		String tableName = "dataset_" + sanitize(dataset.name);
		try {
			String[] selCols = objectMapper.readValue(test.selCols, String[].class);
			List<String> columns =  new ArrayList<>(), delCols = new ArrayList<>();
			Collections.addAll(columns, selCols);
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
			List<RowAddress> examples = execute(script.code(), test.function, delCols, rows, test.selCols,
					objectMapper.readValue(test.params, String[].class));
			for (RowAddress example : examples) {
				saveCaseResults(test, example, 0, null);
			}
		} catch (Exception e) {
			LOGGER.error("Evaluation failed for job {} on dataset {}", test.id, dataset.id, e);
			test.setError(e.getMessage());
			test.status = TestCase.Status.INVALID;
		} finally {
			test.updated = new java.sql.Timestamp(System.currentTimeMillis());
			testCaseRepo.save(test);
		}
	}

	@Async
	public void runTestCase(Long id) {
		Optional<TestCase> testOptional = testCaseRepo.findById(id);
		TestCase test = testOptional.orElseThrow(() -> new RuntimeException("Test-case not found with id: " + id));
		if (test.status != TestCase.Status.RUNNING) {
			LOGGER.info("Test-case {} is not in RUNNING state ({}). Skipping processing request.", id, test.status);
			return; // do not process cancelled/completed jobs
		}

		try {
			String sql = String.format("SELECT lat, lon, output FROM case_result WHERE case_id = %d", id);
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
			for (Map<String, Object> row : rows) {
				String status;
				try {
					status = jdbcTemplate.queryForObject("SELECT status FROM test_case WHERE id = ?", String.class, id);
				} catch (EmptyResultDataAccessException ex) {
					test = null;
					LOGGER.info("Job {} was deleted. Stopping execution.", id);
					return;
				}
				if (!TestCase.Status.RUNNING.name().equals(status)) {
					test.status = TestCase.Status.valueOf(status);
					LOGGER.info("Job {} was cancelled. Stopping execution.", id);
					break; // Exit the loop if the job has been cancelled
				}

				long startTime = System.currentTimeMillis();
				String lat = (String) row.get("lat");
				String lon = (String) row.get("lon");
				LatLon expectedPoint = parseLatLon(lat, lon);
				if (expectedPoint == null) {
					LOGGER.warn("Invalid expected (lat, lon) in input format: {}, {}", lat, lon);
					continue;
				}

				LatLon point;
				if (test.lat == null || test.lon == null) {
					point = expectedPoint;
				} else {
					point = new LatLon(test.lat, test.lon);
				}

				String json = (String) row.get("output");
				for (String address : objectMapper.readValue(json, String[].class))
					try {
						List<Feature> searchResults = searchService.search(point.getLatitude(), point.getLongitude(),
								address, test.locale, test.baseSearch, test.getNorthWest(), test.getSouthEast());
						saveRunResults(test, address, row, searchResults, expectedPoint,
								System.currentTimeMillis() - startTime, null);
					} catch (Exception e) {
						LOGGER.warn("Failed to process row for test-case {}.", test.id, e);
						saveRunResults(test, address, row, Collections.emptyList(), expectedPoint,
								System.currentTimeMillis() - startTime, e.getMessage() == null ? e.toString() :
										e.getMessage());
					}
			}
			if (test.status != TestCase.Status.CANCELED && test.status != TestCase.Status.FAILED) {
				test.status = TestCase.Status.COMPLETED;
			}
		} catch (Exception e) {
			LOGGER.error("Evaluation failed for test-case {}", test.id, e);
			test.setError(e.getMessage());
		} finally {
			if (test != null) {
				test.updated = new java.sql.Timestamp(System.currentTimeMillis());
				testCaseRepo.save(test);
			}
		}
	}

	@Async
	public CompletableFuture<TestCase> cancelRun(Long id) {
		return CompletableFuture.supplyAsync(() -> {
			TestCase job = testCaseRepo.findById(id)
					.orElseThrow(() -> new RuntimeException("Test-case not found with id: " + id));

			if (job.status == TestCase.Status.RUNNING) {
				String sql = "UPDATE test_case SET status = ?, updated = ? WHERE id = ?";
				Timestamp updated = new Timestamp(System.currentTimeMillis());
				jdbcTemplate.update(sql, TestCase.Status.CANCELED, updated, id);

				job.status = TestCase.Status.CANCELED;
				job.updated = updated;
				return job;
			}
			return job;
		});
	}

	@Async
	public CompletableFuture<Void> deleteTestCase(Long id) {
		return CompletableFuture.runAsync(() -> {
			if (!testCaseRepo.existsById(id)) {
				throw new RuntimeException("Test-case not found with id: " + id);
			}

			String sql = "DELETE FROM run_result WHERE job_id = ?";
			jdbcTemplate.update(sql, id);

			testCaseRepo.deleteById(id);
			LOGGER.info("Deleted test-case with id: {}", id);
		});
	}

	public Optional<TestCase> getTestCase(Long id) {
		return testCaseRepo.findById(id);
	}
}
