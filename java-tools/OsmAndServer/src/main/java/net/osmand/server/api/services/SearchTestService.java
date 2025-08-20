package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.DataService;
import net.osmand.server.api.searchtest.dto.GenParam;
import net.osmand.server.api.searchtest.dto.RunParam;
import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.entity.TestCase;
import net.osmand.server.api.searchtest.repo.DatasetRepository;
import net.osmand.server.api.searchtest.repo.TestCaseRepository;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Service
public class SearchTestService extends DataService {
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchTestService.class);
	private final SearchService searchService;

	@Autowired
	public SearchTestService(EntityManager em, DatasetRepository datasetRepository,
							 TestCaseRepository testCaseRepo,
							 @Qualifier("testJdbcTemplate") JdbcTemplate jdbcTemplate,
							 SearchService searchService, WebClient.Builder webClientBuilder,
							 ObjectMapper objectMapper) {
		super(em, datasetRepository, testCaseRepo, jdbcTemplate, webClientBuilder, objectMapper);
		this.searchService = searchService;
	}

	public Page<TestCase> getTestCases(Long datasetId, Pageable pageable) {
		return testCaseRepo.findByDatasetIdOrderByIdDesc(datasetId, pageable);
	}

	public Page<TestCase> getTestCases(Long datasetId, String status, Pageable pageable) {
		if (status != null && !status.isBlank() && !"all".equalsIgnoreCase(status)) {
			try {
				TestCase.Status st = TestCase.Status.valueOf(status.toUpperCase());
				return testCaseRepo.findByDatasetIdAndStatusOrderByIdDesc(datasetId, st, pageable);
			} catch (IllegalArgumentException ex) {
				// Fallback to no-status filtering if invalid status is provided
				return testCaseRepo.findByDatasetIdOrderByIdDesc(datasetId, pageable);
			}
		}
		return testCaseRepo.findByDatasetIdOrderByIdDesc(datasetId, pageable);
	}

	@Async
	public CompletableFuture<TestCase> createTestCase(Long datasetId, GenParam param) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset dataset = datasetRepository.findById(datasetId)
					.orElseThrow(() -> new RuntimeException("Dataset not found for test-case id: " + datasetId));
			TestCase test = new TestCase();
			test.datasetId = datasetId;
			test.name = param.name();
			test.labels = param.labels();
			test.function = param.functionName();
			test.status = TestCase.Status.NEW;
			try {
				test.params = objectMapper.writeValueAsString(param.paramValues());
				test.selCols = objectMapper.writeValueAsString(param.columns());
				dataset.selCols = test.selCols;
				test.allCols = dataset.allCols;

				test.created = LocalDateTime.now();
				test.updated = test.created;
				test = testCaseRepo.save(test);
				datasetRepository.saveAndFlush(dataset);

				test = generate(dataset, test);
			} catch (Exception e) {
				LOGGER.error("Generation of test-case failed for on dataset {}", datasetId, e);
				test.setError(e.getMessage());
				test.status = TestCase.Status.FAILED;
			} finally {
				test.updated = LocalDateTime.now();
			}
			return testCaseRepo.save(test);
		});
	}

	private TestCase generate(Dataset dataset, TestCase test) {
		if (dataset.getSourceStatus() != Dataset.ConfigStatus.OK) {
			test.status = TestCase.Status.FAILED;
			LOGGER.info("Dataset {} is not in OK state ({}).", dataset.id, dataset.getSourceStatus());
			return test;
		}

		try {
			String tableName = "dataset_" + sanitize(dataset.name);

			String[] selCols = objectMapper.readValue(test.selCols, String[].class);
			List<String> columns = new ArrayList<>(), delCols = new ArrayList<>();
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

			String sql = "DELETE FROM gen_result WHERE case_id = ?";
			jdbcTemplate.update(sql, test.id);

			sql = "DELETE FROM run_result WHERE case_id = ?";
			jdbcTemplate.update(sql, test.id);

			sql = String.format("SELECT %s FROM %s", String.join(",", columns), tableName);
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
			List<RowAddress> examples = execute(dataset.script, test.function, delCols, rows, test.selCols,
					objectMapper.readValue(test.params, String[].class));
			for (RowAddress example : examples) {
				saveCaseResults(test, example, 0, null);
			}
			test.status = TestCase.Status.GENERATED;
		} catch (Exception e) {
			LOGGER.error("Generation of test-case failed for on dataset {}", dataset.id, e);
			test.setError(e.getMessage());
			test.status = TestCase.Status.FAILED;
		} finally {
			test.updated = LocalDateTime.now();
		}
		return testCaseRepo.save(test);
	}

	@Async
	public CompletableFuture<TestCase> updateTestCase(Long id, Map<String, String> updates, boolean regen, boolean rerun) {
		return CompletableFuture.supplyAsync(() -> {
			TestCase test = testCaseRepo.findById(id).orElseThrow(() ->
					new RuntimeException("Test case not found with id: " + id));

			updates.forEach((key, value) -> {
				switch (key) {
					case "name" -> test.name = value;
					case "labels" -> test.labels = value;
				}
			});
			TestCase updated = null;
			if (regen) {
				Dataset dataset = datasetRepository.findById(test.datasetId)
						.orElseThrow(() -> new RuntimeException("Dataset not found for test-case id: " + test.datasetId));
				updated = generate(dataset, test);
			}
			if (rerun) {
				updated = run(test);
			}
			return updated != null ? updated : testCaseRepo.save(test);
		});
	}

	@Async
	public void runTestCase(Long caseId, RunParam payload) {
		CompletableFuture.runAsync(() -> {
			TestCase test = testCaseRepo.findById(caseId)
					.orElseThrow(() -> new RuntimeException("Test-case not found with id: " + caseId));

			final Long dsId = test.datasetId;
			Dataset ds = datasetRepository.findById(dsId)
					.orElseThrow(() -> new RuntimeException("Dataset not found for test-case id: " + dsId));
			if (ds.getSourceStatus() != Dataset.ConfigStatus.OK) {
				LOGGER.info("Dataset {} is not in OK state ({})", ds.id, ds.getSourceStatus());
				throw new RuntimeException(String.format("Dataset %s is not in OK state (%s)", ds.id,
						ds.getSourceStatus()));
			}

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

			test.status = TestCase.Status.RUNNING;
			test = testCaseRepo.save(test);

			run(test);
		});
	}

	private TestCase run(TestCase test) {
		try {
			String sql = "DELETE FROM run_result WHERE case_id = ?";
			jdbcTemplate.update(sql, test.id);

			sql = String.format("SELECT lat, lon, output FROM gen_result WHERE case_id = %d", test.id);
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
			for (Map<String, Object> row : rows) {
				String status;
				try {
					status = jdbcTemplate.queryForObject("SELECT status FROM test_case WHERE id = ?", String.class,	test.id);
				} catch (EmptyResultDataAccessException ex) {
					LOGGER.info("Job {} was deleted. Stopping execution.", test.id);
					test = null;
					break;
				}
				if (!TestCase.Status.RUNNING.name().equals(status)) {
					test.status = TestCase.Status.valueOf(status);
					LOGGER.info("Job {} was cancelled. Stopping execution.", test.id);
					break; // Exit the loop if the test-case has been cancelled
				}

				long startTime = System.currentTimeMillis();
				LatLon expectedPoint = new LatLon((Double) row.get("lat"), (Double) row.get("lon"));
				LatLon point = (test.lat == null || test.lon == null) ? expectedPoint : new LatLon(test.lat, test.lon);

				String json = (String) row.get("output");
				int sequence = 0;
				for (String address : objectMapper.readValue(json, String[].class))
					try {
						List<Feature> searchResults = searchService.search(point.getLatitude(), point.getLongitude(),
								address, test.locale, test.baseSearch, test.getNorthWest(), test.getSouthEast());
						saveRunResults(test, sequence++, address, row, searchResults, expectedPoint,
								System.currentTimeMillis() - startTime, null);
					} catch (Exception e) {
						LOGGER.warn("Failed to process row for test-case {}.", test.id, e);
						saveRunResults(test, sequence, address, row, Collections.emptyList(), expectedPoint,
								System.currentTimeMillis() - startTime, e.getMessage() == null ? e.toString() :
										e.getMessage());
					}
			}
			if (test != null && test.status != TestCase.Status.CANCELED && test.status != TestCase.Status.FAILED) {
				test.status = TestCase.Status.COMPLETED;
			}
		} catch (Exception e) {
			LOGGER.error("Evaluation failed for test-case {}", test.id, e);
			test.setError(e.getMessage());
		} finally {
			if (test != null) {
				test.updated = LocalDateTime.now();
				test = testCaseRepo.save(test);
			}
		}
		return test;
	}

	@Async
	public CompletableFuture<TestCase> cancelRun(Long caseId) {
		return CompletableFuture.supplyAsync(() -> {
			TestCase test = testCaseRepo.findById(caseId)
					.orElseThrow(() -> new RuntimeException("Test-case not found with id: " + caseId));

			if (test.status == TestCase.Status.RUNNING) {
				String sql = "UPDATE test_case SET status = ?, updated = ? WHERE id = ?";
				Timestamp updated = new Timestamp(System.currentTimeMillis());
				jdbcTemplate.update(sql, TestCase.Status.CANCELED, updated, caseId);

				test.status = TestCase.Status.CANCELED;
				test.updated = updated.toLocalDateTime();
				return test;
			}
			return test;
		});
	}

	@Async
	public CompletableFuture<Void> deleteTestCase(Long id) {
		return CompletableFuture.runAsync(() -> {
			if (!testCaseRepo.existsById(id)) {
				throw new RuntimeException("Test-case not found with id: " + id);
			}

			String sql = "DELETE FROM run_result WHERE case_id = ?";
			jdbcTemplate.update(sql, id);

			sql = "DELETE FROM gen_result WHERE case_id = ?";
			jdbcTemplate.update(sql, id);

			testCaseRepo.deleteById(id);
			LOGGER.info("Deleted test-case with id: {}", id);
		});
	}

	public Optional<TestCase> getTestCase(Long id) {
		return testCaseRepo.findById(id);
	}
}
