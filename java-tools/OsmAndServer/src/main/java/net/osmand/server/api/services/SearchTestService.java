package net.osmand.server.api.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.ReportService;
import net.osmand.server.api.searchtest.dto.GenParam;
import net.osmand.server.api.searchtest.dto.TestCaseItem;
import net.osmand.server.api.searchtest.dto.TestCaseStatus;
import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.entity.Run;
import net.osmand.server.api.searchtest.entity.RunParam;
import net.osmand.server.api.searchtest.entity.TestCase;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Service
public class SearchTestService extends ReportService {
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchTestService.class);
	private final SearchService searchService;

	@Autowired
	public SearchTestService(SearchService searchService) {
		this.searchService = searchService;
	}

	public Page<TestCase> getTestCases(Long datasetId, Pageable pageable) {
		return testCaseRepo.findByDatasetIdOrderByIdDesc(datasetId, pageable);
	}

	@Async
	public CompletableFuture<TestCase> createTestCase(Long datasetId, GenParam param) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset dataset = datasetRepo.findById(datasetId)
					.orElseThrow(() -> new RuntimeException("Dataset not found for test-case id: " + datasetId));
			TestCase test = new TestCase();
			test.datasetId = datasetId;
			test.name = param.name();
			test.labels = param.labels();
			test.selectFun = param.selectFun();
			test.whereFun = param.whereFun();
			test.status = TestCase.Status.NEW;
			try {
				test.selectParams = objectMapper.writeValueAsString(param.selectParamValues());
				test.whereParams = objectMapper.writeValueAsString(param.whereParamValues());
				test.selCols = objectMapper.writeValueAsString(param.columns());
				dataset.selCols = test.selCols;
				test.allCols = dataset.allCols;

				test.created = LocalDateTime.now();
				test.updated = test.created;
				test = testCaseRepo.save(test);
				datasetRepo.saveAndFlush(dataset);

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

			String sql = String.format("SELECT %s FROM %s", String.join(",", columns), tableName);
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
			List<GenRow> examples = execute(dataset.script, test, delCols, rows);
			for (GenRow example : examples) {
				saveCaseResults(test, example);
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
	public CompletableFuture<TestCase> updateTestCase(Long id, Map<String, String> updates, boolean regen) {
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
				Dataset dataset = datasetRepo.findById(test.datasetId)
						.orElseThrow(() -> new RuntimeException("Dataset not found for test-case id: " + test.datasetId));
				updated = generate(dataset, test);
			}
			return updated != null ? updated : testCaseRepo.save(test);
		});
	}

	@Async
	public CompletableFuture<Run> runTestCase(Long caseId, RunParam payload) {
		TestCase test = testCaseRepo.findById(caseId)
				.orElseThrow(() -> new RuntimeException("Test-case not found with id: " + caseId));

		final Long dsId = test.datasetId;
		Dataset ds = datasetRepo.findById(dsId)
				.orElseThrow(() -> new RuntimeException("Dataset not found for test-case id: " + dsId));
		if (ds.getSourceStatus() != Dataset.ConfigStatus.OK) {
			LOGGER.info("Dataset {} is not in OK state ({})", ds.id, ds.getSourceStatus());
			throw new RuntimeException(String.format("Dataset %s is not in OK state (%s)", ds.id,
					ds.getSourceStatus()));
		}
		if (test.status != TestCase.Status.GENERATED) {
			LOGGER.info("TestCase {} is not in GENERATED state ({})", caseId, test.status);
			throw new RuntimeException(String.format("TestCase %s is not in GENERATED state (%s)", caseId,
					test.status));
		}

		Run run = new Run();
		run.status = Run.Status.RUNNING;
		run.caseId = caseId;
		run.datasetId = test.datasetId;
		String locale = payload.locale;
		if (locale == null || locale.trim().isEmpty()) {
			locale = "en";
		}
		run.locale = locale;
		run.setNorthWest(payload.getNorthWest());
		run.setSouthEast(payload.getSouthEast());
		run.baseSearch = payload.baseSearch;
		// Persist optional lat/lon overrides if provided
		run.lat = payload.lat;
		run.lon = payload.lon;
		run.created = LocalDateTime.now();
		run.updated = run.created;
		run = runRepo.save(run);

		test.lastRunId = run.id;
		test.locale = run.locale;
		test.setNorthWest(run.getNorthWest());
		test.setSouthEast(run.getSouthEast());
		test.baseSearch = run.baseSearch;
		test.lat = run.lat;
		test.lon = run.lon;
		testCaseRepo.save(test);

		Run finalRun = run;
		CompletableFuture.runAsync(() -> run(finalRun));
		return CompletableFuture.completedFuture(finalRun);
	}

	private void run(Run run) {
		String sql = String.format("SELECT id, lat, lon, row, query, count FROM gen_result WHERE case_id = %d ORDER BY id",
				run.caseId);
		try {
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
			for (Map<String, Object> row : rows) {
				String status;
				try {
					status = jdbcTemplate.queryForObject("SELECT status FROM run WHERE id = ?", String.class, run.id);
				} catch (EmptyResultDataAccessException ex) {
					LOGGER.info("Run {} was deleted. Stopping execution.", run.id);
					return;
				}
				if (!Run.Status.RUNNING.name().equals(status)) {
					run.status = Run.Status.valueOf(status);
					LOGGER.info("Run {} was cancelled. Stopping execution.", run.id);
					break; // Exit the loop if the test-case has been cancelled
				}

				long startTime = System.currentTimeMillis();
				LatLon point = (run.lat == null || run.lon == null) ?
						new LatLon((Double) row.get("lat"), (Double) row.get("lon")) :
						new LatLon(run.lat, run.lon);

				Integer id = (Integer) row.get("id");
				String query = (String) row.get("query");
				Map<String, Object> mapRow = objectMapper.readValue((String) row.get("row"), new TypeReference<>() {
				});
				int count = (Integer) row.get("count");
				try {
					List<Feature> searchResults = Collections.emptyList();
					if (query != null && !query.trim().isEmpty())
						searchResults = searchService.search(point.getLatitude(), point.getLongitude(),
								query, run.locale, run.baseSearch, run.getNorthWest(), run.getSouthEast());
					saveRunResults(id, count, run, query, mapRow, searchResults, point,
							System.currentTimeMillis() - startTime, null);
				} catch (Exception e) {
					LOGGER.warn("Failed to process row for run {}.", run.id, e);
					saveRunResults(id, count, run, query, mapRow, Collections.emptyList(), point,
							System.currentTimeMillis() - startTime, e.getMessage() == null ? e.toString() :
									e.getMessage());
				}
			}
			if (run.status != Run.Status.CANCELED && run.status != Run.Status.FAILED) {
				run.status = Run.Status.COMPLETED;
			}
		} catch (Exception e) {
			LOGGER.error("Evaluation failed for test-case {}", run.id, e);
			run.setError(e.getMessage());
		} finally {
			run.timestamp = LocalDateTime.now();
			runRepo.save(run);
		}
	}

	@Async
	public CompletableFuture<Run> cancelRun(Long runId) {
		return CompletableFuture.supplyAsync(() -> {
			Run run = runRepo.findById(runId)
					.orElseThrow(() -> new RuntimeException("Run not found with id: " + runId));

			if (run.status == Run.Status.RUNNING) {
				String sql = "UPDATE run SET status = ?, updated = ? WHERE id = ?";
				Timestamp updated = new Timestamp(System.currentTimeMillis());
				jdbcTemplate.update(sql, Run.Status.CANCELED, updated, runId);

				run.status = Run.Status.CANCELED;
				run.updated = updated.toLocalDateTime();
				return run;
			}
			return run;
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

			sql = "DELETE FROM run WHERE case_id = ?";
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

	public Page<TestCaseItem> getAllTestCases(String name, String labels, Pageable pageable) {
		// Decide which repository query to use:
		// - TestCase status filter for NEW/GENERATED
		// - Latest Run status filter for RUNNING/COMPLETED/CANCELED/FAILED (and we prefer RUN domain for FAILED)
		Page<TestCase> page = testCaseRepo.findAllCasesFiltered(name, labels, pageable);
		List<TestCase> content = page.getContent();

		// Collect dataset IDs and fetch names in batch
		Set<Long> dsIds = new HashSet<>();
		for (TestCase tc : content) {
			if (tc.datasetId != null) dsIds.add(tc.datasetId);
		}

		Map<Long, String> dsNames = new HashMap<>();
		if (!dsIds.isEmpty()) {
			for (Dataset ds : datasetRepo.findAllById(dsIds)) {
				if (ds != null && ds.id != null) {
					dsNames.put(ds.id, ds.name);
				}
			}
		}

		List<TestCaseItem> items = new ArrayList<>(content.size());
		for (TestCase tc : content) {
			String datasetName = tc.datasetId == null ? null : dsNames.get(tc.datasetId);
			Optional<TestCaseStatus> tcOpt = getTestCaseStatus(tc.id);
			if (tcOpt.isEmpty())
				continue;

			TestCaseStatus tcStatus = tcOpt.get();
			items.add(new TestCaseItem(tc.id, tc.name, tc.labels, tc.datasetId, datasetName,
					tc.lastRunId, tcStatus.status().name(), tc.updated,
					tc.getError(), tcStatus.processed(), tcStatus.failed(), tcStatus.duration()));
		}

		return new PageImpl<>(items, pageable, page.getTotalElements());
	}

	public Page<Run> getRuns(Long caseId, Pageable pageable) {
		return runRepo.findByCaseId(caseId, pageable);
	}

	@Async
	public CompletableFuture<Void> deleteRun(Long id) {
		return CompletableFuture.runAsync(() -> runRepo.deleteById(id));
	}

	public Optional<Run> getRun(Long id) {
		return runRepo.findById(id);
	}
}
