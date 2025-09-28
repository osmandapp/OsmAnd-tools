package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import net.osmand.data.LatLon;
import net.osmand.search.core.SearchResult;
import net.osmand.server.api.searchtest.DataService;
import net.osmand.server.api.searchtest.PolyglotEngine;
import net.osmand.server.api.searchtest.ReportService;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.RunParam;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository.Dataset;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Service
public class SearchTestService implements ReportService, DataService {
	/**
	 * Lightweight DTO for listing test-cases with parent dataset name.
	 */
	public static class TestCaseItem {
		public TestCaseItem() {
		}
		public TestCaseItem(Long id, String name, String labels, Long datasetId, String datasetName,
		                    Long lastRunId, String status, LocalDateTime updated, String error,
		                    long total, long failed, long duration) {
			this.id = id;
			this.name = name;
			this.labels = labels;
			this.datasetId = datasetId;
			this.datasetName = datasetName;
			this.lastRunId = lastRunId;
			this.status = status;
			this.updated = updated;
			this.error = error;
			this.total = total;
			this.failed = failed;
			this.duration = duration;
		}
		public Long id;
		public String name;
		public String labels;
		public Long datasetId;
		public String datasetName;
		public Long lastRunId;
		public String status;
		public LocalDateTime updated;
		public String error;
		public long total;
		public long failed;
		public long duration;
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(SearchTestService.class);
	@Autowired
	private ObjectMapper objectMapper;
	@Autowired
	private WebClient.Builder webClientBuilder;
	private WebClient webClient;

	@Autowired
	private SearchTestDatasetRepository datasetRepo;
	@Autowired
	private SearchTestCaseRepository testCaseRepo;
	@Autowired
	private SearchTestRunRepository runRepo;
	@Autowired
	@Qualifier("searchTestJdbcTemplate")
	private JdbcTemplate jdbcTemplate;

	@Value("${searchtest.csv.dir}")
	private String csvDownloadingDir;
	@Value("${osmand.web.location}")
	private String webServerConfigDir;
	@Value("${overpass.url}")
	private String overpassApiUrl;
	@Autowired
	private PolyglotEngine engine;

	@PostConstruct
	protected void init() {
		this.webClient =
				webClientBuilder.baseUrl(overpassApiUrl + "api/interpreter").exchangeStrategies(ExchangeStrategies
						.builder().codecs(configurer
								-> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024)).build()).build();
		// Ensure DB integrity
		try {
			jdbcTemplate.execute("DELETE FROM test_case WHERE dataset_id NOT IN (SELECT id FROM dataset)");
			jdbcTemplate.execute("DELETE FROM gen_result WHERE case_id NOT IN (SELECT id FROM test_case)");
			jdbcTemplate.execute("DELETE FROM run WHERE case_id NOT IN (SELECT id FROM test_case) OR status = 'RUNNING'");
			jdbcTemplate.execute("UPDATE run SET status = 'FAILED' WHERE status = 'RUNNING'");
			jdbcTemplate.execute("DELETE FROM run_result WHERE run_id NOT IN (SELECT id FROM run)");
			// Remove duplicates (SQLite-compatible): keep the smallest id per (run_id, gen_id)
			jdbcTemplate.execute("DELETE FROM run_result WHERE gen_id IS NOT NULL AND id NOT IN " +
					"(SELECT MIN(id) FROM run_result WHERE gen_id IS NOT NULL GROUP BY run_id, gen_id)");
			jdbcTemplate.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_run_result_run_gen ON run_result(run_id, gen_id)");
		} catch (Exception e) {
			LOGGER.warn("Could not ensure DB integrity.", e);
		}
	}

	public SearchService getSearchService() {
		return searchService;
	}

	public String getWebServerConfigDir() {
		return webServerConfigDir;
	}

	public Logger getLogger() {
		return LOGGER;
	}

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public WebClient getWebClient() {
		return webClient;
	}

	public SearchTestDatasetRepository getDatasetRepo() {
		return datasetRepo;
	}

	public SearchTestCaseRepository getTestCaseRepo() {
		return testCaseRepo;
	}

	public PolyglotEngine getEngine() {
		return engine;
	}

	public String getCsvDownloadingDir() {
		return csvDownloadingDir;
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
			test.status = TestCase.Status.NEW;
			try {
				test.selCols = objectMapper.writeValueAsString(param.columns());
				test.progCfg = objectMapper.writeValueAsString(param.programConfig());
				test.nocodeCfg = objectMapper.writeValueAsString(param.nocodeConfig());
				test.testRow = objectMapper.writeValueAsString(param.testRow());

				dataset.selCols = test.selCols;
				dataset.testRow = test.testRow;
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

	public CompletableFuture<Run> rerun(Long runId) {
		Run run = runRepo.findById(runId)
				.orElseThrow(() -> new RuntimeException("Run not found with id: " + runId));

		RunParam param = new RunParam();
		param.name = getSystemBranch();
		param.average = run.average;
		param.locale = run.locale;
		param.setNorthWest(run.getNorthWest());
		param.setSouthEast(run.getSouthEast());
		param.lat = run.lat;
		param.lon = run.lon;

		return runTestCase(run.caseId, param);
	}

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
		run.name = payload.name;
		run.average = payload.average;
		test.average = payload.average;

		String locale = payload.locale;
		if (locale == null || locale.trim().isEmpty()) {
			locale = "en";
		}
		run.locale = locale;
		run.setNorthWest(payload.getNorthWest());
		run.setSouthEast(payload.getSouthEast());
		// Persist optional lat/lon overrides if provided
		run.lat = payload.lat;
		run.lon = payload.lon;
		run.created = LocalDateTime.now();
		run.updated = run.created;
		run = runRepo.save(run);

		test.locale = run.locale;
		test.setNorthWest(run.getNorthWest());
		test.setSouthEast(run.getSouthEast());
		test.lat = run.lat;
		test.lon = run.lon;
		testCaseRepo.save(test);

		Run finalRun = run;
		CompletableFuture.runAsync(() -> run(finalRun));
		return CompletableFuture.completedFuture(finalRun);
	}

	private void run(Run run) {
		String sql = String.format("SELECT id, lat, lon, row, query, gen_count FROM gen_result WHERE case_id = %d ORDER BY" +
				" id", run.caseId);
		try {
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
			if (rows.isEmpty())
				return;

			LatLon srcPoint = null;
			String[] srcBbox = null;
			if (run.lat != null && run.lon != null)
				srcPoint = new LatLon(run.lat, run.lon);
			else if (run.average != null && run.average)
				srcPoint = getAveragePoint(rows);
			if (srcPoint != null) {
				srcBbox = run.getNorthWest() != null && run.getSouthEast() != null ?
						new String[]{run.getNorthWest(), run.getSouthEast()} : new String[]{
						String.format(Locale.US, "%f, %f", srcPoint.getLatitude() + 1.5, srcPoint.getLongitude() - 1.5),
						String.format(Locale.US, "%f, %f", srcPoint.getLatitude() - 1.5, srcPoint.getLongitude() + 1.5)};
			}

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
				Integer gen_id = (Integer) row.get("id");
				String query = (String) row.get("query");
				int count = (Integer) row.get("gen_count");
				String rowJson = (String) row.get("row");
				Map<String, Object> genRow = getObjectMapper().readValue(rowJson, Map.class);

				LatLon targetPoint = new LatLon((Double) row.get("lat"), (Double) row.get("lon"));
				LatLon searchPoint = srcPoint != null ? srcPoint : targetPoint;
				String[] bbox = srcBbox != null ? srcBbox : new String[]{
						String.format(Locale.US, "%f, %f", searchPoint.getLatitude() + 1.5, searchPoint.getLongitude() - 1.5),
						String.format(Locale.US, "%f, %f", searchPoint.getLatitude() - 1.5, searchPoint.getLongitude() + 1.5)};
				try {
					List<SearchResult> searchResults = Collections.emptyList();
					if (query != null && !query.trim().isEmpty())
						searchResults = searchService.searchResults(searchPoint.getLatitude(), searchPoint.getLongitude(),
								query, run.locale, false, bbox[0], bbox[1]);
					saveRunResults(genRow, gen_id, count, run, query, searchResults, targetPoint, searchPoint,
							System.currentTimeMillis() - startTime, bbox[0] + "; " + bbox[1], null);
				} catch (Exception e) {
					LOGGER.warn("Failed to process row for run {}.", run.id, e);
					saveRunResults(genRow, gen_id, count, run, query, Collections.emptyList(), targetPoint, searchPoint,
							System.currentTimeMillis() - startTime, bbox[0] + "; " + bbox[1],
							e.getMessage() == null ? e.toString() : e.getMessage());
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
		Optional<TestCase> opt = testCaseRepo.findById(id);
		opt.ifPresent(tc -> tc.lastRunId = runRepo.findLastRunId(tc.id));
		return opt;
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
			Long lastRunId = runRepo.findLastRunId(tc.id);
			if (tcOpt.isEmpty())
				continue;

			TestCaseStatus tcStatus = tcOpt.get();
			items.add(new TestCaseItem(tc.id, tc.name, tc.labels, tc.datasetId, datasetName, lastRunId,
					tcStatus.status().name(), tc.updated, tc.getError(),
					tcStatus.processed(), tcStatus.failed(), tcStatus.duration()));
		}

		return new PageImpl<>(items, pageable, page.getTotalElements());
	}

	public Page<Run> getRuns(String name, String labels, Pageable pageable) {
		return runRepo.findFiltered(name, labels, pageable);
	}

	public Page<Run> getRuns(Long caseId, Pageable pageable) {
		return runRepo.findByCaseId(caseId, pageable);
	}

	@Async
	public CompletableFuture<Void> deleteRun(Long id) {
		String sql = "DELETE FROM run_result WHERE run_id = ?";
		jdbcTemplate.update(sql, id);

		return CompletableFuture.runAsync(() -> runRepo.deleteById(id));
	}

	public Optional<Run> getRun(Long id) {
		return runRepo.findById(id);
	}
	private final SearchService searchService;

	@Autowired
	public SearchTestService(SearchService searchService) {
		this.searchService = searchService;
	}

	private static LatLon getAveragePoint(List<Map<String, Object>> rows) {
		double sumLat = 0.0, sumLon = 0.0;
		for (Map<String, Object> r : rows) {
			Object latObj = r.get("lat");
			Object lonObj = r.get("lon");
			if (latObj instanceof Double lat && lonObj instanceof Double lon) {
				sumLat += lat;
				sumLon += lon;
			}
		}
		double roundedLat = BigDecimal.valueOf(sumLat /
				rows.size()).setScale(7, RoundingMode.HALF_UP).doubleValue();
		double roundedLon = BigDecimal.valueOf(sumLon /
				rows.size()).setScale(7, RoundingMode.HALF_UP).doubleValue();
		return new LatLon(roundedLat, roundedLon);
	}
}
