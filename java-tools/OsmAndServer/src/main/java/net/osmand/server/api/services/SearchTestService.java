package net.osmand.server.api.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.*;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.RunParam;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository.Dataset;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class SearchTestService implements ReportService, DataService, OBFService {
    /**
     * Lightweight DTO for listing test-cases with parent dataset name.
     */
    public record TestCaseItem(Long id, String name, String labels, Long datasetId, String datasetName,
                                Long lastRunId, String status, LocalDateTime updated, String error,
                                long total, long failed, long duration) {}

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchTestService.class);
    private static volatile ExecutorService EXECUTOR, SAVE_EXECUTOR;
    private final ConcurrentHashMap<Long, AtomicReference<Run.Status>> runStatusFlags = new ConcurrentHashMap<>();

    // Batch insert support for run_result
    private static final int RUN_RESULT_BATCH_SIZE = 10;

    private final ConcurrentHashMap<Long, List<Object[]>> runResultBatches = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, List<CompletableFuture<Void>>> runResultBatchTasks = new ConcurrentHashMap<>();
    private final Set<Long> loggedStoppedRuns = ConcurrentHashMap.newKeySet();

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
	@Autowired
	private OsmAndMapsService mapsService;

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
			jdbcTemplate.execute("UPDATE run SET status = 'FAILED' WHERE status = 'RUNNING'");
			jdbcTemplate.execute("DELETE FROM run WHERE case_id NOT IN (SELECT id FROM test_case) OR status = 'RUNNING'");
			jdbcTemplate.execute("DELETE FROM run_result WHERE run_id NOT IN (SELECT id FROM run)");
			// Remove duplicates (SQLite-compatible): keep the smallest id per (run_id, gen_id)
			jdbcTemplate.execute("DELETE FROM run_result WHERE gen_id IS NOT NULL AND id NOT IN " +
					"(SELECT MIN(id) FROM run_result WHERE gen_id IS NOT NULL GROUP BY run_id, gen_id)");
			jdbcTemplate.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_run_result_run_gen ON run_result(run_id, gen_id)");
			jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS gen_result_index on gen_result(case_id, ds_result_id, id)");
		} catch (Exception e) {
			LOGGER.warn("Could not ensure DB integrity.", e);
		}
		// Cleanup in-memory status flag
		runStatusFlags.clear();

		EXECUTOR = createExecutor();
        SAVE_EXECUTOR = createBatchSaveExecutor();
	}

	private ExecutorService createExecutor() {
		int maxCount = Algorithms.parseIntSilently(System.getenv("MAX_THREAD_NUMBER"),
				Math.max(1, Runtime.getRuntime().availableProcessors()));
		ThreadFactory tf = r -> {
			Thread t = new Thread(r);
			t.setName("search-test-exec-" + t.getId());
			t.setDaemon(true);
			return t;
		};
		LOGGER.info("Global search-test executor created with pool size = {}", maxCount);
		return new ThreadPoolExecutor(maxCount, maxCount, 60L, TimeUnit.SECONDS,
					new LinkedBlockingQueue<>(), tf);
	}

    private ExecutorService createBatchSaveExecutor() {
        int maxCount = Algorithms.parseIntSilently(System.getenv("MAX_BATCH_SAVE_THREAD_NUMBER"), 1);
        maxCount = Math.max(1, maxCount);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r);
            t.setName("search-test-batch-save-" + t.getId());
            t.setDaemon(true);
            return t;
        };
        LOGGER.info("Search-test batch-save executor created with pool size = {}", maxCount);
        return new ThreadPoolExecutor(maxCount, maxCount, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), tf);
    }

	@Override
	public OsmAndMapsService getMapsService() {
		return mapsService;
	}

	@Override
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

	public SearchTestRunRepository getTestRunRepo() {
		return runRepo;
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

				test.updated = LocalDateTime.now();
				test = testCaseRepo.save(test);
				datasetRepo.saveAndFlush(dataset);
				startGeneration(dataset, test);
				return test;
			} catch (Exception e) {
				LOGGER.error("Generation of test-case failed for on dataset {}", datasetId, e);
				test.setError(e.getMessage());
				test.status = TestCase.Status.FAILED;
				test.updated = LocalDateTime.now();
				return testCaseRepo.save(test);
			}
		}, EXECUTOR);
	}

	private void startGeneration(Dataset dataset, TestCase test) {
		CompletableFuture.runAsync(() -> {
			try {
				generate(dataset, test);
			} catch (Exception e) {
				LOGGER.error("Generation of test-case failed for on dataset {}", dataset.id, e);
				test.setError(e.getMessage());
				test.status = TestCase.Status.FAILED;
				test.updated = LocalDateTime.now();
				testCaseRepo.save(test);
			}
		}, EXECUTOR);
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
		// Rerun support: persist reference run id if provided
		run.rerunId = payload.rerunId;

		String locale = payload.locale;
		if (locale == null || locale.trim().isEmpty()) {
			locale = "en";
		}
		run.locale = locale;
		run.average = payload.average;
		test.average = payload.average;
		run.skipFound = payload.skipFound;
		test.skipFound = payload.skipFound;
		run.shift = payload.shift;
		test.shift = payload.shift;
		run.setNorthWest(payload.getNorthWest());
		run.setSouthEast(payload.getSouthEast());
		// Persist optional lat/lon overrides if provided
		run.lat = payload.lat;
		run.lon = payload.lon;
		run.start = LocalDateTime.now();
		run = runRepo.save(run);

		test.locale = run.locale;
		test.setNorthWest(run.getNorthWest());
		test.setSouthEast(run.getSouthEast());
		test.lat = run.lat;
		test.lon = run.lon;
		test.threadsCount = payload.threadsCount;
		run.threadsCount = payload.threadsCount;
		testCaseRepo.save(test);

		Run finalRun = run;
		CompletableFuture.runAsync(() -> doMainRun(finalRun, payload.threadsCount == null ? 1 : payload.threadsCount));
		return CompletableFuture.completedFuture(finalRun);
	}

	private static final int CHUNK_SIZE = 100;

	private void doMainRun(Run run, int threadsCount) {
		List<CompletableFuture<Void>> runTasks = new ArrayList<>();
		AtomicReference<Run.Status> statusRef = runStatusFlags.computeIfAbsent(run.id, id ->
				new AtomicReference<>(Run.Status.RUNNING));
		final int maxParallel = threadsCount > 0 ? threadsCount : 1;

		try {
			if (threadsCount > 1) {
				String sql = "SELECT count(*) FROM gen_result WHERE case_id = ? ORDER BY id";
				final long count;
				if (run.rerunId != null) {
					if (run.skipFound == null || !run.skipFound) {
						sql = "SELECT count(*) FROM gen_result g " +
								"JOIN run_result r ON g.id = r.gen_id WHERE r.run_id = ? ORDER BY g.id";
					} else {
						sql = "SELECT count(*) FROM gen_result g " +
								"JOIN run_result r ON g.id = r.gen_id WHERE r.run_id = ? AND NOT COALESCE(r.found, r.res_distance <= 50) ORDER BY g.id";
					}
					count = jdbcTemplate.queryForObject(sql, Long.class, run.rerunId);
				} else {
					count = jdbcTemplate.queryForObject(sql, Long.class, run.caseId);
				}

				final int chunkSize = Math.min((int) (count / maxParallel) + 1, CHUNK_SIZE);
				final AtomicInteger nextOffset = new AtomicInteger(0);
				for (int workerIndex = 0; workerIndex < maxParallel; workerIndex++) {
					runTasks.add(CompletableFuture.runAsync(() -> {
						while (statusRef.get() == Run.Status.RUNNING) {
							int currentOffset = nextOffset.getAndAdd(chunkSize);
							if (currentOffset >= count) {
								break;
							}
							runChunk(run, chunkSize, currentOffset, statusRef);
						}
					}, EXECUTOR));
				}
			} else {
				runTasks.add(CompletableFuture.runAsync(() ->
						runChunk(run, -1, 0, statusRef), EXECUTOR));
			}

			CompletableFuture.allOf(runTasks.toArray(new CompletableFuture[0])).join();
			List<Object[]> remainingBatches = runResultBatches.remove(run.id);
			if (remainingBatches != null && !remainingBatches.isEmpty()) {
				submitBatchSave(run, remainingBatches);
			}
			List<CompletableFuture<Void>> saveTasks = runResultBatchTasks.remove(run.id);
			if (saveTasks != null && !saveTasks.isEmpty()) {
				CompletableFuture.allOf(saveTasks.toArray(new CompletableFuture[0])).join();
			}
			if (run.status != Run.Status.CANCELED && run.status != Run.Status.FAILED) {
				run.status = Run.Status.COMPLETED;
			}
		} catch (Exception ex) {
			LOGGER.error("Evaluation failed for test-case {}", run.id, ex);
			run.setError(ex.getMessage());
			run.status = Run.Status.FAILED;
		} finally {
			run.finish = LocalDateTime.now();

			runRepo.save(run);
 			// Cleanup in-memory status flag
 			runStatusFlags.remove(run.id);
			loggedStoppedRuns.remove(run.id);
			runResultBatches.remove(run.id);
			runResultBatchTasks.remove(run.id);
		}
	}

	private void runChunk(Run run, int limit, int offset, AtomicReference<Run.Status> statusRef) {
		String sql = "SELECT id, lat, lon, row, query, gen_count FROM gen_result WHERE case_id = ? ORDER BY id";
		if (run.rerunId != null) {
			// Re-run uses items from a previous run's results by joining gen_result with run_result
			if (run.skipFound == null || !run.skipFound) {
				sql = "SELECT g.id, g.lat, g.lon, g.row, g.query, g.gen_count FROM gen_result g " +
					"JOIN run_result r ON g.id = r.gen_id WHERE r.run_id = ? ORDER BY g.id";
			} else {
				sql = "SELECT g.id, g.lat, g.lon, g.row, g.query, g.gen_count FROM gen_result g " +
					"JOIN run_result r ON g.id = r.gen_id WHERE r.run_id = ? AND NOT COALESCE(r.found, r.res_distance <= 50) ORDER BY g.id";
			}
		}

		try {
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(
					sql + (limit > 0 ? " LIMIT " + limit + " OFFSET " + offset : ""),
					run.rerunId != null ? run.rerunId : run.caseId);
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
						String.format(Locale.US, "%.5f, %.5f", srcPoint.getLatitude() + SearchService.SEARCH_RADIUS_DEGREE,
								srcPoint.getLongitude() - SearchService.SEARCH_RADIUS_DEGREE),
						String.format(Locale.US, "%.5f, %.5f", srcPoint.getLatitude() - SearchService.SEARCH_RADIUS_DEGREE,
								srcPoint.getLongitude() + SearchService.SEARCH_RADIUS_DEGREE)};
			}

			for (Map<String, Object> row : rows) {
				Run.Status current = statusRef.get();
				if (current != Run.Status.RUNNING) {
					run.status = current;
					if (loggedStoppedRuns.add(run.id)) {
						LOGGER.info("Run {} was stopped with status {}. Stopping execution.", run.id, current);
					}
					break;
				}

				long startTime = System.currentTimeMillis();
				Integer genId = (Integer) row.get("id");
				String query = (String) row.get("query");
				int count = (Integer) row.get("gen_count");
				String rowJson = (String) row.get("row");
				Map<String, Object> genRow = getObjectMapper().readValue(rowJson, Map.class);

				LatLon targetPoint = new LatLon((Double) row.get("lat"), (Double) row.get("lon"));
				if (run.shift != null && run.shift >= 0.0) {
					double maxShiftMeters = run.shift * 1000.0;
					srcPoint = MapUtils.rhumbDestinationPoint(targetPoint,
							Math.sqrt(Math.random()) * maxShiftMeters, // [0, R] with uniform area
							Math.random() * 360.0); // [0, 360]
				}
				LatLon searchPoint = srcPoint != null ? srcPoint : targetPoint;
				String[] bbox = srcBbox != null ? srcBbox : new String[]{
						String.format(Locale.US, "%f, %f", searchPoint.getLatitude() + SearchService.SEARCH_RADIUS_DEGREE,
								searchPoint.getLongitude() - SearchService.SEARCH_RADIUS_DEGREE),
						String.format(Locale.US, "%f, %f", searchPoint.getLatitude() - SearchService.SEARCH_RADIUS_DEGREE,
								searchPoint.getLongitude() + SearchService.SEARCH_RADIUS_DEGREE)};

				Map<String, Object> newRow = new LinkedHashMap<>();
				long datasetId;
				try {
					datasetId = Long.parseLong((String) genRow.get("id"));
				} catch (NumberFormatException e) {
					datasetId = -1;
				}

				final MapDataObjectFinder finder = new MapDataObjectFinder(targetPoint, newRow, datasetId);
				Object[] args = null;
				try {
					SearchService.SearchResults searchResult = null;
					if (query != null && !query.trim().isEmpty()) {
						searchResult = searchService.getImmediateSearchResults(
								new SearchService.SearchContext(searchPoint.getLatitude(), searchPoint.getLongitude(),
								query, run.locale, false, SearchService.SEARCH_RADIUS_DEGREE, bbox[0], bbox[1]),
								new SearchService.SearchOption(true, null, true), finder);
					}

					args = collectRunResults(finder, genId, count, run, query, searchResult,
							targetPoint, searchPoint, System.currentTimeMillis() - startTime, bbox[0] + "; " + bbox[1], null);
				} catch (Exception e) {
					LOGGER.warn("Failed to process row for run {}.", run.id, e);
					args = collectRunResults(finder, genId, count, run, query, null,
							targetPoint, searchPoint, System.currentTimeMillis() - startTime, bbox[0] + "; " + bbox[1],
							e.getMessage() == null ? e.toString() : e.getMessage());
				} finally {
					if (args != null)
						enqueueRunResult(run, args);
				}
			}
		} catch (Exception e) {
			LOGGER.error("Evaluation batch failed for run {} at offset {}", run.id, offset, e);
			run.setError(e.getMessage());
			run.status = Run.Status.FAILED;
		}
	}

 	private void enqueueRunResult(Run run, Object[] args) {
 		List<Object[]> buffer = runResultBatches.computeIfAbsent(run.id, k -> Collections.synchronizedList(new ArrayList<>()));
 		buffer.add(args);
 		if (buffer.size() >= RUN_RESULT_BATCH_SIZE) {
 			List<Object[]> toSave;
 			synchronized (buffer) {
 				toSave = new ArrayList<>(buffer);
 				buffer.clear();
 			}
 			submitBatchSave(run, toSave);
 		}
 	}

	private void submitBatchSave(Run run, List<Object[]> batchArgs) {
		String sql = "INSERT OR IGNORE INTO run_result (gen_id, gen_count, dataset_id, run_id, case_id, query, row, error, " +
				"duration, res_count, res_distance, res_lat_lon, res_place, lat, lon, bbox, timestamp, found, stat_bytes, stat_time) " +
				"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		CompletableFuture<Void> f = CompletableFuture.runAsync(() -> {
			try {
				jdbcTemplate.batchUpdate(sql, batchArgs);
			} catch (Exception ex) {
				LOGGER.error("Failed batch insert for run {} ({} rows)", run.id, batchArgs.size(), ex);
			}
		}, SAVE_EXECUTOR);
		runResultBatchTasks.computeIfAbsent(run.id, k -> Collections.synchronizedList(new ArrayList<>())).add(f);
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
				AtomicReference<Run.Status> ref = runStatusFlags.get(runId);
				if (ref != null) {
					ref.set(Run.Status.CANCELED);
				}
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

	public static void main(String[] args) {
		String mapDir = System.getenv("MAP_DIR");
		if (mapDir == null || mapDir.trim().isEmpty()) {
			System.err.println("MAP_DIR env is required");
			return;
		}
		String fieldPathEnv = System.getenv("FIELD_PATH");
		String fieldName = System.getenv("FIELD_NAME");
		String filter = System.getenv("MAP_FILTER");
        if (filter != null)
            filter = filter.toLowerCase(Locale.ROOT);
		String fieldPath = fieldPathEnv == null || fieldPathEnv.trim().isEmpty() ? null : fieldPathEnv.trim();
		String statsFieldPath;
		if (fieldPath == null) {
			statsFieldPath = fieldName == null || fieldName.trim().isEmpty() ? null : fieldName.trim();
		} else if (fieldName == null || fieldName.trim().isEmpty()) {
			statsFieldPath = fieldPath;
		} else {
			statsFieldPath = fieldPath + "." + fieldName.trim();
		}
		String jsonFieldPath;
		if (fieldPath == null) {
			jsonFieldPath = (fieldName == null || fieldName.trim().isEmpty()) ? null : fieldName.trim();
		} else if (fieldName == null || fieldName.trim().isEmpty()) {
			jsonFieldPath = fieldPath;
		} else {
			jsonFieldPath = fieldPath + "." + fieldName.trim();
		}

		OBFService svc = new SearchTestService(null);

		Path root = Path.of(mapDir);
		if (!Files.exists(root)) {
			System.err.println("MAP_DIR doesn't exist: " + root);
			return;
		}

		List<Path> obfFiles = new ArrayList<>();
		try {
			try (java.util.stream.Stream<Path> s = Files.walk(root)) {
				s.filter(p -> Files.isRegularFile(p) && p.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".obf"))
						.forEach(obfFiles::add);
			}
		} catch (IOException e) {
			System.err.println("Failed to list OBF files in MAP_DIR: " + e.getMessage());
			return;
		}

		obfFiles.sort(Comparator.comparing(p -> p.toString().toLowerCase(Locale.ROOT)));
		System.out.println("MAP_DIR=" + root.toAbsolutePath());
		System.out.println("MAP_FILTER=" + (filter == null ? "" : filter));
		System.out.println("FIELD_PATH=" + (statsFieldPath == null ? "" : statsFieldPath));
        System.out.println("OBFs count:" + obfFiles.size());

		long totalSize = 0;
		for (Path p : obfFiles) {
			String obfName = p.getFileName().toString().toLowerCase(Locale.ROOT);
			if (filter != null && !obfName.startsWith(filter))
				continue;

			try {
				String obf = p.toAbsolutePath().toString();
				Map<String, long[]> m = svc.getSectionSizes(obf, statsFieldPath);
				long[] s = m == null || statsFieldPath == null ? null : m.get(statsFieldPath);
				long sum = s == null ? 0 : s[0];
				totalSize += sum;
				System.out.println(obf + ", " + sum);

				String json = svc.getSectionJson(obf, jsonFieldPath);
				String obfBaseName = obfName.endsWith(".obf")
						? obfName.substring(0, obfName.length() - 4)
						: obfName;

				Path jsonOutPath = root.resolve(obfBaseName + ".json");
				Files.writeString(jsonOutPath, json, StandardCharsets.UTF_8);
				System.out.println("JSON file: " + jsonOutPath);
			} catch (Exception e) {
				System.err.println(obfName + "\tERROR\t" + (e.getMessage() == null ? e.toString() : e.getMessage()));
			}
		}
		System.out.println("TOTAL: " + totalSize);
	}
}
