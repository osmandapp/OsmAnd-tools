package net.osmand.server.controllers.pub;

import jakarta.servlet.http.HttpServletResponse;
import net.osmand.server.SearchTestRepositoryConfiguration;
import net.osmand.server.api.searchtest.BaseService.GenParam;
import net.osmand.server.api.searchtest.DetectorService;
import net.osmand.server.api.searchtest.OBFService;
import net.osmand.server.api.searchtest.ReportService.RunStatus;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository;
import net.osmand.server.api.services.SearchService;
import net.osmand.server.api.services.SearchTestService.TestCaseItem;
import net.osmand.server.api.searchtest.ReportService.TestCaseStatus;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository.Dataset;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.RunParam;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import net.osmand.server.api.services.SearchTestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Controller
@RequestMapping(path = "/admin/search-test")
public class SearchTestController {

	public record RunTestCaseRequest(RunParam payload, SearchService.SearchOption options) {}
	public record GenerateDbJobResponse(String jobId) {}
	public record CreateTagsDatasourceRequest(String name, Boolean overwrite, List<String> obfs) {}
	public record GenerateDbJobStatus(String jobId, String status, String obfName, int obfIndex, int totalObfs,
									  int processedTokens, int totalTokens, long elapsedMs, long estimatedMs,
									  boolean downloadReady, String error, List<OBFService.GenerateDbObfProgress> obfs) {}

	private static final ConcurrentHashMap<String, GenerateDbJob> GENERATE_DB_JOBS = new ConcurrentHashMap<>();

	private static class GenerateDbJob {
		volatile String status = "PENDING";
		volatile String obfName = "";
		volatile int obfIndex = 0;
		volatile int totalObfs = 0;
		volatile int processedTokens = 0;
		volatile int totalTokens = 0;
		volatile long elapsedMs = 0;
		volatile long estimatedMs = -1;
		volatile String error = null;
		volatile List<OBFService.GenerateDbObfProgress> obfs = Collections.emptyList();
		volatile Path zipFile = null;
		volatile String datasourceName = null;
		volatile boolean cancelRequested = false;
		volatile CompletableFuture<Void> future = null;
	}

	@Autowired
	private SearchTestRepositoryConfiguration dbCfg;
	@Autowired
	private SearchTestService testSearchService;

	@GetMapping
	public String index(Model model) throws IOException {
		return "admin/search-test";
	}

	@GetMapping("/tag_values_classification")
	public String tagValuesClassification(Model model) {
		return "admin/tag_values_classification";
	}
	
	@GetMapping(value = "/initialized", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Boolean> isInitialized() {
		return ResponseEntity.ok(dbCfg.isSearchTestDataSourceInitialized());
	}

	@GetMapping(value = "/datasets/{datasetId}/cases", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<TestCase>> getTestCases(@PathVariable Long datasetId,
													   Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getTestCases(datasetId, pageable));
	}

	@GetMapping(value = "/cases/{caseId}/runs", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<Run>> getRunsPerCase(@PathVariable Long caseId, Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getRuns(caseId, pageable));
	}

	@GetMapping(value = "/runs", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<Run>> getRuns(@RequestParam(required = false) String name,
	                                         @RequestParam(required = false) String labels, Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getRuns(name, labels, pageable));
	}

	@GetMapping(value = "/runs/{runId}/status", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<RunStatus> getRunStatus(@PathVariable Long runId) {
		return testSearchService.getRunStatus(runId, false).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	@GetMapping(value = "/cases/{caseId}/status", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<TestCaseStatus> getTestCaseStatus(@PathVariable Long caseId) {
		return testSearchService.getTestCaseStatus(caseId).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	@GetMapping(value = "/cases/{caseId:\\d+}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<TestCase> getTestCase(@PathVariable Long caseId) {
		return testSearchService.getTestCase(caseId).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	@GetMapping(value = "/cases", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<TestCaseItem>> getAllTestCases(@RequestParam(required = false) String name,
															  @RequestParam(required = false) String labels,
															  Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getAllTestCases(name, labels, pageable));
	}

	@PostMapping(value = "/datasets/{datasetId:\\d+}/gen", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<TestCase>> genTestCase(@PathVariable Long datasetId,
																   @RequestBody GenParam payload) {
		return testSearchService.createTestCase(datasetId, payload).thenApply(ResponseEntity::ok);
	}

	/**
	 * The call create Run entity and do other heavy work asynchronously.
	 */
	@PostMapping(value = "/cases/{caseId:\\d+}/run", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Run>> runTestCase(@PathVariable Long caseId,
															  @RequestBody RunTestCaseRequest request) {
		RunParam payload = request == null || request.payload == null ? new RunParam() : request.payload;
		SearchService.SearchOption options = request == null || request.options == null
				? new SearchService.SearchOption(false, null, null, false,(net.osmand.search.core.ObjectType[]) null)
				: request.options;
		return testSearchService.runTestCase(caseId, payload, options).thenApply(ResponseEntity::ok);
	}

	@PutMapping(value = "/cases/{caseId}", consumes = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<TestCase> updateTestCase(@PathVariable Long caseId,
	                                                                @RequestBody Map<String, String> updatesPayload) {
		return ResponseEntity.ok(testSearchService.updateTestCase(caseId, updatesPayload));
	}

	@PostMapping(value = "/runs/{runId:\\d+}/cancel", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Run>> cancelRun(@PathVariable Long runId) {
		return testSearchService.cancelRun(runId).thenApply(ResponseEntity::ok);
	}

	@DeleteMapping(value = "/cases/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Void>> deleteTestCase(@PathVariable Long id) {
		return testSearchService.deleteTestCase(id).thenApply(v -> ResponseEntity.noContent().build());
	}

	@DeleteMapping(value = "/runs/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Void>> deleteRun(@PathVariable Long id) {
		return testSearchService.deleteRun(id).thenApply(v -> ResponseEntity.noContent().build());
	}

	@PostMapping(value = "/csv/count", consumes = MediaType.APPLICATION_JSON_VALUE, produces =
			MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Map<String, Long>>> countCsvRows(@RequestBody String filePath) {
		return testSearchService.countCsvRows(filePath).thenApply(count -> ResponseEntity.ok(Map.of("count", count)));
	}

	@GetMapping("/browse")
	@ResponseBody
	public ResponseEntity<?> browseCsvFiles() {
		try {
			return ResponseEntity.ok(testSearchService.browseCsvFiles());
		} catch (IOException e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error browsing CSV files: " + e.getMessage());
		}
	}

	@GetMapping(value = "/datasets/{datasetId}/sample", produces = "text/csv")
	public void getDatasetSample(@PathVariable Long datasetId, HttpServletResponse response) throws IOException {
		try {
			String csvData = testSearchService.getDatasetSample(datasetId);
			response.setContentType("text/csv; charset=UTF-8");
			response.setCharacterEncoding(StandardCharsets.UTF_8.name());
			response.setHeader("Content-Disposition", "attachment; filename=\"sample.csv\"");
			try (OutputStreamWriter w = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8)) {
				w.write(csvData);
				w.flush();
			}
		} catch (RuntimeException e) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, e.getMessage());
		}
	}

	/**
	 * Returns a single dataset row by zero-based position for UI testing in the New Test Case form.
	 */
	@GetMapping(value = "/datasets/{datasetId}/sample/{position}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Map<String, Object>> getDatasetSampleRow(@PathVariable Long datasetId,
																		   @PathVariable int position) {
		return ResponseEntity.ok(testSearchService.getDatasetSample(datasetId, position));
	}

	/**
	 * List available name sets for the No-code Values field lookup.
	 * Returns an list of @Domain.
	 */
	@GetMapping(value = "/domains", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<SearchTestDatasetRepository.Domain>> getDomains(
			@RequestParam(name = "q", required = false) String q,
			@RequestParam(name = "limit", required = false, defaultValue = "20") Integer limit) {
		return ResponseEntity.ok(testSearchService.getDomains(q, limit == null ? 20 : limit));
	}

	@PostMapping(value = "/domains", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<SearchTestDatasetRepository.Domain> createDomain(@RequestBody Map<String, String> payload) {
		String name = payload.get("name");
		String data = payload.getOrDefault("data", "");
		SearchTestDatasetRepository.Domain created = testSearchService.createDomain(name, data);
		return ResponseEntity.status(HttpStatus.CREATED).body(created);
	}

	@PutMapping(value = "/domains/{id}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<SearchTestDatasetRepository.Domain> updateDomain(@PathVariable Long id,
                                                                           @RequestBody Map<String, String> updates) {
		return ResponseEntity.ok(testSearchService.updateDomain(id, updates));
	}

	@DeleteMapping(value = "/domains/{id}")
	@ResponseBody
	public ResponseEntity<Void> deleteDomain(@PathVariable Long id) {
		boolean deleted = testSearchService.deleteDomain(id);
		return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
	}

	@GetMapping(value = "/cases/{caseId}/report", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<RunStatus> getTestCaseReport(@PathVariable Long caseId) {
		TestCase tc = testSearchService.getTestCase(caseId).orElseThrow(() ->
				new RuntimeException("TestCase not found with id: " + caseId));
		return testSearchService.getRunStatus(caseId, tc.lastRunId)
				.map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	@GetMapping(value = "/runs/{runId}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Run> getRun(@PathVariable Long runId) {
		Run run = testSearchService.getRun(runId).orElseThrow(() ->
				new RuntimeException("Run not found with id: " + runId));
		return ResponseEntity.ok(run);
	}

	@GetMapping(value = "/runs/{runId}/report", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<RunStatus> getRunReport(@PathVariable Long runId) {
		Run run = testSearchService.getRun(runId).orElseThrow(() ->
				new RuntimeException("Run not found with id: " + runId));
		return testSearchService.getRunStatus(run.caseId, runId)
				.map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	@GetMapping(value = "/cases/{caseId}/results", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<Map<String, Object>>> getTestCaseResults(@PathVariable Long caseId) throws IOException {
		return ResponseEntity.ok(testSearchService.getTestCaseResults(caseId));
	}

	@GetMapping(value = "/runs/{runId}/results", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<Map<String, Object>>> getRunResults(@PathVariable Long runId,
	                                                               @RequestParam(defaultValue = "true") boolean isFull) throws IOException {
		return ResponseEntity.ok(testSearchService.getRunResults(runId, isFull));
	}

	@GetMapping(value = "/runs/{runId}/download")
	public void downloadRunReport(@PathVariable Long runId,
								  HttpServletResponse response) throws IOException {
		Run run = testSearchService.getRun(runId).orElseThrow(() ->
				new RuntimeException("Run not found with id: " + runId));
		response.setContentType("text/csv; charset=UTF-8");
		response.setCharacterEncoding(StandardCharsets.UTF_8.name());
		response.setHeader("Content-Disposition", "attachment; filename=report.csv");
		try (OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8)) {
			testSearchService.downloadCsvResults(writer, run.caseId, runId);
		}
	}

	@GetMapping(value = "/cases/{caseId}/download")
	public void downloadTestCaseReport(@PathVariable Long caseId,
									   HttpServletResponse response) throws IOException {
		TestCase tc = testSearchService.getTestCase(caseId).orElseThrow(() ->
				new RuntimeException("TestCase not found with id: " + caseId));
		response.setContentType("text/csv; charset=UTF-8");
		response.setCharacterEncoding(StandardCharsets.UTF_8.name());
		response.setHeader("Content-Disposition", "attachment; filename=report.csv");
		try (OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8)) {
			testSearchService.downloadCsvResults(writer, caseId, tc.lastRunId);
		}
	}

	@GetMapping(value = "/cases/{caseId}/compare", produces = MediaType.APPLICATION_JSON_VALUE)
	public void compareReport(@PathVariable Long caseId, @RequestParam Long[] runIds,
	                                                                 HttpServletResponse response) throws IOException {
		response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
		response.setHeader("Content-Disposition", "attachment; filename=\"report.xlsx\"");
		testSearchService.compareReport(response.getOutputStream(), caseId, runIds);
	}

	@GetMapping(value = "/branches", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<String>> getBranches() {
		return ResponseEntity.ok(Collections.singletonList(testSearchService.getSystemBranch()));
	}

	// --- Dataset management -------------------------------------------------
	@GetMapping(value = "/datasets", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<Dataset>> getDatasets(@RequestParam(required = false) String name,
													 @RequestParam(required = false) String labels,
													 Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getDatasets(name, labels, pageable));
	}

	@PutMapping(value = "/dataset/{datasetId}", consumes = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Dataset>> updateDataset(@PathVariable Long datasetId,
																	@RequestParam("reload") Boolean reload,
																	@RequestBody Map<String, String> updatesPayload) {
		return testSearchService.updateDataset(datasetId, reload, updatesPayload).thenApply(ResponseEntity::ok);
	}

	/**
	 * Create a new dataset.
	 */
	@PostMapping(value = "/datasets", consumes = MediaType.APPLICATION_JSON_VALUE, produces =
			MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Dataset> createDataset(@RequestBody Dataset payload) {
		return ResponseEntity.status(HttpStatus.CREATED).body(testSearchService.createDataset(payload).join());
	}

	@DeleteMapping(value = "/datasets/{datasetId:\\d+}")
	public ResponseEntity<Void> deleteDataset(@PathVariable Long datasetId) {
		boolean deleted = testSearchService.deleteDataset(datasetId);
		return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
	}

	@GetMapping(value = "/obfs", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<String>> getOBFs(
			@RequestParam(required = false) Double radius,
			@RequestParam(required = false) Double lat,
			@RequestParam(required = false) Double lon) throws IOException {
		return ResponseEntity.ok(testSearchService.getOBFs(radius, lat, lon));
	}

	@GetMapping(value = "/obf-tags", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<OBFService.ObfFileInfo>> getObfTags() throws IOException {
		return ResponseEntity.ok(testSearchService.getObfFileInfos());
	}

	@GetMapping(value = "/addresses", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<Record>> getAddresses(@RequestParam String obf,
	                 @RequestParam(required = false, defaultValue = "false") Boolean includesBoundaryAndPostcode,
	                 @RequestParam(required = false) String lang,
	                 @RequestParam(required = false) String cityRegExp,
	                 @RequestParam(required = false) String streetRegExp,
	                 @RequestParam(required = false) String houseRegExp,
	                 @RequestParam(required = false) String poiRegExp) {
		return ResponseEntity.ok(testSearchService.getAddresses(obf, lang == null ? "en" : lang,
				includesBoundaryAndPostcode != null && includesBoundaryAndPostcode,
				cityRegExp, streetRegExp, houseRegExp, poiRegExp));
	}

	@GetMapping(value = "/sections", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Map<String, long[]>> getSectionSizes(@RequestParam String obf,
	                                                        @RequestParam(required = false) String fieldPath) {
		return ResponseEntity.ok(testSearchService.getSectionSizes(obf, fieldPath));
	}

	@PostMapping(value = "/search", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<DetectorService.ResultsWithStats> getResults(
			@RequestParam String query,
			@RequestParam(required = false) String lang,
			@RequestParam() Double lat,
			@RequestParam() Double lon,
			@RequestBody SearchService.SearchOption options) throws IOException {
		if (query == null || lat == null || lon == null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameters 'query', 'lat' and 'lon' are required");
		}

        SearchService.SearchContext ctx = new SearchService.SearchContext(lat, lon, query, lang, false, null, null);
		return ResponseEntity.ok(testSearchService.getResults(ctx, options));
	}

	@PostMapping(value = "/unit-test", produces = "application/zip")
	@ResponseBody
	public void downloadUnitTest(
			@RequestParam String query,
			@RequestParam() Double lat,
			@RequestParam() Double lon,
			@RequestBody(required = false) DetectorService.UnitTestPayload unitTest,
			HttpServletResponse response) throws IOException, SQLException {
		if (unitTest == null || unitTest.name() == null || query == null || lat == null || lon == null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameters 'unit-test name', 'query', 'lat' and 'lon' are required");
		}
		response.setContentType("application/zip");
		response.setHeader("Content-Disposition", "attachment; filename=\"" + unitTest.name() + ".zip\"");
		testSearchService.createUnitTest(unitTest,
				new SearchService.SearchContext(lat, lon, query, null, false, null, null),
				response.getOutputStream());
	}

	@GetMapping(value = "/index", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<OBFService.IndexTokenPage> getIndex(@RequestParam String obf,
															  @RequestParam(required = false) String prefix,
															  @RequestParam(defaultValue = "0") int pageToShow,
															  @RequestParam(defaultValue = "100") int pageSizeLimit,
															  @RequestParam(required = false) String sortBy,
															  @RequestParam(required = false) String sortOrder) {
		return ResponseEntity.ok(testSearchService.getIndex(obf, prefix, pageToShow, pageSizeLimit, sortBy, sortOrder));
	}

	@PostMapping(value = "/objects", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<OBFService.ObjectAddressPage> getObjects(@RequestParam String obf,
																 @RequestParam(required = false) String lang,
																 @RequestParam(required = false) String regExp,
																 @RequestParam(defaultValue = "0") int pageToShow,
																 @RequestParam(defaultValue = "100") int pageSizeLimit,
																 @RequestParam(required = false) String sortBy,
																 @RequestParam(required = false) String sortOrder,
																 @RequestParam(defaultValue = "true") boolean isFiltered,
																 @RequestParam(defaultValue = "false") boolean invalidOnly,
																 @RequestParam(required = false) String objectType,
																 @RequestBody OBFService.IndexToken token) {
		OBFService.ObjectAddressPage objects = testSearchService.getObjects(obf, lang == null ? "en" : lang, token, regExp, pageToShow, pageSizeLimit, sortBy, sortOrder, isFiltered, invalidOnly, objectType);
		return ResponseEntity.ok(objects);
	}

	@PostMapping(value = "/generate", produces = "application/zip")
	@ResponseBody
	public void generateDb(
			@RequestBody(required = false) List<String> OBFs,
			HttpServletResponse response) throws IOException, SQLException {
		if (OBFs == null || OBFs.isEmpty()) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameters 'OBF file list' is required");
		}
		response.setContentType("application/zip");
		response.setHeader("Content-Disposition", "attachment; filename=\"db.zip\"");
		testSearchService.generateDb(OBFs, response.getOutputStream());
	}

	@PostMapping(value = "/generate/start", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<GenerateDbJobResponse> startGenerateDb(@RequestBody(required = false) List<String> OBFs) {
		if (OBFs == null || OBFs.isEmpty()) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameters 'OBF file list' is required");
		}
		String jobId = UUID.randomUUID().toString();
		GenerateDbJob job = new GenerateDbJob();
		job.totalObfs = OBFs.size();
		GENERATE_DB_JOBS.put(jobId, job);
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
			try {
				Path zipFile = Files.createTempFile("search-test-db-", ".zip");
				job.zipFile = zipFile;
				job.status = "RUNNING";
				try (java.io.OutputStream outputStream = Files.newOutputStream(zipFile)) {
					testSearchService.generateDb(OBFs, outputStream, progress -> {
						if (job.cancelRequested) {
							throw new CancellationException("Generate DB job was canceled");
						}
						job.status = progress.status();
						job.obfName = progress.obfName();
						job.obfIndex = progress.obfIndex();
						job.totalObfs = progress.totalObfs();
						job.processedTokens = progress.processedTokens();
						job.totalTokens = progress.totalTokens();
						job.elapsedMs = progress.elapsedMs();
						job.estimatedMs = progress.estimatedMs();
						job.error = progress.error();
						job.obfs = progress.obfs() == null ? Collections.emptyList() : progress.obfs();
					});
				}
				if (job.cancelRequested) {
					job.status = "CANCELED";
				} else {
					job.status = "DONE";
					job.estimatedMs = 0;
				}
			} catch (CancellationException e) {
				job.status = "CANCELED";
				job.error = null;
			} catch (Exception e) {
				if (job.cancelRequested) {
					job.status = "CANCELED";
					job.error = null;
				} else {
					job.status = "FAILED";
					job.error = e.getMessage();
				}
			} finally {
				if ("CANCELED".equals(job.status) && job.zipFile != null) {
					try {
						Files.deleteIfExists(job.zipFile);
						job.zipFile = null;
					} catch (IOException ignored) {
					}
				}
			}
		});
		job.future = future;
		return ResponseEntity.ok(new GenerateDbJobResponse(jobId));
	}

	@GetMapping(value = "/tags-datasources", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<OBFService.Datasource>> getTagsDatasources() throws IOException {
		return ResponseEntity.ok(testSearchService.getTagsDatasources());
	}

	@PostMapping(value = "/tags-datasources/start", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<GenerateDbJobResponse> startTagsDatasource(@RequestBody CreateTagsDatasourceRequest request) {
		if (request == null || request.obfs() == null || request.obfs().isEmpty()) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameters 'OBF file list' is required");
		}
		if (request.name() == null || request.name().trim().isEmpty()) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Datasource name is required");
		}
		String jobId = UUID.randomUUID().toString();
		GenerateDbJob job = new GenerateDbJob();
		job.totalObfs = request.obfs().size();
		job.datasourceName = request.name();
		GENERATE_DB_JOBS.put(jobId, job);
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
			try {
				job.status = "RUNNING";
				testSearchService.createTagsDatasource(request.name(), request.obfs(), Boolean.TRUE.equals(request.overwrite()), progress -> {
					if (job.cancelRequested) {
						throw new CancellationException("Generate DB job was canceled");
					}
					job.status = progress.status();
					job.obfName = progress.obfName();
					job.obfIndex = progress.obfIndex();
					job.totalObfs = progress.totalObfs();
					job.processedTokens = progress.processedTokens();
					job.totalTokens = progress.totalTokens();
					job.elapsedMs = progress.elapsedMs();
					job.estimatedMs = progress.estimatedMs();
					job.error = progress.error();
					job.obfs = progress.obfs() == null ? Collections.emptyList() : progress.obfs();
				});
				job.status = job.cancelRequested ? "CANCELED" : "DONE";
				job.estimatedMs = 0;
			} catch (CancellationException e) {
				job.status = "CANCELED";
				job.error = null;
			} catch (Exception e) {
				job.status = job.cancelRequested ? "CANCELED" : "FAILED";
				job.error = job.cancelRequested ? null : e.getMessage();
			}
		});
		job.future = future;
		return ResponseEntity.ok(new GenerateDbJobResponse(jobId));
	}

	@DeleteMapping(value = "/tags-datasources/{name}")
	@ResponseBody
	public ResponseEntity<Void> deleteTagsDatasource(@PathVariable String name) throws IOException {
		return testSearchService.deleteTagsDatasource(name) ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
	}

	@GetMapping(value = "/tags-datasources/{name}/download", produces = "application/zip")
	@ResponseBody
	public void downloadTagsDatasource(@PathVariable String name, HttpServletResponse response) throws IOException {
		response.setContentType("application/zip");
		response.setHeader("Content-Disposition", "attachment; filename=\"" + name + ".zip\"");
		testSearchService.downloadTagsDatasource(name, response.getOutputStream());
	}

	@GetMapping(value = "/tags-datasources/{name}/tokens", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<OBFService.DbTokenPage> getTagsDbTokens(@PathVariable String name,
	                                                              @RequestParam(required = false) String prefix,
	                                                              @RequestParam(defaultValue = "all") String objectType,
	                                                              @RequestParam(defaultValue = "false") boolean perObf,
	                                                              @RequestParam(required = false) String tag,
	                                                              @RequestParam(required = false) List<String> values,
	                                                              @RequestParam(defaultValue = "0") int pageToShow,
	                                                              @RequestParam(defaultValue = "100") int pageSizeLimit,
	                                                              @RequestParam(required = false) String sortBy,
	                                                              @RequestParam(required = false) String sortOrder) throws IOException, SQLException {
		try {
			return ResponseEntity.ok(testSearchService.getTagsDbTokens(name, prefix, objectType, perObf, tag, values, pageToShow, pageSizeLimit, sortBy, sortOrder));
		} catch (SQLException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
		}
	}

	@GetMapping(value = "/tags-datasources/{name}/tags", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<OBFService.DbTagName>> getTagsDbTagNames(@PathVariable String name) throws IOException, SQLException {
		try {
			return ResponseEntity.ok(testSearchService.getTagsDbTagNames(name));
		} catch (SQLException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
		}
	}

	@GetMapping(value = "/tags-datasources/{name}/tag-values", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<OBFService.DbTagValue>> getTagsDbTagValues(@PathVariable String name,
	                                                                      @RequestParam String tag) throws IOException, SQLException {
		try {
			return ResponseEntity.ok(testSearchService.getTagsDbTagValues(name, tag));
		} catch (SQLException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
		}
	}

	@GetMapping(value = "/tags-datasources/{name}/objects", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<OBFService.DbObjectPage> getTagsDbObjects(@PathVariable String name,
	                                                                @RequestParam long tokenId,
	                                                                @RequestParam(defaultValue = "all") String objectType,
	                                                                @RequestParam(defaultValue = "false") boolean perObf,
	                                                                @RequestParam(required = false) String regExp,
	                                                                @RequestParam(required = false) String tag,
	                                                                @RequestParam(required = false) List<String> values,
	                                                                @RequestParam(defaultValue = "0") int pageToShow,
	                                                                @RequestParam(defaultValue = "100") int pageSizeLimit,
	                                                                @RequestParam(required = false) String sortBy,
	                                                                @RequestParam(required = false) String sortOrder) throws IOException, SQLException {
		try {
			return ResponseEntity.ok(testSearchService.getTagsDbObjects(name, tokenId, objectType, perObf, regExp, tag, values, pageToShow, pageSizeLimit, sortBy, sortOrder));
		} catch (SQLException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
		}
	}

	@GetMapping(value = "/tags-datasources/{name}/address-poi-objects", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<OBFService.DbObjectPage> getTagsDbAddressPoiObjects(@PathVariable String name,
	                                                                          @RequestParam String objectType,
	                                                                          @RequestParam(required = false) String regExp,
	                                                                          @RequestParam(required = false) String tokenFind,
	                                                                          @RequestParam(required = false) String tag,
	                                                                          @RequestParam(required = false) List<String> values,
	                                                                          @RequestParam(defaultValue = "0") int pageToShow,
	                                                                          @RequestParam(defaultValue = "100") int pageSizeLimit,
	                                                                          @RequestParam(required = false) String sortBy,
	                                                                          @RequestParam(required = false) String sortOrder) throws IOException, SQLException {
		try {
			return ResponseEntity.ok(testSearchService.getTagsDbAddressPoiObjects(name, objectType, regExp, tokenFind, tag, values, pageToShow, pageSizeLimit, sortBy, sortOrder));
		} catch (SQLException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
		}
	}

	@GetMapping(value = "/tags-datasources/{name}/object-tokens", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<OBFService.DbObjectTokenPage> getTagsDbObjectTokens(@PathVariable String name,
	                                                                          @RequestParam long objectId,
	                                                                          @RequestParam(required = false) String find,
	                                                                          @RequestParam(defaultValue = "0") int pageToShow,
	                                                                          @RequestParam(defaultValue = "100") int pageSizeLimit,
	                                                                          @RequestParam(required = false) String sortBy,
	                                                                          @RequestParam(required = false) String sortOrder) throws IOException, SQLException {
		try {
			return ResponseEntity.ok(testSearchService.getTagsDbObjectTokens(name, objectId, find, pageToShow, pageSizeLimit, sortBy, sortOrder));
		} catch (SQLException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
		}
	}

	@GetMapping(value = "/tags-datasources/{name}/report", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<OBFService.DbReport> getTagsDbReport(@PathVariable String name,
	                                                           @RequestParam(defaultValue = "all") String objectType) throws IOException, SQLException {
		try {
			return ResponseEntity.ok(testSearchService.getTagsDbReport(name, objectType));
		} catch (SQLException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
		}
	}

	@GetMapping(value = "/generate/{jobId}/progress", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<GenerateDbJobStatus> getGenerateDbProgress(@PathVariable String jobId) {
		GenerateDbJob job = GENERATE_DB_JOBS.get(jobId);
		if (job == null) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Generate DB job not found");
		}
		return ResponseEntity.ok(new GenerateDbJobStatus(jobId, job.status, job.obfName, job.obfIndex, job.totalObfs,
				job.processedTokens, job.totalTokens, job.elapsedMs, job.estimatedMs,
				"DONE".equals(job.status) && job.zipFile != null, job.error, job.obfs));
	}

	@PostMapping(value = "/generate/{jobId}/cancel", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<GenerateDbJobStatus> cancelGenerateDb(@PathVariable String jobId) throws IOException {
		GenerateDbJob job = GENERATE_DB_JOBS.get(jobId);
		if (job == null) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Generate DB job not found");
		}
		if (!"DONE".equals(job.status) && !"FAILED".equals(job.status) && !"CANCELED".equals(job.status)) {
			job.cancelRequested = true;
			job.status = "CANCELED";
		}
		if (job.zipFile != null && !"DONE".equals(job.status) && (job.future == null || job.future.isDone())) {
			Files.deleteIfExists(job.zipFile);
			job.zipFile = null;
		}
		return ResponseEntity.ok(new GenerateDbJobStatus(jobId, job.status, job.obfName, job.obfIndex, job.totalObfs,
				job.processedTokens, job.totalTokens, job.elapsedMs, job.estimatedMs, false, job.error, job.obfs));
	}

	@GetMapping(value = "/generate/{jobId}/download", produces = "application/zip")
	@ResponseBody
	public void downloadGeneratedDb(@PathVariable String jobId, HttpServletResponse response) throws IOException {
		GenerateDbJob job = GENERATE_DB_JOBS.get(jobId);
		if (job == null) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Generate DB job not found");
		}
		if (!"DONE".equals(job.status) || job.zipFile == null) {
			throw new ResponseStatusException(HttpStatus.CONFLICT, "Generate DB job is not ready");
		}
		response.setContentType("application/zip");
		response.setHeader("Content-Disposition", "attachment; filename=\"db.zip\"");
		try {
			Files.copy(job.zipFile, response.getOutputStream());
		} finally {
			GENERATE_DB_JOBS.remove(jobId);
			Files.deleteIfExists(job.zipFile);
		}
	}
}
