package net.osmand.server.controllers.pub;

import jakarta.servlet.http.HttpServletResponse;
import net.osmand.server.SearchTestRepositoryConfiguration;
import net.osmand.server.api.searchtest.BaseService.GenParam;
import net.osmand.server.api.searchtest.DataService;
import net.osmand.server.api.searchtest.ReportService.RunStatus;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository;
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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Controller
@RequestMapping(path = "/admin/search-test")
public class SearchTestController {

	@Autowired
	private SearchTestRepositoryConfiguration dbCfg;
	@Autowired
	private SearchTestService testSearchService;

	@GetMapping
	public String index(Model model) throws IOException {
		return "admin/search-test";
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
		return testSearchService.getRunStatus(runId).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
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
	@PostMapping(value = "/cases/{caseId:\\d+}/run", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Run>> runTestCase(@PathVariable Long caseId,
															  @RequestBody RunParam payload) {
		return testSearchService.runTestCase(caseId, payload).thenApply(ResponseEntity::ok);
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
	public ResponseEntity<SearchTestDatasetRepository.Domain> updateDomain(@PathVariable("id") Long id,
																	 @RequestBody Map<String, String> updates) {
		return ResponseEntity.ok(testSearchService.updateDomain(id, updates));
	}

	@DeleteMapping(value = "/domains/{id}")
	@ResponseBody
	public ResponseEntity<Void> deleteDomain(@PathVariable("id") Long id) {
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

	private static final double SEARCH_RADIUS_DEGREE = 1.0;

	@GetMapping(value = "/search", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<DataService.ResultsWithStats> getResults(
			@RequestParam String query,
			@RequestParam(required = false) String lang,
			@RequestParam(required = false) Double radius,
			@RequestParam Double lat,
			@RequestParam Double lon) throws IOException {
		radius = radius == null ? SEARCH_RADIUS_DEGREE : radius;
		return ResponseEntity.ok(testSearchService.getResults(radius, lat, lon, query, lang));
	}

	@GetMapping(value = "/unit-test", produces = "application/zip")
	@ResponseBody
	public void downloadUnitTest(
			@RequestParam String query,
			@RequestParam(required = false) String unitTestName,
			@RequestParam(required = false) Double radius,
			@RequestParam Double lat,
			@RequestParam Double lon,
			HttpServletResponse response) throws IOException, SQLException {
		response.setContentType("application/zip");
		response.setHeader("Content-Disposition", "attachment; filename=unit-test.zip");
		// write a ZIP containing 2 entries
		//  - unit-test_name.json (JSON request)
		//  - unit-test_name.zip.gz (gzip-compressed data archive)
		testSearchService.createUnitTest(query, unitTestName, radius, lat, lon, response.getOutputStream());
	}

}
