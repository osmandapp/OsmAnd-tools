package net.osmand.server.controllers.pub;

import jakarta.servlet.http.HttpServletResponse;
import net.osmand.server.api.searchtest.BaseService.GenParam;
import net.osmand.server.api.searchtest.ReportService.RunStatus;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Controller
@RequestMapping(path = "/admin/search-test")
public class SearchTestController {

	@Autowired
	private SearchTestService testSearchService;

	@GetMapping
	public String index(Model model) throws IOException {
		return "admin/search-test";
	}

	@GetMapping(value = "/datasets/{datasetId}/cases", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<TestCase>> getTestCases(@PathVariable Long datasetId,
													   Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getTestCases(datasetId, pageable));
	}

	@GetMapping(value = "/cases/{caseId}/runs", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<Run>> getRuns(@PathVariable Long caseId, Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getRuns(caseId, pageable));
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

	@PutMapping(value = "/cases/{caseId:\\d+}", consumes = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<TestCase>> updateTestCase(@PathVariable Long caseId,
																	  @RequestBody Map<String, String> payload,
																	  @RequestParam("regen") Boolean regen) {
		return testSearchService.updateTestCase(caseId, payload, regen).thenApply(ResponseEntity::ok);
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
			response.setContentType("text/csv");
			response.setHeader("Content-Disposition", "attachment; filename=\"sample.csv\"");
			response.getWriter().write(csvData);
		} catch (RuntimeException e) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, e.getMessage());
		}
	}

	@GetMapping(value = "/cases/{caseId}/report", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<RunStatus> getTestCaseReport(@PathVariable Long caseId,
													   @RequestParam(defaultValue = "10") Integer placeLimit,
													   @RequestParam(defaultValue = "50") Integer distLimit) {
		TestCase tc = testSearchService.getTestCase(caseId).orElseThrow(() ->
				new RuntimeException("TestCase not found with id: " + caseId));
		return testSearchService.getRunReport(caseId, tc.lastRunId, placeLimit, distLimit)
				.map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	@GetMapping(value = "/runs/{runId}/report", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<RunStatus> getRunReport(@PathVariable Long runId,
												  @RequestParam(defaultValue = "10") Integer placeLimit,
												  @RequestParam(defaultValue = "50") Integer distLimit) {
		Run run = testSearchService.getRun(runId).orElseThrow(() ->
				new RuntimeException("Run not found with id: " + runId));
		return testSearchService.getRunReport(run.caseId, runId, placeLimit, distLimit)
				.map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	@GetMapping(value = "/runs/{runId}/download")
	public void downloadRunReport(@PathVariable Long runId,
								  @RequestParam(defaultValue = "10") Integer placeLimit,
								  @RequestParam(defaultValue = "50") Integer distLimit,
								  @RequestParam(defaultValue = "csv") String format,
								  HttpServletResponse response) throws IOException {
		Run run = testSearchService.getRun(runId).orElseThrow(() ->
				new RuntimeException("Run not found with id: " + runId));
		String contentType = "csv".equalsIgnoreCase(format) ? "text/csv" : "application/json";
		response.setContentType(contentType);
		response.setHeader("Content-Disposition", "attachment; filename=\"report." + format + "\"");

		testSearchService.downloadRawResults(response.getWriter(), placeLimit, distLimit, run.caseId, runId, format);
	}

	@GetMapping(value = "/cases/{caseId}/download")
	public void downloadTestCaseReport(@PathVariable Long caseId,
									   @RequestParam(defaultValue = "10") Integer placeLimit,
									   @RequestParam(defaultValue = "50") Integer distLimit,
									   @RequestParam(defaultValue = "csv") String format,
									   HttpServletResponse response) throws IOException {
		TestCase tc = testSearchService.getTestCase(caseId).orElseThrow(() ->
				new RuntimeException("TestCase not found with id: " + caseId));
		String contentType = "csv".equalsIgnoreCase(format) ? "text/csv" : "application/json";
		response.setContentType(contentType);
		response.setHeader("Content-Disposition", "attachment; filename=\"report." + format + "\"");

		testSearchService.downloadRawResults(response.getWriter(), placeLimit, distLimit, caseId, tc.lastRunId,
				format);
	}

	@GetMapping(value = "/labels", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<String[]> getLabels() {
		return ResponseEntity.ok(testSearchService.getAllLabels().toArray(new String[0]));
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
}
