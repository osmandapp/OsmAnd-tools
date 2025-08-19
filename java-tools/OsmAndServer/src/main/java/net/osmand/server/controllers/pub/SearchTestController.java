package net.osmand.server.controllers.pub;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.osmand.server.api.searchtest.dto.GenParam;
import net.osmand.server.api.searchtest.dto.TestCaseStatus;
import net.osmand.server.api.searchtest.dto.RunParam;
import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.entity.TestCase;
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
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Controller
@RequestMapping(path = "/admin/search_test")
public class SearchTestController {

	@Autowired
	private SearchTestService testSearchService;

	@GetMapping
	public String index(Model model) throws IOException {
		return "admin/search_test";
	}

	@GetMapping(value = "/datasets/{datasetId}/cases", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<TestCase>> getTestCases(@PathVariable Long datasetId, Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getTestCases(datasetId, pageable));
	}

	@GetMapping(value = "/cases/{caseId:\\d+}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<TestCase> getTestCase(@PathVariable Long caseId) {
		return testSearchService.getTestCase(caseId).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	@GetMapping(value = "/cases/{caseId}/status", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<TestCaseStatus> getTestCaseStatus(@PathVariable Long caseId) {
		return testSearchService.getTestCaseStatus(caseId).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	@PostMapping(value = "/datasets/{datasetId:\\d+}/gen", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Void> genTestCase(@PathVariable Long datasetId, @RequestBody GenParam payload) {
		testSearchService.genTestCase(datasetId, payload);
		return ResponseEntity.accepted().build();
	}

	/**
	 * Step 1 – create a new evaluation job (synchronous).
	 * Returns the freshly created {@link TestCase} in PENDING/RUNNING state.
	 */
	@PostMapping(value = "/cases/{caseId:\\d+}/start", consumes = MediaType.APPLICATION_JSON_VALUE, produces =
			MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<TestCase> createRun(@PathVariable Long caseId, @RequestBody RunParam payload,
											  HttpServletRequest request) {
		String baseUrl = ServletUriComponentsBuilder.fromRequestUri(request).replacePath(null).build().toUriString();
		TestCase test = testSearchService.startTestCase(caseId, payload);
		URI location =
				ServletUriComponentsBuilder.fromUriString(baseUrl).path("/admin/test/eval/{caseId:\\d+}").buildAndExpand(test.id).toUri();
		return ResponseEntity.accepted().location(location).body(test);
	}

	/**
	 * Step 2 – continue processing of an existing job asynchronously.
	 * The call returns immediately with 202 while the heavy work happens in the background.
	 */
	@PostMapping(value = "/cases/{caseId:\\d+}/process", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Void> processRun(@PathVariable Long caseId) {
		testSearchService.runTestCase(caseId); // @Async – returns immediately
		return ResponseEntity.accepted().build();
	}

	@PostMapping(value = "/cases/{caseId:\\d+}/cancel", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<TestCase>> cancelRun(@PathVariable Long caseId) {
		return testSearchService.cancelRun(caseId).thenApply(ResponseEntity::ok);
	}

	@DeleteMapping(value = "/cases/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Void>> deleteTestCase(@PathVariable Long id) {
		return testSearchService.deleteTestCase(id).thenApply(v -> ResponseEntity.noContent().build());
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

	@GetMapping(value = "/reports/{caseId}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<TestCaseStatus> getRunReport(@PathVariable Long caseId,
													   @RequestParam(defaultValue = "10") Integer placeLimit,
													   @RequestParam(defaultValue = "50") Integer distLimit) {
		return testSearchService.getRunReport(caseId, placeLimit, distLimit).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	@GetMapping(value = "/reports/{caseId}/download")
	public void downloadReport(@PathVariable Long caseId,
							   @RequestParam(defaultValue = "10") Integer placeLimit,
							   @RequestParam(defaultValue = "50") Integer distLimit,
							   @RequestParam(defaultValue = "csv") String format,
							   HttpServletResponse response) throws IOException {

		String contentType = "csv".equalsIgnoreCase(format) ? "text/csv" : "application/json";
		response.setContentType(contentType);
		response.setHeader("Content-Disposition", "attachment; filename=\"report." + format + "\"");

		testSearchService.downloadRawResults(response.getWriter(), placeLimit, distLimit, caseId, format);
	}

	// --- Dataset management -------------------------------------------------
	@GetMapping(value = "/datasets", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<Dataset>> getDatasets(@RequestParam(required = false) String search,
													 @RequestParam(required = false) String status,
													 Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getDatasets(search, status, pageable));
	}

	@PutMapping(value = "/dataset/{datasetId}", consumes = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Dataset>> updateDataset(@PathVariable Long datasetId, @RequestParam("reload") Boolean reload,
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
