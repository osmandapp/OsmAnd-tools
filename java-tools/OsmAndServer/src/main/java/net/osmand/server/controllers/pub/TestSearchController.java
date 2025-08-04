package net.osmand.server.controllers.pub;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.osmand.server.api.services.TestSearchService;
import net.osmand.server.api.test.dto.EvaluationReport;
import net.osmand.server.api.test.entity.Dataset;
import net.osmand.server.api.test.entity.EvalJob;
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
@RequestMapping(path = "/admin/test")
public class TestSearchController {

	@Autowired
	private TestSearchService testSearchService;

	@GetMapping
	public String index(Model model) throws IOException {
		return "admin/test_search";
	}

	@GetMapping(value = "/datasets", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<Dataset>> getDatasets(
			@RequestParam(required = false) String search,
			@RequestParam(required = false) String status,
			Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getDatasets(search, status, pageable));
	}

	@GetMapping(value = "/datasets/{datasetId}/jobs", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Page<EvalJob>> getEvalJobs(@PathVariable Long datasetId, Pageable pageable) {
		return ResponseEntity.ok(testSearchService.getDatasetJobs(datasetId, pageable));
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

	@GetMapping(value = "/reports/{jobId}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<EvaluationReport> getEvaluationReport(
			@PathVariable Long jobId) {
		return testSearchService.getEvaluationReport(jobId)
				.map(ResponseEntity::ok)
				.orElse(ResponseEntity.notFound().build());
	}

	@GetMapping(value = "/reports/{jobId}/download")
	public void downloadReport(
			@PathVariable Long jobId,
			@RequestParam(defaultValue = "csv") String format,
			HttpServletResponse response) throws IOException {

		String contentType = "csv".equalsIgnoreCase(format) ? "text/csv" : "application/json";
		response.setContentType(contentType);
		response.setHeader("Content-Disposition", "attachment; filename=\"report." + format + "\"");

		testSearchService.downloadRawResults(response.getWriter(), jobId, format);
	}

	/**
	 * Step 1 – create a new evaluation job (synchronous).
	 * Returns the freshly created {@link EvalJob} in PENDING/RUNNING state.
	 */
	@PostMapping(value = "/eval/{datasetId:\\d+}", consumes = MediaType.APPLICATION_JSON_VALUE, produces =
            MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<EvalJob> createEvaluation(@PathVariable Long datasetId,
													@RequestBody Map<String, String> payload,
													HttpServletRequest request) {
		String baseUrl = ServletUriComponentsBuilder.fromRequestUri(request)
				.replacePath(null)
				.build()
				.toUriString();

		EvalJob job = testSearchService.startEvaluation(datasetId, payload);

		URI location = ServletUriComponentsBuilder.fromUriString(baseUrl)
				.path("/admin/test/eval/{jobId:\\d+}")
				.buildAndExpand(job.getId())
				.toUri();
		return ResponseEntity.accepted().location(location).body(job);
	}

	/**
	 * Step 2 – continue processing of an existing job asynchronously.
	 * The call returns immediately with 202 while the heavy work happens in the background.
	 */
	@PostMapping(value = "/eval/process/{jobId:\\d+}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Void> processEvaluation(@PathVariable Long jobId) {
		testSearchService.processEvaluation(jobId); // @Async – returns immediately
		return ResponseEntity.accepted().build();
	}

	@GetMapping(value = "/job/{jobId:\\d+}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<EvalJob> getJob(@PathVariable Long jobId) {
		return testSearchService.getJob(jobId)
				.map(ResponseEntity::ok)
				.orElse(ResponseEntity.notFound().build());
	}

	@PostMapping(value = "/eval/cancel/{jobId:\\d+}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<EvalJob>> cancelEvaluation(@PathVariable Long jobId) {
		return testSearchService.cancelEvaluation(jobId)
				.thenApply(ResponseEntity::ok);
	}

	@DeleteMapping(value = "/jobs/{jobId}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Void>> deleteJob(@PathVariable Long jobId) {
		return testSearchService.deleteJob(jobId)
				.thenApply(v -> ResponseEntity.noContent().build());
	}

	@PostMapping(value = "/csv/count", consumes = MediaType.APPLICATION_JSON_VALUE, produces =
            MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Map<String, Long>>> countCsvRows(@RequestBody String filePath) {
		return testSearchService.countCsvRows(filePath)
				.thenApply(count -> ResponseEntity.ok(Map.of("count", count)));
	}

	@PostMapping(value = "/refresh", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<?>> refreshDataset(@RequestParam("datasetId") Long datasetId,
                                                               @RequestParam("reload") Boolean reload) {
		final var locationBuilder = ServletUriComponentsBuilder.fromCurrentRequest();
		return testSearchService.refreshDataset(datasetId, reload).thenApply(path -> {
			URI location = locationBuilder.buildAndExpand().toUri();
			return ResponseEntity.created(location).body(path);
		});
	}

	@PutMapping(value = "/dataset/{datasetId}", consumes = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public CompletableFuture<ResponseEntity<Dataset>> updateDataset(@PathVariable Long datasetId,
                                                                    @RequestBody Map<String, String> updates) {
		return testSearchService.updateDataset(datasetId, updates)
				.thenApply(ResponseEntity::ok);
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

	/**
	 * Create a new dataset.
	 */
	@PostMapping(value = "/datasets", consumes = MediaType.APPLICATION_JSON_VALUE, produces =
            MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Dataset> createDataset(@RequestBody Dataset payload) {
		return ResponseEntity.status(HttpStatus.CREATED)
				.body(testSearchService.createDataset(payload).join());
	}


	// --- Dataset management -------------------------------------------------

	@DeleteMapping(value = "/datasets/{datasetId:\\d+}")
	public ResponseEntity<Void> deleteDataset(@PathVariable Long datasetId) {
		boolean deleted = testSearchService.deleteDataset(datasetId);
		return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
	}
}
