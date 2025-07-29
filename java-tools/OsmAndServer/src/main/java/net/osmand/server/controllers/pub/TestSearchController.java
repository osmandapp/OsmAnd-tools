package net.osmand.server.controllers.pub;

import net.osmand.server.api.entity.Dataset;
import net.osmand.server.api.entity.EvalJob;
import net.osmand.server.api.dto.EvaluationReport;
import net.osmand.server.api.services.TestSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import jakarta.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping(path = "/admin/test")
public class TestSearchController {

    @Autowired
    private TestSearchService testSearchService;

    @MessageMapping("/eval/ws/{jobId}")
    @SendTo("/topic/test-job-progress/{jobId}")
    public EvalJob handleJobUpdates(@DestinationVariable Long jobId) {
        return testSearchService.getEvaluationJob(jobId).orElse(null);
    }

    @GetMapping(value = "/datasets", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Page<Dataset>> getDatasets(Pageable pageable) {
        return ResponseEntity.ok(testSearchService.getDatasets(pageable));
    }

    @GetMapping(value = "/datasets/{datasetId}/jobs", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Page<EvalJob>> getEvalJobs(@PathVariable Long datasetId, Pageable pageable) {
        return ResponseEntity.ok(testSearchService.getDatasetJobs(datasetId, pageable));
    }

    @GetMapping(value = "/reports/{datasetId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EvaluationReport> getEvaluationReport(
            @PathVariable Long datasetId,
            @RequestParam(required = false) Long jobId) {
        return testSearchService.getEvaluationReport(datasetId, Optional.ofNullable(jobId))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping(value = "/eval/{datasetId}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EvalJob> startEvaluation(@PathVariable Long datasetId, @RequestBody Map<String, String> payload, HttpServletRequest request) {
        String baseUrl = ServletUriComponentsBuilder.fromRequestUri(request)
                .replacePath(null)
                .build()
                .toUriString();

        EvalJob job = testSearchService.startEvaluation(datasetId, payload);

        URI location = ServletUriComponentsBuilder.fromUriString(baseUrl)
                .path("/admin/test/eval/{jobId}")
                .buildAndExpand(job.getId())
                .toUri();
        return ResponseEntity.accepted().location(location).body(job);
    }

    @GetMapping(value = "/eval/{jobId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EvalJob> getEvaluation(@PathVariable Long jobId) {
        return testSearchService.getEvaluationJob(jobId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping(value = "/eval/cancel/{jobId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<EvalJob>> cancelEvaluation(@PathVariable Long jobId) {
        return testSearchService.cancelEvaluation(jobId)
                .thenApply(ResponseEntity::ok);
    }

    @PostMapping(value = "/csv/count", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<Map<String, Long>>> countCsvRows(@RequestBody String filePath) {
        return testSearchService.countCsvRows(filePath)
                .thenApply(count -> ResponseEntity.ok(Map.of("count", count)));
    }

    @PostMapping(value = "/refresh", produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<?>> refreshDataset(@RequestParam("datasetId") Long datasetId, @RequestParam("sizeLimit") Integer sizeLimit) {
        final var locationBuilder = ServletUriComponentsBuilder.fromCurrentRequest();
        return testSearchService.refreshDataset(datasetId, sizeLimit).thenApply(path -> {
            URI location = locationBuilder.buildAndExpand().toUri();
            return ResponseEntity.created(location).body(path);
        });
    }

    @PostMapping(value = "/dataset", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<?>> createDataset(@RequestBody Map<String, String> body) {
        String name = body.getOrDefault("name", "");
        String type = body.getOrDefault("type", "CSV");
        String source = body.getOrDefault("source", "");
        final var locationBuilder = ServletUriComponentsBuilder.fromCurrentRequest();
        return testSearchService.createDataset(name, type, source)
                .thenApply(dataset -> {
                    URI location = locationBuilder.path("/{id}").buildAndExpand(dataset.getId()).toUri();
                    return ResponseEntity.created(location).body(dataset);
                });
    }

    @GetMapping("/browse")
    public ResponseEntity<?> browseCsvFiles() {
        try {
            return ResponseEntity.ok(testSearchService.browseCsvFiles());
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error browsing CSV files: " + e.getMessage());
        }
    }
}
