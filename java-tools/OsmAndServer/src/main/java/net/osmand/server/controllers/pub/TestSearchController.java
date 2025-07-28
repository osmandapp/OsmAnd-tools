package net.osmand.server.controllers.pub;

import net.osmand.server.api.dto.CsvRequest;
import net.osmand.server.api.dto.OverpassQueryRequest;
import net.osmand.server.api.dto.OverpassQueryResult;
import net.osmand.server.api.services.TestSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping(path = "/admin/test")
public class TestSearchController {

    @Autowired
    private TestSearchService testSearchService;

    @PostMapping(value = "/csv/count", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<Map<String, Long>>> countCsvRows(@RequestBody CsvRequest request) {
        return testSearchService.countCsvRows(request.filePath())
                .thenApply(count -> ResponseEntity.ok(Map.of("count", count)));
    }

    @PostMapping(value = "/refresh", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<?>> refreshDataset(@RequestParam("datasetId") Long datasetId, @RequestParam("sizeLimit") Integer sizeLimit) {
        final var locationBuilder = ServletUriComponentsBuilder.fromCurrentRequest();
        return testSearchService.refreshDataset(datasetId, sizeLimit).thenApply(path -> {
            URI location = locationBuilder.buildAndExpand().toUri();
            return ResponseEntity.created(location).body(path);
        });
    }
}
