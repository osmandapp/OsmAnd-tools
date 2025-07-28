package net.osmand.server.controllers.pub;

import net.osmand.server.api.dto.OverpassTestRequest;
import net.osmand.server.api.services.TestSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

@Controller
@RequestMapping(path = "/admin/test", produces = MediaType.APPLICATION_JSON_VALUE)
public class TestSearchController {

    @Autowired
    private TestSearchService testSearchService;

    @PostMapping("/overpass")
    public CompletableFuture<ResponseEntity<?>> testFromOverpass(@RequestBody OverpassTestRequest request) {
        return testSearchService.ingestFromOverpass(request).thenApply(dataset -> {
            URI location = ServletUriComponentsBuilder
                    .fromCurrentRequest()
                    .path("/{id}")
                    .buildAndExpand(dataset.getId())
                    .toUri();
            return ResponseEntity.created(location).body(dataset);
        });
    }
}
