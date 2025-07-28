package net.osmand.server.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public record OverpassTestRequest(
        @Schema(description = "Overpass API query", required = true)
        @NotBlank
        String query,

        @Schema(description = "Maximum number of rows to ingest. If the query returns more rows, a random sample is taken. Defaults to 10,000.")
        Integer sizeLimit
) {}
