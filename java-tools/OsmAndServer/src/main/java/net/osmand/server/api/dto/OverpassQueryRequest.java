package net.osmand.server.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public record OverpassQueryRequest(
        @Schema(description = "Overpass API query", required = true)
        @NotBlank
        String query
) {}
