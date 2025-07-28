package net.osmand.server.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public record CsvRequest(
        @Schema(description = "Relative path to the CSV file on the server.", required = true)
        @NotBlank
        String filePath,

        @Schema(description = "Maximum number of rows to ingest. If the CSV contains more rows, a random sample is taken. Defaults to 10,000.")
        Integer sizeLimit
) {}
