package net.osmand.server.api.dto;

import java.util.Map;

public record EvaluationReport(
        long jobId,
        long total,
		long processed,
        long error,
		long duration,
		double averagePlace,
        Map<String, Number> distanceHistogram) {
}
