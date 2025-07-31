package net.osmand.server.api.dto;

import java.util.Map;

public record EvaluationReport(
        long jobId,
        long totalRequests,
        long failedRequests,
        double errorRate,
        double averageDuration,
		double averagePlace,
        Map<String, Long> distanceHistogram) {
}
