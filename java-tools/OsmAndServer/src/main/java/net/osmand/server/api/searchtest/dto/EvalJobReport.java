package net.osmand.server.api.searchtest.dto;

import java.util.Map;

public record EvalJobReport(
        long jobId,
        long notFound,
		long processed,
        long error,
		long duration,
		double averagePlace,
        Map<String, Number> distanceHistogram) {
}
