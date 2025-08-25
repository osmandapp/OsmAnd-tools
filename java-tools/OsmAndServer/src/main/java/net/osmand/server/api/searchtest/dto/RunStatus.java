package net.osmand.server.api.searchtest.dto;

import net.osmand.server.api.searchtest.entity.Run;

import java.util.Map;

public record RunStatus(
		Run.Status status,
		long total,
		long processed,
		long failed,
		long duration,
		double averagePlace,
		long found,
		Map<String, Number> distanceHistogram,
		TestCaseStatus generatedChart) {
}
