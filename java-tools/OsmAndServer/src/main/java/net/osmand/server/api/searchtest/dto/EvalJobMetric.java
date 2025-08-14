package net.osmand.server.api.searchtest.dto;

import net.osmand.server.api.searchtest.entity.EvalJob;

import java.util.Map;

public record EvalJobMetric(
		EvalJob.Status status,
		long noResult,
		long processed,
		long error,
		long duration,
		double averagePlace, Map<String, Number> distanceHistogram) {}
