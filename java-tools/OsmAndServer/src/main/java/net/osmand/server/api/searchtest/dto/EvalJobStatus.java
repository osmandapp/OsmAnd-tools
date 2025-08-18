package net.osmand.server.api.searchtest.dto;

import net.osmand.server.api.searchtest.entity.EvalJob;

import java.util.Map;

public record EvalJobStatus(
		EvalJob.Status status,
		long noResult,
		long processed,
		long failed,
		long duration,
		double averagePlace, Map<String, Number> distanceHistogram) {}
