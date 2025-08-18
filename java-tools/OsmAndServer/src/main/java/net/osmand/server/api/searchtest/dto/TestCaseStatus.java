package net.osmand.server.api.searchtest.dto;

import net.osmand.server.api.searchtest.entity.TestCase;

import java.util.Map;

public record TestCaseStatus(
		TestCase.Status status,
		long noResult,
		long processed,
		long failed,
		long duration,
		double averagePlace, Map<String, Number> distanceHistogram) {}
