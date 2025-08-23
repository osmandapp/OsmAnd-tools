package net.osmand.server.api.searchtest.dto;

import net.osmand.server.api.searchtest.entity.Run;
import net.osmand.server.api.searchtest.entity.TestCase;

import java.util.Map;

public record RunStatus(
		Run.Status status,
		long noResult,
		long processed,
		long failed,
		long duration,
		double averagePlace,
		long empty,
		Map<String, Number> distanceHistogram) {}
