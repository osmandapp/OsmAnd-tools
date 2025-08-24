package net.osmand.server.api.searchtest.dto;

import net.osmand.server.api.searchtest.entity.TestCase;

public record TestCaseStatus(
		TestCase.Status status,
		long processed,
		long failed,
		long filtered,
		long empty,
		long duration) {}
