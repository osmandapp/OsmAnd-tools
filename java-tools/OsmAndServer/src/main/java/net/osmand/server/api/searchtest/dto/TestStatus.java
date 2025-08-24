package net.osmand.server.api.searchtest.dto;

import net.osmand.server.api.searchtest.entity.TestCase;

import java.util.Map;

public record TestStatus(
		TestCase.Status status,
		long processed,
		long failed,
		long filtered,
		long empty,
		long duration) {}
