package net.osmand.server.api.searchtest.dto;

import net.osmand.server.api.searchtest.entity.EvalJob;

public record EvalJobProgress(
	long jobId,
	long notFound,
	long processed,
	long error,
	long duration,
	double averagePlace) {}
