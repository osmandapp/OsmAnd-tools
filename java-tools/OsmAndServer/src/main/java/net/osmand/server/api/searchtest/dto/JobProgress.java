package net.osmand.server.api.searchtest.dto;

import net.osmand.server.api.searchtest.entity.EvalJob;

public record JobProgress (
	long jobId,
	long datasetId,
	EvalJob.Status status,
	long total,
	long processed,
	long error,
	long duration) {}
