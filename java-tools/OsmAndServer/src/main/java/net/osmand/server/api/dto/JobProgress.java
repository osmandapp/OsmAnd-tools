package net.osmand.server.api.dto;

import net.osmand.server.api.entity.JobStatus;

public record JobProgress (
		long jobId,
		long datasetId,
    JobStatus status,
    long total,
    long processed,
    long error,
    long duration) {
}
