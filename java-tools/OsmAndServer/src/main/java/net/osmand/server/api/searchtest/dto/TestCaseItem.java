package net.osmand.server.api.searchtest.dto;

import net.osmand.server.api.searchtest.entity.TestCase;

import java.time.LocalDateTime;

/**
 * Lightweight DTO for listing test-cases with parent dataset name.
 */
public class TestCaseItem {
    public Long id;
    public String name;
    public String labels;
    public Long datasetId;
    public String datasetName;
    public Long lastRunId;
    public TestCase.Status status;
    public LocalDateTime updated;
    public String error;
    public long total;
    public long failed;
    public long duration;

    public TestCaseItem() {}

    public TestCaseItem(Long id, String name, String labels, Long datasetId, String datasetName,
                        Long lastRunId, TestCase.Status status, LocalDateTime updated, String error,
                        long total, long failed, long duration) {
        this.id = id;
        this.name = name;
        this.labels = labels;
        this.datasetId = datasetId;
        this.datasetName = datasetName;
        this.lastRunId = lastRunId;
        this.status = status;
        this.updated = updated;
        this.error = error;
        this.total = total;
        this.failed = failed;
        this.duration = duration;
    }
}
