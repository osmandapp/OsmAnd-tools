package net.osmand.server.api.dto;

public class JobProgress {
    private Long jobId;
    private Long datasetId;
    private String status;
    private long total;
    private long processed;
    private long error;

    public JobProgress(Long jobId, Long datasetId, String status, long total, long processed, long error) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.status = status;
        this.total = total;
        this.processed = processed;
        this.error = error;
    }

    // Getters and setters

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public Long getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(Long datasetId) {
        this.datasetId = datasetId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getProcessed() {
        return processed;
    }

    public void setProcessed(long processed) {
        this.processed = processed;
    }

    public long getError() {
        return error;
    }

    public void setError(long error) {
        this.error = error;
    }
}
