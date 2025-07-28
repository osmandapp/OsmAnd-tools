package net.osmand.server.api.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MapsId;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Objects;

@Entity
@Table(name = "test_result")
public class TestResult {

	public enum ResultStatus {
		MATCHED, NOT_MATCHED, ERROR
	}

	@EmbeddedId
	private ResultId id;

	@ManyToOne(fetch = FetchType.LAZY)
	@MapsId("jobId")
	@JoinColumn(name = "job_id")
	private DatasetJob job;

	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "dataset_id", nullable = false)
	private Dataset dataset;

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(columnDefinition = "jsonb")
	private Map<String, String> sourceData;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	private ResultStatus status;

	@Column
	private Integer duration;

	@Column(nullable = false)
	private Timestamp timestamp;

	@Column(length = 512)
	private String address;

	@Column(name = "min_distance")
	private Integer minDistance;

	@Column(name = "closest_result", length = 512)
	private String closestResult;

	@Column(name = "actual_place")
	private Integer actualPlace;

	@Column(name = "results_count")
	private Integer resultsCount;

	public String getOriginal() {
		return original;
	}

	public void setOriginal(String original) {
		this.original = original;
	}

	@Column(name = "original")
	private String 	original;
	// Getters and Setters

	public ResultId getId() {
		return id;
	}

	public void setId(ResultId id) {
		this.id = id;
	}

	public DatasetJob getJob() {
		return job;
	}

	public void setJob(DatasetJob job) {
		this.job = job;
	}

	public Dataset getDataset() {
		return dataset;
	}

	public void setDataset(Dataset dataset) {
		this.dataset = dataset;
	}

	public Map<String, String> getSourceData() {
		return sourceData;
	}

	public void setSourceData(Map<String, String> sourceData) {
		this.sourceData = sourceData;
	}

	public ResultStatus getStatus() {
		return status;
	}

	public void setStatus(ResultStatus status) {
		this.status = status;
	}

	public Integer getDuration() {
		return duration;
	}

	public void setDuration(Integer duration) {
		this.duration = duration;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Integer getMinDistance() {
		return minDistance;
	}

	public void setMinDistance(Integer minDistance) {
		this.minDistance = minDistance;
	}

	public String getClosestResult() {
		return closestResult;
	}

	public void setClosestResult(String closestResult) {
		this.closestResult = closestResult;
	}

	public Integer getActualPlace() {
		return actualPlace;
	}

	public void setActualPlace(Integer actualPlace) {
		this.actualPlace = actualPlace;
	}

	public Integer getResultsCount() {
		return resultsCount;
	}

	public void setResultsCount(Integer resultsCount) {
		this.resultsCount = resultsCount;
	}

	@Embeddable
	public static class ResultId implements Serializable {

		@Column(name = "job_id")
		private Long jobId;

		@Column(name = "row_id")
		private Long rowId;

		public ResultId() {}

		public ResultId(Long jobId, Long rowId) {
			this.jobId = jobId;
			this.rowId = rowId;
		}

		// Getters, Setters, equals, and hashCode

		public Long getJobId() {
			return jobId;
		}

		public void setJobId(Long jobId) {
			this.jobId = jobId;
		}

		public Long getRowId() {
			return rowId;
		}

		public void setRowId(Long rowId) {
			this.rowId = rowId;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ResultId resultId = (ResultId) o;
			return Objects.equals(jobId, resultId.jobId) &&
					Objects.equals(rowId, resultId.rowId);
		}

		@Override
		public int hashCode() {
			return Objects.hash(jobId, rowId);
		}
	}
}
