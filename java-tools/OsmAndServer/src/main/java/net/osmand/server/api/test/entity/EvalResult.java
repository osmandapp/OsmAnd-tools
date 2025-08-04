package net.osmand.server.api.test.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.sql.Timestamp;
import java.util.Map;

@Entity
@Table(name = "eval_result")
public class EvalResult {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(columnDefinition = "INTEGER")
	private Long id;

	@Column(name = "job_id", nullable = false)
	private Long jobId;

	@Column(name = "dataset_id", nullable = false)
	private Long datasetId;

	@JdbcTypeCode(SqlTypes.JSON)
	@Column
	private Map<String, String> original;

	@Column(columnDefinition = "TEXT")
	private String error;

	@Column
	private Integer duration;

	@Column(nullable = false)
	private Timestamp timestamp = new Timestamp(System.currentTimeMillis());

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

	@Column
	private Double lat;

	@Column
	private Double lon;
	// Getters and Setters

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Double getLat() {
		return lat;
	}

	public void setLat(Double lat) {
		this.lat = lat;
	}

	public Double getLon() {
		return lon;
	}

	public void setLon(Double lon) {
		this.lon = lon;
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
	public Long getJobId() {
		return jobId;
	}

	public void setJob(Long job) {
		this.jobId = job;
	}

	public Map<String, String> getOriginal() {
		return original;
	}

	public void setOriginal(Map<String, String> sourceData) {
		this.original = sourceData;
	}

	public String getError() {
		return error;
	}

	public void setError(String status) {
		this.error = error;
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
}
