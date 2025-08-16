package net.osmand.server.api.searchtest.entity;

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
	public Long id;

	@Column(name = "job_id", nullable = false)
	public Long jobId;

	@Column(name = "dataset_id", nullable = false)
	public Long datasetId;

	@JdbcTypeCode(SqlTypes.JSON)
	@Column
	public Map<String, String> original;

	@Column(columnDefinition = "TEXT")
	public String error;

	@Column
	public Integer duration;

	@Column(nullable = false)
	public Timestamp timestamp = new Timestamp(System.currentTimeMillis());

	@Column(length = 512)
	public String address;

	@Column(name = "min_distance")
	public Integer minDistance;

	@Column(name = "closest_result", length = 512)
	public String closestResult;

	@Column(name = "actual_place")
	public Integer actualPlace;

	@Column(name = "results_count")
	public Integer resultsCount;

	@Column
	public Double lat;

	@Column
	public Double lon;
}
