package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.sql.Timestamp;
import java.util.Map;

@MappedSuperclass
public abstract class Result {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(columnDefinition = "INTEGER")
	public Long id;

	@Column(name = "case_id", nullable = false)
	public Long caseId;

	@Column(name = "dataset_id", nullable = false)
	public Long datasetId;

	@JdbcTypeCode(SqlTypes.JSON)
	@Column
	public Map<String, String> row;

	@Column(columnDefinition = "TEXT")
	public String error;

	@Column
	public Integer duration;

	@Column(nullable = false)
	public Integer count;

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(columnDefinition = "TEXT")
	public String query;

	@Column(nullable = false)
	public Double lat;

	@Column(nullable = false)
	public Double lon;

	@Column(nullable = false)
	public Timestamp timestamp = new Timestamp(System.currentTimeMillis());
}
