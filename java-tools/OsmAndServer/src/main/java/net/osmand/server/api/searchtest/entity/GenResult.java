package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.sql.Timestamp;
import java.util.Map;

@Entity
@Table(name = "gen_result")
public class GenResult {
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

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(nullable = false, columnDefinition = "TEXT")
	public String output;

	@Column(nullable = false)
	public Double lat;

	@Column(nullable = false)
	public Double lon;

	@Column(nullable = false)
	public Timestamp timestamp = new Timestamp(System.currentTimeMillis());
}
