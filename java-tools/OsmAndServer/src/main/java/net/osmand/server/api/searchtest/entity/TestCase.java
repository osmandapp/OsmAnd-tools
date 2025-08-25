package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;

@Entity
@Table(name = "test_case")
public class TestCase extends RunParam {
	public enum Status {
		NEW, GENERATED, FAILED
	}

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(columnDefinition = "INTEGER")
	public Long id;

	@Column()
	public String name;

	@Column()
	public String labels;

	@Column(name = "dataset_id", nullable = false)
	public Long datasetId;

	@Column(name = "last_run_id")
	public Long lastRunId;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	public Status status;

	@CreationTimestamp
	public LocalDateTime created;

	@UpdateTimestamp
	public LocalDateTime updated;

	@Column(name = "select_fun")
	public String selectFun; // Selected JS function name to calculate output

	@Column(name = "where_fun")
	public String whereFun; // Selected JS function name to calculate output

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name = "all_cols", columnDefinition = "TEXT")
	public String allCols; // column names as JSON string array

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name = "sel_cols", columnDefinition = "TEXT")
	public String selCols; // selected column names as JSON string array

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name = "select_params", columnDefinition = "TEXT")
	public String selectParams; // function param values as JSON string array

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name = "where_params", columnDefinition = "TEXT")
	public String whereParams; // function param values as JSON string array

	@Column(columnDefinition = "TEXT")
	private String error;

	public String getError() {
		return error;
	}

	public void setError(String error) {
		if (error != null) {
			status = TestCase.Status.FAILED;
		}
		this.error = error == null ? null : error.substring(0, Math.min(error.length(), 255));
	}
}
