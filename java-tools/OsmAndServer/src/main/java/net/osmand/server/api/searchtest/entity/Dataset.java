package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;

@Entity
@Table(name = "dataset")
public class Dataset {
	public enum ConfigStatus {
		UNKNOWN, OK, ERROR
	}

	public enum Source {
		CSV, Overpass
	}

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(columnDefinition = "INTEGER")
	public Long id;

	@Column(nullable = false, unique = true)
	public String name;

	@Column()
	public String labels;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	public Source type; // e.g., "Overpass", "CSV"

	@Column(nullable = false, columnDefinition = "TEXT")
	public String source; // Overpass query or file path

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name = "all_cols", columnDefinition = "TEXT")
	public String allCols;

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name = "sel_cols", columnDefinition = "TEXT")
	public String selCols; // selected column names as JSON string array

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(columnDefinition = "TEXT")
	public String script;

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(columnDefinition = "TEXT")
	public String meta; // JS script with functions meta-info

	@Column
	public Integer sizeLimit = 10000;

	@Column
	public Integer total;

	@Column(nullable = false, updatable = false)
	public LocalDateTime created = LocalDateTime.now();

	@Column(nullable = false)
	public LocalDateTime updated = LocalDateTime.now();

	@Column(columnDefinition = "TEXT")
	private String error;

	@Enumerated(EnumType.STRING)
	@Column(name = "source_status", nullable = false)
	private ConfigStatus sourceStatus = ConfigStatus.UNKNOWN;

	// Getters and Setters
	public String getError() {
		return error;
	}

	public void setError(String error) {
		if (error != null) {
			sourceStatus = ConfigStatus.ERROR;
		}
		this.error = error == null ? null : error.substring(0, Math.min(error.length(), 255));
	}

	public ConfigStatus getSourceStatus() {
		return sourceStatus;
	}

	public void setSourceStatus(ConfigStatus status) {
		if (ConfigStatus.OK.equals(sourceStatus)) {
			error = null;
		}
		this.sourceStatus = status;
	}
}
