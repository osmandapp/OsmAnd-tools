package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "run")
public class Run {
	public enum Status {
		NEW, RUNNING, COMPLETED, CANCELED, FAILED
	}

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(columnDefinition = "INTEGER")
	public Long id;

	@Column(name = "case_id", nullable = false)
	public Long caseId;

	@CreationTimestamp
	public LocalDateTime timestamp;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	public Status status;

	@Column
	public String locale;

	@Column(name = "base_search")
	public Boolean baseSearch;

	@Column(name = "north_west")
	private String northWest;

	@Column(name = "south_east")
	private String southEast;

	@Column()
	public Double lat;

	@Column()
	public Double lon;

	@CreationTimestamp
	public LocalDateTime created;

	@UpdateTimestamp
	public LocalDateTime updated;

	@Column(columnDefinition = "TEXT")
	private String error;

	public String getError() {
		return error;
	}

	public void setError(String error) {
		if (error != null) {
			status = Status.FAILED;
		}
		this.error = error == null ? null : error.substring(0, 256);
	}

	public String getNorthWest() {
		return northWest;
	}

	public void setNorthWest(String val) {
		this.northWest = val != null && val.trim().isEmpty() ? null : val;
	}

	public String getSouthEast() {
		return southEast;
	}

	public void setSouthEast(String val) {
		this.southEast = val != null && val.trim().isEmpty() ? null : val;
	}
}
