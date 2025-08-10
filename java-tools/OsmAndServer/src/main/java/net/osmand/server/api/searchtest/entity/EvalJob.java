package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.sql.Timestamp;

@Entity
@Table(name = "eval_job")
public class EvalJob extends Config {
	public enum Status {
		NEW, RUNNING, CANCELED, FAILED, COMPLETED
	}

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(columnDefinition = "INTEGER")
	public Long id;

	@Column(name = "dataset_id", nullable = false)
	public Long datasetId;

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

	@CreationTimestamp
	public Timestamp created;

	@UpdateTimestamp
	public Timestamp updated;

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
