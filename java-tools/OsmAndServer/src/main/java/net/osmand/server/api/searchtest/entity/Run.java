package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "run")
public class Run extends RunParam {
	public enum Status {
		NEW, RUNNING, COMPLETED, CANCELED, FAILED
	}

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(columnDefinition = "INTEGER")
	public Long id;

	@Column(name = "case_id", nullable = false)
	public Long caseId;

	@Column(name = "dataset_id", nullable = false)
	public Long datasetId;

	@CreationTimestamp
	public LocalDateTime timestamp;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	public Status status;


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
}
