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
@Table(name = "test_case")
public class TestCase extends Config {
	public enum Status {
		NEW, GENERATED, INVALID, FAILED, RUNNING, COMPLETED, CANCELED
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

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	public Status status;

	@CreationTimestamp
	public Timestamp created;

	@UpdateTimestamp
	public Timestamp updated;

	public String getError() {
		return error;
	}

	public void setError(String error) {
		if (error != null) {
			status = TestCase.Status.FAILED;
		}
		this.error = error;
	}
}
