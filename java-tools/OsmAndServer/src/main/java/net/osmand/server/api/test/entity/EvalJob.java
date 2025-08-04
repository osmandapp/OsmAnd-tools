package net.osmand.server.api.test.entity;

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
public class EvalJob {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "dataset_id", nullable = false)
	private Long datasetId;

	@Column(name = "address_expression", nullable = false, columnDefinition = "TEXT")
	private String addressExpression;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	private JobStatus status;

	@Column
	private String locale;

	@Column(name = "base_search")
	private Boolean baseSearch;

	@Column(name = "north_west")
	private String northWest;

	@Column(name = "south_east")
	private String southEast;

	@CreationTimestamp
	private Timestamp created;

	@UpdateTimestamp
	private Timestamp updated;

	@Column(columnDefinition = "TEXT")
	private String error;

	public Long getId() {
		return id;
	}

	public void setId(Long jobId) {
		this.id = jobId;
	}

	public Long getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(Long datasetId) {
		this.datasetId = datasetId;
	}

	public String getAddressExpression() {
		return addressExpression;
	}

	public void setAddressExpression(String addressExpression) {
		this.addressExpression = addressExpression;
	}

	public JobStatus getStatus() {
		return status;
	}

	public void setStatus(JobStatus status) {
		this.status = status;
	}

	public String getLocale() {
		return locale;
	}

	public void setLocale(String locale) {
		this.locale = locale;
	}

	public Boolean getBaseSearch() {
		return baseSearch;
	}

	public void setBaseSearch(Boolean baseSearch) {
		this.baseSearch = baseSearch;
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

	public Timestamp getCreated() {
		return created;
	}

	public void setCreated(Timestamp created) {
		this.created = created;
	}

	public Timestamp getUpdated() {
		return updated;
	}

	public void setUpdated(Timestamp updated) {
		this.updated = updated;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}
}
