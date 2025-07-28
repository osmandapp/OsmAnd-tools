package net.osmand.server.api.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import net.osmand.server.api.services.TestSearchService.JobStatus;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.sql.Timestamp;

@Entity
@Table(name = "dataset_job")
public class DatasetJob {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "job_id")
	private Long jobId;

	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "dataset_id", nullable = false)
	private Dataset dataset;

	@Column(name = "address_expression", nullable = false)
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

	@Column
	private String error;

	public Long getJobId() {
		return jobId;
	}

	public void setJobId(Long jobId) {
		this.jobId = jobId;
	}

	public Dataset getDataset() {
		return dataset;
	}

	public void setDataset(Dataset dataset) {
		this.dataset = dataset;
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

	public void setNorthWest(String northWest) {
		this.northWest = northWest;
	}

	public String getSouthEast() {
		return southEast;
	}

	public void setSouthEast(String southEast) {
		this.southEast = southEast;
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
