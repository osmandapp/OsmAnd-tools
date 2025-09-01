package net.osmand.server.api.searchtest.repo;

import jakarta.persistence.*;
import net.osmand.server.SearchTestRepository;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.RunParam;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;

@SearchTestRepository
public interface SearchTestRunRepository extends JpaRepository<Run, Long> {
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

	@Entity
	@Table(name = "gen_result")
	public class GenResult extends Result {
	}

	@Entity
	@Table(name = "run_result")
	public class RunResult extends Result {

		@Column(name = "run_id", nullable = false)
		public Long runId;

		@Column(name = "gen_id", nullable = false)
		public Long genId;

		@Column(name = "min_distance")
		public Integer minDistance;

		@Column(name = "closest_result", length = 512)
		public String closestResult;

		@Column(name = "actual_place")
		public Integer actualPlace;

		@Column(name = "results_count")
		public Integer resultsCount;
	}

	@Query("SELECT j FROM SearchTestRunRepository$Run j WHERE j.caseId = :caseId ORDER by j.updated DESC")
	Page<Run> findByCaseId(@Param("caseId") Long caseId, Pageable pageable);
}
