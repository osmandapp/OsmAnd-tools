package net.osmand.server.api.searchtest.repo;

import jakarta.persistence.*;
import net.osmand.server.SearchTestRepository;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.RunParam;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository.Dataset;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;

@SearchTestRepository
public interface SearchTestRunRepository extends JpaRepository<Run, Long> {
	@Entity(name = "Run")
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

		// Relationships (read-only mappings to parent entities)
		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "case_id", referencedColumnName = "id", insertable = false, updatable = false)
		public TestCase testCase;

		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "dataset_id", referencedColumnName = "id", insertable = false, updatable = false)
		public Dataset dataset;

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

	@Query(value = "SELECT j FROM Run j JOIN FETCH j.testCase c JOIN FETCH j.dataset d WHERE j.caseId = :caseId ORDER BY j.updated DESC, j.id DESC",
			countQuery = "SELECT COUNT(j) FROM Run j WHERE j.caseId = :caseId")
	Page<Run> findByCaseId(@Param("caseId") Long caseId, Pageable pageable);

	@Query(value = "SELECT DISTINCT j FROM Run j " +
			"JOIN FETCH j.testCase c " +
			"JOIN FETCH j.dataset d " +
			"WHERE ( (:name IS NULL OR :name = '') " +
			"        OR LOWER(j.name) LIKE CONCAT('%', LOWER(:name), '%') " +
			"        OR LOWER(c.name) LIKE CONCAT('%', LOWER(:name), '%') " +
			"        OR LOWER(d.name) LIKE CONCAT('%', LOWER(:name), '%') ) " +
			"AND  ( (:labels IS NULL OR :labels = '') " +
			"        OR LOWER(COALESCE(c.labels, '')) LIKE CONCAT('%', LOWER(:labels), '%') " +
			"        OR LOWER(COALESCE(d.labels, '')) LIKE CONCAT('%', LOWER(:labels), '%') ) " +
			"ORDER BY j.updated DESC, j.id DESC",
			countQuery = "SELECT COUNT(j) FROM Run j " +
					"JOIN j.testCase c " +
					"JOIN j.dataset d " +
					"WHERE ( (:name IS NULL OR :name = '') " +
					"        OR LOWER(j.name) LIKE CONCAT('%', LOWER(:name), '%') " +
					"        OR LOWER(c.name) LIKE CONCAT('%', LOWER(:name), '%') " +
					"        OR LOWER(d.name) LIKE CONCAT('%', LOWER(:name), '%') ) " +
					"AND  ( (:labels IS NULL OR :labels = '') " +
					"        OR LOWER(COALESCE(c.labels, '')) LIKE CONCAT('%', LOWER(:labels), '%') " +
					"        OR LOWER(COALESCE(d.labels, '')) LIKE CONCAT('%', LOWER(:labels), '%') ) ")
	Page<Run> findFiltered(@Param("name") String name,
			@Param("labels") String labels,
			Pageable pageable);

	@Query("SELECT MAX(j.id) FROM Run j WHERE j.caseId = :caseId")
	Long findLastRunId(@Param("caseId") Long caseId);
}
