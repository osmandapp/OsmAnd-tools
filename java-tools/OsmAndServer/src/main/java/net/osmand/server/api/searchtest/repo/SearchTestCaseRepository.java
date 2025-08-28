package net.osmand.server.api.searchtest.repo;

import jakarta.persistence.*;
import net.osmand.server.SearchTestRepository;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.Optional;

@SearchTestRepository
public interface SearchTestCaseRepository extends JpaRepository<TestCase, Long> {
	@MappedSuperclass
	public class RunParam {
		@Column
		public String locale;

		@Column(name = "base_search")
		public Boolean baseSearch;

		@Column()
		public Double lat;

		@Column()
		public Double lon;

		@Column(name = "north_west")
		private String northWest;

		@Column(name = "south_east")
		private String southEast;

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

	Optional<TestCase> findByName(@Param("name") String name);

	@Query(value = "SELECT * FROM test_case WHERE dataset_id = :datasetId ORDER BY updated DESC", nativeQuery = true)
	Page<TestCase> findByDatasetIdOrderByIdDesc(Long datasetId, Pageable pageable);

	@Query(value = "SELECT * FROM test_case WHERE dataset_id = :datasetId AND status = :status ORDER BY id DESC",
            nativeQuery = true)
	Page<TestCase> findByDatasetIdAndStatusOrderByIdDesc(Long datasetId, TestCase.Status status, Pageable pageable);

	@Query(value = "SELECT * FROM test_case WHERE dataset_id = :datasetId ORDER BY id DESC LIMIT 1", nativeQuery =
            true)
	Optional<TestCase> findTopByDatasetIdOrderByIdDesc(@Param("datasetId") Long datasetId);

	@Query(value = "SELECT * FROM test_case j " +
			"WHERE (COALESCE(:name, '') = '' OR lower(j.name) LIKE '%' || lower(:name) || '%') " +
			"AND (COALESCE(:labels, '') = '' OR lower(j.labels) LIKE '%' || lower(:labels) || '%') " +
			"ORDER BY updated DESC",
			countQuery = "SELECT count(j.id) FROM test_case j " +
					"WHERE (COALESCE(:name, '') = '' OR lower(j.name) LIKE '%' || lower(:name) || '%') " +
					"AND (COALESCE(:labels, '') = '' OR lower(j.labels) LIKE '%' || lower(:labels) || '%') ",
			nativeQuery = true)
	Page<TestCase> findAllCasesFiltered(@Param("name") String name,
										@Param("labels") String labels,
										Pageable pageable);
}
