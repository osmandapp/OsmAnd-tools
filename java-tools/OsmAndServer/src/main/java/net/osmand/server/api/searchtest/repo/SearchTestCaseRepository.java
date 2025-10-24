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

		// if 'Search Around' = 'Global'        then average = true, (lat, lon) == null, shift == null
		// if 'Search Around' = 'Custom'        then average = null, (lat, lon) != null, shift == null
		// if 'Search Around' = 'Individual'    then average = null, (lat, lon) == null, shift >= 0
		@Column()
		public Boolean average;
		
		@Column()
		public Double lat;

		@Column()
		public Double lon;

		@Column()
		public Integer shift;


		@Column(name = "north_west")
		private String northWest;

		@Column(name = "south_east")
		private String southEast;

		@Column(name = "rerun_id")
		public Long rerunId;

		@Column(name = "skip_found")
		public Boolean skipFound;

		// branch
		@Column()
		public String name;

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

	@Entity(name = "TestCase")
	@Table(name = "test_case")
	public class TestCase extends RunParam {
		public enum Status {
			NEW, GENERATED, FAILED
		}

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		@Column(columnDefinition = "INTEGER")
		public Long id;


		// Latest Run id for this test-case (computed in service layer)
		@Transient
		public Long lastRunId;

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
		public LocalDateTime created;

		@UpdateTimestamp
		public LocalDateTime updated;

		@JdbcTypeCode(SqlTypes.JSON)
		@Column(name = "all_cols", columnDefinition = "TEXT")
		public String allCols; // column names as JSON string array

		@JdbcTypeCode(SqlTypes.JSON)
		@Column(name = "sel_cols", columnDefinition = "TEXT")
		public String selCols; // selected column names as JSON string array

		@JdbcTypeCode(SqlTypes.JSON)
		@Column(name = "prog_cfg", columnDefinition = "TEXT")
		public String progCfg; // @ProgrammaticConfig as JSON

		@JdbcTypeCode(SqlTypes.JSON)
		@Column(name = "nocode_cfg", columnDefinition = "TEXT")
		public String nocodeCfg; // @Tuple[] as JSON

		@JdbcTypeCode(SqlTypes.JSON)
		@Column(name = "test_row", columnDefinition = "TEXT")
		public String testRow; // @Map<String, String> as JSON

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

	@Query(value = "SELECT j FROM TestCase j WHERE j.datasetId = :datasetId ORDER BY j.updated DESC")
	Page<TestCase> findByDatasetIdOrderByIdDesc(@Param("datasetId") Long datasetId, Pageable pageable);

	@Query(value = "SELECT j FROM TestCase j WHERE j.datasetId = :datasetId AND j.status = :status ORDER BY j.id DESC")
	Page<TestCase> findByDatasetIdAndStatusOrderByIdDesc(@Param("datasetId") Long datasetId, @Param("status") TestCase.Status status, Pageable pageable);

	Optional<TestCase> findTopByDatasetIdOrderByIdDesc(@Param("datasetId") Long datasetId);

	@Query("SELECT j FROM TestCase j " +
			"WHERE (:name IS NULL OR :name = '' OR LOWER(j.name) LIKE LOWER(CONCAT('%', :name, '%'))) " +
			"AND (:labels IS NULL OR :labels = '' OR LOWER(COALESCE(j.labels, '')) LIKE LOWER(CONCAT('%', :labels, '%'))) " +
			"ORDER BY j.updated DESC")
	Page<TestCase> findAllCasesFiltered(@Param("name") String name,
										@Param("labels") String labels,
										Pageable pageable);
}
