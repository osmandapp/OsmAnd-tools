package net.osmand.server.api.searchtest.repo;

import jakarta.persistence.*;
import net.osmand.server.SearchTestRepository;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository.Dataset;

import java.time.LocalDateTime;
import java.util.Optional;

@SearchTestRepository
public interface SearchTestDatasetRepository extends JpaRepository<Dataset, Long> {
	@Entity
	@Table(name = "name_set")
	public class NameSet {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		@Column(columnDefinition = "INTEGER")
		public Long id;

		@Column(nullable = false, unique = true)
		public String name;

		@Column()
		public String data;
	}

	@Entity
	@Table(name = "dataset")
	public class Dataset {
		public enum ConfigStatus {
			UNKNOWN, OK, ERROR
		}

		public enum Source {
			CSV, Overpass
		}

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		@Column(columnDefinition = "INTEGER")
		public Long id;

		@Column(nullable = false, unique = true)
		public String name;

		@Column()
		public String labels;

		@Enumerated(EnumType.STRING)
		@Column(nullable = false)
		public Source type; // e.g., "Overpass", "CSV"

		@Column(nullable = false, columnDefinition = "TEXT")
		public String source; // Overpass query or file path

		@JdbcTypeCode(SqlTypes.JSON)
		@Column(name = "all_cols", columnDefinition = "TEXT")
		public String allCols;

		@JdbcTypeCode(SqlTypes.JSON)
		@Column(name = "sel_cols", columnDefinition = "TEXT")
		public String selCols; // selected column names as JSON string array

		@JdbcTypeCode(SqlTypes.JSON)
		@Column(name = "test_row", columnDefinition = "TEXT")
		public String testRow; // @Map<String, String> as JSON

		@Column
		public Integer sizeLimit = 10000;

		@Column
		public Integer total;

		@Column(nullable = false, updatable = false)
		public LocalDateTime created = LocalDateTime.now();

		@Column(nullable = false)
		public LocalDateTime updated = LocalDateTime.now();

		@Column(columnDefinition = "TEXT")
		private String error;

		@Enumerated(EnumType.STRING)
		@Column(name = "source_status", nullable = false)
		private ConfigStatus sourceStatus = ConfigStatus.UNKNOWN;

		// Getters and Setters
		public String getError() {
			return error;
		}

		public void setError(String error) {
			if (error != null) {
				sourceStatus = ConfigStatus.ERROR;
			}
			this.error = error == null ? null : error.substring(0, Math.min(error.length(), 255));
		}

		public ConfigStatus getSourceStatus() {
			return sourceStatus;
		}

		public void setSourceStatus(ConfigStatus status) {
			if (ConfigStatus.OK.equals(sourceStatus)) {
				error = null;
			}
			this.sourceStatus = status;
		}
	}

	@Query("SELECT d FROM SearchTestDatasetRepository$Dataset d WHERE d.name = :name")
	Optional<Dataset> findByName(@Param("name") String name);

	@Query(value = "SELECT d.* FROM dataset d " +
			"WHERE (COALESCE(:name, '') = '' " +
			"       OR lower(d.name)   LIKE '%' || lower(:name) || '%' " +
			"       OR lower(d.source) LIKE '%' || lower(:name) || '%') " +
			"  AND (COALESCE(:labels, '') = '' " +
			"       OR lower(COALESCE(d.labels, '')) LIKE '%' || lower(:labels) || '%') " +
			"ORDER BY d.updated DESC",
			countQuery = "SELECT count(d.id) FROM dataset d " +
					"WHERE (COALESCE(:name, '') = '' " +
					"       OR lower(d.name)   LIKE '%' || lower(:name) || '%' " +
					"       OR lower(d.source) LIKE '%' || lower(:name) || '%') " +
					"  AND (COALESCE(:labels, '') = '' " +
					"       OR lower(COALESCE(d.labels, '')) LIKE '%' || lower(:labels) || '%' )",
			nativeQuery = true)
	Page<Dataset> findAllDatasets(@Param("name") String name, @Param("labels") String labels, Pageable pageable);
}
