package net.osmand.server.api.searchtest.repo;

import net.osmand.server.api.searchtest.entity.TestCase;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface SearchTestCaseRepository extends JpaRepository<TestCase, Long> {
	@Query("SELECT d FROM TestCase d WHERE d.name = :name")
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
