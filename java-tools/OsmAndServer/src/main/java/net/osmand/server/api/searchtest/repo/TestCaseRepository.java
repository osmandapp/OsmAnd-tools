package net.osmand.server.api.searchtest.repo;

import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.entity.TestCase;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface TestCaseRepository extends JpaRepository<TestCase, Long> {
    @Query("SELECT d FROM TestCase d WHERE d.name = :name")
    Optional<TestCase> findByName(@Param("name") String name);

    @Query("SELECT j FROM TestCase j WHERE j.datasetId = :datasetId")
    Page<TestCase> findByDatasetId(@Param("datasetId") Long datasetId, Pageable pageable);

    Page<TestCase> findByDatasetIdOrderByIdDesc(Long datasetId, Pageable pageable);

    @Query(value = "SELECT * FROM eval_job WHERE dataset_id = :datasetId ORDER BY id DESC LIMIT 1", nativeQuery = true)
    Optional<TestCase> findTopByDatasetIdOrderByIdDesc(@Param("datasetId") Long datasetId);
}
