package net.osmand.server.api.searchtest.repo;

import net.osmand.server.api.searchtest.entity.EvalJob;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface DatasetJobRepository extends JpaRepository<EvalJob, Long> {
    @Query("SELECT j FROM EvalJob j WHERE j.datasetId = :datasetId")
    Page<EvalJob> findByDatasetId(@Param("datasetId") Long datasetId, Pageable pageable);

    Page<EvalJob> findByDatasetIdOrderByIdDesc(Long datasetId, Pageable pageable);

    @Query(value = "SELECT * FROM eval_job WHERE dataset_id = :datasetId ORDER BY id DESC LIMIT 1", nativeQuery = true)
    Optional<EvalJob> findTopByDatasetIdOrderByIdDesc(@Param("datasetId") Long datasetId);
}
