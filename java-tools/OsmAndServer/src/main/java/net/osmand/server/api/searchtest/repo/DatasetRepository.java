package net.osmand.server.api.searchtest.repo;

import net.osmand.server.api.searchtest.entity.Dataset;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface DatasetRepository extends JpaRepository<Dataset, Long> {
    @Query("SELECT d FROM Dataset d WHERE d.name = :name")
    Optional<Dataset> findByName(@Param("name") String name);

    @Query(value = "SELECT d.*, ej.status as last_job_status, ej.error as last_job_error FROM dataset d " +
            "LEFT JOIN (SELECT dataset_id, MAX(id) as max_id FROM eval_job GROUP BY dataset_id) as latest_job ON d.id = latest_job.dataset_id " +
            "LEFT JOIN eval_job ej ON latest_job.max_id = ej.id " +
            "WHERE (COALESCE(:search, '') = '' OR lower(d.name) LIKE '%' || lower(:search) || '%' OR lower(d.source) LIKE '%' || lower(:search) || '%') " +
            "AND (COALESCE(:status, '') = '' OR ej.status = :status)",
            countQuery = "SELECT count(d.id) FROM dataset d " +
                    "LEFT JOIN (SELECT dataset_id, MAX(id) as max_id FROM eval_job GROUP BY dataset_id) as latest_job ON d.id = latest_job.dataset_id " +
                    "LEFT JOIN eval_job ej ON latest_job.max_id = ej.id " +
                    "WHERE (COALESCE(:search, '') = '' OR lower(d.name) LIKE '%' || lower(:search) || '%' OR lower(d.source) LIKE '%' || lower(:search) || '%') " +
                    "AND (COALESCE(:status, '') = '' OR ej.status = :status)",
            nativeQuery = true)
    Page<Dataset> findAllDatasets(@Param("search") String search, @Param("status") String status, Pageable pageable);
}
