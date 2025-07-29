package net.osmand.server.api.repo;

import net.osmand.server.api.entity.EvalJob;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DatasetJobRepository extends JpaRepository<EvalJob, Long> {
    Page<EvalJob> findByDatasetId(Long datasetId, Pageable pageable);
}
