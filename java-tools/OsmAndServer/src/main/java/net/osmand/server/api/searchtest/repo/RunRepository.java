package net.osmand.server.api.searchtest.repo;

import net.osmand.server.api.searchtest.entity.Run;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface RunRepository extends JpaRepository<Run, Long> {

	@Query("SELECT j FROM Run j WHERE j.caseId = :caseId ORDER by j.updated DESC")
	Page<Run> findByCaseId(@Param("caseId") Long caseId, Pageable pageable);
}
