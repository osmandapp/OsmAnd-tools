package net.osmand.server.api.searchtest.repo;

import net.osmand.server.api.searchtest.entity.Run;
import net.osmand.server.api.searchtest.entity.TestCase;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

public interface RunRepository extends JpaRepository<Run, Long> {

    @Query("SELECT j FROM Run j WHERE j.caseId = :caseId ORDER by j.updated DESC")
    Page<Run> findByCaseId(@Param("caseId") Long caseId, Pageable pageable);

    @Query(value = "SELECT * FROM run WHERE case_id = :caseId ORDER BY id", nativeQuery = true)
    Page<Run> findByCaseIdOrderById(Long caseId, Pageable pageable);

    @Query(value = "SELECT * FROM run WHERE case_id = :caseId AND status = :status ORDER BY id", nativeQuery = true)
    Page<Run> findByCaseIdAndStatusOrderById(Long caseId, TestCase.Status status, Pageable pageable);

}
