package net.osmand.server.api.searchtest.repo;

import net.osmand.server.SearchTestRepository;
import net.osmand.server.api.searchtest.entity.Dataset;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@SearchTestRepository
public interface SearchTestDatasetRepository extends JpaRepository<Dataset, Long> {
	@Query("SELECT d FROM Dataset d WHERE d.name = :name")
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
