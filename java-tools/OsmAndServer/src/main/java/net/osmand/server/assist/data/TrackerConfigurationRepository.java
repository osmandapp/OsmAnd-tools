package net.osmand.server.assist.data;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TrackerConfigurationRepository extends JpaRepository<TrackerConfiguration, Long> {

	List<TrackerConfiguration> findByUserIdOrderByCreatedDate(long userId);
	

}
