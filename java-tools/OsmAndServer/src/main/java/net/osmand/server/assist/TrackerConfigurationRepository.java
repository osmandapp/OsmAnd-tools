package net.osmand.server.assist;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TrackerConfigurationRepository extends JpaRepository<TrackerConfiguration, Long> {

}
