package net.osmand.server.repo;
import net.osmand.server.model.MonitoringChatId;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MonitoringChatRepository extends JpaRepository<MonitoringChatId, Long> {

}
