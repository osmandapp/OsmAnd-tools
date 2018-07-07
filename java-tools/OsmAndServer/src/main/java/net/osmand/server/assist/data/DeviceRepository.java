package net.osmand.server.assist.data;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DeviceRepository extends JpaRepository<Device, Long> {
	
	List<Device> findByUserIdOrderByCreatedDate(long userId);

}
