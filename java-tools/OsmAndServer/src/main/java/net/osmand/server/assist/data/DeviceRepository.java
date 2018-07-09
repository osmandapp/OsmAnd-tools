package net.osmand.server.assist.data;
import java.util.List;

import javax.transaction.Transactional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Repository
@EnableTransactionManagement
public interface DeviceRepository extends JpaRepository<Device, Long> {
	
	List<Device> findByUserIdOrderByCreatedDate(long userId);
	
	
	@Transactional
	void deleteAllByExternalConfiguration(TrackerConfiguration cfg);


	boolean isLocationMonitored(Device d, Long chatId);

}
