package net.osmand.server.api.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository.SupporterDeviceSubscription;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.transaction.Transactional;
import java.sql.Timestamp;

public interface SupportersDeviceSubscriptionRepository extends JpaRepository<SupporterDeviceSubscription, Long> {

    @Transactional
    @Modifying
    @Query(value = "INSERT INTO supporters_device_sub(userid, sku, purchasetoken, timestamp)\n" +
            "      SELECT ?1, ?2, ?3, ?4\n" +
            "      WHERE NOT EXISTS (SELECT 1 FROM supporters_device_sub\n" +
            "             WHERE userid = ?1 AND sku = ?2 AND purchasetoken = ?3)", nativeQuery = true)
    int createSupporterDeviceSubscriptionIfNotExists(Long userId, String sku, String purchaseToken, Timestamp timestamp);

    @Entity
    @Table(name = "supporters_device_sub")
    class SupporterDeviceSubscription {

        @Id
        @Column(name = "userid")
        public Long userId;

        @Column(name = "sku")
        public String sku;

        @Column(name = "purchasetoken")
        public String purchaseToken;

        @Column(name = "timestamp")
        public Timestamp timestamp;
    }
}
