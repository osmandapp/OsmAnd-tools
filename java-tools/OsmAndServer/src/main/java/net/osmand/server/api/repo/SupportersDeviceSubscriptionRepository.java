package net.osmand.server.api.repo;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.transaction.Transactional;

import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository.SupporterDeviceSubscription;
import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository.SupporterDeviceSubscriptionPrimaryKey;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

public interface SupportersDeviceSubscriptionRepository extends JpaRepository<SupporterDeviceSubscription, SupporterDeviceSubscriptionPrimaryKey> {

    @Transactional
    @Modifying
    @Query(value = "INSERT INTO supporters_device_sub(userid, sku, purchasetoken, timestamp)\n" +
            "      SELECT ?1, ?2, ?3, to_timestamp(?4)\n" +
            "      WHERE NOT EXISTS (SELECT 1 FROM supporters_device_sub\n" +
            "             WHERE userid = ?1 AND sku = ?2 AND purchasetoken = ?3)", nativeQuery = true)
    int createSupporterDeviceSubscriptionIfNotExists(Long userId, String sku, String purchaseToken, long timestamp);

    @Entity
    @Table(name = "supporters_device_sub")
    @IdClass(SupporterDeviceSubscriptionPrimaryKey.class)
    public class SupporterDeviceSubscription {

        @Id
        @Column(name = "userid")
        public String userId;

        @Id
        @Column(name = "sku")
        public String sku;

        @Id
        @Column(name = "purchasetoken")
        public String purchaseToken;

        @Column(name = "timestamp")
        @Temporal(TemporalType.TIMESTAMP)
        public Date timestamp;
		
    }

    class SupporterDeviceSubscriptionPrimaryKey implements Serializable {
        private static final long serialVersionUID = 7941117922381685104L;

        public String userId;
        public String sku;
        public String purchaseToken;
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((purchaseToken == null) ? 0 : purchaseToken.hashCode());
			result = prime * result + ((sku == null) ? 0 : sku.hashCode());
			result = prime * result + ((userId == null) ? 0 : userId.hashCode());
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			SupporterDeviceSubscriptionPrimaryKey other = (SupporterDeviceSubscriptionPrimaryKey) obj;
			if (purchaseToken == null) {
				if (other.purchaseToken != null) {
					return false;
				}
			} else if (!purchaseToken.equals(other.purchaseToken)) {
				return false;
			}
			if (sku == null) {
				if (other.sku != null) {
					return false;
				}
			} else if (!sku.equals(other.sku)) {
				return false;
			}
			if (userId == null) {
				if (other.userId != null) {
					return false;
				}
			} else if (!userId.equals(other.userId)) {
				return false;
			}
			return true;
		}

    }
}
