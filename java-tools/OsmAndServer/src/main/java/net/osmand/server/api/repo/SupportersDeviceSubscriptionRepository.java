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

import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository.SupporterDeviceSubscription;
import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository.SupporterDeviceSubscriptionPrimaryKey;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SupportersDeviceSubscriptionRepository extends JpaRepository<SupporterDeviceSubscription, SupporterDeviceSubscriptionPrimaryKey> {

    @Entity
    @Table(name = "supporters_device_sub")
    @IdClass(SupporterDeviceSubscriptionPrimaryKey.class)
    public class SupporterDeviceSubscription {

        @Id
        @Column(name = "userid")
        public Long userId;

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

    public class SupporterDeviceSubscriptionPrimaryKey implements Serializable {
        private static final long serialVersionUID = 7941117922381685104L;

        public Long userId;
        public String sku;
        public String purchaseToken;
        
        public SupporterDeviceSubscriptionPrimaryKey() {
        }
        
		public SupporterDeviceSubscriptionPrimaryKey(Long userId, String sku, String purchaseToken) {
			super();
			this.userId = userId;
			this.sku = sku;
			this.purchaseToken = purchaseToken;
		}

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
