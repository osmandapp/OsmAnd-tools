package net.osmand.server.api.repo;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.springframework.data.jpa.repository.JpaRepository;

import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscriptionPrimaryKey;

public interface DeviceSubscriptionsRepository extends JpaRepository<SupporterDeviceSubscription, SupporterDeviceSubscriptionPrimaryKey> {


	List<SupporterDeviceSubscription> findByOrderId(String orderId);

	// PRIMARY KEY is (orderId + SKU) or (purchaseToken + SKU), orderId could be restored from purchaseToken and sku
	@Entity
    @Table(name = "supporters_device_sub")
    @IdClass(SupporterDeviceSubscriptionPrimaryKey.class)
	public class SupporterDeviceSubscription {

		@Id
		@Column(name = "sku")
		public String sku;

		@Id
		@Column(name = "orderid")
		public String orderId;

		@Column(name = "purchasetoken")
		public String purchaseToken;


		@Column(name = "payload")
		public String payload;

		@Column(name = "timestamp")
		@Temporal(TemporalType.TIMESTAMP)
		public Date timestamp;

		@Column(name = "starttime")
		@Temporal(TemporalType.TIMESTAMP)
		public Date starttime;

		@Column(name = "expiretime")
		@Temporal(TemporalType.TIMESTAMP)
		public Date expiretime;

		@Column(name = "checktime")
		@Temporal(TemporalType.TIMESTAMP)
		public Date checktime;

		@Column(name = "autorenewing")
		public Boolean autorenewing;

		@Column(name = "paymentstate")
		public Integer paymentstate;

		@Column(name = "valid")
		public Boolean valid;

		@Column(name = "kind")
		public String kind;

		@Column(name = "prevvalidpurchasetoken")
		public String prevvalidpurchasetoken;

		@Column(name = "introcycles")
		public Integer introcycles ;
	}

	public class SupporterDeviceSubscriptionPrimaryKey implements Serializable {
		private static final long serialVersionUID = 7941117922381685104L;

		public String sku;
		public String orderId;

		public SupporterDeviceSubscriptionPrimaryKey() {
		}

		public SupporterDeviceSubscriptionPrimaryKey(String sku, String orderId) {
			super();
			this.sku = sku;
			this.orderId = orderId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((orderId == null) ? 0 : orderId.hashCode());
			result = prime * result + ((sku == null) ? 0 : sku.hashCode());
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
			if (orderId == null) {
				if (other.orderId != null) {
					return false;
				}
			} else if (!orderId.equals(other.orderId)) {
				return false;
			}
			if (sku == null) {
				if (other.sku != null) {
					return false;
				}
			} else if (!sku.equals(other.sku)) {
				return false;
			}
			return true;
		}

	}
}
