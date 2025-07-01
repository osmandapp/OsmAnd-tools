package net.osmand.server.api.repo;

import jakarta.persistence.*;
import org.springframework.data.jpa.repository.JpaRepository;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static net.osmand.server.api.repo.DeviceInAppPurchasesRepository.*;

public interface DeviceInAppPurchasesRepository extends JpaRepository<SupporterDeviceInAppPurchase, SupporterDeviceInAppPurchasePrimaryKey> {

    List<SupporterDeviceInAppPurchase> findByUserId(Integer userId);

    List<SupporterDeviceInAppPurchase> findByOrderId(String orderId);

    List<SupporterDeviceInAppPurchase> findByOrderIdAndSku(String orderId, String sku);

    List<SupporterDeviceInAppPurchase> findByUserIdAndValidTrue(Integer userId);

    List<SupporterDeviceInAppPurchase> findBySupporterId(Long supporterId);

    List<SupporterDeviceInAppPurchase> findBySupporterIdAndValidTrue(Long supporterId);

    @Entity
    @Table(name = "supporters_device_iap")
    @IdClass(SupporterDeviceInAppPurchasePrimaryKey.class)
    public class SupporterDeviceInAppPurchase implements Serializable {
        @Serial
        private static final long serialVersionUID = 2L; // Different from subscription

        @Id
        @Column(name = "sku", nullable = false)
        public String sku;

        @Id
        @Column(name = "orderid", nullable = false)
        public String orderId; // Google: orderId, Apple: transaction_id

        @Column(name = "purchasetoken")
        public String purchaseToken;

        @Column(name = "purchase_time")
        @Temporal(TemporalType.TIMESTAMP)
        public Date purchaseTime;

        @Column(name = "checktime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date checktime; // Last time checked with platform API

        @Column(name = "valid")
        public Boolean valid; // Is the purchase currently considered valid by platform API?

        @Column(name = "userid")
        public Integer userId; // Link to user_accounts

        @Column(name = "supporterid")
        public Long supporterId; // Link to supporters

        @Column(name = "timestamp") // When record was created/updated in *our* DB
        @Temporal(TemporalType.TIMESTAMP)
        public Date timestamp;
    }

    // --- Primary Key Class ---
    public class SupporterDeviceInAppPurchasePrimaryKey implements Serializable {
        @Serial
        private static final long serialVersionUID = 3L;

        public String sku;
        public String orderId;

        public SupporterDeviceInAppPurchasePrimaryKey() {}

        public SupporterDeviceInAppPurchasePrimaryKey(String sku, String orderId) {
            this.sku = sku;
            this.orderId = orderId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SupporterDeviceInAppPurchasePrimaryKey that = (SupporterDeviceInAppPurchasePrimaryKey) o;
            return Objects.equals(sku, that.sku) && Objects.equals(orderId, that.orderId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sku, orderId);
        }
    }
}
