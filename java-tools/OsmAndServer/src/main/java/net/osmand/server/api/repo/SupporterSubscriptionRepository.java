package net.osmand.server.api.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import net.osmand.server.api.repo.SupporterSubscriptionRepository.SupporterSubscription;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

public interface SupporterSubscriptionRepository extends JpaRepository<SupporterSubscription, Long> {

    @Entity
    @Table(name = "supporters_subscription")
    class SupporterSubscription {

        @Id
        @Column(name = "userid")
        private Long userId;

        @Column(name = "sku")
        private String sku;

        @Column(name = "purchasetoken")
        private String purchaseToken;

        @Column(name = "checktime")
        private long checkTime;

        public SupporterSubscription() {}

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public String getSku() {
            return sku;
        }

        public void setSku(String sku) {
            this.sku = sku;
        }

        public String getPurchaseToken() {
            return purchaseToken;
        }

        public void setPurchaseToken(String purchaseToken) {
            this.purchaseToken = purchaseToken;
        }

        public long getCheckTime() {
            return checkTime;
        }

        public void setCheckTime(long checkTime) {
            this.checkTime = checkTime;
        }
    }
}
