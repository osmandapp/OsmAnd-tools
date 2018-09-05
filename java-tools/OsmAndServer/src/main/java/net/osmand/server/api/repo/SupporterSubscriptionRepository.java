package net.osmand.server.api.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

public interface SupporterSubscriptionRepository extends JpaRepository<SupporterSubscriptionRepository.SupporterSubscription, String> {

    @Entity
    @Table(name = "supporters_subscription")
    class SupporterSubscription {

        @Id
        @Column(name = "userid")
        private String userId;

        @Column(name = "sku")
        private String sku;

        @Column(name = "purchasetoken")
        private String purchaseToken;

        @Column(name = "time")
        private Date time;

        @Column(name = "checktime")
        private long checkTime;

        @Column(name = "autorenewing")
        private String autorenewing;

        @Column(name = "starttime")
        private long startTime;

        @Column(name = "expiretime")
        private long expireTime;

        @Column(name = "kind")
        private String kind;

        public SupporterSubscription() {}

        public SupporterSubscription(String userId, String sku, String purchaseToken, Date time, long checkTime,
                                     String autorenewing, long startTime, long expireTime, String kind) {
            this.userId = userId;
            this.sku = sku;
            this.purchaseToken = purchaseToken;
            this.time = time;
            this.checkTime = checkTime;
            this.autorenewing = autorenewing;
            this.startTime = startTime;
            this.expireTime = expireTime;
            this.kind = kind;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
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

        public Date getTime() {
            return time;
        }

        public void setTime(Date time) {
            this.time = time;
        }

        public long getCheckTime() {
            return checkTime;
        }

        public void setCheckTime(long checkTime) {
            this.checkTime = checkTime;
        }

        public String getAutorenewing() {
            return autorenewing;
        }

        public void setAutorenewing(String autorenewing) {
            this.autorenewing = autorenewing;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getExpireTime() {
            return expireTime;
        }

        public void setExpireTime(long expireTime) {
            this.expireTime = expireTime;
        }

        public String getKind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }
    }
}
