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
        public Long userId;

        @Column(name = "sku")
        public String sku;

        @Column(name = "purchasetoken")
        public String purchaseToken;

        @Column(name = "checktime")
        public long checkTime;
    }
}
