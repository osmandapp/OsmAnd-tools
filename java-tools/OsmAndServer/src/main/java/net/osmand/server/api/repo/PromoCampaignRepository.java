package net.osmand.server.api.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import jakarta.persistence.*;
import java.util.Date;
import java.util.List;

@Repository
public interface PromoCampaignRepository extends JpaRepository<PromoCampaignRepository.Promo, String> {
    
    @Entity
    @Table(name = "promo_campaigns")
    class Promo {
        
        @Id
        @Column(name = "name")
        public String name;
        
        @Column(name = "starttime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date startTime;
        
        @Column(name = "endtime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date endTime;
        
        @Column(name = "subactivemonths")
        public int subActiveMonths;
        
        @Column(name = "numberlimit")
        public int numberLimit;
        
        @Column(name = "used")
        public int used;
    
        @Column(name = "lastusers")
        public String lastUsers;
    }
    
    List<PromoCampaignRepository.Promo> findAllByOrderByStartTimeDesc();
    
    PromoCampaignRepository.Promo findByName(String name);
}
