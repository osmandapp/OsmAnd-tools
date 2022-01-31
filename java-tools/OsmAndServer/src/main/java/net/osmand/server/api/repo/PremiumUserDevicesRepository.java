package net.osmand.server.api.repo;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import net.osmand.server.api.repo.PremiumUserDevicesRepository.PremiumUserDevice;

@Repository
public interface PremiumUserDevicesRepository extends JpaRepository<PremiumUserDevice, Long> {
	
	
	PremiumUserDevice findById(int id);
	
	PremiumUserDevice findTopByUseridAndDeviceidOrderByUpdatetimeDesc(int userid, String deviceid);


    @Entity
    @Table(name = "user_account_devices")
    class PremiumUserDevice {

        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        public int id;

        @Column(name = "userid")
        public int userid;
        
        @Column(name = "deviceid")
        public String deviceid;
        
        
        @Column(name = "accesstoken")
        public String accesstoken;

        @Column(name = "udpatetime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date udpatetime;
        
    }

    
}
