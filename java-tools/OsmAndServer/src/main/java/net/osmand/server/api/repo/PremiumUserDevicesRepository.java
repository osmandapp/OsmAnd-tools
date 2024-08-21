package net.osmand.server.api.repo;

import java.util.Date;
import java.util.List;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import net.osmand.server.api.repo.PremiumUserDevicesRepository.PremiumUserDevice;

@Repository
public interface PremiumUserDevicesRepository extends JpaRepository<PremiumUserDevice, Long> {
	PremiumUserDevice findById(int id);

	PremiumUserDevice findTopByUseridAndDeviceidOrderByUdpatetimeDesc(int userid, String deviceid);

	List<PremiumUserDevice> findByUserid(int userid);

    int deleteByUserid(int userid);

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

        @Column(name = "lang")
        public String lang;

        @Column(name = "brand")
        public String brand;

        @Column(name = "model")
        public String model;

        // TYPO don't fix unless fixed on client side
        @Column(name = "udpatetime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date udpatetime;
    }
}
