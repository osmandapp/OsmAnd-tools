package net.osmand.server.api.repo;

import java.io.Serial;
import java.io.Serializable;
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

import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;

@Repository
public interface CloudUserDevicesRepository extends JpaRepository<CloudUserDevice, Long> {
    CloudUserDevice findById(int id);

    CloudUserDevice findTopByUseridAndDeviceidOrderByUdpatetimeDesc(int userid, String deviceid);
    CloudUserDevice findTopByUseridAndDeviceidAndModelOrderByUdpatetimeDesc(int userid, String deviceid, String model);

    List<CloudUserDevice> findByUserid(int userid);
    
    List<CloudUserDevice> findByUseridAndDeviceid(int userid, String deviceid);
    
    CloudUserDevice findByAccesstoken(String accesstoken);

    int deleteByUserid(int userid);

    @Entity
    @Table(name = "user_account_devices")
    class CloudUserDevice implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
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
