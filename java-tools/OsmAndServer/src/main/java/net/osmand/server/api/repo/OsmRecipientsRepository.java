package net.osmand.server.api.repo;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import net.osmand.server.api.repo.OsmRecipientsRepository.OsmRecipient;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;


// This needs to be refactored cause data source has cnaged to changeset
@Deprecated
public interface OsmRecipientsRepository extends JpaRepository<OsmRecipient, String> {
    
    @Transactional
    void deleteOsmRecipientByEmail(String email);
    
    @Transactional
    void deleteOsmRecipientByOsmId(String osmid);
    
    OsmRecipient getOsmRecipientByOsmId(@Param("osmid") String osmId);

    @Entity
    @Table(name = "osm_recipients")
    class OsmRecipient {

        @Id
        @Column(name = "osmid")
        public String osmId;

        @Column(name = "email")
        public String email;

        @Column(name = "btcaddr")
        public String bitcoinAddress;

        @Column(name = "updatetime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date updateTime;
    }
}
