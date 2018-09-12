package net.osmand.server.api.repo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.data.jpa.repository.JpaRepository;
import net.osmand.server.api.repo.OsmRecipientsRepository.OsmRecipient;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

public interface OsmRecipientsRepository extends JpaRepository<OsmRecipient, String> {

    @Entity
    @Table(name = "osm_recipients")
    class OsmRecipient {

        @Id
        @Column(name = "osmid")
        public String osmId;

        @Column(name = "email")
        @JsonIgnore
        public String email;

        @Column(name = "btcaddr")
        public String bitcoinAddress;

        @Column(name = "updatetime")
        public long updateTime;
    }
}
