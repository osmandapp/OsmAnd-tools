package net.osmand.server.api.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import net.osmand.server.api.repo.OsmRecipientsRepository.OsmRecipient;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

public interface OsmRecipientsRepository extends JpaRepository<OsmRecipient, String> {

    @Entity
    @Table(name = "osm_recipients")
    class OsmRecipient {

        @Column(name = "osmid")
        private String osmId;

        @Column(name = "email")
        private String email;

        @Column(name = "btcaddr")
        private String bitcoinAddress;

        @Column(name = "updatetime")
        private long updateTime;

        protected OsmRecipient() {}

        public OsmRecipient(String osmId, String email, String bitcoinAddress, long updateTime) {
            this.osmId = osmId;
            this.email = email;
            this.bitcoinAddress = bitcoinAddress;
            this.updateTime = updateTime;
        }

        public String getOsmId() {
            return osmId;
        }

        public void setOsmId(String osmId) {
            this.osmId = osmId;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getBitcoinAddress() {
            return bitcoinAddress;
        }

        public void setBitcoinAddress(String bitcoinAddress) {
            this.bitcoinAddress = bitcoinAddress;
        }

        public long getUpdateTime() {
            return updateTime;
        }

        public void setUpdateTime(long updateTime) {
            this.updateTime = updateTime;
        }
    }
}
