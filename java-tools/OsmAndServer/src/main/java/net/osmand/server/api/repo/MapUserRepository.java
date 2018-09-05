package net.osmand.server.api.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

public interface MapUserRepository extends JpaRepository<MapUserRepository.MapUser, String> {

    @Entity
    @Table(name = "map_users")
    class MapUser {

        @Id
        @Column(name = "aid")
        private String aid;

        @Column(name = "email")
        private String email;

        @Column(name = "updatetime")
        private long updateTime;

        public MapUser() {}

        public MapUser(String email, String aid, long updateTime) {
            this.email = email;
            this.aid = aid;
            this.updateTime = updateTime;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getAid() {
            return aid;
        }

        public void setAid(String aid) {
            this.aid = aid;
        }

        public long getUpdateTime() {
            return updateTime;
        }

        public void setUpdateTime(long updateTime) {
            this.updateTime = updateTime;
        }
    }
}
