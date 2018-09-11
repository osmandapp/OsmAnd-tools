package net.osmand.server.api.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import net.osmand.server.api.repo.MapUserRepository.MapUser;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

public interface MapUserRepository extends JpaRepository<MapUser, String> {

    @Entity
    @Table(name = "map_users")
    class MapUser {

        @Id
        @Column(name = "aid")
        public String aid;

        @Column(name = "email")
        public String email;

        @Column(name = "updatetime")
        public long updateTime;
    }
}
