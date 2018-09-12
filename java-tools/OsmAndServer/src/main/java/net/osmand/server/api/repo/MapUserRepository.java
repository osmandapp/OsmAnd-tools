package net.osmand.server.api.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import net.osmand.server.api.repo.MapUserRepository.MapUser;
import net.osmand.server.api.repo.MapUserRepository.MapUserPrimaryKey;

import javax.persistence.*;

import java.io.Serializable;

@Repository
public interface MapUserRepository extends JpaRepository<MapUser, MapUserPrimaryKey> {

    @Entity
    @Table(name = "map_users")
    @IdClass(MapUserPrimaryKey.class)
    class MapUser {

        @Id
        @Column(name = "aid")
        public String aid;

        @Id
        @Column(name = "email")
        public String email;

        @Column(name = "updatetime")
        public long updateTime;
    }

    class MapUserPrimaryKey implements Serializable {
        private static final long serialVersionUID = -6244205107567456100L;

        public String aid;
        public String email;
    }
}
