package net.osmand.server.api.repo;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;

import net.osmand.server.api.repo.MapUserRepository.MapUser;
import net.osmand.server.api.repo.MapUserRepository.MapUserPrimaryKey;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MapUserRepository extends JpaRepository<MapUser, MapUserPrimaryKey> {

    @Entity
    @Table(name = "email_free_users")
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
        
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((aid == null) ? 0 : aid.hashCode());
			result = prime * result + ((email == null) ? 0 : email.hashCode());
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			MapUserPrimaryKey other = (MapUserPrimaryKey) obj;
			if (aid == null) {
				if (other.aid != null) {
					return false;
				}
			} else if (!aid.equals(other.aid)) {
				return false;
			}
			if (email == null) {
				if (other.email != null) {
					return false;
				}
			} else if (!email.equals(other.email)) {
				return false;
			}
			return true;
		}
        
    }
}
