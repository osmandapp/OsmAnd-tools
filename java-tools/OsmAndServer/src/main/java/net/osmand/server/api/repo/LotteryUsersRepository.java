package net.osmand.server.api.repo;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import net.osmand.server.api.repo.LotteryUsersRepository.LotteryUser;
import net.osmand.server.api.repo.LotteryUsersRepository.LotteryUserPrimaryKey;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LotteryUsersRepository extends JpaRepository<LotteryUser, LotteryUserPrimaryKey> {

	
	List<LotteryUser> findBySeriesOrderByUpdateTime(String series);
	
	
	List<LotteryUser> findBySeriesAndHashcodeOrderByUpdateTime(String series, String hashcode);
	
    @Entity
    @Table(name = "lottery_users")
    @IdClass(LotteryUserPrimaryKey.class)
    class LotteryUser {

        @Column(name = "ip")
        public String ip;

        @Id
        @Column(name = "email")
        public String email;
        
        @Id
        @Column(name = "series")
        public String series;
        
        @Column(name = "promocode")
        public String promocode;
        
        @Column(name = "hashcode")
        public String hashcode;
        
        @Column(name = "round")
        public Integer roundId;
        
        @Column(name = "sent")
        public boolean sent;

        @Column(name = "updatetime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date updateTime;

        @Transient
		public String message;
        
        @Transient
		public String status;
    }

    class LotteryUserPrimaryKey implements Serializable {

		private static final long serialVersionUID = 1560021253924781406L;
		public String series;
        public String email;
        
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((series == null) ? 0 : series.hashCode());
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
			LotteryUserPrimaryKey other = (LotteryUserPrimaryKey) obj;
			if (series == null) {
				if (other.series != null) {
					return false;
				}
			} else if (!series.equals(other.series)) {
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
