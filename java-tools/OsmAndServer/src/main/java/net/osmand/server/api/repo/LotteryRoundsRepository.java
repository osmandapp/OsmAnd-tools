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

import net.osmand.server.api.repo.LotteryRoundsRepository.LotteryRound;
import net.osmand.server.api.repo.LotteryRoundsRepository.LotteryUserPrimaryKey;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LotteryRoundsRepository extends JpaRepository<LotteryRound, LotteryUserPrimaryKey> {
	
	List<LotteryRound> findByMonthOrderByUpdateTimeDesc(String month);
	
    @Entity
    @Table(name = "lottery_rounds")
    @IdClass(LotteryUserPrimaryKey.class)
    class LotteryRound {

        @Id
        @Column(name = "month")
        public String month;

        @Id
        @Column(name = "roundId")
        public int roundId;

        @Column(name = "winner")
        public String winner; // comma separated
        
        @Column(name = "seed")
        public String seed;
        
        @Column(name = "selection")
        public int selection;
        
        @Column(name = "size")
        public int size;
        
        @Column(name = "participants")
        public String participants; // comma separated
        

        @Column(name = "updatetime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date updateTime;
        
        // for Rest api
        public String message;
    }

    class LotteryUserPrimaryKey implements Serializable {

		private static final long serialVersionUID = 7950645758626335237L;
		
		public String month;
        public int roundId;
        
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((month == null) ? 0 : month.hashCode());
			result = prime * result + roundId;
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			LotteryUserPrimaryKey other = (LotteryUserPrimaryKey) obj;
			if (month == null) {
				if (other.month != null)
					return false;
			} else if (!month.equals(other.month))
				return false;
			if (roundId != other.roundId)
				return false;
			return true;
		}
        
        
    }
}
