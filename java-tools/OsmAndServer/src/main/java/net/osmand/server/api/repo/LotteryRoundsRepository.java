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

import net.osmand.server.api.repo.LotteryRoundsRepository.LotteryRound;
import net.osmand.server.api.repo.LotteryRoundsRepository.LotteryRoundPrimaryKey;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LotteryRoundsRepository extends JpaRepository<LotteryRound, LotteryRoundPrimaryKey> {
	
	List<LotteryRound> findBySeriesOrderByUpdateTimeDesc(String series);
	
    @Entity
    @Table(name = "lottery_rounds")
    @IdClass(LotteryRoundPrimaryKey.class)
    class LotteryRound {

        @Id
        @Column(name = "series")
        public String series;

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
        @Transient
        public String message;
        @Transient
        public String seedInteger;
    }

    class LotteryRoundPrimaryKey implements Serializable {

		private static final long serialVersionUID = 7950645758626335237L;
		
		public String series;
        public int roundId;
        
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((series == null) ? 0 : series.hashCode());
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
			LotteryRoundPrimaryKey other = (LotteryRoundPrimaryKey) obj;
			if (series == null) {
				if (other.series != null)
					return false;
			} else if (!series.equals(other.series))
				return false;
			if (roundId != other.roundId)
				return false;
			return true;
		}
        
        
    }
}
