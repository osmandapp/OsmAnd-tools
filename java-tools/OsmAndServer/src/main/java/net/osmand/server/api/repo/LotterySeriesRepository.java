package net.osmand.server.api.repo;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import net.osmand.server.api.repo.LotteryRoundsRepository.LotteryRound;
import net.osmand.server.api.repo.LotterySeriesRepository.LotterySeries;
import net.osmand.util.Algorithms;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LotterySeriesRepository extends JpaRepository<LotterySeries, String> {

	List<LotterySeries> findAllByOrderByUpdateTimeDesc();
	
	enum LotteryStatus {
		NOTPUBLIC,
		DRAFT,
		PREPARE,
		RUNNING,
		FINISHED
	}

    @Entity
    @Table(name = "lottery_series")
    class LotterySeries {

    	@Id
        @Column(name = "name")
        public String name;
    	
        @Column(name = "promocodes")
        public String promocodes;
    	
        @Column(name = "usedpromos")
        public String usedPromos;
        
        @Column(name = "rounds")
        public int rounds;
        
        @Column(name = "status")
        @Enumerated(EnumType.STRING)
        public LotteryStatus status;
        
        @Column(name = "type")
        public String type;
        
        @Column(name = "emailtemplate")
        public String emailTemplate;

        @Column(name = "updatetime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date updateTime;

        @Transient
		public List<LotteryRound> roundObjects = new ArrayList<>();
        
        public int promocodesSize() {
        	if(promocodes != null && promocodes.length() > 0) {
        		return promocodes.split(",").length;
        	}
        	return 0;
        }
        
        public int usedPromocodes() {
        	if(usedPromos != null && usedPromos.length() > 0) {
        		return usedPromos.split(",").length;
        	}
        	return 0;
        }
        
        public void usePromo(String p) {
        	if(Algorithms.isEmpty(usedPromos)) {
        		usedPromos = p;
        	} else {
        		usedPromos += "," + p;
        	}
        }

		public boolean isOpenForRegistration() {
			return status == LotteryStatus.RUNNING || status == LotteryStatus.PREPARE;
		}

		public Set<String> getNonUsedPromos() {
			TreeSet<String> s = new TreeSet<String>();
			if(promocodes != null && promocodes.length() > 0) {
				for(String p : promocodes.split(",")) {
					s.add(p);
				}
				
			}
			if(usedPromos != null && usedPromos.length() > 0) {
				for(String p : usedPromos.split(",")) {
					s.remove(p);
				}
				
			}
			return s;
			
		}
        
    }
}
