package net.osmand.server.api.repo;

import java.util.Date;
import java.util.List;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import net.osmand.server.api.repo.PremiumUsersRepository.PremiumUser;

@Repository
public interface PremiumUsersRepository extends JpaRepository<PremiumUser, Long> {
	
	
	PremiumUser findByEmail(String email);
    
    int deleteByEmail(String email);
	
	PremiumUser findByOrderid(String orderid);
    
    List<PremiumUser> findPremiumUsersByOrderidAndTokendevice(String orderid, String tokendevice);
	
	PremiumUser findById(int id);
	
    @Entity
    @Table(name = "user_accounts")
    class PremiumUser {

        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        public int id;

        @Column(name = "email")
        public String email;
        
        @Column(name = "token")
        public String token;
        
        @Column(name = "tokendevice")
        public String tokendevice;
        
        @Column(name = "orderid")
        public String orderid;

        @Column(name = "regtime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date regTime;
        
        @Column(name = "tokentime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date tokenTime;
        
    }

    
}
