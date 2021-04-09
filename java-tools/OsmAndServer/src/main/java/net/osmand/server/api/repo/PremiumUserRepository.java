package net.osmand.server.api.repo;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import net.osmand.server.api.repo.PremiumUserRepository.PremiumUser;

@Repository
public interface PremiumUserRepository extends JpaRepository<PremiumUser, Long> {
	
	
    boolean existsByEmail(String email);

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

        @Column(name = "regtime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date regTime;
        
        @Column(name = "tokentime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date tokenTime;
    }

    
}
