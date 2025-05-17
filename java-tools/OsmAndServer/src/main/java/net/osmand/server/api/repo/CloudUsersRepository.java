package net.osmand.server.api.repo;

import java.io.Serial;
import java.io.Serializable;
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
import org.springframework.data.domain.Pageable;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;

@Repository
public interface CloudUsersRepository extends JpaRepository<CloudUser, Long> {
	
	
	CloudUser findByEmailIgnoreCase(String email);
    
    int deleteByEmailIgnoreCase(String email);
	
	CloudUser findByOrderid(String orderid);

    List<CloudUser> findByEmailStartingWith(String prefix, Pageable pageable);
    
    List<CloudUser> findUsersByOrderidAndTokendevice(String orderid, String tokendevice);
	
	CloudUser findById(int id);
	
    @Entity
    @Table(name = "user_accounts")
    class CloudUser implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        public int id;

        @Column(name = "email")
        public String email;

        @Column(name = "nickname")
        public String nickname;
        
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
