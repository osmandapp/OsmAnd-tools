package net.osmand.server.api.repo;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SupportersRepository extends JpaRepository<SupportersRepository.Supporter, Long> {

    Optional<Supporter> findByUserId(Long userId);

    Optional<Supporter> findByUserEmail(String userEmail);

    Optional<Supporter> findTopByOrderIdIn(Collection<String> orderIds);

    @Entity
    @Table(name = "supporters")
    class Supporter implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        @Id
        @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "supporters_seq")
        @SequenceGenerator(sequenceName = "supporters_seq", allocationSize = 1, name = "supporters_seq")
        @Column(name = "userid")
        public Long userId;

        @Column(name = "token")
        public String token;

        @Column(name = "visiblename")
        public String visibleName;

        @Column(name = "useremail")
        public String userEmail;

        @Column(name = "preferred_region")
        public String preferredRegion;
        
        @Column(name = "os")
        public String os;
        
        @Column(name = "orderid")
        public String orderId;

    }
}
