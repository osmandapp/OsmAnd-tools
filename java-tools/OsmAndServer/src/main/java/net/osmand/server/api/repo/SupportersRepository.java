package net.osmand.server.api.repo;

import java.util.Optional;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SupportersRepository extends JpaRepository<SupportersRepository.Supporter, Long> {

    Optional<Supporter> findByUserEmail(String userEmail);

    @Entity
    @Table(name = "supporters")
    class Supporter {

        @Id
        @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "supporters_seq")
        @SequenceGenerator(sequenceName = "supporters_seq", allocationSize = 1, name = "supporters_seq")
        @Column(name = "userid")
        public String userId;

        @Column(name = "token")
        public String token;

        @Column(name = "visiblename")
        public String visibleName;

        @Column(name = "useremail")
        public String userEmail;

        @Column(name = "preferred_region")
        public String preferredRegion;

    }
}
