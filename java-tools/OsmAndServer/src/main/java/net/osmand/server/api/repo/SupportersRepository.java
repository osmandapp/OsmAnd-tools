package net.osmand.server.api.repo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.data.jpa.repository.JpaRepository;

import javax.persistence.*;
import java.util.Optional;

public interface SupportersRepository extends JpaRepository<SupportersRepository.Supporter, Long> {

    Optional<Supporter> findByUserEmail(String userEmail);

    @Entity(name = "Supporter")
    @Table(name = "supporters")
    class Supporter {

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

        @Column(name = "prefered_region")
        public String preferedRegion;

        @Column(name = "disable")
        @JsonIgnore
        public int disabled;
    }
}
