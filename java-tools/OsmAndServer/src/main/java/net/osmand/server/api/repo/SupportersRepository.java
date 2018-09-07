package net.osmand.server.api.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import javax.persistence.*;
import java.util.Optional;

public interface SupportersRepository extends JpaRepository<SupportersRepository.Supporter, Long> {

    Optional<Supporter> findByUserEmail(String userEmail);

    @Entity(name = "Supporter")
    @Table(name = "supporters")
    class Supporter {

        @Id @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "supporters_seq")
        @SequenceGenerator(sequenceName = "supporters_seq", allocationSize = 1, name = "supporters_seq")
        @Column(name = "userid")
        private Long userId;

        @Column(name = "token")
        private String token;

        @Column(name = "visiblename")
        private String visibleName;

        @Column(name = "useremail")
        private String userEmail;

        @Column(name = "prefered_region")
        private String preferedRegion;

        @Column(name = "disable")
        private int disabled;

        protected Supporter() {}

        public Supporter(Long userId, String token, String visibleName, String userEmail, String preferedRegion,
                         int disabled) {
            this.userId = userId;
            this.token = token;
            this.visibleName = visibleName;
            this.userEmail = userEmail;
            this.preferedRegion = preferedRegion;
            this.disabled = disabled;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public String getVisibleName() {
            return visibleName;
        }

        public void setVisibleName(String visibleName) {
            this.visibleName = visibleName;
        }

        public String getUserEmail() {
            return userEmail;
        }

        public void setUserEmail(String userEmail) {
            this.userEmail = userEmail;
        }

        public String getPreferedRegion() {
            return preferedRegion;
        }

        public void setPreferedRegion(String preferedRegion) {
            this.preferedRegion = preferedRegion;
        }

        public int getDisabled() {
            return disabled;
        }

        public void setDisabled(int disabled) {
            this.disabled = disabled;
        }

        @Override
        public String toString() {
            return "Supporter{" +
                    "userId='" + userId + '\'' +
                    ", token='" + token + '\'' +
                    ", visibleName='" + visibleName + '\'' +
                    ", userEmail='" + userEmail + '\'' +
                    ", preferedRegion='" + preferedRegion + '\'' +
                    ", disabled=" + disabled +
                    '}';
        }
    }
}
