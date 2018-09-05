package net.osmand.server.api.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

public interface SupportersRepository extends JpaRepository<SupportersRepository.Supporter, String> {

    @Entity
    @Table(name = "supporters")
    class Supporter {

        @Id
        @Column(name = "userid")
        private String userId;

        @Column(name = "token")
        private String token;

        @Column(name = "visiblename")
        private String visbleName;

        @Column(name = "useremail")
        private String userEmail;

        @Column(name = "prefered_region")
        private String preferedRegion;

        @Column(name = "disable")
        private int disabled;

        public Supporter() {}

        public Supporter(String userId, String token, String visbleName, String userEmail, String preferedRegion,
                         int disabled) {
            this.userId = userId;
            this.token = token;
            this.visbleName = visbleName;
            this.userEmail = userEmail;
            this.preferedRegion = preferedRegion;
            this.disabled = disabled;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public String getVisbleName() {
            return visbleName;
        }

        public void setVisbleName(String visbleName) {
            this.visbleName = visbleName;
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
                    ", visbleName='" + visbleName + '\'' +
                    ", userEmail='" + userEmail + '\'' +
                    ", preferedRegion='" + preferedRegion + '\'' +
                    ", disabled=" + disabled +
                    '}';
        }
    }
}
