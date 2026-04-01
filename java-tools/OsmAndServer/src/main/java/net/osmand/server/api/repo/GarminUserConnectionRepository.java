package net.osmand.server.api.repo;

import java.io.Serial;
import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface GarminUserConnectionRepository extends JpaRepository<GarminUserConnectionRepository.GarminUserConnection, Integer> {

	GarminUserConnection findByUserid(int userid);

	GarminUserConnection findByGarminUserId(String garminUserId);

	@Entity
	@Table(
			name = "garmin_user_connection",
			uniqueConstraints = @UniqueConstraint(name = "uk_garmin_connection_garmin_user", columnNames = "garmin_user_id")
	)
	class GarminUserConnection implements Serializable {
		@Serial
		private static final long serialVersionUID = 1L;

		@Id
		@Column(name = "userid", nullable = false)
		public int userid;

		@Column(name = "garmin_user_id", nullable = false, length = 128)
		public String garminUserId;

		@Column(name = "access_token", nullable = false, columnDefinition = "TEXT")
		public String accessToken;

		@Column(name = "refresh_token", columnDefinition = "TEXT")
		public String refreshToken;

		@Column(name = "access_expires_time", nullable = false)
		public long accessExpiresTime;
	}
}
