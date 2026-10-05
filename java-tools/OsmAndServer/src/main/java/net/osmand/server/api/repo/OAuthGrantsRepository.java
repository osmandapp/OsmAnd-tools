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

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import net.osmand.server.api.repo.OAuthGrantsRepository.OAuthGrant;

// One row per connection of an OAuth client to a user account: authorization code, then access + refresh token.
// Only SHA-256 hashes of code and tokens are stored.
@Repository
public interface OAuthGrantsRepository extends JpaRepository<OAuthGrant, Integer> {

	OAuthGrant findByCodehash(String codehash);

	OAuthGrant findByAccesshash(String accesshash);

	OAuthGrant findByRefreshhash(String refreshhash);

	List<OAuthGrant> findByUserid(int userid);

	// only this column: saving the whole entity could bring back a scope change or a revoked grant
	@Transactional
	@Modifying
	@Query(value = "UPDATE user_oauth_grants SET lastusetime = :time WHERE id = :id", nativeQuery = true)
	void updateLastUseTime(int id, Date time);

	@Entity
	@Table(name = "user_oauth_grants")
	class OAuthGrant implements Serializable {
		@Serial
		private static final long serialVersionUID = 1L;

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public int id;

		@Column(name = "userid")
		public int userid;

		@Column(name = "clientid")
		public String clientid;

		@Column(name = "scope")
		public String scope;

		@Column(name = "resource")
		public String resource;

		@Column(name = "redirecturi")
		public String redirecturi;

		@Column(name = "codehash")
		public String codehash;

		@Column(name = "codechallenge")
		public String codechallenge;

		// null once the code was exchanged
		@Column(name = "codeexpire")
		@Temporal(TemporalType.TIMESTAMP)
		public Date codeexpire;

		@Column(name = "accesshash")
		public String accesshash;

		@Column(name = "accessexpire")
		@Temporal(TemporalType.TIMESTAMP)
		public Date accessexpire;

		@Column(name = "refreshhash")
		public String refreshhash;

		@Column(name = "refreshexpire")
		@Temporal(TemporalType.TIMESTAMP)
		public Date refreshexpire;

		@Column(name = "createtime")
		@Temporal(TemporalType.TIMESTAMP)
		public Date createtime;

		@Column(name = "lastusetime")
		@Temporal(TemporalType.TIMESTAMP)
		public Date lastusetime;
	}
}
