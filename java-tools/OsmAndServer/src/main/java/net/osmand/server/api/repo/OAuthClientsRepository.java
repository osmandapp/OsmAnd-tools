package net.osmand.server.api.repo;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import net.osmand.server.api.repo.OAuthClientsRepository.OAuthClient;

// OAuth clients registered dynamically (RFC 7591), e.g. Claude or ChatGPT connecting to /mcp
@Repository
public interface OAuthClientsRepository extends JpaRepository<OAuthClient, String> {

	OAuthClient findByClientid(String clientid);

	@Entity
	@Table(name = "oauth_clients")
	class OAuthClient implements Serializable {
		@Serial
		private static final long serialVersionUID = 1L;

		@Id
		@Column(name = "clientid")
		public String clientid;

		@Column(name = "clientname")
		public String clientname;

		// space separated
		@Column(name = "redirecturis")
		public String redirecturis;

		// null for public clients (PKCE only)
		@Column(name = "secrethash")
		public String secrethash;

		@Column(name = "createtime")
		@Temporal(TemporalType.TIMESTAMP)
		public Date createtime;
	}
}
