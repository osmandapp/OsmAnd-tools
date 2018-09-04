package net.osmand.server.api.repo;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.transaction.Transactional;

import net.osmand.server.api.repo.EmailUnsubscribedRepository.EmailUnsubscribed;
import net.osmand.server.api.repo.EmailUnsubscribedRepository.EmailUnsubscribedKey;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EmailUnsubscribedRepository extends JpaRepository<EmailUnsubscribed, EmailUnsubscribedKey> {

	@Transactional
	void deleteAllByEmail(String email);
	
	@Entity
	@Table(name = "email_unsubscribed")
	@IdClass(EmailUnsubscribedKey.class)
	public class EmailUnsubscribed {
	
		@Id
		@Column(nullable = false, length = 100)
		public String email;
		
		@Id
		@Column(nullable = false, length = 100)
		public String channel;
		
		@Column(nullable = true)
		public long timestamp;
		
	}
	
	public class EmailUnsubscribedKey implements Serializable {
		private static final long serialVersionUID = -175873391176388054L;
		public String email;
		public String channel;
	}


}
