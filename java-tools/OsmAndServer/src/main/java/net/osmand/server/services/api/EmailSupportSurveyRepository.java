package net.osmand.server.services.api;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import net.osmand.server.services.api.EmailSupportSurveyRepository.EmailSupportSurveyFeedback;
import net.osmand.server.services.api.EmailSupportSurveyRepository.EmailSupportSurveyFeedbackKey;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EmailSupportSurveyRepository extends JpaRepository<EmailSupportSurveyFeedback, EmailSupportSurveyFeedbackKey> {

	@Entity
	@Table(name = "email_support_survey")
	@IdClass(EmailSupportSurveyFeedbackKey.class)
	public class EmailSupportSurveyFeedback {
	
		@Id
		@Column(nullable = false, length = 100)
		public String ip;
		
		@Column(nullable = false, length = 100)
		public String response;
		
		@Id
		@Column(nullable = false)
		@Temporal(TemporalType.TIMESTAMP)
		public Date timestamp;
		
	}
	
	public class EmailSupportSurveyFeedbackKey implements Serializable {
		private static final long serialVersionUID = 1192225751597358995L;
		public String ip;
		public Date timestamp;
	}


}
