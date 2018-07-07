package net.osmand.server.assist.data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import net.osmand.server.assist.ext.ITrackerManager;

import org.hibernate.annotations.Type;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import com.google.gson.JsonObject;

@Entity
@Table(name = "telegram_tracker_configuration")
@EntityListeners(AuditingEntityListener.class)
public class TrackerConfiguration {
	
	public static final String CHATS = "chats";
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	public long id;
	
	@Column(nullable = false, length = 100)
	public String trackerId;
	
	@Column(nullable = true, length = 200)
	public String token;
	
	@Column(nullable = false, length = 200)
	public String trackerName;
	
	@Column(name = "created_date", nullable = false, updatable = false)
	@CreatedDate
	@Temporal(TemporalType.TIMESTAMP)
	public java.util.Date createdDate;

	@Column(name = "modified_date")
	@LastModifiedDate
	@Temporal(TemporalType.TIMESTAMP)
	public java.util.Date modifiedDate;
	
	
	@Column(nullable = false)
	public long userId;
	
	@Column(nullable = false)
	public long chatId;
	
	
	@Column(name = "data", columnDefinition = "jsonb")
	@Type(type = "net.osmand.server.assist.data.JsonbType") 
	public JsonObject data = new JsonObject();

	@Transient
	public ITrackerManager mgr;

	@Override
	public String toString() {
		return "TrackerConfiguration [id=" + id + ", trackerId=" + trackerId 
				+ ", trackerName=" + trackerName + ", createdDate=" + createdDate + ", data=" + data + "]";
	}
	
	
		
}
