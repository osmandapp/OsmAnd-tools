package net.osmand.server.assist;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

@Entity
@Table(name = "telegram_tracker_configuration")
// @EntityListeners(AuditingEntityListener.class)
public class TrackerConfiguration {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	public Long id;
	
	@Column(nullable = false, length = 100)
	public String trackerId;
	
	@Column(nullable = true, length = 200)
	public String token;
	
	@Column(nullable = false, length = 200)
	public String trackerName;
	
	@Column(nullable = false)
	public Long userId;
	
	@Column(nullable = false)
	public long dateCreated;

	@Column(nullable = true, length = 100)
	public String firstName;
	
	@Column(nullable = true, length = 100)
	public String lastName;
	
	
	@Lob
	@Column
	public byte[] data;


	@Override
	public String toString() {
		return "TrackerConfiguration [id=" + id + ", trackerId=" + trackerId + ", token=" + token + ", trackerName="
				+ trackerName + ", userId=" + userId + ", firstName=" + firstName + ", lastName=" + lastName + "]";
	}	

	
		
}
