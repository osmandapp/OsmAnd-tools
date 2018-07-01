package net.osmand.server.monitor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "telegram_monitoring")
// @EntityListeners(AuditingEntityListener.class)
public class MonitoringChatId {
	
	@Id
	//@GeneratedValue(strategy = GenerationType.IDENTITY)
	public Long id;

	@Column(nullable = true)
	public String firstName;
	
	@Column(nullable = true)
	public String lastName;

	@Column(nullable = true)
	public Long userId;

	@Override
	public String toString() {
		return "MonitoringChatId [id=" + id + ", firstName=" + firstName + ", lastName=" + lastName + ", userId="
				+ userId + "]";
	}
	
}
