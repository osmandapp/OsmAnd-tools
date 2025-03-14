package net.osmand.server.monitor;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

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
