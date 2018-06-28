package net.osmand.server.model;

import javax.annotation.Nullable;
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

	@Nullable
	public String firstName;
	
	@Nullable
	public String lastName;
	
	@Nullable
	public Long userId;

	@Override
	public String toString() {
		return "MonitoringChatId [id=" + id + ", firstName=" + firstName + ", lastName=" + lastName + ", userId="
				+ userId + "]";
	}
	
}
