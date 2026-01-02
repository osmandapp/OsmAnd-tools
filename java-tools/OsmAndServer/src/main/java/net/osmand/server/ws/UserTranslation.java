package net.osmand.server.ws;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import net.osmand.shared.gpx.primitives.WptPt;

public class UserTranslation {

	private final String id;
	private final long owner;
	private long creationDate;
	private String password;
	
	
	private Deque<TranslationMessage> messages = new ConcurrentLinkedDeque<>();
	private Map<String, Deque<WptPt>> locations = new ConcurrentHashMap<>();
	
	public UserTranslation(String id, long ownerId) {
		this.id = id;
		this.owner = ownerId;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public String getPassword() {
		return password;
	}
	
	public String getId() {
		return id;
	}
	
	public long getCreationDate() {
		return creationDate;
	}
	
	public void setCreationDate(long creationDate) {
		this.creationDate = creationDate;
	}
	
	public long getOwner() {
		return owner;
	}
	
	public Deque<TranslationMessage> getMessages() {
		return messages;
	}
	
	public Map<String, Deque<WptPt>> getLocations() {
		return locations;
	}
	
}
