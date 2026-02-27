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
	
	private Deque<TranslationSharingOptions> sharingOptions = new ConcurrentLinkedDeque<>();
	
	private Deque<TranslationMessage> messages = new ConcurrentLinkedDeque<>();
	private Map<Integer, Deque<WptPt>> locations = new ConcurrentHashMap<>();
	
	public UserTranslation(String id, long ownerId) {
		this.id = id;
		this.owner = ownerId;
	}
	
	public void sendLocation(int userid, WptPt wptPt) {
		Deque<WptPt> deque = locations.get(userid);
		if (deque == null) {
			locations.putIfAbsent(userid, new ConcurrentLinkedDeque<WptPt>());
			deque = locations.get(userid);
		}
		deque.push(wptPt);
	}
	
	public void clearLocation(int userid) {
		Deque<WptPt> deque = locations.get(userid);
		if (deque != null) {
			deque.clear();
		}
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
	
	public Deque<TranslationSharingOptions> getSharingOptions() {
		return sharingOptions;
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
	
	public Map<Integer, Deque<WptPt>> getLocations() {
		return locations;
	}
	
	public static class TranslationSharingOptions {
		public int userId;
//		public long deviceId; // possibly limit to device id
		
		public long startTime;
		public long expireTime;

		public String nickname;
	}

	
	
}
