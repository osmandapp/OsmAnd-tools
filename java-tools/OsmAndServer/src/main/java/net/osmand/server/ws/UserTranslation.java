package net.osmand.server.ws;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import net.osmand.shared.gpx.primitives.WptPt;

public class UserTranslation {

	private final String sessionId;
	private final long ownerId;
	private long creationDate;
	private String password;
	
	private final Deque<TranslationSharingOptions> activeSharers = new ConcurrentLinkedDeque<>();

	private final Deque<TranslationMessage> messages = new ConcurrentLinkedDeque<>();
	private final Map<Integer, Deque<WptPt>> locationsByUser = new ConcurrentHashMap<>();

	public UserTranslation(String sessionId, long ownerId) {
		this.sessionId = sessionId;
		this.ownerId = ownerId;
	}

	public void sendLocation(int userid, WptPt wptPt) {
		Deque<WptPt> locations = locationsByUser.get(userid);
		if (locations == null) {
			locationsByUser.putIfAbsent(userid, new ConcurrentLinkedDeque<>());
			locations = locationsByUser.get(userid);
		}
		locations.push(wptPt);
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getPassword() {
		return password;
	}

	public String getSessionId() {
		return sessionId;
	}

	public long getCreationDate() {
		return creationDate;
	}

	public Deque<TranslationSharingOptions> getActiveSharers() {
		return activeSharers;
	}

	public void setCreationDate(long creationDate) {
		this.creationDate = creationDate;
	}

	public long getOwnerId() {
		return ownerId;
	}

	public Deque<TranslationMessage> getMessages() {
		return messages;
	}

	public Map<Integer, Deque<WptPt>> getLocationsByUser() {
		return locationsByUser;
	}

	public static class TranslationSharingOptions {
		public int userId;
//		public long deviceId; // possibly limit to device id
		
		public long startTime;
		public long expireTime;

		public String nickname;
	}

	
	
}
