package net.osmand.server.ws;

import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import net.osmand.shared.gpx.primitives.WptPt;

public class UserTranslation {

	private final String sessionId;
	private final long ownerId;
	private long creationDate;
	private String password;
	
	private final Deque<TranslationSharingOptions> activeSharers = new ConcurrentLinkedDeque<>();
	
	// Users who have successfully verified password access to this translation
	private final Set<Integer> verifiedUsers = ConcurrentHashMap.newKeySet();

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

	/**
	 * Sets the password (should be BCrypt hash, not plain text).
	 * Use UserTranslationsService.setTranslationPassword() to set password from plain text.
	 * 
	 * @param password BCrypt hash of the password
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Returns the password (BCrypt hash).
	 * @return password hash or null if no password is set
	 */
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
	
	public Set<Integer> getVerifiedUsers() {
		return verifiedUsers;
	}

	public static class TranslationSharingOptions {
		public int userId;
		// Device ID to limit sharing to specific device (0 means any device for this user)
		public long deviceId;
		
		public long startTime;
		public long expireTime;

		public String nickname;
	}
}
