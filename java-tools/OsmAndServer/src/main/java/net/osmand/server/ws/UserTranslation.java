package net.osmand.server.ws;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

public class UserTranslation {

	private static final long MS_PER_HOUR = 60 * 60 * 1000L;
	public static final long PERMANENT_DURATION_MS = 365 * 24 * MS_PER_HOUR;

	private final String id;
	private final long owner;
	private long creationDate;
	private long durationMs = MS_PER_HOUR; // default 1 hour

	private final Deque<TranslationSharingOptions> sharingOptions = new ConcurrentLinkedDeque<>();

	// Used as in-memory fallback when Redis is unavailable.
	private final Deque<TranslationMessage> messages = new ConcurrentLinkedDeque<>();

	public UserTranslation(String id, long ownerId) {
		this.id = id;
		this.owner = ownerId;
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

	public long getDurationMs() {
		return durationMs;
	}

	public void setDurationMs(long durationMs) {
		this.durationMs = durationMs;
	}

	public long getOwner() {
		return owner;
	}

	public Deque<TranslationMessage> getMessages() {
		return messages;
	}

	public static class TranslationSharingOptions {
		public int userId;
		public long startTime;
		public long expireTime;
		public String nickname;
	}
}
