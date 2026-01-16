package net.osmand.server.ws;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;
import net.osmand.shared.gpx.primitives.WptPt;

public class UserTranslationDTO {

	public final String sessionId;
	
	public List<TranslationMessage> history = null;
	
	public List<UserLocationSharing> sharingUsers = null;
	
	public UserTranslationDTO(String sessionId) {
		this.sessionId = sessionId;
	}
	
	public void setHistory(Collection<TranslationMessage> history) {
		this.history = new ArrayList<>(history);
	}
	
	public void setSharingUsers(UserTranslation us) {
		this.sharingUsers = new ArrayList<>();
		for (TranslationSharingOptions o : us.getActiveSharers()) {
			UserLocationSharing sh = new UserLocationSharing();
			sh.expireTime = o.expireTime;
			sh.startTime = o.startTime;
			sh.nickname = o.nickname;
			Deque<WptPt> deque = us.getLocationsByUser().get(o.userId);
			if (deque != null) {
				sh.lastLocation = deque.getLast();
			}
			this.sharingUsers.add(sh);
		}
	}
	
	public static class UserLocationSharing {
		public String nickname;
		public long expireTime;
		public long startTime;
		public WptPt lastLocation;
	}
	
}
