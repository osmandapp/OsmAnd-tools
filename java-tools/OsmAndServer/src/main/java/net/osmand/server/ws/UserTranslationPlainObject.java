package net.osmand.server.ws;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;

// Serializable snapshot of a translation sent to the client (load reply / METADATA).
public class UserTranslationPlainObject {

	public final String id;
	public long ownerUserId;
	public long creationDate;

	public List<TranslationMessage> history = null;     // past messages in the requested time range

	public List<SharingLocation> shareLocations = null; // who is currently sharing
	public List<String> viewers = null;                 // current viewer nicknames
	public List<ShareRequest> pendingRequests = null;   // owner-only: requests awaiting approval

	public UserTranslationPlainObject(String id) {
		this.id = id;
	}

	public void setPendingRequests(Map<Integer, String> pending) {
		this.pendingRequests = new ArrayList<>();
		for (Map.Entry<Integer, String> e : pending.entrySet()) {
			ShareRequest r = new ShareRequest();
			r.userId = e.getKey();
			r.nickname = e.getValue();
			this.pendingRequests.add(r);
		}
	}

	public static class ShareRequest {
		public int userId;
		public String nickname;
	}

	public void setHistory(Collection<TranslationMessage> history) {
		this.history = new ArrayList<>(history);
	}

	// Broadcast variant: no card is marked as "mine".
	public void setShareLocations(UserTranslation us) {
		setShareLocations(us, 0);
	}

	// Personal variant (load): marks the viewer's own card with mine = true.
	public void setShareLocations(UserTranslation us, int viewerUserId) {
		this.shareLocations = new ArrayList<>();
		for (TranslationSharingOptions o : us.getSharingOptions()) {
			SharingLocation sh = new SharingLocation();
			sh.expireTime = o.expireTime;
			sh.startTime = o.startTime;
			sh.nickname = o.nickname;
			sh.owner = o.userId == us.getOwner();
			if (viewerUserId != 0 && o.userId == viewerUserId) {
				sh.mine = Boolean.TRUE;
			}
			this.shareLocations.add(sh);
		}
	}
	
	public static class SharingLocation {
		public String nickname;
		public long expireTime;
		public long startTime;
		public boolean owner;
		public Boolean mine;   // set only in the personal load reply for the viewer's own card
	}
	
}
