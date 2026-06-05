package net.osmand.server.ws;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;

public class UserTranslationPlainObject {

	public final String id;
	public long ownerUserId;
	public long creationDate;

	public List<TranslationMessage> history = null;

	public List<SharingLocation> shareLocations = null;

	public UserTranslationPlainObject(String id) {
		this.id = id;
	}
	
	public void setHistory(Collection<TranslationMessage> history) {
		this.history = new ArrayList<>(history);
	}
	
	public void setShareLocations(UserTranslation us) {
		this.shareLocations = new ArrayList<>();
		for (TranslationSharingOptions o : us.getSharingOptions()) {
			SharingLocation sh = new SharingLocation();
			sh.expireTime = o.expireTime;
			sh.startTime = o.startTime;
			sh.nickname = o.nickname;
			this.shareLocations.add(sh);
		}
	}
	
	public static class SharingLocation {
		public String nickname;
		public long expireTime;
		public long startTime;
	}
	
}
