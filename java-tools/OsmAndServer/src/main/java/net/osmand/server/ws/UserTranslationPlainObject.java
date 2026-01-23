package net.osmand.server.ws;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;
import net.osmand.shared.gpx.primitives.WptPt;

public class UserTranslationPlainObject {

	public final String id;
	
	public List<TranslationMessage> history = null;
	
	public List<SharingLocation> shareLocations = null;
	
	public UserTranslationPlainObject(String id) {
		this.id = id;
	}
	
	public void setHistory(Collection<TranslationMessage> history) {
		this.history = new ArrayList<TranslationMessage>(history);
	}
	
	public void setShareLocations(UserTranslation us) {
		this.shareLocations = new ArrayList<>();
		for (TranslationSharingOptions o : us.getSharingOptions()) {
			SharingLocation sh = new SharingLocation();
			sh.expireTime = o.expireTime;
			sh.startTime = o.startTime;
			sh.nickname = o.nickname;
			Deque<WptPt> deque = us.getLocations().get(o.userId);
			if (deque != null) {
				sh.lastLocation = deque.getLast();
			}
			this.shareLocations.add(sh);
		}
	}
	
	public static class SharingLocation {
		public String nickname;
		public long expireTime;
		public long startTime;
		public WptPt lastLocation;
	}
	
}
