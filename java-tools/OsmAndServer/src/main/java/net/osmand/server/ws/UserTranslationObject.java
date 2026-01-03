package net.osmand.server.ws;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;
import net.osmand.shared.gpx.primitives.WptPt;

public class UserTranslationObject {

	public final String id;
	
	List<TranslationMessage> history = null;
	
	List<SharingLocation> shareLocations = null;
	
	public UserTranslationObject(String id) {
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
		}
	}
	
	static class SharingLocation {
		String nickname;
		long expireTime;
		long startTime;
		WptPt lastLocation;
	}
	
}
