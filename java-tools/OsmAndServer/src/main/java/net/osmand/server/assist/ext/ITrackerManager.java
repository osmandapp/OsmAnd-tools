package net.osmand.server.assist.ext;

import java.util.List;

import net.osmand.server.assist.data.TrackerConfiguration;

public interface ITrackerManager {

	public String getTrackerId();

	public void init(TrackerConfiguration ext);
	
	public List<? extends DeviceInfo> getDevicesList(TrackerConfiguration config);
	
	public interface DeviceInfo {
		
		public String getName();
		
		public String getId();
	}
	
}
