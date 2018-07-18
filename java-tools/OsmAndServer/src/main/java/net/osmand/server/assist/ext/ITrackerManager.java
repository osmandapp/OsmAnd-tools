package net.osmand.server.assist.ext;

import java.util.List;
import java.util.Map;

import net.osmand.server.assist.data.Device;
import net.osmand.server.assist.data.TrackerConfiguration;

public interface ITrackerManager {

	public String getTrackerId();

	public void init(TrackerConfiguration ext);
	
	public List<? extends DeviceInfo> getDevicesList(TrackerConfiguration config);
	
	
	public interface DeviceInfo {
		
		public String getName();
		
		public String getId();
	}


	public void updateDeviceMonitors(TrackerConfiguration ext, Map<String, List<Device>> mp);
	
}
