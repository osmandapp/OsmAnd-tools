package net.osmand.server.assist;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.osmand.server.assist.data.Device;
import net.osmand.server.assist.data.DeviceMonitor;
import net.osmand.server.assist.data.DeviceMonitor.LocationChatMessage;
import net.osmand.server.assist.data.DeviceRepository;
import net.osmand.server.assist.data.TrackerConfiguration;
import net.osmand.server.assist.ext.ITrackerManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.telegram.telegrambots.exceptions.TelegramApiException;

@Component
public class DeviceLocationManager {
	
	@Autowired
	OsmAndAssistantBot assistantBot;
	
	@Autowired
	DeviceRepository deviceRepo;
	
	private static final Log LOG = LogFactory.getLog(DeviceLocationManager.class);
	
	ConcurrentHashMap<Long, DeviceMonitor> devices = new ConcurrentHashMap<>();

	private ThreadPoolExecutor exe;
	public static final long INTERVAL_TO_UPDATE_GROUPS = 10000;
	public static final long INITIAL_TIMESTAMP_TO_DISPLAY = 120000;
	public static final Integer DEFAULT_LIVE_PERIOD = 24 * 60 * 60;
	
	
	public DeviceLocationManager() {
		this.exe = new ThreadPoolExecutor(1, 10, 1L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>());
	}
	
	public String sendLocation(String deviceId, String lat, String lon)  throws DeviceNotFoundException {
		long did = Device.getDecodedId(deviceId);
		if(!deviceRepo.existsById(did)) {
            throw new DeviceNotFoundException(); 
		}
		DeviceMonitor dm = devices.get(did);
		if (dm != null) {
			double lt = Double.NaN;
			double ln = Double.NaN;
			if (lat != null && lon != null) {
				lt = Double.parseDouble(lat);
				ln = Double.parseDouble(lon);
			}
			dm.setLocation(lt, ln);
			dm.notifyChats();
			
		}
		return "OK";
	}
	
	@ResponseStatus(value = HttpStatus.NOT_FOUND)
	public static class DeviceNotFoundException extends RuntimeException {

		private static final long serialVersionUID = 8429349261883069752L;

		
	}

	public boolean isLocationMonitored(Device d, Long chatId) {
		DeviceMonitor dm = devices.get(d.id);
		if(dm != null) {
			return dm.isLocationMonitored(chatId);
		}
		return false;
	}
	

	public void startMonitoringLocation(Device d, Long chatId) throws TelegramApiException {
		DeviceMonitor dm = devices.get(d.id);
		if(dm == null) {
			dm = new DeviceMonitor(d, assistantBot);
			devices.put(d.id, dm);
		}
		LocationChatMessage lm = dm.getOrCreateLocationChat(chatId);
		lm.enable();
		lm.sendMessage();
	}

	public void stopMonitoringLocation(Device d, Long chatId) {
		DeviceMonitor dm = devices.get(d.id);
		if(dm != null) {
			LocationChatMessage lm = dm.getLocationChat(chatId);
			if(lm != null) {
				lm.disable();
			}
		}		
	}
	
	

	@Scheduled(fixedRate = 15 * 1000)
	public void updateExternalMonitoring() {
		// could be compare TrackerConfiguration by trackerId & token - to avoid duplication
		Map<TrackerConfiguration, Map<String, List<DeviceMonitor>>> dms = new TreeMap<>(
				TrackerConfiguration.getGlobalUniqueComparator());
		for (DeviceMonitor dm : devices.values()) {
			TrackerConfiguration ext = dm.getDevice().externalConfiguration;
			if (ext != null && dm.isOneChatMonitored()) {
				Map<String, List<DeviceMonitor>> mp = dms.get(ext);
				if (mp == null) {
					mp = new HashMap<>();
					dms.put(ext, mp);
				}
				if (!mp.containsKey(dm.getDevice().externalId)) {
					List<DeviceMonitor> lst = new ArrayList<>();
					lst.add(dm);
					mp.put(dm.getDevice().externalId, lst);
				} else {
					mp.get(dm.getDevice().externalId).add(dm);
				}
			}
		}
		Iterator<Entry<TrackerConfiguration, Map<String, List<DeviceMonitor>>>> it = dms.entrySet().iterator();
		// long timestamp = System.currentTimeMillis();
		while (it.hasNext()) {
			Entry<TrackerConfiguration, Map<String, List<DeviceMonitor> > > en = it.next();
			// if(timestamp - locGroup.updateTime > INTERVAL_TO_UPDATE_GROUPS) {
			TaskToUpdateLocationGroup task = new TaskToUpdateLocationGroup(en.getKey(), en.getValue());
			if (!exe.getQueue().contains(task)) {
				exe.submit(task);
			}
		}

	}
	
	
	public class TaskToUpdateLocationGroup implements Runnable {

		
		private ITrackerManager mgr;
		private TrackerConfiguration ext;
		private Map<String, List<DeviceMonitor>> mp;

		public TaskToUpdateLocationGroup(TrackerConfiguration ext, Map<String, List<DeviceMonitor>> mp) {
			this.ext = ext;
			this.mp = mp;
			mgr = assistantBot.getTrackerManagerById(ext.trackerId);
		}
		
		@Override
		public void run() {
			if(mgr == null) {
				return;
			}
			try {
				mgr.updateDeviceMonitors(ext, mp);
			} catch (Exception e) {
				LOG.error(e.getMessage() ,e);
			}
		}


		@Override
		public boolean equals(Object obj) {
			if (this == obj || (obj instanceof TaskToUpdateLocationGroup)){
				TrackerConfiguration.getGlobalUniqueComparator().compare(ext, ((TaskToUpdateLocationGroup)obj).ext);
					
				return true;
			}
			return false;
		}
		
		@Override
		public int hashCode() {
			return ext == null ? 0 : ext.hashCodeGlobalUnique();
		}

	}
	
	
	
	
	
}
