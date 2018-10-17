package net.osmand.server.assist;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.osmand.server.assist.data.*;
import net.osmand.server.assist.ext.ITrackerManager;
import net.osmand.server.assist.ext.ITrackerManager.DeviceInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.gson.JsonParser;

@Component
public class DeviceLocationManager {
	
	@Autowired
	OsmAndAssistantBot assistantBot;
	
	@Autowired
	DeviceRepository deviceRepo;
	
	private static final int LIMIT_DEVICES_PER_USER = 200;
	public static final long INTERVAL_TO_UPDATE_GROUPS = 20000;
	public static final long INTERVAL_TO_RUN_UPDATES = 15000;
	public static final long INITIAL_TIMESTAMP_TO_DISPLAY = 120000;
	public static final Integer DEFAULT_LIVE_PERIOD = 24 * 60 * 60;
	private static final int USER_UNIQUENESS = 1 << 20;
	private static final Log LOG = LogFactory.getLog(DeviceLocationManager.class);
	
	// it could be stored in redis so information won't be lost after restart
	ConcurrentHashMap<Long, Device> devicesCache = new ConcurrentHashMap<>();
	
	Random rnd = new Random();
	
	private ThreadPoolExecutor exe;
	
	public DeviceLocationManager() {
		this.exe = new ThreadPoolExecutor(5, 5, 1L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>());
	}
	
	public List<Device> getDevicesByUserId(long userId) {
		List<DeviceBean> beans = deviceRepo.findByUserIdOrderByCreatedDate(userId);
		ArrayList<Device> dvs = new ArrayList<Device>();
		for(DeviceBean bn : beans) {
			dvs.add(getFromCache(bn));
		}
		return dvs;
	}
	
	private Device getFromCache(DeviceBean db) {
		Device ch = devicesCache.get(db.id);
		if(ch == null) {
			ch = new Device(db, assistantBot, deviceRepo);
			devicesCache.put(db.id, ch);
		}
		return ch;
	}
	
	public Device getDevice(String strDeviceId) {
		long deviceId = DeviceBean.getDecodedId(strDeviceId);
		Device dv = devicesCache.get(deviceId);
		if (dv == null) {
			Optional<DeviceBean> bean = deviceRepo.findById(deviceId);
			if (bean.isPresent()) {
				dv = getFromCache(bean.get());
			}
		}
		return dv;
	}
	

	
	/// CRUD 
	public String registerNewDevice(UserChatIdentifier chatIdentifier, String name) {
		DeviceBean d = newDevice(chatIdentifier, name);
		return saveDevice(d);		
	}
	
	public String registerNewDevice(UserChatIdentifier ci, TrackerConfiguration trackerConfiguration, DeviceInfo info) {
		DeviceBean device = newDevice(ci, info.getName());
		device.externalConfiguration = trackerConfiguration;
		device.externalId = info.getId();
		return saveDevice(device);
	}
	
	private DeviceBean newDevice(UserChatIdentifier chatIdentifier, String name) {
		String js = chatIdentifier.getUserJsonString();
		DeviceBean device = new DeviceBean();
		device.data.add(DeviceBean.USER_INFO, new JsonParser().parse(js));
		device.chatId = chatIdentifier.getChatId();
		device.userId = chatIdentifier.getUserId();
		device.deviceName = name;
		device.id =  rnd.nextInt(USER_UNIQUENESS) + USER_UNIQUENESS;
		return device;
	}

	private String saveDevice(DeviceBean device) {
		if (deviceRepo.findByUserIdOrderByCreatedDate(device.userId).size() > LIMIT_DEVICES_PER_USER) {
			return String.format("Currently 1 user is allowed to have maximum '%d' devices.", LIMIT_DEVICES_PER_USER);
		}
		deviceRepo.save(device);
		return String.format("Device '%s' is successfully added. Check it with /mydevices", device.deviceName);
	}
	
	
	public void delete(Device d) {
		deviceRepo.delete(d.getDevice());	
		devicesCache.remove(d.getDevice().id);
	}
	
	public void deleteAllByExternalConfiguration(TrackerConfiguration config) {
		List<Device> dvs = getDevicesByUserId(config.userId);
		deviceRepo.deleteAllByExternalConfiguration(config);
		for(Device d : dvs) {
			if(d.getExternalConfiguration() != null && d.getExternalConfiguration().id == config.id) {
				devicesCache.remove(d.getDeviceName());
			}
		}
	}

	public String sendLocation(String deviceId, LocationInfo info)  throws DeviceNotFoundException {
		Device d = getDevice(deviceId);
		if(d == null) {
            throw new DeviceNotFoundException(); 
		}
		d.sendLocation(info);
		return "OK";
	}
	
	
	@Scheduled(fixedRate = INTERVAL_TO_RUN_UPDATES)
	public void updateExternalMonitoring() {
		// could be compare TrackerConfiguration by trackerId & token - to avoid duplication
		long timestamp = System.currentTimeMillis();
		Map<TrackerConfiguration, Map<String, List<Device>>> dms = new TreeMap<>(
				TrackerConfiguration.getGlobalUniqueComparator());
		for (Device dm : devicesCache.values()) {
			LocationInfo sig = dm.getLastSignal();
			if(sig != null && timestamp - sig.getTimestamp() < INTERVAL_TO_UPDATE_GROUPS) {
				continue;
			}
			TrackerConfiguration ext = dm.getDevice().externalConfiguration;
			if (ext != null && dm.isLocationMonitored()) {
				Map<String, List<Device>> mp = dms.get(ext);
				if (mp == null) {
					mp = new HashMap<>();
					dms.put(ext, mp);
				}
				if (!mp.containsKey(dm.getDevice().externalId)) {
					List<Device> lst = new ArrayList<>();
					lst.add(dm);
					mp.put(dm.getDevice().externalId, lst);
				} else {
					mp.get(dm.getDevice().externalId).add(dm);
				}
			}
		}
		Iterator<Entry<TrackerConfiguration, Map<String, List<Device>>>> it = dms.entrySet().iterator();
		// long timestamp = System.currentTimeMillis();
		while (it.hasNext()) {
			Entry<TrackerConfiguration, Map<String, List<Device> > > en = it.next();
			TaskToUpdateLocationGroup task = new TaskToUpdateLocationGroup(en.getKey(), en.getValue());
			if (!exe.getQueue().contains(task)) {
				exe.execute(task);
			}
		}

	}
	
	
	public class TaskToUpdateLocationGroup implements Runnable {
		private ITrackerManager mgr;
		private TrackerConfiguration ext;
		private Map<String, List<Device>> mp;

		public TaskToUpdateLocationGroup(TrackerConfiguration ext, Map<String, List<Device>> mp) {
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
				return TrackerConfiguration.getGlobalUniqueComparator().compare(ext, ((TaskToUpdateLocationGroup)obj).ext) == 0;
			}
			return false;
		}
		
		@Override
		public int hashCode() {
			return ext == null ? 0 : ext.hashCodeGlobalUnique();
		}

	}

	@ResponseStatus(value = HttpStatus.NOT_FOUND)
	public static class DeviceNotFoundException extends RuntimeException {

		private static final long serialVersionUID = 8429349261883069752L;
		
	}

}
