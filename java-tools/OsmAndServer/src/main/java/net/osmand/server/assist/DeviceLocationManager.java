package net.osmand.server.assist;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.gson.JsonParser;

import net.osmand.server.assist.data.Device;
import net.osmand.server.assist.data.DeviceBean;
import net.osmand.server.assist.data.DeviceRepository;
import net.osmand.server.assist.data.LocationInfo;
import net.osmand.server.assist.data.UserChatIdentifier;

@Component
public class DeviceLocationManager {
	
	@Autowired
	OsmAndAssistantBot assistantBot;
	
	@Autowired
	DeviceRepository deviceRepo;
	
	public static final int LIMIT_DEVICES_PER_USER = 200;
	public static final Integer DEFAULT_LIVE_PERIOD = 24 * 60 * 60;
	private static final int USER_UNIQUENESS = 1 << 20;
	protected static final Log LOG = LogFactory.getLog(DeviceLocationManager.class);

	ConcurrentHashMap<Long, Device> devicesCache = new ConcurrentHashMap<>();

	Random rnd = new Random();


	public DeviceLocationManager() {
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
			ch = new Device(db, assistantBot);
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

	public DeviceBean newDevice(UserChatIdentifier chatIdentifier, String name) {
		String js = chatIdentifier.getUserJsonString();
		DeviceBean device = new DeviceBean();
		device.data.add(DeviceBean.USER_INFO, new JsonParser().parse(js));
		device.chatId = chatIdentifier.getChatId();
		device.userId = chatIdentifier.getUserId();
		device.deviceName = name;
		device.id =  rnd.nextInt(USER_UNIQUENESS) + USER_UNIQUENESS;
		return device;
	}

	public String saveDevice(DeviceBean device) {
		if (deviceRepo.findByUserIdOrderByCreatedDate(device.userId).size() > LIMIT_DEVICES_PER_USER) {
			return String.format("Currently 1 user is allowed to have maximum '%d' devices.", LIMIT_DEVICES_PER_USER);
		}
		return saveNoCheck(device);
	}

	public String saveNoCheck(DeviceBean device) {
		deviceRepo.save(device);
		return String.format("Device '%s' is successfully added. Check it with /mydevices", device.deviceName);
	}


	public void delete(Device d) {
		deviceRepo.delete(d.getDevice());
		devicesCache.remove(d.getDevice().id);
	}

	public void delete(long deviceId) {
		deviceRepo.deleteDeviceById(deviceId);
		devicesCache.remove(deviceId);
	}
	
	public String sendLocation(String deviceId, LocationInfo info)  throws DeviceNotFoundException {
		Device d = getDevice(deviceId);
		if(d == null) {
            throw new DeviceNotFoundException(); 
		}
		d.sendLocation(info);
		return "OK";
	}
	
	
	

	@ResponseStatus(value = HttpStatus.NOT_FOUND)
	public static class DeviceNotFoundException extends RuntimeException {

		private static final long serialVersionUID = 8429349261883069752L;
		
	}

}
