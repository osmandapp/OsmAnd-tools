package net.osmand.server.controllers.pub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import net.osmand.server.assist.DeviceLocationManager;
import net.osmand.server.assist.data.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.google.gson.Gson;

@RestController
public class DeviceController {
	
	@Autowired
	DeviceLocationManager deviceLocationManager;
	
	@Autowired
	DeviceRepository deviceRepo;
	
	Gson gson = new Gson();
	
	public static class DevicesInfo {
		public List<DeviceBean> devices = new ArrayList<DeviceBean>();
	}

	private boolean isUserDeviceNameUnique(List<DeviceBean> devices, String deviceName) {
		 for (DeviceBean device : devices) {
		 	if (device.deviceName.equals(deviceName)) {
		 		return false;
			}
		 }
		 return true;
	}

	private UserChatIdentifier createUserChatIdentifier(NewDevice newDevice) {
		UserChatIdentifier uci = new UserChatIdentifier();
		uci.setChatId(newDevice.getChatId());
		uci.setUser(newDevice.getUser());
		return uci;
	}

	private DeviceBean simplifyDevice(DeviceBean deviceBean) {
		DeviceBean db = new DeviceBean();
		db.id = deviceBean.id;
		db.userId = deviceBean.userId;
		db.deviceName = deviceBean.deviceName;
		db.externalId = deviceBean.getEncodedId();
		return db;
	}

	/*
		Without tracker configuration
	 */
	@PostMapping(value = "/device/new",
			consumes = MediaType.APPLICATION_JSON_UTF8_VALUE,
			produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public ResponseEntity<String> registerNewDevice(@RequestBody NewDevice newDevice) {
		long userId = newDevice.getUser().getId();
		String newDeviceName = newDevice.getDeviceName();
		List<DeviceBean> devices = deviceRepo.findByUserIdOrderByCreatedDate(userId);
		if (devices.size() > DeviceLocationManager.LIMIT_DEVICES_PER_USER) {
			String response = String.format("{\"status\": \"FAILED\", " +
							"\"message\": \"Currently 1 user is allowed to have maximum '%d' devices.\"}",
					DeviceLocationManager.LIMIT_DEVICES_PER_USER);
			return ResponseEntity.badRequest()
					.body(response);
		}
		if (isUserDeviceNameUnique(devices, newDeviceName)) {
			UserChatIdentifier uci = createUserChatIdentifier(newDevice);
			DeviceBean newDeviceBean = deviceLocationManager.newDevice(uci	, newDeviceName);
			newDeviceBean = deviceRepo.save(newDeviceBean);
			String response = String.format("{\"status\": \"OK\", \"device\":%s}", gson.toJson(simplifyDevice(newDeviceBean)));
			System.out.println(response);
			return ResponseEntity.ok().body(response);
		}
		String response = "{\"status\": \"FAILED\", \"message\": \"Device with this name already exsits\"}";
		return ResponseEntity.badRequest().body(response);
	}

	@DeleteMapping(value = "/device/{deviceId}",
			produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public ResponseEntity<String> deleteDeviceById(@PathVariable(name = "deviceId") Long deviceId) {
		boolean isDeviceExists = deviceRepo.existsById(deviceId);
		if (isDeviceExists) {
			deviceLocationManager.delete(deviceId);
			return ResponseEntity.ok().body("{\"status\": \"OK\", \"message\": \"Device deleted successfully\"}");
		}
		return ResponseEntity.badRequest().body("{\"status\": \"FAILED\", \"message\": \"Device deleted or does not exist\"}");
	}

	@RequestMapping("/device/send-devices")
	public @ResponseBody String getDevicesByUserId(@RequestParam(required = false) String uid) {
		List<DeviceBean> devices = deviceRepo.findByUserIdOrderByCreatedDate(Long.parseLong(uid));
		DevicesInfo result = new DevicesInfo();
		for(DeviceBean b : devices) {
			if(b.externalConfiguration == null && b.externalId == null) {
				DeviceBean db = simplifyDevice(b);
				result.devices.add(db);
			}
		}
		return gson.toJson(result);
	}
	
	@RequestMapping("/device/{deviceId}/send")
	public @ResponseBody String sendLocation(@PathVariable("deviceId") String deviceId,
			@RequestParam Map<String,String> allRequestParams,  HttpServletRequest request) {
		LocationInfo li = new LocationInfo();
		li.setIpSender(request.getRemoteAddr());
		if(allRequestParams.containsKey("lat") && allRequestParams.containsKey("lon")) {
			li.setLatLon(Double.parseDouble(allRequestParams.get("lat")), Double.parseDouble(allRequestParams.get("lon")));
		}
		li.setTemperature(parseDouble(allRequestParams, "temp"));
		li.setAzi(parseDouble(allRequestParams, "azi"));
//		li.setAzi(parseDouble(allRequestParams, "bearing"));
		li.setSpeed(parseDouble(allRequestParams, "spd"));
		li.setAltitude(parseDouble(allRequestParams, "alt"));
		li.setHdop(parseDouble(allRequestParams, "hdop"));
		if(allRequestParams.containsKey("temp")) {
			li.setTemperature(Double.parseDouble(allRequestParams.get("temp")));
		}
		String res = deviceLocationManager.sendLocation(deviceId, li);
		return "{'status':'"+res+"'}";
	}

	private double parseDouble(Map<String, String> allRequestParams, String key) {
		if(allRequestParams.containsKey(key)) {
			try {
				return Double.parseDouble(allRequestParams.get(key));
			} catch (RuntimeException e) {
			}
		}
		return Double.NaN;
	}
}
