package net.osmand.server.controllers.pub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.google.gson.*;
import net.osmand.server.assist.DeviceLocationManager;
import net.osmand.server.assist.data.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.telegram.telegrambots.api.objects.User;

@RestController
public class DeviceController {

	private static final String DEVICE_NAME = "deviceName";
	private static final String USER = "user";
	private static final String CHAT_ID = "chatId";

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

	private UserChatIdentifier createUserChatIdentifier(User telegramUser, long chatId) {
		UserChatIdentifier uci = new UserChatIdentifier();
		uci.setChatId(chatId);
		uci.setUser(telegramUser);
		return uci;
	}

	private DeviceBean simplifyDevice(DeviceBean deviceBean) {
		DeviceBean db = new DeviceBean();
		db.id = deviceBean.id;
		db.userId = deviceBean.userId;
		db.deviceName = deviceBean.deviceName;
		db.externalId = deviceBean.getEncodedId();
		db.data = deviceBean.data.deepCopy();
		return db;
	}

	private ResponseEntity<String> missingParameterResponse(String param) {
		String response = String.format("{\"status\": \"FAILED\", \"message\": \"%s is missing.\"}", param);
		return ResponseEntity.badRequest().body(response);
	}

	/*
		Without tracker configuration
	 */
	@PostMapping(value = "/device/new",
			consumes = MediaType.APPLICATION_JSON_UTF8_VALUE,
			produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public ResponseEntity<String> registerNewDevice(@RequestBody String body) {
		long chatId = 0;
		JsonObject json = new JsonParser().parse(body).getAsJsonObject();
		JsonElement deviceNameElem = json.get(DEVICE_NAME);
		if (deviceNameElem == null) {
			return missingParameterResponse(DEVICE_NAME);
		}
		String deviceName = deviceNameElem.getAsString();
		JsonElement userDataElem = json.get(USER);
		if (userDataElem == null) {
			return missingParameterResponse(USER);
		}
		User telegramUser = gson.fromJson(userDataElem.getAsJsonObject(), User.class);
		JsonElement chatIdElem = json.get(CHAT_ID);
		if (chatIdElem != null) {
			chatId = chatIdElem.getAsLong();
		}
		List<DeviceBean> devices = deviceRepo.findByUserIdOrderByCreatedDate(telegramUser.getId());
		if (devices.size() > DeviceLocationManager.LIMIT_DEVICES_PER_USER) {
			String response = String.format("{\"status\": \"FAILED\", " +
							"\"message\": \"Currently 1 user is allowed to have maximum '%d' devices.\"}",
					DeviceLocationManager.LIMIT_DEVICES_PER_USER);
			return ResponseEntity.badRequest()
					.body(response);
		}
		if (isUserDeviceNameUnique(devices, deviceName)) {
			UserChatIdentifier uci = createUserChatIdentifier(telegramUser, chatId);
			DeviceBean newDeviceBean = deviceLocationManager.newDevice(uci, deviceName);
			newDeviceBean = deviceRepo.save(newDeviceBean);
			String response = String.format("{\"status\": \"OK\", \"device\":%s}", gson.toJson(simplifyDevice(newDeviceBean)));
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
