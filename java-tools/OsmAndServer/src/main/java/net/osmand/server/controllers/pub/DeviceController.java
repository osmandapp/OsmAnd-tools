package net.osmand.server.controllers.pub;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import net.osmand.server.assist.DeviceLocationManager;
import net.osmand.server.assist.data.LocationInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DeviceController {
	
	@Autowired
	DeviceLocationManager deviceLocationManager;

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
