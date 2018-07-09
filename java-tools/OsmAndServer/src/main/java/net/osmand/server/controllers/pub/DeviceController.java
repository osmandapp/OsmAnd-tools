package net.osmand.server.controllers.pub;

import net.osmand.server.assist.DeviceLocationManager;

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
			@RequestParam String lat, @RequestParam String lon) {
		String res = deviceLocationManager.sendLocation(deviceId, lat, lon);
		return "{'status':'"+res+"'}";
	}
}
