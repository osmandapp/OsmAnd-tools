package net.osmand.server.controllers.admin;

import java.util.Map;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class AdminController {


    @GetMapping("/admin/send-bitcoins")
	public String welcome(@RequestParam(name="name", required=false, defaultValue="World") String name, 
			Map<String, Object> model) {
		model.put("message", "Hello " + name);
		return "admin/send-bitcoins";
	}

}