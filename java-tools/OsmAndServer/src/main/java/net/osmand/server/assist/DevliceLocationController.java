package net.osmand.server.assist;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class DevliceLocationController {

	@RequestMapping("/tracker/c/")
    public String sendLocation() {
        return "{'status':'OK'}";
    }
}
