package net.osmand.server.controllers.user;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/map/")
public class MapApiController {
	protected static final Log LOGGER = LogFactory.getLog(MapApiController.class);

	@GetMapping(path = { "/loginForm" }, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public AbstractResource loginForm() {
		return new ClassPathResource("/test-map-pro-login.html");
	}

}
