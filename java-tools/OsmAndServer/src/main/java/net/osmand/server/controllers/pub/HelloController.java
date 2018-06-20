package net.osmand.server.controllers.pub;

import net.osmand.MapCreatorVersion;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

	protected static final Log logger = LogFactory.getLog(HelloController.class);	

    @RequestMapping("/hello")
    public String index() {
        return "Greetings from: " + MapCreatorVersion.APP_MAP_CREATOR_FULL_NAME+ " ";
    }

}