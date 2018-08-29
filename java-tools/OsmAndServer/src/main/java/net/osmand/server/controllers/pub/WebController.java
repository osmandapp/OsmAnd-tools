package net.osmand.server.controllers.pub;

import net.osmand.server.services.motd.MotdService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;

@Controller
public class WebController {
    private static final Log LOGGER = LogFactory.getLog(WebController.class);

    private final List<String> errors = new ArrayList<>();

    @Autowired
    private MotdService motdService;

    @RequestMapping(path = {"publish"}, method = RequestMethod.GET)
    @ResponseBody
    public String publish() {
        return motdService.updateSettings(errors);
    }


}