package net.osmand.server.controllers.pub;


import net.osmand.server.service.UpdateIndexes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;


import java.io.IOException;

@Controller
public class ApiController {

    private static final Log LOGGER = LogFactory.getLog(ApiController.class);

    @Autowired
    UpdateIndexes updateIndexes;

    @RequestMapping(value = "update", method = RequestMethod.GET)
    public String update(Model model) throws IOException {
        updateIndexes.update();
        return "pub/indexes";
    }
}
