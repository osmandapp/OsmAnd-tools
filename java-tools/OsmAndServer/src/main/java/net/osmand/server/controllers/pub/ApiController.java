package net.osmand.server.controllers.pub;


import net.osmand.server.index.Index;
import net.osmand.server.index.IndexAttributes;
import net.osmand.server.index.RegionDAO;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;


import java.io.IOException;
import java.util.List;

@Controller
public class ApiController {

    private static final Log LOGGER = LogFactory.getLog(ApiController.class);

    @RequestMapping(value = "load", method = RequestMethod.GET)
    public String load(Model model) throws IOException {
        RegionDAO regionDAO = new RegionDAO();
        List<Index> regions = regionDAO.process();
        for (Index index : regions) {
            IndexAttributes ia = index.getAttributes();
            System.out.println(String.format("%s %s %s %s %s %s %s", ia.getType(), ia.getName(), ia.getDate(), ia.getSize(), ia.getTargetSize(), ia.getTimestamp(), ia.getDescription()));
        }
        return "pub/indexes";
    }
}
