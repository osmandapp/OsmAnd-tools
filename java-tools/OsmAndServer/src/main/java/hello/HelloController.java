package hello;

import java.io.IOException;
import java.sql.SQLException;

import net.osmand.MapCreatorVersion;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.xmlpull.v1.XmlPullParserException;

@RestController
public class HelloController {

    @RequestMapping("/")
    public String index() throws IOException, SQLException, InterruptedException, XmlPullParserException {
//    	IndexCreator.main(null);
        return "Greetings from: " + MapCreatorVersion.APP_MAP_CREATOR_FULL_NAME+ " ";
    }

}