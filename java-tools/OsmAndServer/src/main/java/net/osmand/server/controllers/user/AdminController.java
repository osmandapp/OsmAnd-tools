package net.osmand.server.controllers.user;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.osmand.server.services.motd.MotdService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/admin")
@Configuration
@PropertySource("classpath:git.properties")
public class AdminController {

	@Autowired
	private MotdService motdService;

	// TODO change to POST
	@RequestMapping(path = { "publish" }, method = RequestMethod.GET)
	@ResponseBody
	public String publish() {
		List<String> errors = new ArrayList<>();
        motdService.reloadconfig(errors);
        String msg = "{\"status\": \"OK\", \"message\" : \"Configurations are reloaded.\"}";
        if(!errors.isEmpty()) {
        	StringBuilder eb = new StringBuilder();
        	for(String e : errors) {
        		if(eb.length() > 0) {
        			eb.append(", ");
        		}
        		eb.append('"').append(e).append('"');
        	}
        	msg = String.format("{\"status\": \"FAILED\", \"errors\": [%s]", eb.toString());
        }
        return msg;
	}

	@Value("${git.commit.format}")
	private String commit;

	@RequestMapping("/info")
	public String index() {
		return String.format("OsmAnd Live server. Revision: %s", commit);
	}

	@GetMapping("/bitcoins/report.json")
	public ResponseEntity<InputStreamResource> bitcoinsUnderpaidReport() throws IOException {
		HttpHeaders headers = new HttpHeaders();
		URL report = new URL("http://builder.osmand.net/reports/report_underpaid.json.html");
		HttpURLConnection connection = (HttpURLConnection) report.openConnection();
		int length = connection.getContentLength();
		InputStream is = (InputStream) report.getContent();
		InputStreamResource inputStreamResource = new InputStreamResource(is);
		headers.setContentLength(length);
		return new ResponseEntity<InputStreamResource>(inputStreamResource, headers, HttpStatus.OK);
	}

	@GetMapping("/send-bitcoins")
	public String sendBitcoins(@RequestParam(name = "name", required = false, defaultValue = "World") String name,
			Map<String, Object> model) {

		model.put("message", "Hello " + name);
		return "admin/send-bitcoins";
	}
}