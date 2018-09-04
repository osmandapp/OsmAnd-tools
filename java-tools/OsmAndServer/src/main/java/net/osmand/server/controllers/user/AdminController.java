package net.osmand.server.controllers.user;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.osmand.server.controllers.pub.WebController;
import net.osmand.server.services.api.DownloadIndexesService;
import net.osmand.server.services.api.DownloadIndexesService.DownloadProperties;
import net.osmand.server.services.api.MotdService;
import net.osmand.server.services.api.MotdService.MotdSettings;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Controller
@RequestMapping("/admin")
@PropertySource("classpath:git.properties")
public class AdminController {

	@Autowired
	private MotdService motdService;
	
	@Autowired
	private DownloadIndexesService downloadService;
	
	@Autowired
	private WebController web;
	
	@Autowired
	private ApplicationContext appContext;
	
	@Value("${git.commit.format}")
	private String commit;

	protected ObjectMapper mapper;
	
	public AdminController() {
		ObjectMapper objectMapper = new ObjectMapper();
        this.mapper = objectMapper;
	}

	@RequestMapping(path = { "/publish" }, method = RequestMethod.POST)
	public String publish(Model model) throws JsonProcessingException {
		List<String> errors = publish();
        model.addAttribute("update_status", "OK");
        model.addAttribute("update_errors", "");
        model.addAttribute("update_message", "Configurations are reloaded");
        model.addAttribute("services", new String[]{"motd", "download"});
        if(!errors.isEmpty()) {
        	model.addAttribute("update_status", "FAILED");
        	model.addAttribute("update_errors", "Errors: " +errors);
        }
        return index(model);
	}

	private List<String> publish() {
		List<String> errors = new ArrayList<>();
        motdService.reloadconfig(errors);
        downloadService.reloadConfig(errors);
        web.reloadConfigs(errors);
		return errors;
	}


	@RequestMapping("/info")
	public String index(Model model) {
		model.addAttribute("server_startup", String.format("%1$tF %1$tR", new Date(appContext.getStartupDate())));
		model.addAttribute("server_commit", commit);
		model.addAttribute("web_commit", "TODO");
		if(!model.containsAttribute("update_status")) {
			model.addAttribute("update_status", "OK");
	        model.addAttribute("update_errors", "");
	        model.addAttribute("update_message", "");
		}
		MotdSettings settings = motdService.getSettings();
		model.addAttribute("motdSettings", settings);
		
		List<Map<String, Object>> list = getDownloadSettings();
		model.addAttribute("downloadServers", list);
		return "admin/info";
	}

	private List<Map<String, Object>> getDownloadSettings() {
		DownloadProperties dProps = downloadService.getSettings();
		int ms = dProps.getMainServers().size();
		int hs = dProps.getHelpServers().size();
		int mload = ms == 0 ? 0 : dProps.getMainLoad() / ms;
		int mmload = ms == 0 ? 0 : 100 / ms;
		int hload = hs == 0 ? 0 : (100-dProps.getMainLoad()) / hs;
		List<Map<String, Object>> list = new ArrayList<>();
		for(String s : dProps.getMainServers()) {
			Map<String, Object> mo = new TreeMap<>();
			mo.put("name", s);
			mo.put("mainLoad", mload +"%");
			mo.put("srtmLoad", mmload +"%");
			list.add(mo);
		}
		for(String s : dProps.getHelpServers()) {
			Map<String, Object> mo = new TreeMap<>();
			mo.put("name", s);
			mo.put("mainLoad", hload +"%");
			mo.put("srtmLoad", "0%");
			list.add(mo);
		}
		return list;
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