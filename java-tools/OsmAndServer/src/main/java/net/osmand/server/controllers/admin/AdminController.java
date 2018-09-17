package net.osmand.server.controllers.admin;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class AdminController {


	@GetMapping("/admin/bitcoins/report.json")
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
	
    @GetMapping("/admin/send-bitcoins")
	public String sendBitcoins(@RequestParam(name="name", required=false, defaultValue="World") String name, 
			Map<String, Object> model) {
    	
		model.put("message", "Hello " + name);
		return "admin/send-bitcoins";
	}
}