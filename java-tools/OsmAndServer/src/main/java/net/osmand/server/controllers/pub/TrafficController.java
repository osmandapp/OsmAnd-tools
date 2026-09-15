package net.osmand.server.controllers.pub;

import java.io.File;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import net.osmand.server.api.services.TrafficService;

@RestController
@RequestMapping("/api/traffic")
public class TrafficController {

	@Value("${osmand.web.location}")
	private String websiteLocation;

	@Autowired
	private TrafficService trafficService;

	// test page from web-server-config
	@GetMapping(path = "/test.html", produces = MediaType.TEXT_HTML_VALUE)
	public ResponseEntity<FileSystemResource> testPage() {
		return file(new File(websiteLocation, "test/traffic.html"), MediaType.TEXT_HTML);
	}

	// sources and their days
	@GetMapping(path = "/index", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<FileSystemResource> index() {
		return file(trafficService.getIndexFile(), MediaType.APPLICATION_JSON);
	}

	// one day of one source, e.g. /api/traffic/ndw/2026-09-11
	@GetMapping(path = "/{source}/{day}", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<FileSystemResource> day(@PathVariable String source, @PathVariable String day) {
		return file(trafficService.getDayFile(source, day), MediaType.APPLICATION_JSON);
	}

	private static ResponseEntity<FileSystemResource> file(File file, MediaType type) {
		if (file == null || !file.exists()) {
			return ResponseEntity.notFound().build();
		}
		ResponseEntity.BodyBuilder response = ResponseEntity.ok().contentType(type);
		if (file.getName().endsWith(".gz")) {
			// day files are stored gzipped, the browser unpacks them
			response.header(HttpHeaders.CONTENT_ENCODING, "gzip");
		}
		return response.body(new FileSystemResource(file));
	}
}
