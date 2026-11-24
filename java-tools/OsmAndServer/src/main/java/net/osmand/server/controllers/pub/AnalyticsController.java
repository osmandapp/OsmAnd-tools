package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.io.InputStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import jakarta.annotation.PostConstruct;
import net.osmand.server.utils.UploadFolder;

// Opt-in app analytics parcels (gzipped JSON, ~500 events) from the Android app. Files only, like crash reports:
// no database and no IP address; the parcel itself carries aid, nd, ns, version and lang.
// data.osmand.net copies the files by rsync, cron on this server deletes them after 3 hours
// (web-server-config/servers/analytics).
@RestController
@RequestMapping("/api")
public class AnalyticsController {

	// empty = endpoint disabled; the app keeps the events and retries later
	@Value("${osmand.analytics.location:}")
	private String location;

	@Value("${osmand.analytics.max-file-size:1048576}")
	private long maxFileSize;

	@Value("${osmand.analytics.max-incoming-size:2147483648}")
	private long maxIncomingSize;

	private UploadFolder folder;

	@PostConstruct
	void init() {
		if (location != null && !location.isEmpty()) {
			folder = new UploadFolder(location, maxFileSize, maxIncomingSize);
		}
	}

	// multipart: "file" part plus os, version, startDate, finishDate, nd, ns, lang, aid params (all repeated in the file)
	@PostMapping(path = "/submit_analytics", consumes = "multipart/form-data")
	public ResponseEntity<String> submitAnalytics(@RequestParam(required = false, defaultValue = "android") String os,
	                                              @RequestParam(required = false, defaultValue = "") String version,
	                                              @RequestParam MultipartFile file) throws IOException {
		if (folder == null) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Analytics is disabled");
		}
		// e.g. 20261006-142233_android_5.4.6_a1b2c3d4.gz
		UploadFolder.Result result;
		try (InputStream in = file.getInputStream()) {
			result = folder.store(in, file.getSize(), UploadFolder.simplePlatform(os), UploadFolder.simpleVersion(version));
		}
		return CrashReportController.response(result);
	}
}
