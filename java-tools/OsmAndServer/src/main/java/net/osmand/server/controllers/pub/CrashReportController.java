package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.HttpServletRequest;
import net.osmand.server.utils.UploadFolder;

// Crash reports from release builds (e.g. a zip of Android tombstone .pb files). Files only, no database and no personal data:
// the metadata lives in the file name. data.osmand.net copies the files by rsync,
// cron on this server deletes them after 3 hours (web-server-config/servers/crash-reports).
@RestController
@RequestMapping("/api")
public class CrashReportController {

	// empty = endpoint disabled
	@Value("${osmand.crash-reports.location:}")
	private String location;

	@Value("${osmand.crash-reports.max-file-size:52428800}")
	private long maxFileSize;

	// incoming folder is only a buffer; refuse uploads when the cleanup cron does not run
	@Value("${osmand.crash-reports.max-incoming-size:2147483648}")
	private long maxIncomingSize;

	private UploadFolder folder;

	@PostConstruct
	void init() {
		if (location != null && !location.isEmpty()) {
			folder = new UploadFolder(location, maxFileSize, maxIncomingSize);
		}
	}

	// POST /api/crash-report?platform=android&version=5.4.5&osversion=14
	// body: a zip or gzip archive (a raw tombstone is ~2 MB, zipped ~0.3 MB) as Content-Type: application/octet-stream, or multipart with a "file" part
	@PostMapping("/crash-report")
	public ResponseEntity<String> crashReport(@RequestParam String platform, @RequestParam String version,
	                                          @RequestParam(required = false, defaultValue = "") String osversion,
	                                          HttpServletRequest request) throws IOException {
		if (folder == null) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Crash reports are disabled");
		}
		if (MediaType.APPLICATION_FORM_URLENCODED_VALUE.equals(contentType(request))) {
			// the servlet container has already consumed such a body as form parameters
			return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
					.body("Use Content-Type: application/octet-stream or multipart/form-data");
		}
		MultipartFile file = request instanceof MultipartHttpServletRequest multipart ? multipart.getFile("file") : null;
		long size = file != null ? file.getSize() : request.getContentLengthLong();
		// e.g. 20260916-142233_android_5.4.5_14_a1b2c3d4.zip
		UploadFolder.Result result;
		try (InputStream in = file != null ? file.getInputStream() : request.getInputStream()) {
			result = folder.store(in, size, UploadFolder.simplePlatform(platform),
					UploadFolder.simpleVersion(version), UploadFolder.simpleVersion(osversion));
		}
		return response(result);
	}

	static ResponseEntity<String> response(UploadFolder.Result result) {
		return switch (result) {
			case OK -> ResponseEntity.ok("OK");
			case EMPTY -> ResponseEntity.badRequest().body("Empty file");
			case TOO_LARGE -> ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body("File is too large");
			case FULL -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Try later");
		};
	}

	private static String contentType(HttpServletRequest request) {
		String type = request.getContentType();
		return type == null ? "" : type.split(";")[0].trim().toLowerCase(Locale.ROOT);
	}
}
