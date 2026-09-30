package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import jakarta.servlet.http.HttpServletRequest;

// Crash reports from release builds (e.g. a zip of Android tombstone .pb files). Files only, no database and no personal data:
// the metadata lives in the file name. data.osmand.net copies the files by rsync,
// cron on this server deletes them after 3 hours (web-server-config/servers/crash-reports).
@RestController
@RequestMapping("/api")
public class CrashReportController {

	private static final Log LOG = LogFactory.getLog(CrashReportController.class);

	private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyyMMdd");
	private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
	private static final Pattern VERSION = Pattern.compile("\\d+(\\.\\d+)*");
	private static final int MAX_PARAM_LENGTH = 16;
	private static final long FULL_CHECK_INTERVAL_MS = 60_000;

	// empty = endpoint disabled
	@Value("${osmand.crash-reports.location:}")
	private String location;

	@Value("${osmand.crash-reports.max-file-size:52428800}")
	private long maxFileSize;

	// incoming folder is only a buffer; refuse uploads when the cleanup cron does not run
	@Value("${osmand.crash-reports.max-incoming-size:2147483648}")
	private long maxIncomingSize;

	private volatile long lastFullCheck;
	private volatile boolean full;

	// POST /api/crash-report?platform=android&version=5.4.5&osversion=14
	// body: a zip or gzip archive (a raw tombstone is ~2 MB, zipped ~0.3 MB) as Content-Type: application/octet-stream, or multipart with a "file" part
	@PostMapping("/crash-report")
	public ResponseEntity<String> crashReport(@RequestParam String platform, @RequestParam String version,
	                                          @RequestParam(required = false, defaultValue = "") String osversion,
	                                          HttpServletRequest request) throws IOException {
		if (location == null || location.isEmpty()) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Crash reports are disabled");
		}
		if (MediaType.APPLICATION_FORM_URLENCODED_VALUE.equals(contentType(request))) {
			// the servlet container has already consumed such a body as form parameters
			return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
					.body("Use Content-Type: application/octet-stream or multipart/form-data");
		}
		MultipartFile file = request instanceof MultipartHttpServletRequest multipart ? multipart.getFile("file") : null;
		long size = file != null ? file.getSize() : request.getContentLengthLong();
		if (size > maxFileSize) {
			return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body("File is too large");
		}
		File incoming = new File(location);
		if (isFull(incoming)) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Try later");
		}
		ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
		File dir = new File(incoming, DAY.format(now));
		if (!dir.exists() && !dir.mkdirs() && !dir.exists()) {
			throw new IOException("Cannot create " + dir);
		}
		// e.g. 20260916-142233_android_5.4.5_14_a1b2c3d4.zip
		String name = TIME.format(now) + "_" + simplePlatform(platform) + "_" + simpleVersion(version) + "_"
				+ simpleVersion(osversion) + "_" + UUID.randomUUID().toString().substring(0, 8);
		// hidden while written, rsync on data.osmand.net skips dot files
		File tmp = new File(dir, "." + name + ".tmp");
		String ext;
		try {
			try (InputStream in = file != null ? file.getInputStream() : request.getInputStream()) {
				ext = copy(in, tmp);
			}
		} catch (IOException e) {
			Files.deleteIfExists(tmp.toPath());
			throw e;
		}
		if (tmp.length() == 0 || tmp.length() > maxFileSize) {
			boolean empty = tmp.length() == 0;
			Files.deleteIfExists(tmp.toPath());
			return empty ? ResponseEntity.badRequest().body("Empty file")
					: ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body("File is too large");
		}
		File target = new File(dir, name + ext);
		Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE);
		return ResponseEntity.ok("OK");
	}

	// copies at most maxFileSize + 1 bytes, returns the file extension by the content
	private String copy(InputStream in, File file) throws IOException {
		byte[] buf = new byte[16 * 1024];
		long total = 0;
		String ext = ".bin";
		try (OutputStream out = Files.newOutputStream(file.toPath())) {
			int read;
			while (total <= maxFileSize && (read = in.read(buf, 0, (int) Math.min(buf.length, maxFileSize + 1 - total))) != -1) {
				if (total == 0 && read >= 2) {
					if ((buf[0] & 0xff) == 0x1f && (buf[1] & 0xff) == 0x8b) {
						ext = ".gz";
					} else if (buf[0] == 'P' && buf[1] == 'K') {
						ext = ".zip";
					}
				}
				out.write(buf, 0, read);
				total += read;
			}
		}
		return ext;
	}

	private static String contentType(HttpServletRequest request) {
		String type = request.getContentType();
		return type == null ? "" : type.split(";")[0].trim().toLowerCase(Locale.ROOT);
	}

	private boolean isFull(File incoming) {
		long now = System.currentTimeMillis();
		if (now - lastFullCheck > FULL_CHECK_INTERVAL_MS) {
			lastFullCheck = now;
			long size = incoming.exists() ? FileUtils.sizeOfDirectory(incoming) : 0;
			full = size > maxIncomingSize;
			if (full) {
				LOG.warn("Crash reports folder is not cleaned up: " + size + " bytes in " + incoming);
			}
		}
		return full;
	}

	// "Android", "iOS " -> "android", "ios"
	private static String simplePlatform(String value) {
		String v = value == null ? "" : value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
		return v.isEmpty() ? "unknown" : v.substring(0, Math.min(v.length(), MAX_PARAM_LENGTH));
	}

	// "5.4.5 (4545)", "OsmAnd~ 5.4.5", "Android 14" -> "5.4.5", "5.4.5", "14"
	private static String simpleVersion(String value) {
		Matcher m = VERSION.matcher(value == null ? "" : value);
		return m.find() ? m.group().substring(0, Math.min(m.group().length(), MAX_PARAM_LENGTH)) : "unknown";
	}
}
