package net.osmand.server.utils;

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

// Incoming folder of uploaded files: one folder per UTC day, metadata in the file name, no database.
// Another server copies the files by rsync and a cron deletes them here, so the folder is only a buffer.
public class UploadFolder {

	private static final Log LOG = LogFactory.getLog(UploadFolder.class);

	private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyyMMdd");
	private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
	private static final Pattern VERSION = Pattern.compile("\\d+(\\.\\d+)*");
	private static final int MAX_PARAM_LENGTH = 16;
	private static final long FULL_CHECK_INTERVAL_MS = 60_000;

	public enum Result {OK, EMPTY, TOO_LARGE, FULL}

	private final File location;
	private final long maxFileSize;
	private final long maxIncomingSize;

	private volatile long lastFullCheck;
	private volatile boolean full;

	public UploadFolder(String location, long maxFileSize, long maxIncomingSize) {
		this.location = new File(location);
		this.maxFileSize = maxFileSize;
		this.maxIncomingSize = maxIncomingSize;
	}

	public long getMaxFileSize() {
		return maxFileSize;
	}

	// stores YYYYMMDD/YYYYMMDD-HHMMSS_<name parts>_<random>.<ext by content>
	public Result store(InputStream in, long declaredSize, String... nameParts) throws IOException {
		if (declaredSize > maxFileSize) {
			return Result.TOO_LARGE;
		}
		if (isFull()) {
			return Result.FULL;
		}
		ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
		File dir = new File(location, DAY.format(now));
		if (!dir.exists() && !dir.mkdirs() && !dir.exists()) {
			throw new IOException("Cannot create " + dir);
		}
		StringBuilder name = new StringBuilder(TIME.format(now));
		for (String part : nameParts) {
			name.append('_').append(part);
		}
		name.append('_').append(UUID.randomUUID().toString(), 0, 8);
		// hidden while written, rsync skips dot files
		File tmp = new File(dir, "." + name + ".tmp");
		String ext;
		try {
			ext = copy(in, tmp);
		} catch (IOException e) {
			Files.deleteIfExists(tmp.toPath());
			throw e;
		}
		if (tmp.length() == 0 || tmp.length() > maxFileSize) {
			boolean empty = tmp.length() == 0;
			Files.deleteIfExists(tmp.toPath());
			return empty ? Result.EMPTY : Result.TOO_LARGE;
		}
		File target = new File(dir, name + ext);
		Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE);
		return Result.OK;
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

	private boolean isFull() {
		long now = System.currentTimeMillis();
		if (now - lastFullCheck > FULL_CHECK_INTERVAL_MS) {
			lastFullCheck = now;
			long size = location.exists() ? FileUtils.sizeOfDirectory(location) : 0;
			full = size > maxIncomingSize;
			if (full) {
				LOG.warn("Upload folder is not cleaned up: " + size + " bytes in " + location);
			}
		}
		return full;
	}

	// "Android", "iOS " -> "android", "ios"
	public static String simplePlatform(String value) {
		String v = value == null ? "" : value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
		return v.isEmpty() ? "unknown" : v.substring(0, Math.min(v.length(), MAX_PARAM_LENGTH));
	}

	// "5.4.5 (4545)", "OsmAnd~ 5.4.5", "Android 14" -> "5.4.5", "5.4.5", "14"
	public static String simpleVersion(String value) {
		Matcher m = VERSION.matcher(value == null ? "" : value);
		return m.find() ? m.group().substring(0, Math.min(m.group().length(), MAX_PARAM_LENGTH)) : "unknown";
	}
}
