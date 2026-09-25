package net.osmand.server.utils;

import java.util.Locale;

public class FileSizeFormatter {

	public static String format(long bytes) {
		return bytes < 1024 * 1024
				? String.format(Locale.US, "%.1f KB", bytes / 1024.0)
				: String.format(Locale.US, "%.1f MB", bytes / (1024.0 * 1024));
	}
}
