package net.osmand.server.api.utils;

public final class StringUtils {

    private StringUtils() {
        // Private constructor to prevent instantiation
    }

	public static String sanitize(String input) {
		if (input == null) {
			return "";
		}
		// Replace all non-alphanumeric characters with an underscore
		return input.replaceAll("[^a-zA-Z0-9_]", "_");
	}
}
