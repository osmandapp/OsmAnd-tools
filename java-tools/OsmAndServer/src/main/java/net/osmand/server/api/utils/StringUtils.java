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

	public static String unquote(String input) {
		if (input == null)
			return "";

		if (input.length() >= 2 && input.startsWith("\"") && input.endsWith("\"")) {
			return input.substring(1, input.length() - 1);
		}
		return input;
	}

	public static String crop(String input, int length) {
		if (input == null)
			return null;

		return input.substring(0, Math.min(length, input.length()));
	}
}
