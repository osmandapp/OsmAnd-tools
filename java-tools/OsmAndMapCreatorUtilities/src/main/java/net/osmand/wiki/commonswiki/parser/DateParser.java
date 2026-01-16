package net.osmand.wiki.commonswiki.parser;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.osmand.wiki.commonswiki.parser.ParserUtils.FIELD_DATE;

/**
 * Parser for Wikimedia/Commons date fields (e.g. values of the {@code |date=} parameter
 * inside {{Information}} / {{Artwork}} templates).
 * <p>
 * It extracts a plain date in YYYY-MM-DD format from various wiki markup:
 * templates (Original upload date, Taken on, other date, etc.) and plain text.
 */
public final class DateParser {

	private static final String DATE_PATTERN = "\\d{4}-\\d{2}-\\d{2}";
	private static final String OTHER_DATE_TEMPLATE = "other date";

	private DateParser() {
		// Utility class - no instantiation
	}

	public static String parse(String line) {
		String dateValue = extractDateValue(line);
		if (dateValue == null || dateValue.isEmpty()) {
			return null;
		}

		String date = parseDateFromValue(dateValue);
		return cleanDate(date);
	}

	private static String extractDateValue(String line) {
		return ParserUtils.extractFieldValue(line, FIELD_DATE);
	}

	private static String parseDateFromValue(String dateValue) {
		if (dateValue.startsWith("{{")) {
			return parseDateFromTemplate(dateValue);
		} else {
			return parseDateFromPlainText(dateValue);
		}
	}

	private static String parseDateFromTemplate(String dateValue) {
		String templateContent = ParserUtils.extractTemplateContent(dateValue);
		if (templateContent == null) {
			return null;
		}

		String[] parts = templateContent.split("\\|");

		if (parts.length > 1 && parts[0].equalsIgnoreCase(OTHER_DATE_TEMPLATE)) {
			// For "other date" template, return all parts after the template name
			return String.join(" ", Arrays.copyOfRange(parts, 1, parts.length)).trim();
		}

		// Find the part matching YYYY-MM-DD format
		Pattern DATE_PATTERN_C = Pattern.compile(DATE_PATTERN);
		for (String part : parts) {
			Matcher m = DATE_PATTERN_C.matcher(part);
			if (m.find()) {
				return m.group(0); // extracted YYYY-MM-DD
			}
		}

		return null;
	}

	private static String parseDateFromPlainText(String dateValue) {
		// Extract the date part (before first space, if any)
		String[] parts = dateValue.split(" ");
		return parts[0].trim();
	}

	private static String cleanDate(String date) {
		if (date == null || date.isEmpty()) {
			return null;
		}
		return ParserUtils.removeWikiLinkBrackets(date);
	}
}

