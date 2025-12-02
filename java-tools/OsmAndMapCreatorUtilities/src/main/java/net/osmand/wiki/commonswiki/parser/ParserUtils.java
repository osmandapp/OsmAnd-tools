package net.osmand.wiki.commonswiki.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility methods for wiki parsers.
 * Contains common functionality used across different parser classes.
 */
public final class ParserUtils {

	// Metadata field names for information-like templates
	public static final String FIELD_AUTHOR = "author";
	public static final String FIELD_PHOTOGRAPHER = "photographer";
	public static final String FIELD_DATE = "date";
	public static final String FIELD_DESCRIPTION = "description";
	public static final String FIELD_LICENSE = "license";

	private ParserUtils() {
		// Utility class - no instantiation
	}

	/**
	 * Extracts field value from a line in format "fieldName=value" or "|fieldName=value".
	 * 
	 * @param line The line containing the field
	 * @param fieldName The name of the field (case-insensitive)
	 * @return The extracted value, or null if field not found
	 */
	public static String extractFieldValue(String line, String fieldName) {
		if (line == null || fieldName == null) {
			return null;
		}

		line = line.trim();
		String lineLc = line.toLowerCase();
		String fieldNameLc = fieldName.toLowerCase() + "=";

		// Check if line starts with field name followed by =
		if (lineLc.startsWith(fieldNameLc)) {
			return line.substring(fieldName.length() + 1).trim();
		}

		// Check if line starts with |fieldName=
		if (line.startsWith("|")) {
			String afterPipe = line.substring(1).trim();
			if (afterPipe.toLowerCase().startsWith(fieldNameLc)) {
				int eqPos = line.indexOf("=");
				if (eqPos != -1) {
					return line.substring(eqPos + 1).trim();
				}
			}
		}

		// Check if field name appears anywhere in the line (for regex matching with spaces)
		if (lineLc.matches(".*" + fieldName.toLowerCase() + "\\s*=\\s*.*")) {
			return line.replaceFirst("(?i).*" + fieldName + "\\s*=\\s*", "").trim();
		}

		return null;
	}

	/**
	 * Removes wiki link brackets [[...]] from a string.
	 * 
	 * @param text The text to clean
	 * @return Cleaned text, or null if input is null
	 */
	public static String removeWikiLinkBrackets(String text) {
		if (text == null) {
			return null;
		}
		return text.replaceAll("\\[+|]+", "");
	}

	/**
	 * Removes template brackets {{...}} from a string.
	 * 
	 * @param text The text to clean
	 * @return Cleaned text, or null if input is null
	 */
	public static String removeTemplateBrackets(String text) {
		if (text == null) {
			return null;
		}
		return text.replaceAll("\\{\\{|\\}\\}", "");
	}

	/**
	 * Removes both wiki link and template brackets from a string.
	 * 
	 * @param text The text to clean
	 * @return Cleaned text, or null if input is null
	 */
	public static String removeWikiMarkup(String text) {
		if (text == null) {
			return null;
		}
		String withoutLinks = removeWikiLinkBrackets(text);
		// removeWikiLinkBrackets returns null only if input is null, which we already checked
		return removeTemplateBrackets(withoutLinks);
	}

	/**
	 * Normalizes whitespace in a string: replaces newlines with spaces,
	 * collapses multiple spaces into one, and trims.
	 * 
	 * @param text The text to normalize
	 * @return Normalized text, or null if input is null
	 */
	public static String normalizeWhitespace(String text) {
		if (text == null) {
			return null;
		}
		return text.replaceAll("\n", " ").replaceAll("\\s{2,}", " ").trim();
	}

	/**
	 * Extracts template content from a string in format {{content}}.
	 * 
	 * @param text The text containing the template
	 * @return The template content (without {{}}), or null if not found
	 */
	public static String extractTemplateContent(String text) {
		if (text == null || !text.startsWith("{{") || !text.endsWith("}}")) {
			return null;
		}
		return text.substring(2, text.length() - 2).trim();
	}

	/**
	 * Splits a string by pipe character '|' while respecting nested braces {{ }} and [[ ]].
	 * This is useful for parsing wiki template parameters.
	 * 
	 * @param input The input string to split
	 * @param splitByPipe If true, splits by '|'; if false, returns the whole string as a single part
	 * @return List of parts split by pipe (outside braces)
	 */
	public static List<String> splitByPipeOutsideBraces(String input, boolean splitByPipe) {
		List<String> parts = new ArrayList<>();
		StringBuilder currentPart = new StringBuilder();
		int curlyBraceDepth = 0;  // To track the nesting level inside {{ }}
		int squareBraceDepth = 0; // To track the nesting level inside [[ ]]
		
		for (int i = 0; i < input.length(); i++) {
			char c = input.charAt(i);
			
			// Increase nesting level for {{ and [[
			if (i < input.length() - 1 && input.charAt(i) == '{' && input.charAt(i + 1) == '{') {
				curlyBraceDepth++;
				currentPart.append(c);
				currentPart.append(input.charAt(++i));
			} else if (i < input.length() - 1 && input.charAt(i) == '[' && input.charAt(i + 1) == '[') {
				squareBraceDepth++;
				currentPart.append(c);
				currentPart.append(input.charAt(++i));
			}
			
			// Decrease nesting level for }} and ]]
			else if (i < input.length() - 1 && input.charAt(i) == '}' && input.charAt(i + 1) == '}') {
				curlyBraceDepth--;
				currentPart.append(c);
				currentPart.append(input.charAt(++i));
			} else if (i < input.length() - 1 && input.charAt(i) == ']' && input.charAt(i + 1) == ']') {
				squareBraceDepth--;
				currentPart.append(c);
				currentPart.append(input.charAt(++i));
			}
			
			// Split by '|' if we are not inside {{ }} or [[ ]]
			else if (c == '|' && curlyBraceDepth == 0 && squareBraceDepth == 0 && splitByPipe) {
				parts.add(currentPart.toString().trim());
				currentPart.setLength(0); // Clear the current string for the next part
			} else {
				currentPart.append(c); // Add character to the current part
			}
		}
		
		// Add the last part
		if (!currentPart.isEmpty()) {
			parts.add(currentPart.toString().trim());
		}
		
		return parts;
	}

	/**
	 * Checks if a template string contains information-like fields (author, photographer, date, description, license, permission).
	 * 
	 * @param vallc The template string in lowercase
	 * @return true if the template contains information-like fields
	 */
	public static boolean isInformationLikeTemplate(String vallc) {
		Pattern patternAuthor = Pattern.compile("\\|\\s*" + FIELD_AUTHOR + "\\s*=");
		Pattern patternPhotographer = Pattern.compile("\\|\\s*" + FIELD_PHOTOGRAPHER + "\\s*=");
		Pattern patternDate = Pattern.compile("\\|\\s*" + FIELD_DATE + "\\s*=");
		Pattern patternDescription = Pattern.compile("\\|\\s*" + FIELD_DESCRIPTION + "\\s*=");
		Pattern patternLicense = Pattern.compile("\\|\\s*" + FIELD_LICENSE + "\\s*=");
		Pattern patternPermission = Pattern.compile("\\|\\s*permission\\s*=");
		
		return patternAuthor.matcher(vallc).find() || patternPhotographer.matcher(vallc).find() || 
			   patternDate.matcher(vallc).find() || patternDescription.matcher(vallc).find() ||
			   patternLicense.matcher(vallc).find() || patternPermission.matcher(vallc).find();
	}
}

