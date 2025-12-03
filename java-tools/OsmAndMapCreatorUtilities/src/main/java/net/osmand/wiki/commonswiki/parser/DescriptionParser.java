package net.osmand.wiki.commonswiki.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import net.osmand.wiki.WikiDatabasePreparation;

import static net.osmand.wiki.commonswiki.parser.ParserUtils.FIELD_DESCRIPTION;

/**
 * Parser for Wikimedia/Commons description fields (e.g. values of the {@code |description=} parameter
 * inside {{Information}} / {{Artwork}} / {{Photograph}} templates).
 * <p>
 * Handles multilingual descriptions in format {@code {{lang|1=Description text}}} and plain text descriptions.
 * <p>
 * Examples:
 * <ul>
 *   <li>{@code |description={{uk|1=Київ}} => Map with "uk" -> "Київ"}</li>
 *   <li>{@code |description=Some text => Map with "en" -> "Some text"}</li>
 * </ul>
 */
public final class DescriptionParser {

	private static final int MAX_LANGUAGE_CODE_LENGTH = 5;
	private static final Pattern LINK_PATTERN = Pattern.compile("\\[(https?://\\S+)\\s([^]]+)]");
	private static final Pattern PROVIDED_DESC_PATTERN = Pattern.compile("\\w+\\s+provided\\s+description\\s*:", Pattern.CASE_INSENSITIVE);

	private DescriptionParser() {
		// Utility class - no instantiation
	}

	/**
	 * Parses a description field from wiki markup.
	 * 
	 * @param line The line containing the description field
	 * @return Map of language codes to description texts, empty map if no description found
	 * @throws IOException if wiki rendering fails
	 */
	public static Map<String, String> parse(String line) throws IOException {
		Map<String, String> result = new HashMap<>();
		String descriptionBlock = line;

		// Parse multilingual descriptions in format {{lang|1=text}}
		descriptionBlock = parseMultilingualDescriptions(descriptionBlock, result);

		// If no multilingual descriptions found, parse as plain text
		if (result.isEmpty()) {
			parsePlainTextDescription(descriptionBlock, result);
		}

		return result;
	}

	private static String parseMultilingualDescriptions(String descriptionBlock, Map<String, String> result) {
		while (descriptionBlock.contains("{{") && descriptionBlock.contains("}}")) {
			LanguageBlock block = extractLanguageBlock(descriptionBlock);
			if (block == null) {
				break;
			}

			String cleanedDescription = cleanDescriptionText(block.content);
			result.put(block.language, cleanedDescription);
			descriptionBlock = descriptionBlock.substring(block.endIndex).trim();
		}
		return descriptionBlock;
	}

	private static LanguageBlock extractLanguageBlock(String descriptionBlock) {
		int langStart = descriptionBlock.indexOf("{{") + 2;
		int langEnd = descriptionBlock.indexOf("|", langStart);
		if (langEnd == -1) {
			return null;
		}

		String lang = descriptionBlock.substring(langStart, langEnd).trim();
		if (lang.length() > MAX_LANGUAGE_CODE_LENGTH) {
			return null;
		}

		int descStart = findDescriptionStart(descriptionBlock, langEnd);
		int descEnd = findDescriptionEnd(descriptionBlock, descStart);

		if (descEnd == -1) {
			return null;
		}

		String content = descriptionBlock.substring(descStart, descEnd);
		return new LanguageBlock(lang, content, descEnd);
	}

	private static int findDescriptionStart(String descriptionBlock, int langEnd) {
		int explicitStart = descriptionBlock.indexOf("|1=", langEnd);
		if (explicitStart != -1) {
			return explicitStart + 3;
		}
		return langEnd + 1;
	}

	private static int findDescriptionEnd(String descriptionBlock, int startIndex) {
		int openBraces = 1;
		int currentIndex = startIndex;

		while (openBraces > 0 && currentIndex < descriptionBlock.length()) {
			if (descriptionBlock.startsWith("{{", currentIndex)) {
				openBraces++;
				currentIndex += 2;
			} else if (descriptionBlock.startsWith("}}", currentIndex)) {
				openBraces--;
				currentIndex += 2;
			} else {
				currentIndex++;
			}
		}

		return openBraces == 0 ? currentIndex : -1;
	}

	private static String cleanDescriptionText(String description) {
		return description
				.replaceAll("\\{\\{[^|]+\\|", "")  // Remove nested templates {{...|
				.replaceAll("}}", "")               // Remove closing braces }}
				.replaceAll("\\[\\[:[^|]+\\|", "")   // Remove links [[...|
				.replaceAll("]]", "")               // Remove closing ]]
				.replaceAll("\\{\\{", "")            // Remove remaining opening braces {{
				.replaceAll("}$", "")                // Remove trailing brace
				.trim();
	}

	private static void parsePlainTextDescription(String descriptionBlock, Map<String, String> result) throws IOException {
		String description = ParserUtils.extractFieldValue(descriptionBlock, FIELD_DESCRIPTION);
		if (description == null) {
			description = descriptionBlock.trim();
		}
		
		// Remove leading/trailing newlines and whitespace
		description = description.trim();

		if (description.startsWith("{{")) {
			String plainText = renderWikiText(description);
			plainText = plainText.trim();
			// If bliki returned empty string or "Template:..." (unknown template), extract template name
			if (plainText.isEmpty() || plainText.startsWith("Template:")) {
				plainText = extractTemplateName(description);
			}
			if (!plainText.isEmpty()) {
				result.put(WikiDatabasePreparation.DEFAULT_LANG, plainText);
			}
			return;
		}

		// Remove prefix pattern like "X provided description: " if present
		// This handles cases like "500px provided description: text" -> "text"
		// Pattern: word(s) + "provided description:" (case-insensitive)
		Matcher matcher = PROVIDED_DESC_PATTERN.matcher(description);
		if (matcher.find() && matcher.start() < description.length() / 3) {
			// Only apply if pattern is in the first third (likely a prefix)
			description = description.substring(matcher.end()).trim();
		}

		String plainText = renderWikiText(description);
		// Remove leading/trailing newlines from rendered text
		plainText = plainText.trim();
		
		List<String> links = extractLinks(description);
		
		if (!links.isEmpty()) {
			plainText = appendLinks(plainText, links);
		}

		result.put(WikiDatabasePreparation.DEFAULT_LANG, plainText);
	}

	private static String renderWikiText(String description) throws IOException {
		WikiModel wikiModel = new WikiModel("", "");
		return wikiModel.render(new PlainTextConverter(true), description);
	}

	/**
	 * Extracts template name from a template when bliki can't render it properly.
	 * For {{TemplateName|param1|param2}}, returns "TemplateName".
	 */
	private static String extractTemplateName(String template) {
		if (!template.startsWith("{{") || !template.endsWith("}}")) {
			return "";
		}
		
		String content = template.substring(2, template.length() - 2).trim();
		List<String> parts = ParserUtils.splitByPipeOutsideBraces(content, true);
		
		if (parts.isEmpty()) {
			return "";
		}
		
		// Return the template name (first part)
		return parts.get(0).trim();
	}

	private static List<String> extractLinks(String description) {
		List<String> links = new ArrayList<>();
		Matcher matcher = LINK_PATTERN.matcher(description);
		while (matcher.find()) {
			links.add(matcher.group(1)); // URL
		}
		return links;
	}

	private static String appendLinks(String text, List<String> links) {
		StringBuilder result = new StringBuilder(text);
		result.append("\n\nLinks:");
		for (int i = 0; i < links.size(); i++) {
			result.append("\n[").append(i + 1).append("] ").append(links.get(i));
		}
		return result.toString();
	}

	private record LanguageBlock(String language, String content, int endIndex) {
	}
}

