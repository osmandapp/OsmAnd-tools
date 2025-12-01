package net.osmand.wiki.commonswiki.parser;

import java.util.Arrays;
import java.util.List;

import net.osmand.wiki.WikiDatabasePreparation;

/**
 * Parser for Wikimedia/Commons author fields (e.g. values of the {@code |author=} parameter
 * inside {{Information}} / {{Artwork}} templates).
 * <p>
 * It extracts a plain author name from typical wiki markup:
 * templates (Creator/User/self/FlickreviewR, etc.), wikilinks, external links and plain text.
 */
public final class AuthorParser {

	public static String parse(String line) {
		String normalized = stripAuthorPrefix(line);
		return tryParserChain(normalized);
	}

	private static String tryParserChain(String normalized) {
		String[] parsers = {
			tryFromTemplateWithPipes(normalized),
			tryFromSimpleTemplate(normalized),
			tryFromParts(normalized)
		};

		for (String result : parsers) {
			if (result != null) {
				return result;
			}
		}

		return WikiDatabasePreparation.DEFAULT_STRING;
	}

	private static String stripAuthorPrefix(String line) {
		line = line.trim();
		String lineLc = line.toLowerCase();
		if (lineLc.startsWith("author=")) {
			return line.substring(7).trim();
		} else if (lineLc.startsWith("photographer=")) {
			return line.substring(13).trim();
		} else if (line.startsWith("|")) {
			String afterPipe = line.substring(1).trim();
			String afterPipeLc = afterPipe.toLowerCase();
			if (afterPipeLc.startsWith("author=") || afterPipeLc.startsWith("photographer=")) {
				int eqPos = line.indexOf("=");
				if (eqPos != -1) {
					return line.substring(eqPos + 1).trim();
				}
			}
		}
		return line;
	}

	private static String tryFromTemplateWithPipes(String line) {
		if (!line.startsWith("{{") || !line.endsWith("}}") || !line.contains("|")) {
			return null;
		}

		String templateContent = line.substring(2, line.length() - 2);
		List<String> params = WikiDatabasePreparation.splitByPipeOutsideBraces(templateContent, true);

		if (params.isEmpty()) {
			return null;
		}

		String fromAuthorParam = findAuthorParameter(params);
		if (fromAuthorParam != null) {
			return fromAuthorParam;
		}

		return tryUserCreatorFromTemplateName(params.get(0).trim());
	}

	private static String findAuthorParameter(List<String> params) {
		for (int i = 1; i < params.size(); i++) {
			String param = params.get(i).trim();
			if (param.toLowerCase().startsWith("author=")) {
				String authorValue = param.substring(7).trim();
				String author = processAuthorValue(authorValue);
				if (!author.equals(WikiDatabasePreparation.DEFAULT_STRING)) {
					return cleanAuthor(author);
				}
			}
		}
		return null;
	}

	private static String tryUserCreatorFromTemplateName(String templateName) {
		String extracted = extractUserCreatorName(templateName);
		return extracted != null ? cleanAuthor(extracted) : null;
	}

	private static String tryFromSimpleTemplate(String line) {
		if (!line.startsWith("{{") || !line.endsWith("}}") || line.contains("|")) {
			return null;
		}

		String content = line.substring(2, line.length() - 2).trim();
		String extracted = extractUserCreatorName(content);
		if (extracted != null) {
			return cleanAuthor(extracted);
		}

		return null;
	}

	private static String extractUserCreatorName(String content) {
		String contentLc = content.toLowerCase();
		if (!contentLc.startsWith("user:") && !contentLc.startsWith("creator:")) {
			return null;
		}

		String namePart = content.substring(content.indexOf(":") + 1);
		// Handle /Autor suffix (e.g., "Ralf Roletschek/Autor")
		int slashPos = namePart.indexOf("/");
		if (slashPos != -1) {
			namePart = namePart.substring(0, slashPos);
		}
		return namePart.trim();
	}

	private static String tryFromParts(String line) {
		List<String> parts = WikiDatabasePreparation.splitByPipeOutsideBraces(line, true);

		for (String part : parts) {
			String result = tryPartFormats(part.trim());
			if (result != null) {
				return result;
			}
		}

		return tryFallbackFromSinglePart(parts);
	}

	private static String tryPartFormats(String part) {
		String result = tryEditedBy(part);
		if (result != null) {
			return result;
		}

		result = tryPublisher(part);
		if (result != null) {
			return result;
		}

		result = tryHttpLink(part);
		if (result != null) {
			return result;
		}

		result = tryWikiLink(part);
		if (result != null) {
			return result;
		}

		return tryUserTemplateInPart(part);
	}

	private static String tryFallbackFromSinglePart(List<String> parts) {
		if (parts.size() != 1) {
			return null;
		}

		String singlePart = parts.get(0).trim();
		if (isSimpleText(singlePart)) {
			return cleanAuthor(singlePart);
		}

		return null;
	}

	private static boolean isSimpleText(String text) {
		return !text.startsWith("{{")
				&& !text.startsWith("[[")
				&& !text.startsWith("[http");
	}

	private static String tryEditedBy(String part) {
		if (part.contains("edited by")) {
			String author = part.substring(0, part.indexOf("edited by")).trim();
			return cleanAuthor(author);
		}
		return null;
	}

	private static String tryPublisher(String part) {
		if (part.startsWith("Publisher:")) {
			String author = part.substring("Publisher:".length()).trim();
			return cleanAuthor(author);
		}
		return null;
	}

	private static String tryHttpLink(String part) {
		if (!part.startsWith("[http")) {
			return null;
		}

		if (part.contains(" ")) {
			// [https://... Text] - extract text
			int spacePos = part.indexOf(" ", 5);
			int closePos = part.indexOf("]", spacePos);
			if (closePos != -1) {
				return cleanAuthor(part.substring(spacePos + 1, closePos).trim());
			}
		} else {
			// [https://...] - no text
			return WikiDatabasePreparation.DEFAULT_STRING;
		}

		return null;
	}

	private static String tryWikiLink(String part) {
		if (!part.startsWith("[[") || !part.endsWith("]]")) {
			return null;
		}

		String linkContent = part.substring(2, part.length() - 2);
		List<String> linkParts = WikiDatabasePreparation.splitByPipeOutsideBraces(linkContent, true);

		if (linkParts.size() > 1) {
			return cleanAuthor(linkParts.get(1).trim());
		}

		return extractNameFromWikiLink(linkParts.get(0).trim());
	}

	private static String extractNameFromWikiLink(String linkText) {
		if (linkText.contains(":")) {
			int lastColonPos = linkText.lastIndexOf(":");
			return cleanAuthor(linkText.substring(lastColonPos + 1).trim());
		}
		return cleanAuthor(linkText);
	}

	private static String tryUserTemplateInPart(String part) {
		for (String template : Arrays.asList("User", "Creator")) {
			String result = tryUserCreatorTemplate(part, template);
			if (result != null) {
				return result;
			}
		}
		return null;
	}

	private static String tryUserCreatorTemplate(String part, String template) {
		String prefix1 = "{{" + template + ":";
		String prefix2 = "[[" + template + ":";
		if (!part.startsWith(prefix1) && !part.startsWith(prefix2)) {
			return null;
		}

		int start = part.indexOf(":") + 1;
		int end = findUserTemplateEnd(part, start);
		if (end == -1) {
			return null;
		}

		String userSection = part.substring(start, end).trim();
		return extractUserSectionName(userSection);
	}

	private static int findUserTemplateEnd(String part, int start) {
		int end = part.indexOf("/", start);
		if (end != -1) {
			return end;
		}
		return part.indexOf(part.startsWith("{{") ? "}}" : "]]", start);
	}

	private static String extractUserSectionName(String userSection) {
		List<String> userParts = WikiDatabasePreparation.splitByPipeOutsideBraces(userSection, true);
		if (userParts.size() > 1) {
			return cleanAuthor(userParts.get(1).trim());
		}
		return cleanAuthor(userSection.trim());
	}

	/**
	 * Processes the extracted author value, handling various formats.
	 */
	private static String processAuthorValue(String authorPart) {
		if (authorPart.isEmpty()) {
			return WikiDatabasePreparation.DEFAULT_STRING;
		}

		// Handle [[User:Name|DisplayName]] or [[User:Name]] - reuse wiki link parsing
		if (authorPart.startsWith("[[") && authorPart.endsWith("]]")) {
			String result = tryWikiLink(authorPart);
			if (result != null) {
				return result;
			}
		}

		// Handle comma-separated values (take first part)
		if (authorPart.contains(",")) {
			return authorPart.split(",")[0].trim();
		}

		// Handle pipe-separated values in simple cases
		if (authorPart.contains("|") && !authorPart.startsWith("{{") && !authorPart.startsWith("[[")) {
			// Simple pipe, not in template/link
			String[] parts = authorPart.split("\\|", 2);
			return parts[0].trim();
		}

		return authorPart.trim();
	}

	/**
	 * Cleans up the author string, removing brackets and extra formatting.
	 */
	private static String cleanAuthor(String author) {
		if (author.equals(WikiDatabasePreparation.DEFAULT_STRING)) {
			return author;
		}
		// Remove brackets and pipes
		author = author.replaceAll("\\[+|]+", "");
		// Remove template markers if any
		author = author.replaceAll("\\{\\{|\\}\\}", "");
		// Clean up whitespace
		author = author.trim();
		return author.isEmpty() ? WikiDatabasePreparation.DEFAULT_STRING : author;
	}
}


