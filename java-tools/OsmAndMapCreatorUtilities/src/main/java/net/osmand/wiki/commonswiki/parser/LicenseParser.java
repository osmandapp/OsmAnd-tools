package net.osmand.wiki.commonswiki.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for Wikimedia/Commons license information from license blocks
 * (after =={{int:license-header}}==).
 * <p>
 * Extracts license information from various license templates and formats.
 */
public final class LicenseParser {

	private static final String SELF_TEMPLATE = "self";
	private static final String STATUS_PARAM = "status=";
	private static final String COUNTRY_PARAM = "country";
	private static final String AUTHOR_PARAM = "author";

	private LicenseParser() {
		// Utility class - no instantiation
	}

	/**
	 * Parses license blocks and extracts license information.
	 * 
	 * @param licenseBlocks List of license block strings
	 * @return Comma-separated license string, or null if no license found
	 */
	public static String parse(List<String> licenseBlocks) {
		if (licenseBlocks == null || licenseBlocks.isEmpty()) {
			return null;
		}

		List<String> licenses = new ArrayList<>();

		for (String block : licenseBlocks) {
			String cleaned = cleanBlock(block);
			List<String> parts = ParserUtils.splitByPipeOutsideBraces(cleaned, false);

			for (String part : parts) {
				String license = processLicensePart(part);
				if (license != null && !license.isEmpty()) {
					licenses.add(license);
				}
			}
		}

		return licenses.isEmpty() ? null : String.join(" - ", licenses);
	}

	/**
	 * Parses license value from Information block (e.g., |license={{cc-by-sa-3.0|Author}}).
	 * 
	 * @param licenseValue The license value string
	 * @return Extracted license string, or null if not found
	 */
	public static String parseFromInformationBlock(String licenseValue) {
		if (licenseValue == null || licenseValue.isEmpty()) {
			return null;
		}

		// Extract license from template if present
		if (licenseValue.startsWith("{{") && licenseValue.endsWith("}}")) {
			return extractLicenseFromTemplate(licenseValue);
		} else {
			return licenseValue;
		}
	}

	/**
	 * Extracts license information from a template string.
	 * 
	 * @param template The template string in format {{template|...}}
	 * @return Extracted license string, or null if not found
	 */
	private static String extractLicenseFromTemplate(String template) {
		String templateContent = ParserUtils.extractTemplateContent(template);
		if (templateContent == null) {
			return null;
		}

		List<String> templateParts = ParserUtils.splitByPipeOutsideBraces(templateContent, true);
		if (templateParts.isEmpty()) {
			return null;
		}

		String templateName = templateParts.get(0).trim().toLowerCase();
		
		// For certain templates, extract license text from parameters
		if (templateName.contains("flickreviewr") || templateName.contains("reviewed")) {
			// Look for license description in parameters (usually the longest non-URL text)
			for (int i = templateParts.size() - 1; i > 0; i--) {
				String part = templateParts.get(i).trim();
				if (!part.isEmpty() && !part.startsWith("http") && part.length() > 10) {
					return part;
				}
			}
		}
		
		// For other templates, use template name
		return templateParts.get(0).trim();
	}

	/**
	 * Processes a single license string (e.g., from prepareMetaData).
	 * 
	 * @param license The license string to process
	 * @return Processed license string
	 */
	public static String process(String license) {
		if (license == null || license.isEmpty()) {
			return license;
		}

		String cleaned = ParserUtils.removeWikiLinkBrackets(license);

		return cleaned
				.replace("CC-", "CC ")
				.replace("PD-", "PD ")
				.replace("-expired", " expired")
				.toUpperCase();
	}

	private static String cleanBlock(String block) {
		if (block == null) {
			return "";
		}
		return ParserUtils.normalizeWhitespace(block);
	}

	private static String processLicensePart(String part) {
		part = part.trim();

		// Skip certain templates and parameters
		if (shouldSkipPart(part)) {
			return null;
		}

		// Handle parts with nested pipes
		if (part.contains("|")) {
			part = processNestedPipes(part);
		}

		// Handle author parameter
		if (part.toLowerCase().startsWith(AUTHOR_PARAM)) {
			return extractAuthorLicense(part);
		}

		// Extract license from template if present
		if (part.startsWith("{{") && part.endsWith("}}")) {
			return extractLicenseFromTemplate(part);
		}

		return part;
	}

	private static boolean shouldSkipPart(String part) {
		return part.equalsIgnoreCase(SELF_TEMPLATE) || part.contains(STATUS_PARAM);
	}

	private static String processNestedPipes(String part) {
		List<String> subParts = ParserUtils.splitByPipeOutsideBraces(part, true);
		String result = part;

		for (String subPart : subParts) {
			String subPartLc = subPart.toLowerCase();
			if (subPartLc.matches(".*" + COUNTRY_PARAM + "\\s*=\\s*.*") ||
				subPartLc.matches(".*" + AUTHOR_PARAM + "\\s*=\\s*.*")) {
				result = result.replace("|" + subPart, "");
			}
		}

		// Replace | with " - " (replace uses literal string, not regex)
		return result.replace("|", " - ");
	}

	private static String extractAuthorLicense(String part) {
		List<String> authorParts = ParserUtils.splitByPipeOutsideBraces(part, true);
		authorParts.removeIf(p -> p.trim().toLowerCase().startsWith(AUTHOR_PARAM));

		if (authorParts.isEmpty()) {
			return null;
		}

		return String.join(" - ", authorParts);
	}
}

