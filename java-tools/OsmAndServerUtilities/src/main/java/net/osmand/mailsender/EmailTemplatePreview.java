package net.osmand.mailsender;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Stream;

/*

Run as Application in module OsmAndServerUtilities:

	EMAIL_TEMPLATES=/path/to/web-server-config/templates/email (environment)
	--id=marketing/start (optional argument, default all emails)
	--lang=de (optional argument, default en)

Prints a link to each email built in web-server-config/templates/email-preview/<lang>/.

*/

public class EmailTemplatePreview {

	private static final String ID_ARG = "--id=";
	private static final String LANG_ARG = "--lang=";
	private static final String SAMPLES_FILE = "preview.properties";
	private static final String PREVIEW_DIR = "email-preview";
	private static final String LAYOUT_TEMPLATES = "LAYOUT_TEMPLATES";
	private static final String SAMPLE_TO = "user@example.com";

	private final String templates;
	private final Map<String, String> samples;
	private final String layoutTemplates;

	public EmailTemplatePreview() {
		templates = EmailSenderTemplate.templatesDirectory();
		samples = EmailTemplate.readProperties(new File(templates, SAMPLES_FILE));
		layoutTemplates = samples.remove(LAYOUT_TEMPLATES);
		if (layoutTemplates == null) {
			throw new IllegalStateException(LAYOUT_TEMPLATES + " is missing in " + new File(templates, SAMPLES_FILE));
		}
	}

	public static void main(String[] args) throws IOException {
		String id = null;
		String lang = EmailTemplate.DEFAULT_LANG;
		for (String arg : args) {
			if (arg.startsWith(ID_ARG)) {
				id = arg.substring(ID_ARG.length());
			} else if (arg.startsWith(LANG_ARG)) {
				lang = EmailSenderTemplate.safeLang(arg.substring(LANG_ARG.length()));
			}
		}
		EmailTemplatePreview preview = new EmailTemplatePreview();
		List<String> ids = id == null ? preview.templateIds() : List.of(id);

		Path out = Path.of(preview.templates).toAbsolutePath().normalize().resolveSibling(PREVIEW_DIR).resolve(lang);
		for (String template : ids) {
			Path file = out.resolve(template + EmailTemplate.HTML);
			Files.createDirectories(file.getParent());
			String html = preview.render(template, lang);
			Files.writeString(file, html);
			List<String> missing = missingVariables(html);
			System.out.println(file.toUri() + (missing.isEmpty() ? "" : " - no value for " + String.join(", ", missing)));
		}
	}

	public List<String> templateIds() throws IOException {
		List<String> ids = new ArrayList<>(List.of(layoutTemplates.split(",")));
		Path emails = Path.of(templates, EmailTemplate.EMAILS_DIR);
		try (Stream<Path> files = Files.walk(emails)) {
			files.map(emails::relativize)
					.filter(file -> file.toString().endsWith(EmailTemplate.HTML))
					.map(file -> file.toString().replace(File.separatorChar, '/'))
					.map(file -> file.substring(0, file.length() - EmailTemplate.HTML.length()))
					.forEach(ids::add);
		}
		Collections.sort(ids);
		return ids;
	}

	public List<String> languages() {
		return EmailTemplate.languages(templates);
	}

	public String render(String id, @Nullable String lang) {
		return new EmailSenderTemplate().load(id, lang).set(samples).fillBodyKeepingMissing(SAMPLE_TO);
	}

	public Draft renderMarkup(String id, String markup, String texts, String params, String styles, String headers,
	                          @Nullable String lang) {
		Map<String, Map<String, String>> addedLines = Map.of(
				EmailTemplate.translationsFile(EmailTemplate.DEFAULT_LANG), lines(texts), EmailTemplate.PARAMS_FILE, lines(params),
				EmailTemplate.STYLES_FILE, lines(styles), EmailTemplate.HEADERS_FILE, lines(headers));
		EmailSenderTemplate email = new EmailSenderTemplate().loadMarkup(id, markup, addedLines, lang).set(samples);
		String html = email.fillBodyKeepingMissing(SAMPLE_TO);
		return new Draft(email.subjectKeepingMissing(), html, email.headersKeepingMissing());
	}

	public static List<String> missingVariables(String html) {
		Set<String> missing = new TreeSet<>();
		Matcher matcher = EmailSenderTemplate.VARIABLE.matcher(html);
		while (matcher.find()) {
			missing.add(matcher.group(1));
		}
		return new ArrayList<>(missing);
	}

	private static Map<String, String> lines(String properties) {
		return EmailTemplate.readProperties(new StringReader(properties));
	}

	public record Draft(String subject, String html, Map<String, String> headers) {
	}
}
