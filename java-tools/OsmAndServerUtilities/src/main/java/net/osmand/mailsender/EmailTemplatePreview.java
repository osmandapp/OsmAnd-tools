package net.osmand.mailsender;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
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
	private static final Set<String> SHARED_DIRS = Set.of(EmailTemplate.BLOCKS_DIR, EmailTemplate.LAYOUTS_DIR);

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
		String templates = System.getenv(EmailSenderTemplate.TEMPLATES_ENV);
		if (templates == null) {
			throw new IllegalStateException("set " + EmailSenderTemplate.TEMPLATES_ENV + "=/path/to/templates/email (environment)");
		}
		Properties samples = EmailTemplate.readProperties(new File(templates, SAMPLES_FILE));
		String layoutTemplates = (String) samples.remove(LAYOUT_TEMPLATES);
		List<String> ids = id == null ? templateIds(Path.of(templates), layoutTemplates) : List.of(id);
		Map<String, String> vars = new HashMap<>();
		samples.stringPropertyNames().forEach(key -> vars.put(key, samples.getProperty(key)));

		Path out = Path.of(templates).toAbsolutePath().normalize().resolveSibling(PREVIEW_DIR).resolve(lang);
		for (String template : ids) {
			Path file = out.resolve(template + EmailTemplate.HTML);
			Files.createDirectories(file.getParent());
			Files.writeString(file, new EmailSenderTemplate().load(template, lang).set(vars).render(SAMPLE_TO));
			System.out.println(file.toUri());
		}
	}

	private static List<String> templateIds(Path templates, String layoutTemplates) throws IOException {
		List<String> ids = new ArrayList<>(List.of(layoutTemplates.split(",")));
		try (Stream<Path> files = Files.walk(templates)) {
			files.map(templates::relativize)
					.filter(file -> file.toString().endsWith(EmailTemplate.HTML))
					.filter(file -> !SHARED_DIRS.contains(file.getName(0).toString()))
					.map(file -> file.toString().replace(File.separatorChar, '/'))
					.map(file -> file.substring(0, file.length() - EmailTemplate.HTML.length()))
					.forEach(ids::add);
		}
		Collections.sort(ids);
		return ids;
	}
}
