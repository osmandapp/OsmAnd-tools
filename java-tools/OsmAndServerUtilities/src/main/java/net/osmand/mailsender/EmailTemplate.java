package net.osmand.mailsender;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

class EmailTemplate {

	static final String DEFAULT_LANG = "en";

	private static final String PARAMS_FILE = "params.properties";
	private static final String STYLES_FILE = "styles.properties";
	private static final String HEADERS_FILE = "headers.properties";
	private static final String TRANSLATIONS_DIR = "translations";
	static final String BLOCKS_DIR = "blocks";
	static final String LAYOUTS_DIR = "layouts";
	static final String HTML = ".html";
	private static final String PROPERTIES = ".properties";

	private static final String FROM = "FROM";
	private static final String FROM_NAME = "FROM_NAME";
	private static final String LAYOUT = "LAYOUT";

	private static final Map<String, String> NAME_TEXTS = Map.of(
			"EMAIL_TITLE", "TITLE", "PREHEADER", "PREHEADER", "FOOTER_REASON", "REASON", "INTRO", "INTRO", "NOTE", "NOTE");

	final Map<String, String> vars = new HashMap<>();
	final Map<String, String> headers;
	final String from, fromName, subject, body;

	private final String directory;

	EmailTemplate(String directory, String template, String lang) {
		this.directory = directory;
		String name = template.substring(template.lastIndexOf('/') + 1).toUpperCase(Locale.ROOT).replace('-', '_');
		subject = "@" + name + "_SUBJECT@";
		// EMAIL_TITLE   → TITLE
		// PREHEADER     → PREHEADER
		// FOOTER_REASON → REASON
		// INTRO         → INTRO
		// NOTE          → NOTE
		// For cloud/account/password the name is PASSWORD, and the line puts into the variables:
		// EMAIL_TITLE   = @PASSWORD_TITLE@
		// PREHEADER     = @PASSWORD_PREHEADER@
		// FOOTER_REASON = @PASSWORD_REASON@
		// INTRO         = @PASSWORD_INTRO@
		// NOTE          = @PASSWORD_NOTE@
		NAME_TEXTS.forEach((key, suffix) -> vars.put(key, "@" + name + "_" + suffix + "@"));
		vars.putAll(readProperties(new File(directory, STYLES_FILE)));
		vars.putAll(readScoped(PARAMS_FILE, template));
		putBlocks();
		putTranslations(lang);
		from = vars.remove(FROM);
		fromName = vars.remove(FROM_NAME);
		String layout = vars.remove(LAYOUT);
		body = read(layout != null ? LAYOUTS_DIR + "/" + layout : template);
		headers = readScoped(HEADERS_FILE, template);
		headers.values().removeIf(String::isEmpty);
	}

	private Map<String, String> readScoped(String file, String template) {
		Map<String, String> properties = readProperties(new File(directory, file));
		Map<String, String> values = new HashMap<>();
		for (String key : properties.keySet()) {
			if (!key.contains(".")) {
				values.put(key, properties.get(key));
			}
		}
		for (String scope : template.split("/")) {
			String prefix = scope + ".";
			for (String key : properties.keySet()) {
				if (key.startsWith(prefix)) {
					values.put(key.substring(prefix.length()), properties.get(key));
				}
			}
		}
		return values;
	}

	private void putBlocks() {
		File[] blocks = new File(directory, BLOCKS_DIR).listFiles((dir, file) -> file.endsWith(HTML));
		if (blocks != null) {
			for (File block : blocks) {
				String file = block.getName();
				vars.put(file.substring(0, file.length() - HTML.length()), readText(block).trim());
			}
		}
	}

	private void putTranslations(String lang) {
		String baseLang = lang.split("[-_]")[0]; // ru-RU, pt_BR -> ru, pt
		for (String fileLang : new LinkedHashSet<>(List.of(DEFAULT_LANG, baseLang, lang))) {
			vars.putAll(readProperties(translationsFile(fileLang)));
		}
	}

	private File translationsFile(String lang) {
		return new File(directory, TRANSLATIONS_DIR + "/" + lang + PROPERTIES);
	}

	private String read(String template) {
		File file = new File(directory, template + HTML);
		if (!file.isFile()) {
			throw new IllegalStateException(template + ": template not found in " + directory +
					" - fetch web-server-config and set " + EmailSenderTemplate.TEMPLATES_HINT);
		}
		return readText(file);
	}

	static Map<String, String> readProperties(File file) {
		Properties properties = new Properties();
		if (file.isFile()) {
			try (Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
				properties.load(reader);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
		Map<String, String> values = new HashMap<>();
		properties.stringPropertyNames().forEach(key -> values.put(key, properties.getProperty(key).strip()));
		return values;
	}

	private static String readText(File file) {
		try {
			return Files.readString(file.toPath(), StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
