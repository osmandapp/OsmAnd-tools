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
		putAll(readProperties(new File(directory, STYLES_FILE)));
		putParams(template);
		putBlocks();
		putTranslations(lang);
		from = vars.remove(FROM);
		fromName = vars.remove(FROM_NAME);
		String layout = vars.remove(LAYOUT);
		body = read(layout != null ? LAYOUTS_DIR + "/" + layout : template);
	}

	private void putParams(String template) {
		Properties params = readProperties(new File(directory, PARAMS_FILE));
		for (String key : params.stringPropertyNames()) {
			if (!key.contains(".")) {
				vars.put(key, params.getProperty(key));
			}
		}
		for (String scope : template.split("/")) {
			String prefix = scope + ".";
			for (String key : params.stringPropertyNames()) {
				if (key.startsWith(prefix)) {
					vars.put(key.substring(prefix.length()), params.getProperty(key));
				}
			}
		}
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
			putAll(readProperties(translationsFile(fileLang)));
		}
	}

	private File translationsFile(String lang) {
		return new File(directory, TRANSLATIONS_DIR + "/" + lang + PROPERTIES);
	}

	private void putAll(Properties properties) {
		properties.stringPropertyNames().forEach(key -> vars.put(key, properties.getProperty(key)));
	}

	private String read(String template) {
		File file = new File(directory, template + HTML);
		if (!file.isFile()) {
			throw new IllegalStateException(template + ": template not found in " + directory +
					" - fetch web-server-config and set " + EmailSenderTemplate.TEMPLATES_HINT);
		}
		return readText(file);
	}

	static Properties readProperties(File file) {
		Properties properties = new Properties();
		if (file.isFile()) {
			try (Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
				properties.load(reader);
				properties.replaceAll((key, value) -> ((String) value).strip());
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
		return properties;
	}

	private static String readText(File file) {
		try {
			return Files.readString(file.toPath(), StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
