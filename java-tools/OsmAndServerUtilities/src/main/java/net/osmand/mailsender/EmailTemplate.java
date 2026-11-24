package net.osmand.mailsender;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

class EmailTemplate {

	static final String DEFAULT_LANG = "en";

	static final String PARAMS_FILE = "params.properties";
	static final String STYLES_FILE = "styles.properties";
	static final String HEADERS_FILE = "headers.properties";
	private static final String TRANSLATIONS_DIR = "translations";
	static final String EMAILS_DIR = "emails";
	private static final String BLOCKS_DIR = "blocks";
	private static final String LAYOUTS_DIR = "layouts";
	static final String HTML = ".html";
	private static final String PROPERTIES = ".properties";

	private static final String FROM = "FROM";
	private static final String FROM_NAME = "FROM_NAME";
	private static final String LAYOUT = "LAYOUT";

	private static final String SUBJECT = "_SUBJECT";
	private static final Map<String, String> NAME_TEXTS = Map.of(
			"EMAIL_TITLE", "TITLE", "PREHEADER", "PREHEADER", "FOOTER_REASON", "REASON", "INTRO", "INTRO", "NOTE", "NOTE");

	final Map<String, String> vars = new HashMap<>();
	final Map<String, String> headers;
	final String from, fromName, subject, body;

	private final String directory;
	private final Map<String, Map<String, String>> addedLines;

	static EmailTemplate fromFile(String directory, String template, String lang) {
		return new EmailTemplate(directory, template, lang, null, Map.of());
	}

	static EmailTemplate fromMarkup(String directory, String template, String markup,
	                                Map<String, Map<String, String>> addedLines, String lang) {
		return new EmailTemplate(directory, template, lang, markup, addedLines);
	}

	private EmailTemplate(String directory, String template, String lang, @Nullable String markup,
	                      Map<String, Map<String, String>> addedLines) {
		this.directory = directory;
		this.addedLines = addedLines;
		vars.putAll(readFile(STYLES_FILE));
		vars.putAll(readScoped(PARAMS_FILE, template));
		putBlocks();
		putTranslations(lang);
		String name = textsName(template);
		subject = "@" + name + SUBJECT + "@";
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
		NAME_TEXTS.forEach((key, suffix) -> vars.putIfAbsent(key, "@" + name + "_" + suffix + "@"));
		from = vars.remove(FROM);
		fromName = vars.remove(FROM_NAME);
		String layout = vars.remove(LAYOUT);
		body = markup != null ? markup : read(layout != null ? LAYOUTS_DIR + "/" + layout : EMAILS_DIR + "/" + template);
		headers = readScoped(HEADERS_FILE, template);
		headers.values().removeIf(String::isEmpty);
	}

	private String textsName(String template) {
		String path = template.toUpperCase(Locale.ROOT).replace('-', '_');
		String fullName = path.replace('/', '_');
		String fileName = path.substring(path.lastIndexOf('/') + 1);
		boolean oldName = vars.containsKey(fileName + SUBJECT) && !vars.containsKey(fullName + SUBJECT);
		return oldName ? fileName : fullName;
	}

	private Map<String, String> readScoped(String file, String template) {
		Map<String, String> properties = readFile(file);
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
			vars.putAll(readFile(translationsFile(fileLang)));
		}
	}

	static String translationsFile(String lang) {
		return TRANSLATIONS_DIR + "/" + lang + PROPERTIES;
	}

	private Map<String, String> readFile(String path) {
		Map<String, String> properties = readProperties(new File(directory, path));
		properties.putAll(addedLines.getOrDefault(path, Map.of()));
		return properties;
	}

	private String read(String path) {
		File file = new File(directory, path + HTML);
		if (!file.isFile()) {
			throw new IllegalStateException(path + ": template not found in " + directory +
					" - fetch web-server-config and set " + EmailSenderTemplate.TEMPLATES_HINT);
		}
		return readText(file);
	}

	static List<String> languages(String directory) {
		List<String> languages = new ArrayList<>();
		File[] files = new File(directory, TRANSLATIONS_DIR).listFiles((dir, file) -> file.endsWith(PROPERTIES));
		if (files != null) {
			for (File file : files) {
				String name = file.getName();
				languages.add(name.substring(0, name.length() - PROPERTIES.length()));
			}
		}
		Collections.sort(languages);
		return languages;
	}

	static Map<String, String> readProperties(File file) {
		if (!file.isFile()) {
			return new HashMap<>();
		}
		try (Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
			return readProperties(reader);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	static Map<String, String> readProperties(Reader reader) {
		Properties properties = new Properties();
		try {
			properties.load(reader);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
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
