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

	private static final String PARAMS_FILE = "params.properties";
	private static final String STYLES_FILE = "styles.properties";
	private static final String TRANSLATIONS_DIR = "translations";
	private static final String BLOCKS_DIR = "blocks";
	private static final String LAYOUTS_DIR = "layouts";
	private static final String HTML = ".html";
	private static final String PROPERTIES = ".properties";

	private static final String FROM = "FROM";
	private static final String FROM_NAME = "FROM_NAME";
	private static final String FRAME = "FRAME";
	private static final String LAYOUT = "LAYOUT";

	private static final String UNSUBSCRIBE = "@UNSUBSCRIBE@";

	private static final Map<String, String> NAME_TEXTS = Map.of(
			"EMAIL_TITLE", "TITLE", "PREHEADER", "PREHEADER", "FOOTER_REASON", "REASON", "INTRO", "INTRO", "NOTE", "NOTE");

	final Map<String, String> vars = new HashMap<>();
	final String from, fromName, subject, body;

	private final String directory;

	EmailTemplate(String directory, String template, String lang) {
		this.directory = directory;
		String name = template.substring(template.lastIndexOf('/') + 1).toUpperCase(Locale.ROOT).replace('-', '_');
		subject = "@" + name + "_SUBJECT@";
		NAME_TEXTS.forEach((key, suffix) -> vars.put(key, "@" + name + "_" + suffix + "@"));
		putStyles();
		putParams(template);
		putBlocks();
		putTranslations(lang);
		from = vars.remove(FROM);
		fromName = vars.remove(FROM_NAME);

		String markup = read(vars.containsKey(LAYOUT) ? LAYOUTS_DIR + "/" + vars.get(LAYOUT) : template);
		String frame = vars.get(FRAME);
		body = frame == null ? withUnsubscribe(markup) : "@" + frame + "_HEADER@" + markup + "@" + frame + "_FOOTER@";
	}

	private static String withUnsubscribe(String html) {
		int bodyEnd = html.toLowerCase(Locale.ROOT).lastIndexOf("</body>");
		return bodyEnd < 0 ? html + UNSUBSCRIBE : html.substring(0, bodyEnd) + UNSUBSCRIBE + html.substring(bodyEnd);
	}

	private void putStyles() {
		Properties styles = readProperties(new File(directory, STYLES_FILE));
		styles.stringPropertyNames().forEach(key -> vars.put(key, styles.getProperty(key)));
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
		File defaultFile = new File(directory, TRANSLATIONS_DIR + "/" + DEFAULT_LANG + PROPERTIES);
		Properties translations = readProperties(defaultFile);
		File langFile = findTranslationsFile(lang);
		if (langFile != null && !langFile.equals(defaultFile)) {
			translations.putAll(readProperties(langFile));
		}
		translations.stringPropertyNames().forEach(key -> vars.put(key, translations.getProperty(key)));
	}

	@Nullable
	private File findTranslationsFile(String lang) {
		String baseLang = lang.split("[-_]")[0]; // ru-RU, pt_BR -> ru, pt
		for (String fileLang : new LinkedHashSet<>(List.of(lang, baseLang))) {
			File file = new File(directory, TRANSLATIONS_DIR + "/" + fileLang + PROPERTIES);
			if (file.isFile()) {
				return file;
			}
		}
		return null;
	}

	private String read(String template) {
		File file = new File(directory, template + HTML);
		if (!file.isFile()) {
			throw new IllegalStateException(template + ": template not found in " + directory +
					" - fetch web-server-config and set EMAIL_TEMPLATES=/path/to/templates/email (environment)");
		}
		return readText(file);
	}

	private static Properties readProperties(File file) {
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
