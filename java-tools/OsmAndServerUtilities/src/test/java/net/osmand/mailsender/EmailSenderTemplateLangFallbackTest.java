package net.osmand.mailsender;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertTrue;

public class EmailSenderTemplateLangFallbackTest {

	@Rule
	public TemporaryFolder templates = new TemporaryFolder();

	@Test
	public void exactLanguageFileIsUsed() throws IOException {
		assertTrue(load("ru").contains("Русский"));
	}

	@Test
	public void regionTaggedLanguageFallsBackToItsBaseLanguage() throws IOException {
		assertTrue(load("ru-RU").contains("Русский"));
		assertTrue(load("ru_RU").contains("Русский"));
	}

	@Test
	public void languageWithoutFileFallsBackToEnglish() throws IOException {
		assertTrue(load("de").contains("English"));
		assertTrue(load("de-DE").contains("English"));
	}

	private String load(String lang) throws IOException {
		File dir = templates.getRoot();
		write(new File(dir, "defaults.html"), "<!--From: a@a.com-->\n<!--Name: A-->\n");
		write(new File(dir, "test/en.html"), "Subject: test\nEnglish\n");
		write(new File(dir, "test/ru.html"), "Subject: test\nРусский\n");
		EmailSenderTemplate t = new EmailSenderTemplate();
		t.defaultTemplatesDirectory = dir.getPath();
		return t.load("test", lang).to("b@b.com").toString();
	}

	private static void write(File file, String content) throws IOException {
		file.getParentFile().mkdirs();
		Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
	}
}
