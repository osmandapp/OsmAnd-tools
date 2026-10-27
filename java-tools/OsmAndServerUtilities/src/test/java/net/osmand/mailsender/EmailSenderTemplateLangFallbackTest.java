package net.osmand.mailsender;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class EmailSenderTemplateLangFallbackTest {

	@Rule
	public TemporaryFolder templates = new TemporaryFolder();

	@Before
	public void createTemplates() throws IOException {
		templates.newFolder("test");
		templates.newFile("test/en.html");
		templates.newFile("test/ru.html");
	}

	@Test
	public void exactLanguageFileIsUsed() {
		assertEquals("ru.html", find("ru"));
	}

	@Test
	public void regionTaggedLanguageFallsBackToItsBaseLanguage() {
		assertEquals("ru.html", find("ru-RU"));
		assertEquals("ru.html", find("ru_RU"));
	}

	@Test
	public void languageWithoutFileFallsBackToEnglish() {
		assertEquals("en.html", find("xx"));
		assertEquals("en.html", find("xx-XX"));
	}

	private String find(String lang) {
		return EmailSenderTemplate.findTemplateFile(templates.getRoot().getPath(), "test", lang).getName();
	}
}
