package net.osmand.mailsender;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MsoConditionalCommentTest {

	@Rule
	public TemporaryFolder templates = new TemporaryFolder();

	private static final String MSO_SNIPPET =
			"Subject: test\n" +
			"<!--[if mso]>\n" +
			"<v:roundrect href=\"#\">VML button</v:roundrect>\n" +
			"<![endif]-->\n" +
			"<!--[if !mso]><!-->\n" +
			"<a href=\"#\">Real button</a>\n" +
			"<!--<![endif]-->\n";

	@Test
	public void bulletproofButtonMarkersSurviveIntactWithNoHeaderPollution_whenUseBase() {
		EmailSenderTemplate t = new EmailSenderTemplate()
				.set("USE_BASE", "true")
				.template(MSO_SNIPPET)
				.from("a@a.com").name("A").to("b@b.com");
		String body = t.toString();

		assertTrue("[if mso] opener must survive", body.contains("<!--[if mso]>"));
		assertTrue("VML content must survive", body.contains("<v:roundrect href=\"#\">VML button</v:roundrect>"));
		assertTrue("closing endif must survive", body.contains("<![endif]-->"));
		assertTrue("[if !mso] opener must survive", body.contains("<!--[if !mso]><!-->"));
		assertTrue("real anchor fallback must survive", body.contains("<a href=\"#\">Real button</a>"));
		assertTrue("closing marker must survive", body.contains("<!--<![endif]-->"));

		assertEquals("mso markers must never leak into email headers", 0, t.headers().size());
	}

	@Test
	public void legacyNonUseBaseTemplatesKeepTheOldBlindStripBehavior() {
		EmailSenderTemplate t = new EmailSenderTemplate()
				.template(MSO_SNIPPET)
				.from("a@a.com").name("A").to("b@b.com");
		String body = t.toString();

		assertTrue("[if mso] block, VML content included, is entirely deleted - old blind-strip behavior",
				!body.contains("<!--[if mso]>") && !body.contains("<v:roundrect href=\"#\">VML button</v:roundrect>"));
		assertTrue("the [if !mso] opener must NOT survive outside USE_BASE - old blind-strip behavior",
				!body.contains("<!--[if !mso]><!-->"));
		assertTrue("the real anchor itself is plain HTML (no <!-- around it) and always survives",
				body.contains("<a href=\"#\">Real button</a>"));

		assertEquals("mso markers must never leak into email headers, even outside USE_BASE",
				0, t.headers().size());
	}

	@Test
	public void headerKeepsMsoBlock_whenTemplateLoadedWithUseBase() throws IOException {
		EmailSenderTemplate t = loadFromTemplatesDir("<!--Set USE_BASE=true-->\n");
		String body = t.toString();

		assertTrue("[if mso] opener from header.html must survive", body.contains("<!--[if mso]>"));
		assertTrue("VML content from header.html must survive", body.contains("<v:roundrect href=\"#\">VML button</v:roundrect>"));
		assertTrue("template content must be wrapped by the header", body.contains("Template body"));
		assertEquals("mso markers must never leak into email headers", 0, t.headers().size());
	}

	@Test
	public void headerIsNotIncluded_whenTemplateLoadedWithoutUseBase() throws IOException {
		String body = loadFromTemplatesDir("").toString();

		assertFalse("header.html is only included for USE_BASE templates", body.contains("VML button"));
		assertTrue(body.contains("Template body"));
	}

	private EmailSenderTemplate loadFromTemplatesDir(String templateFlags) throws IOException {
		File dir = templates.getRoot();
		write(new File(dir, "defaults.html"), "<!--From: a@a.com-->\n<!--Name: A-->\n");
		write(new File(dir, "header.html"), MSO_SNIPPET.replace("Subject: test\n", ""));
		write(new File(dir, "test/en.html"), templateFlags + "Subject: test\n<p>Template body</p>\n");
		EmailSenderTemplate t = new EmailSenderTemplate();
		t.defaultTemplatesDirectory = dir.getPath();
		return t.load("test", "en").to("b@b.com");
	}

	private static void write(File file, String content) throws IOException {
		file.getParentFile().mkdirs();
		Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
	}
}
