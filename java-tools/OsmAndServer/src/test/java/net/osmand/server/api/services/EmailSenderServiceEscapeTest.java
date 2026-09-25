package net.osmand.server.api.services;

import net.osmand.mailsender.EmailSenderTemplate;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EmailSenderServiceEscapeTest {

	private static final String MSO_SNIPPET =
			"Subject: test\n" +
			"<!--[if mso]>\n" +
			"<v:roundrect href=\"#\">VML button</v:roundrect>\n" +
			"<![endif]-->\n" +
			"<!--[if !mso]><!-->\n" +
			"<a href=\"#\">Real button</a>\n" +
			"<!--<![endif]-->\n";

	@Test
	public void htmlTextEscapesMarkupAndTemplateTokens() {
		String v = EmailSenderService.htmlText("<a href=\"https://evil.example\">@SUPPORT_EMAIL@</a> & 'x'");
		assertFalse(v.contains("<"));
		assertFalse(v.contains(">"));
		assertFalse(v.contains("\""));
		assertFalse(v.contains("'"));
		assertFalse("no raw @ left to form an @VAR@ token", v.contains("@"));
		assertTrue(v.contains("&lt;a href=&quot;https://evil.example&quot;&gt;"));
		assertTrue(v.contains("&#64;SUPPORT_EMAIL&#64;"));
	}

	@Test
	public void plainTextStripsLineBreaksAndTemplateTokens() {
		String v = EmailSenderService.plainText("trip.gpx\r\nBcc: victim@example.com @TOKEN@ @A@B_1@");
		assertFalse(v.contains("\r"));
		assertFalse(v.contains("\n"));
		assertFalse("no @VAR@ token left", v.matches("(?s).*@[A-Z0-9_]+@.*"));
		assertEquals("trip.gpx Bcc: victim@example.com ＠TOKEN@ ＠A＠B_1@", v);
	}

	@Test
	public void plainTextKeepsOrdinaryAtSign() {
		assertEquals("a@b.gpx", EmailSenderService.plainText("a@b.gpx"));
	}

	@Test
	public void nullBecomesEmpty() {
		assertEquals("", EmailSenderService.htmlText(null));
		assertEquals("", EmailSenderService.plainText(null));
	}

	@Test
	public void bulletproofButtonMarkersSurviveIntactWithNoHeaderPollution_whenUseBase() throws Exception {
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

		assertEquals("mso markers must never leak into email headers", 0, headersOf(t).size());
	}

	@Test
	public void legacyNonUseBaseTemplatesKeepTheOldBlindStripBehavior() throws Exception {
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
				0, headersOf(t).size());
	}

	private static HashMap<String, String> headersOf(EmailSenderTemplate t) throws Exception {
		Field f = EmailSenderTemplate.class.getDeclaredField("headers");
		f.setAccessible(true);
		@SuppressWarnings("unchecked")
		HashMap<String, String> headers = (HashMap<String, String>) f.get(t);
		return headers;
	}
}
