package net.osmand.server.api.services;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EmailSenderServiceEscapeTest {

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
}
