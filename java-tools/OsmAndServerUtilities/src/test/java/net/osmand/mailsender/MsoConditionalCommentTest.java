package net.osmand.mailsender;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MsoConditionalCommentTest {

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
}
