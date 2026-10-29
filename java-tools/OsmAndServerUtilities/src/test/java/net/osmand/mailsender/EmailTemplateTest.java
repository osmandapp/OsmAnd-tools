package net.osmand.mailsender;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EmailTemplateTest {

	@Rule
	public TemporaryFolder templates = new TemporaryFolder();

	private static final String HEADER = "<!--[if mso]>\n<v:roundrect href=\"#\">Header button</v:roundrect>\n<![endif]-->";
	private static final String MAILING_CONTENT =
			"<!--[if mso]>\n" +
			"<v:roundrect href=\"#\">VML button</v:roundrect>\n" +
			"<![endif]-->\n" +
			"<!--[if !mso]><!-->\n" +
			"<a href=\"#\">Real button</a>\n" +
			"<!--<![endif]-->\n" +
			"<!-- Hidden preheader -->\n" +
			"<p>Mailing body</p>\n";
	private static final String UNSUBSCRIBE = "<a href=\"@UNSUB_URL@\">Unsubscribe</a>";

	@Before
	public void createTemplates() throws IOException {
		templates.newFolder("translations");
		templates.newFolder("blocks");
		templates.newFolder("layouts");
		templates.newFolder("cloud", "account");
		templates.newFolder("cloud", "purchase");
		write("translations/en.properties",
				"LOGIN_SUBJECT=Login\n" +
				"LOGIN_TITLE=Title\n" +
				"SETUP_SUBJECT=Setup\n" +
				"SETUP_INTRO=Setup intro\n" +
				"PASSWORD_SUBJECT=Password\n" +
				"PASSWORD_INTRO=Password intro\n" +
				"RECEIPT_SUBJECT=Receipt\n" +
				"LINKED_SUBJECT=Linked\n" +
				"WELCOME_SUBJECT=Welcome\n" +
				"MAILING_SUBJECT=Mailing\n");
		write("translations/ru.properties", "LOGIN_TITLE=Заголовок\n");
		write("params.properties",
				"FROM=a@a.com\n" +
				"FROM_NAME=A\n" +
				"UNSUB_URL=https://osmand.net/unsubscribe?id=@TO_BASE64@\n" +
				"\n" +
				"purchase.COLOR=orange \n" +
				"welcome.FRAME=BASE\n" +
				"setup.LAYOUT=code\n" +
				"password.LAYOUT=code\n" +
				"linked.CARD_DETAILS=@DETAILS@\n");
		write("styles.properties", "COLOR=gray\n");
		write("blocks/BASE_HEADER.html", HEADER + "\n");
		write("blocks/BASE_FOOTER.html", "<p>Footer</p>\n");
		write("blocks/UNSUBSCRIBE.html", UNSUBSCRIBE + "\n");
		write("blocks/UNSUBSCRIBE_LINK.html", UNSUBSCRIBE + "\n");
		write("blocks/CARD.html", "<div>@CARD_DETAILS@</div>\n");
		write("blocks/DETAILS.html", "<i>@COLOR@</i>\n");
		write("layouts/code.html", "<p>@INTRO@</p>");
		write("cloud/account/login.html", "<h1>@EMAIL_TITLE@</h1><p>@COLOR@</p>");
		write("cloud/account/welcome.html", "<p>Welcome body</p>\n");
		write("cloud/purchase/receipt.html", "<p>@COLOR@</p>@UNSUBSCRIBE_LINK@");
		write("cloud/purchase/linked.html", "@CARD@");
		write("mailing.html", "<html><body>\n" + MAILING_CONTENT + "</body></html>\n");
	}

	@Test
	public void exactLanguageFileIsUsed() {
		assertTrue(load("cloud/account/login", "ru").toString().contains("<h1>Заголовок</h1>"));
	}

	@Test
	public void regionTaggedLanguageFallsBackToItsBaseLanguage() {
		assertTrue(load("cloud/account/login", "ru-RU").toString().contains("<h1>Заголовок</h1>"));
		assertTrue(load("cloud/account/login", "ru_RU").toString().contains("<h1>Заголовок</h1>"));
	}

	@Test
	public void languageWithoutFileFallsBackToEnglish() {
		assertTrue(load("cloud/account/login", "xx").toString().contains("<h1>Title</h1>"));
		assertTrue(load("cloud/account/login", "xx-XX").toString().contains("<h1>Title</h1>"));
	}

	@Test
	public void keyMissingInLanguageFileFallsBackToEnglish() {
		assertTrue(load("cloud/account/login", "ru").toString().contains("Subject: Login"));
	}

	@Test
	public void senderComesFromParams() {
		assertTrue(load("cloud/account/login").toString().startsWith("From: a@a.com (A)"));
	}

	@Test
	public void scopedParamAppliesOnlyToEmailsInItsFolder() {
		assertTrue(load("cloud/purchase/receipt").toString().contains("<p>orange</p>"));
		assertTrue(load("cloud/account/login").toString().contains("<p>gray</p>"));
	}

	@Test
	public void paramValuesAreTrimmed() {
		assertFalse(load("cloud/purchase/receipt").toString().contains("orange "));
	}

	@Test
	public void emailsWithOneLayoutShareItsMarkupWithTheirOwnTexts() {
		assertTrue(load("cloud/account/setup").toString().contains("<p>Setup intro</p>"));
		assertTrue(load("cloud/account/password").toString().contains("<p>Password intro</p>"));
	}

	@Test
	public void blockFileBecomesVariable() {
		assertTrue(load("cloud/purchase/receipt").toString().contains("<a href=\"https://osmand.net/unsubscribe?id="));
	}

	@Test
	public void blockInsideBlockIsResolved() {
		assertTrue(load("cloud/purchase/linked").toString().contains("<div><i>orange</i></div>"));
	}

	@Test
	public void frameWrapsEmails() {
		String body = load("cloud/account/welcome").toString();
		assertTrue("the header block must be included as is", body.contains(HEADER));
		assertTrue(body.contains("Welcome body"));
		assertFalse("the header block is only included for emails with a frame", load("cloud/account/login").toString().contains("Header button"));
	}

	@Test
	public void htmlIsSentAsIs() {
		assertTrue("comments and Outlook blocks must survive verbatim", load("mailing").toString().contains(MAILING_CONTENT));
	}

	@Test
	public void unsubscribeGoesBeforeBodyEnd() {
		String body = load("mailing").toString();
		assertTrue(body.indexOf("\">Unsubscribe</a>") < body.indexOf("</body>"));
	}

	@Test
	public void onlyUnsubscribeHeadersAreAdded() {
		assertEquals(Set.of("List-Unsubscribe", "List-Unsubscribe-Post"), renderedHeaders("mailing").keySet());
		assertTrue(renderedHeaders("cloud/purchase/receipt").containsKey("List-Unsubscribe"));
		assertTrue(renderedHeaders("cloud/account/welcome").isEmpty());
	}

	@Test(expected = IllegalStateException.class)
	public void missingTemplateFails() {
		load("cloud/account/missing");
	}

	private EmailSenderTemplate load(String template) {
		return load(template, EmailTemplate.DEFAULT_LANG);
	}

	private EmailSenderTemplate load(String template, String lang) {
		EmailSenderTemplate t = new EmailSenderTemplate();
		t.defaultTemplatesDirectory = templates.getRoot().getPath();
		return t.load(template, lang).to("b@b.com");
	}

	private Map<String, String> renderedHeaders(String template) {
		EmailSenderTemplate t = load(template);
		t.render("b@b.com");
		return t.headers();
	}

	private void write(String file, String content) throws IOException {
		Files.writeString(templates.newFile(file).toPath(), content);
	}
}
