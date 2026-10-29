package net.osmand.mailsender;

import com.sendgrid.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Year;
import java.time.ZoneOffset;
import java.util.*;
import java.util.regex.Pattern;

/*

Environment:

	SMTP_SERVER - address of smtp-server
	EMAIL_TEMPLATES - path to templates directory
	SENDGRID_KEY - SendGrid API key (optional for fallback)
	EMAIL_DELAY - optional delay before each send (in seconds)
	TEST_EMAIL_COPY - copy each email to this address (testing)

	DEFAULT_ENGINE = SMTP|SENDGRID (default = SMTP)

	EXCLUDE_DOMAINS - exclude from mailing *
	SMTP_DOMAINS - first try via SMTP server *
	SENDGRID_DOMAINS - first try via SendGrid *
	* Format is comma-separated substrings to match a part of the email address.

Template files structure:

	See templates/email/README.md in web-server-config.

Special variables:

	@TO@, @TO_BASE64@ - automatically set with To-address before sending each email

	@UNSUB_URL@ - unsubscribe link: an email that contains it gets List-Unsubscribe headers

Template variables:

	Variables may reference other variables at any depth (e.g. FIRST=1 SECOND=@FIRST@ THIRD=@SECOND@)

Public methods:

	load() - load template from file (+lang)

	set() - set template variable (@TOKEN@ etc)

	from() / name() - set From: email / name (optional)
	subject() - set Subject: (optional)

	to() - add email(s) To:
	send() - send emails

	isSuccess() - return boolean status (true if all emails sent)

Return values:

	Most methods return "this" to allow chained-style calls.
	Finally, isSuccess() returns boolean to check the status.

Examples:

	new EmailSenderTemplate().load("cloud/account/login", "de").to("test@email").set("TOKEN", "777").send().isSuccess();

*/

public class EmailSenderTemplate {
	private static final Log LOG = LogFactory.getLog(EmailSenderTemplate.class);
	private static final String UNSUB_URL = "UNSUB_URL";
	static final String TEMPLATES_ENV = "EMAIL_TEMPLATES";

	public String defaultTemplatesDirectory = "./templates";

	private final SmtpSendGridSender sender;
	private int totalEmails, sentEmails;
	private final String testEmailCopy; // TEST_EMAIL_COPY set by env
	private final List<String> toList = new ArrayList<>(); // added by to()
	private final HashMap<String, String> vars = new HashMap<>(); // set by set()
	private String fromEmail, fromName, subject, body; // read from the template
	private final HashMap<String, String> headers = new HashMap<>(); // List-Unsubscribe headers (added by render())

	private final String defaultEngine = System.getenv("DEFAULT_ENGINE");
	private final String excludedDomains = System.getenv("EXCLUDE_DOMAINS");
	private final String sendGridDomains = System.getenv("SENDGRID_DOMAINS");
	private final String smtpDomains = System.getenv("SMTP_DOMAINS");

	public String toString() {
		return String.format("From: %s (%s)\nSubject: %s\n\n%s\n",
				fill(String.valueOf(fromEmail)), fill(String.valueOf(fromName)), fill(String.valueOf(subject)), fill(String.valueOf(body)));
	}

	public boolean isSuccess() {
		return totalEmails > 0 && totalEmails == sentEmails;
	}

	// configure by environment
	public EmailSenderTemplate() {
		final String templates = System.getenv(TEMPLATES_ENV);
		if (templates != null) {
			this.defaultTemplatesDirectory = templates;
			// LOG.info("Using env EMAIL_TEMPLATES: " + templates);
		}

		final String smtpServer = System.getenv("SMTP_SERVER");
		final String apiKey = System.getenv("SENDGRID_KEY");

		testEmailCopy = System.getenv("TEST_EMAIL_COPY");

		this.sender = new SmtpSendGridSender(smtpServer, apiKey);
	}

	public EmailSenderTemplate send() {
		validateLoadedTemplates(); // final validation before send

		final String seconds = System.getenv("EMAIL_DELAY");
		if (seconds != null) {
			try {
				Thread.sleep(Integer.parseInt(seconds) * 1000L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		for (String to : toList) {
			String html = render(to);

			// build SendGrid-compatible objects

			Email toObject = new Email(to);

			Email fromObject = new Email(fill(fromEmail));
			fromObject.setName(fill(fromName));

			Content contentObject = new Content("text/html", html);

			Mail mailObject = new Mail(fromObject, fill(subject), toObject, contentObject);
			mailObject.from = fromObject;

			try {
				Response response = sender.send(mailObject);
				LOG.info(concealEmail(to) + ": sender response code: " + response.getStatusCode());
				if (response.getStatusCode() == sender.STATUS_CODE_SENT) {
					sentEmails++;
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}
		return this;
	}

	// lang comes from request parameters (e.g. the public /mapapi/auth/register) and becomes part of a translations
	// file path (translations/<lang>.properties), so only locale-shaped values are accepted - anything else, such as
	// "../../some/file", falls back to English instead of loading an arbitrary file into the email.
	private static final Pattern LANG_PATTERN = Pattern.compile("[a-zA-Z]{2,3}([-_][a-zA-Z0-9]{2,4})?");

	public static String safeLang(@Nullable String lang) {
		return lang != null && LANG_PATTERN.matcher(lang).matches() ? lang : EmailTemplate.DEFAULT_LANG;
	}

	public EmailSenderTemplate load(String template, @Nullable String lang) {
		EmailTemplate emailTemplate = new EmailTemplate(defaultTemplatesDirectory, template, safeLang(lang));
		vars.putAll(emailTemplate.vars);
		set("YEAR", String.valueOf(Year.now(ZoneOffset.UTC).getValue()));
		fromEmail = emailTemplate.from;
		fromName = emailTemplate.fromName;
		subject = emailTemplate.subject;
		body = emailTemplate.body;
		return this;
	}

	public EmailSenderTemplate load(String template) {
		return load(template, EmailTemplate.DEFAULT_LANG);
	}

	public EmailSenderTemplate from(String from) {
		fromEmail = from;
		return this;
	}

	public EmailSenderTemplate name(String name) {
		fromName = name;
		return this;
	}

	public EmailSenderTemplate subject(String subject) {
		this.subject = subject;
		return this;
	}

	public EmailSenderTemplate to(String email) {
		setVarsByTo(email);
		if (isDomainAllowed(email)) {
			toList.add(email);
			totalEmails++;
		}
		addTestEmail();
		return this;
	}

	public EmailSenderTemplate to(List<String> emails) {
		setVarsByTo(emails.isEmpty() ? "no@email" : emails.get(0));
		for (String email : emails) {
			if (isDomainAllowed(email)) {
				toList.add(email);
				totalEmails++;
			}
		}
		addTestEmail();
		return this;
	}

	public EmailSenderTemplate set(String key, String val) {
		vars.put(key, val);
		return this;
	}

	public EmailSenderTemplate set(Map<String, String> keyval) {
		vars.putAll(keyval);
		return this;
	}

	public boolean hasEmptyToList() {
		return toList.isEmpty();
	}

	Map<String, String> headers() {
		return headers;
	}

	private String fill(String in) {
		String filled = in;
		String previous;
		int passes = 0;
		do {
			if (passes++ > vars.size()) {
				throw new IllegalStateException("Template variables reference each other in a cycle");
			}
			previous = filled;
			for (String key : vars.keySet()) {
				filled = filled.replace("@" + key + "@", vars.get(key));
			}
		} while (!filled.equals(previous));
		if (filled.matches("(?s)^.*@[A-Z0-9_]+@.*$")) {
			throw new IllegalStateException(filled + ": error - please fill all tokens @A-Z0-9_@");
		}
		return filled;
	}

	private boolean isDomainAllowed(String email) {
		return ! sender.matchEmailAddress(email, excludedDomains);
	}

	// should be called with distinct To before each send() iteration
	private void setVarsByTo(String to) {
		String to64 = URLEncoder.encode(Base64.getEncoder().encodeToString(to.getBytes()), StandardCharsets.UTF_8);
		set("TO", URLEncoder.encode(to, StandardCharsets.UTF_8)); // @TO@
		set("TO_BASE64", to64); // @TO_BASE64@
	}

	String render(String to) {
		setVarsByTo(to);
		String html = fill(body);
		if (hasUnsubscribeLink(html)) {
			headers.put("List-Unsubscribe", "<@" + UNSUB_URL + "@>");
			headers.put("List-Unsubscribe-Post", "List-Unsubscribe=One-Click");
		}
		return html;
	}

	private boolean hasUnsubscribeLink(String html) {
		return vars.containsKey(UNSUB_URL) && html.contains(fill("@" + UNSUB_URL + "@"));
	}

	private void validateLoadedTemplates() {
		if (fromEmail == null || fromEmail.isEmpty()) {
			throw new IllegalStateException("Fatal: fromEmail is not found (try FROM=from@email in params.properties)");
		}
		if (fromName == null || fromName.isEmpty()) {
			throw new IllegalStateException("Fatal: fromName is not found (try FROM_NAME=CompanyName in params.properties)");
		}
		if (subject == null || subject.isEmpty()) {
			throw new IllegalStateException("Fatal: subject is not found (try <NAME>_SUBJECT=hello in translations/en.properties)");
		}
		if (body == null || body.isEmpty()) {
			throw new IllegalStateException("Fatal: body is not found (please fill template in)");
		}
	}

	public String concealEmail(String email) {
		return email.replaceFirst(".....", "....."); // do not discover email in logs
	}

	// compatible with SendGrid
	// derived from EmailSenderService
	private class SmtpSendGridSender {
		private final String smtpServer;
		private final SendGrid sendGridClient;
		private final int STATUS_CODE_ERROR = 500;
		private final int STATUS_CODE_SENT = 202; // success code 202 comes originally from SendGrid

		private enum Engine {
			SMTP,
			SENDGRID
		}

		private SmtpSendGridSender(String smtpServer, String apiKeySendGrid) {
			this.smtpServer = smtpServer;
			if (apiKeySendGrid != null) {
				sendGridClient = new SendGrid(apiKeySendGrid);
			} else {
				sendGridClient = new SendGrid(null) {
					@Override
					public Response api(Request request) {
						LOG.error("SendGrid sender is not configured: " + request.getBody());
						return error();
					}
				};
			}
		}

		private boolean matchEmailAddress(String email, String matchDomains) {
			if (matchDomains != null) {
				for (String domain : matchDomains.split("[,|]")) {
					if (email.toLowerCase().contains(domain.trim().toLowerCase())) {
						return true;
					}
				}
			}
			return false;
		}

		private Mail mail;

		private Response error() {
			return new Response(STATUS_CODE_ERROR, "ERROR", null);
		}

		private String getTo() {
			return mail.getPersonalization().get(0).getTos().get(0).getEmail();
		}

		private Response send(Mail mail) throws IOException {
			this.mail = mail;

			Engine firstTryEngine;

			if ("SMTP".equalsIgnoreCase(defaultEngine)) {
				firstTryEngine = Engine.SMTP;
			} else if ("SENDGRID".equalsIgnoreCase(defaultEngine)) {
				firstTryEngine = Engine.SENDGRID;
			} else {
				firstTryEngine = Engine.SMTP; // default
			}

			String to = getTo();
			boolean isSmtpDomain = matchEmailAddress(to, smtpDomains);
			boolean isSendGridDomain = matchEmailAddress(to, sendGridDomains);

			if (isSmtpDomain && !isSendGridDomain) {
				firstTryEngine = Engine.SMTP;
			} else if (isSendGridDomain && !isSmtpDomain) {
				firstTryEngine = Engine.SENDGRID;
			}

			Response firstResponse = firstTryEngine == Engine.SMTP ? sendWithSmtp() : sendWithSendGrid();

			if (firstResponse.getStatusCode() == STATUS_CODE_SENT) {
				return firstResponse;
			}

			LOG.warn("First Engine failed (" + firstResponse.getStatusCode() + ") - use fallback");
			return firstTryEngine == Engine.SMTP ? sendWithSendGrid() : sendWithSmtp();
		}

		// this.headers are not passed to SendGrid
		private Response sendWithSendGrid() throws IOException {
			Request request = new Request();
			request.setMethod(Method.POST);
			request.setEndpoint("mail/send");
			request.setBody(mail.build());
			return sendGridClient.api(request);
		}

		private Response sendWithSmtp() {
			if (smtpServer == null) {
				LOG.error("SMTP_SERVER is not configured");
				return error();
			}

			String to = getTo();

			try {
				org.apache.commons.mail.HtmlEmail smtp = new HtmlEmail();

				// server self-signed SSL cert requires fine tuning of mail.smtp.ssl.trust
				System.setProperty("mail.smtp.ssl.trust","*"); // required
				// smtp.setSSLCheckServerIdentity(false); // optional
				// smtp.setDebug(true); // optional

				smtp.setHostName(smtpServer);

				smtp.setSSLOnConnect(true); smtp.setSslSmtpPort("465"); // pure SMTPS (SSL)
				// smtp.setStartTLSEnabled(true); smtp.setSmtpPort(587); // plain SMTP + STARTTLS

				// smtp.setAuthentication("username", "password"); // optional SMTP auth

				smtp.setCharset("UTF-8");

				String name = mail.getFrom().getName();
				String from = mail.getFrom().getEmail();
				String subject = mail.getSubject();
				String body = mail.getContent().get(0).getValue(); // html content

				for (String key : headers.keySet()) {
					String val = fill(headers.get(key));
					smtp.addHeader(key, val);
				}

				if (body.toLowerCase().contains("<html")) {
					smtp.setHtmlMsg(body); // already html body
				} else {
					smtp.setHtmlMsg("<html>\n" + body + "</html>\n");
				}

				smtp.setFrom(from, name);
				smtp.setSubject(subject);
				smtp.addTo(to);

				smtp.send();

			} catch(EmailException e) {
				LOG.error("SMTP error: " + e.getMessage() + " (" + e.getCause().getMessage() + ")");
				return error();
			}

			return new Response(STATUS_CODE_SENT, "", null);
		}
	}

	private void addTestEmail() {
		if (testEmailCopy != null) {
			toList.add(testEmailCopy);
			totalEmails++;
		}
	}
}
