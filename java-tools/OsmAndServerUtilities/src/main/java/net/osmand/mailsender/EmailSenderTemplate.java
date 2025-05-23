package net.osmand.mailsender;

import com.sendgrid.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*

Environment:

	SMTP_SERVER - address of smtp-server
	EMAIL_TEMPLATES - path to templates directory
	SENDGRID_KEY - SendGrid API key (optional for fallback)
	EMAIL_DELAY - optional delay before each send (in seconds)
	TEST_EMAIL_COPY - copy each email to this address (testing)

	EXCLUDE_DOMAINS - exclude from mailing (comma-separated substrings to match email)
	SENDGRID_DOMAINS - send directly via SendGrid (comma-separated substrings to match email)

Template files structure:

	Check load() to see which common templates may be included.
	Check findTemplateFile() to see how files are loaded from EMAIL_TEMPLATES directory.

Special variables:

	@TO@, @TO_BASE64@ - automatically set with To-address before sending each email

	@HTML_NEWLINE_TO_BR@ - user-defined option to convert \n to <br>\n (text-like templates)
		This option is disabled by default and never applied to "already-html" templates.
		If it is enabled in defaults.html, particular template might disable it with:
			<!--Set HTML_NEWLINE_TO_BR=false--> OR <!--Unset HTML_NEWLINE_TO_BR-->

Template variables:

	The template engine supports 1st and 2nd level variables (e.g. FIRST=1 SECOND=@FIRST@ THIRD=@SECOND@)

Public methods:

	load() - load template from file (+lang)
	template() - use string(s) as template (not file)

	set() - set template variable (@TOKEN@ etc)
	unset() - unset template variable

	from() / name() - set From: email / name (optional)
	subject() - set Subject: (optional)

	to() - add email(s) To:
	send() - send emails

	isSuccess() - return boolean status (true if all emails sent)

Return values:

	Most methods return "this" to allow chained-style calls.
	Finally, isSuccess() returns boolean to check the status.

Examples:

	new EmailSenderTemplate().template("Subject: test\nHi!").to("test@email").to("other@email").send().isSuccess();
	new EmailSenderTemplate().load("cloud/web", "en").to("osmand@t-online.de").set("TOKEN", "777").send().isSuccess();

*/

public class EmailSenderTemplate {
	private static final Log LOG = LogFactory.getLog(EmailSenderTemplate.class);

	public String defaultTemplatesDirectory = "./templates";

	private SmtpSendGridSender sender;
	private int totalEmails, sentEmails;
	private String testEmailCopy; // TEST_EMAIL_COPY set by env
	private List<String> toList = new ArrayList<>(); // added by to()
	private HashMap<String, String> vars = new HashMap<>(); // set by set()
	private String fromEmail, fromName, subject, body; // read from the template
	private HashMap<String, String> headers = new HashMap<>(); // optional email headers (read from the template)

	public static void test(String[] args) {
		EmailSenderTemplate sender = new EmailSenderTemplate()
				.load("cloud/web", "en")
				.to("dmarc-reports@osmand.net")
				.set("ACTION", "REGISTER")
				.set("TOKEN", "777")
				.send();
		System.err.printf("SUCCESS (%b)\n", sender.isSuccess());
	}

	public String toString() {
		return String.format("From: %s (%s)\nSubject: %s\n\n%s\n",
				fill(fromEmail), fill(fromName), fill(subject), fill(body));
	}

	public boolean isSuccess() {
		return totalEmails > 0 && totalEmails == sentEmails;
	}

	// configure by environment
	public EmailSenderTemplate() {
		final String templates = System.getenv("EMAIL_TEMPLATES");
		if (templates != null) {
			this.defaultTemplatesDirectory = templates;
			// LOG.info("Using env EMAIL_TEMPLATES: " + templates);
		}

		final String smtpServer = System.getenv("SMTP_SERVER");
		if (smtpServer != null) {
			// LOG.info("Using env SMTP_SERVER: " + smtpServer);
		}

		final String apiKey = System.getenv("SENDGRID_KEY");
		if (apiKey != null) {
			// LOG.info("Using env SENDGRID_KEY: qwerty :-)");
		}

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
			setVarsByTo(to);

			// build SendGrid-compatible objects

			Email toObject = new Email(to);

			Email fromObject = new Email(fill(fromEmail));
			fromObject.setName(fill(fromName));

			Content contentObject = new Content("text/html", fill(body));

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

	public EmailSenderTemplate load(String template, @Nullable String langNullable) {
		String lang = langNullable == null ? "en" : langNullable;
		include("defaults", lang, false); // settings (email-headers, vars, etc)
		include("header", lang, false); // optional
		include(template, lang, true); // template required
		include("footer", lang, false); // optional
		include("unsubscribe", lang, false); // optional
		return this;
	}

	public EmailSenderTemplate load(String template) {
		return load(template, "en");
	}

	public EmailSenderTemplate template(String templateAsString) {
		parse(Arrays.asList(templateAsString.split("\n")));
		return this;
	}

	public EmailSenderTemplate template(List<String> templateAsList) {
		parse(templateAsList);
		return this;
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

	public EmailSenderTemplate set(HashMap<String, String> keyval) {
		vars.putAll(keyval);
		return this;
	}

	public EmailSenderTemplate unset(String key) {
		vars.remove(key);
		return this;
	}

	public boolean hasEmptyToList() {
		return toList.isEmpty();
	}

	private String fill(String in) {
		String filled = in;
		if (filled != null) {
			final int PASSES = 2; // enough
			for (int i = 0; i < PASSES; i++) {
				for (String key : vars.keySet()) {
					filled = filled.replace("@" + key + "@", vars.get(key));
				}
			}
			if (filled.matches("(?s)^.*@[A-Z_]+@.*$")) {
				throw new IllegalStateException(filled + ": error - please fill all tokens @A-Z@");
			}
		}
		return filled;
	}

	private boolean isDomainAllowed(String email) {
		String excludedDomains = System.getenv("EXCLUDE_DOMAINS");
		if (excludedDomains != null) {
			for (String domain : excludedDomains.split("[,|]")) {
				if (email.toLowerCase().contains(domain.trim().toLowerCase())) {
					return false;
				}
			}
		}
		return true;
	}

	// should be called with distinct To before each send() iteration
	private void setVarsByTo(String to) {
		try {
			String to64 = null;
			to64 = URLEncoder.encode(Base64.getEncoder().encodeToString(to.getBytes()), "UTF-8");
			set("TO", URLEncoder.encode(to, "UTF-8")); // @TO@
			set("TO_BASE64", to64); // @TO_BASE64@
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	private void validateLoadedTemplates() {
		if (fromEmail == null || fromEmail.isEmpty()) {
			throw new IllegalStateException("Fatal: fromEmail is not found (try <!--From: from@email-->)");
		}
		if (fromName == null || fromName.isEmpty()) {
			throw new IllegalStateException("Fatal: fromName is not found (try <!--Name: CompanyName-->)");
		}
		if (subject == null || subject.isEmpty()) {
			throw new IllegalStateException("Fatal: subject is not found (try <!--Subject: hello-->)");
		}
		if (body == null || body.isEmpty()) {
			throw new IllegalStateException("Fatal: body is not found (please fill template in)");
		}
	}

	private final String HTML_COMMENT_MATCH = "(?s).*<!--.*?-->*.";
	private final String HTML_COMMENT_REPLACE = "(?s)<!--.*?-->"; // (?s) Pattern.DOTALL (multiline)
	private final String HTML_NEWLINE_TO_BR = "HTML_NEWLINE_TO_BR"; // user-defined var from templates

	private void parseCommandArgumentsFromComment(String line) {
		// <!--  Name  OsmAnd and co    -->
		// <!--From: @NOREPLY_MAIL_FROM@-->
		// <!--Set: HTML_NEWLINE_TO_BR=true-->
		// <!-- Set DEFAULT_MAIL_FROM = noreply@domain -->
		if (!line.matches(HTML_COMMENT_MATCH)) {
			return;
		}
		Matcher matcher = Pattern.compile("<!--.*?([A-Za-z-]+)[:\\s]+(.*?)\\s*-->").matcher(line);
		if (matcher.find()) {
			String command = matcher.group(1);
			String argument = matcher.group(2);
			if ("Set".equalsIgnoreCase(command)) {
				Matcher keyval = Pattern.compile("^(.*?)\\s*=\\s*(.*?)$").matcher(argument);
				if (keyval.find()) {
					set(keyval.group(1), keyval.group(2));
				}
			} else if ("Unset".equalsIgnoreCase(command)) {
				unset(argument);
			} else if ("From".equalsIgnoreCase(command)) {
				fromEmail = argument;
			} else if ("Name".equalsIgnoreCase(command)) {
				fromName = argument;
			} else if ("Subject".equalsIgnoreCase(command)) {
				subject = argument;
			} else {
				// optional headers, eg:
				// <!--List-Unsubscribe: <URL>-->
				// <!--List-Unsubscribe-Post: List-Unsubscribe=One-Click-->
				if (!argument.isEmpty()) {
					headers.put(command, argument);
				}
			}
		}
	}

	private File findTemplateFile(String template, String lang) {
		List<String> search = Arrays.asList(
				defaultTemplatesDirectory + "/" + template + "/" + lang + ".html", // dir/template/name/lang.html
				defaultTemplatesDirectory + "/" + template + "/" + "en" + ".html", // dir/template/name/en.html
				defaultTemplatesDirectory + "/" + template + "/" + "index" + ".html", // dir/template/name/index.html
				defaultTemplatesDirectory + "/" + template + ".html" // dir/template/name.html
		);

		for (String path : search) {
			String cleaned = path
					.replaceAll("\\\\+", "/") // replace \ to /
					.replaceAll("/+", "/") // replace // to /
					.replace("(../)+", ""); // remove ../
			File foundFile = new File(cleaned);
			if (foundFile.isFile()) {
				return foundFile;
			}
		}

		return null;
	}

	private void include(String template, String lang, boolean required) {
		File foundFile = findTemplateFile(template, lang);

		if (foundFile == null) {
			if (required) {
				throw new IllegalStateException(template + ": template not found in " + defaultTemplatesDirectory +
						" - fetch web-server-config and set EMAIL_TEMPLATES=/path/to/templates/email (environment)");
			}
			return; // silent
		}

		List<String> templateLines = new ArrayList<>();
		Scanner reader = null;
		try {
			reader = new Scanner(foundFile);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		while (reader.hasNextLine()) {
			templateLines.add(reader.nextLine());
		}
		reader.close();

		parse(templateLines);
	}

	private void parse(List<String> lines) {
		List<String> bodyLines = new ArrayList<>();
		boolean htmlTagFound = false;

		for (String line : lines) {
			parseCommandArgumentsFromComment(line);

			String cleaned = line.replaceAll(HTML_COMMENT_REPLACE, "");

			// allow to specify Subject-line at the beginning of the template
			if (bodyLines.isEmpty() && cleaned.trim().startsWith("Subject:")) {
				subject = cleaned.replaceFirst(".*?:", "").trim();
				continue;
			}

			// blank lines are allowed after non-blank
			if (bodyLines.isEmpty() && cleaned.isEmpty()) {
				continue;
			}

			// add <br> at end of line if options is enabled and <br> is not already added
			String lowercase = cleaned.toLowerCase();
			if (lowercase.contains("<html") || lowercase.contains("html>")) {
				htmlTagFound = true;
			}
			if (!htmlTagFound && "true".equals(vars.get(HTML_NEWLINE_TO_BR)) && !lowercase.endsWith("<br>")) {
				bodyLines.add(cleaned + "<br>" + "\n");
			} else {
				bodyLines.add(cleaned + "\n");
			}
		}

		String joined = String.join("", bodyLines).replaceAll(HTML_COMMENT_REPLACE, "");
		body = body == null ? joined : body + joined; // concat bodies from all included templates
	}

	public String concealEmail(String email) {
		return email.replaceFirst(".....", "....."); // do not discover email in logs
	}

	// compatible with SendGrid
	// derived from EmailSenderService
	private class SmtpSendGridSender {
		private String smtpServer;
		private SendGrid sendGridClient;
		private final int STATUS_CODE_ERROR = 500;
		private final int STATUS_CODE_SENT = 202; // success code 202 comes originally from SendGrid

		private SmtpSendGridSender(String smtpServer, String apiKeySendGrid) {
			this.smtpServer = smtpServer;
			if (apiKeySendGrid != null) {
				sendGridClient = new SendGrid(apiKeySendGrid);
			} else {
				sendGridClient = new SendGrid(null) {
					@Override
					public Response api(Request request) throws IOException {
						LOG.error("SendGrid sender is not configured: " + request.getBody());
						return error();
					}
				};
				LOG.error("SendGrid sender is not configured");
			}
		}

		private boolean enforceViaSendGrid(String to) {
			String sendGridDomains = System.getenv("SENDGRID_DOMAINS");
			if (sendGridDomains != null) {
				for (String domain : sendGridDomains.split("[,|]")) {
					if (to.toLowerCase().contains(domain.trim().toLowerCase())) {
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

			Response first = sendWithSmtp();

			if (first.getStatusCode() == STATUS_CODE_SENT) {
				return first;
			}

			LOG.warn("SMTP failed (" + first.getStatusCode() + ") - fallback to SendGrid");
			return sendWithSendGrid(); // fallback
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

			if (enforceViaSendGrid(to)) {
				LOG.warn(concealEmail(to) + ": send via SendGrid");
				return error();
			}

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
