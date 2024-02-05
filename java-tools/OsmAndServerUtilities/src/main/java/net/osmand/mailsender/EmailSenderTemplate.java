package net.osmand.mailsender;

import com.sendgrid.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

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

Special variables:

	@TO@, @TO_BASE64@ - automatically set with To-address before sending each email
	@HTML_NEWLINE_TO_BR@ - user-defined option to convert \n to <br>\n (text-like templates)

Examples:

	new EmailSenderTemplate().template("Subject: test\nHi!").to("test@email").to("other@email").send().isSuccess();
	new EmailSenderTemplate().load("cloud/web", "en").to("osmand@t-online.de").set("TOKEN", "777").send().isSuccess();

*/

public class EmailSenderTemplate {
	private static final Log LOG = LogFactory.getLog(EmailSenderTemplate.class);

	public String defaultTemplatesDirectory = "/opt/osmand/web-server-config/templates/email";

	private SmtpSendGridSender sender;
	private int totalEmails, sentEmails;
	private List<String> toList = new ArrayList<>(); // added by to()
	private HashMap<String, String> vars = new HashMap<>(); // set by set()
	private String fromEmail, fromName, subject, body; // read from the template

	public static void test(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
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
			LOG.info("Using env EMAIL_TEMPLATES: " + templates);
		}

		final String smtpServer = System.getenv("SMTP_SERVER");
		if (smtpServer != null) {
			LOG.info("Using env SMTP_SERVER: " + smtpServer);
		}

		final String apiKey = System.getenv("SENDGRID_KEY");
		if (apiKey != null) {
			LOG.info("Using env SENDGRID_KEY: qwerty :-)");
		}

		this.sender = new SmtpSendGridSender(smtpServer, apiKey);
	}

	// send out email(s)
	public EmailSenderTemplate send() throws UnsupportedEncodingException {
		validateLoadedTemplates("send"); // final validation

		for(String to : toList) {
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
				LOG.info(to + ": sender response code: " + response.getStatusCode());
				if (response.getStatusCode() == sender.STATUS_CODE_SENT) {
					sentEmails++;
				}
			} catch (Exception e) {
				LOG.warn(e.getMessage(), e);
			}
		}
		return this;
	}

	// load template from file by template name and language code
	public EmailSenderTemplate load(String template, String lang) throws FileNotFoundException {
		include("defaults", lang, false); // settings (email-headers, vars, etc)
		include("header", lang, false); // optional
		include(template, lang, true); // template required
		include("footer", lang, false); // optional
		include("unsubscribe", lang, false); // optional
		validateLoadedTemplates(template);
		return this;
	}

	// load template from file (using default lang)
	public EmailSenderTemplate load(String template) throws FileNotFoundException {
		return load(template, "en");
	}

	// load template from string
	public EmailSenderTemplate template(String templateAsString) {
		parse(Arrays.asList(templateAsString.split("\n")));
		return this;
	}

	// load template from list
	public EmailSenderTemplate template(List<String> templateAsList) {
		parse(templateAsList);
		return this;
	}

	// optional set fromEmail, use before load()
	public EmailSenderTemplate from(String from) {
		fromEmail = from;
		return this;
	}

	// optional set fromName, use before load()
	public EmailSenderTemplate name(String name) {
		fromName = name;
		return this;
	}

	// optional set subject, use before load()
	public EmailSenderTemplate subject(String subject) {
		this.subject = subject;
		return this;
	}

	// add To from string
	public EmailSenderTemplate to(String email) throws UnsupportedEncodingException {
		setVarsByTo(email);
		toList.add(email);
		totalEmails++;
		return this;
	}

	// bulk add To(s) from list
	public EmailSenderTemplate to(List<String> emails) throws UnsupportedEncodingException {
		setVarsByTo(emails.isEmpty() ? "no@email" : emails.get(0));
		totalEmails += emails.size();
		toList.addAll(emails);
		return this;
	}

	// set variable (might be used later as @VAR@)
	public EmailSenderTemplate set(String key, String val) {
		vars.put(key, val);
		return this;
	}

	// bulk set variable(s) from map
	public EmailSenderTemplate set(HashMap<String, String> keyval) {
		vars.putAll(keyval);
		return this;
	}

	private String fill(String in) {
		String filled = in;
		if (filled != null) {
			for (String key : vars.keySet()) {
				filled = filled.replace("@" + key + "@", vars.get(key));
			}
			if (filled.matches("(?s)^.*@[A-Z_]+@.*$")) {
				throw new IllegalStateException(filled + ": error - please fill all tokens @A-Z@");
			}
		}
		return filled;
	}

	// should be called with distinct To before each send() iteration
	private void setVarsByTo(String to) throws UnsupportedEncodingException {
		String to64 = URLEncoder.encode(Base64.getEncoder().encodeToString(to.getBytes()), "UTF-8");
		set("TO", URLEncoder.encode(to, "UTF-8")); // @TO@
		set("TO_BASE64", to64); // @TO_BASE64@
	}

	private void validateLoadedTemplates(String template) {
		if (fromEmail == null || fromEmail.isEmpty()) {
			throw new IllegalStateException(template + ": fromEmail is not found (try <!--From: from@email-->)");
		}
		if (fromName == null || fromName.isEmpty()) {
			throw new IllegalStateException(template + ": fromName is not found (try <!--Name: CompanyName-->)");
		}
		if (subject == null || subject.isEmpty()) {
			throw new IllegalStateException(template + ": subject is not found (try <!--Subject: hello-->)");
		}
		if (body == null || body.isEmpty()) {
			throw new IllegalStateException(template + ": body is not found (please fill in template)");
		}
	}

	private final String HTML_COMMENTS = "(?s).*<!--.*?-->.*"; // (?s) for Pattern.DOTALL multiline mode
	private final String HTML_NEWLINE_TO_BR = "HTML_NEWLINE_TO_BR"; // user-defined var from templates

	private void parseCommandArgumentsFromComment(String line) {
		// <!--  Name  OsmAnd and co    -->
		// <!--From: @NOREPLY_MAIL_FROM@-->
		// <!--Set: HTML_NEWLINE_TO_BR=true-->
		// <!-- Set DEFAULT_MAIL_FROM = noreply@domain -->
		if (!line.matches(HTML_COMMENTS)) {
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
			} else if ("From".equalsIgnoreCase(command)) {
				fromEmail = argument;
			} else if ("Name".equalsIgnoreCase(command)) {
				fromName = argument;
			} else if ("Subject".equalsIgnoreCase(command)) {
				subject = argument;
			} else {
				throw new IllegalStateException(command + ": unknown template command");
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

	private void include(String template, String lang, boolean required) throws FileNotFoundException {
		File foundFile = findTemplateFile(template, lang);

		if (foundFile == null) {
			if (required) {
				throw new IllegalStateException(template + " template is not found. Use web-server-config repo.");
			}
			return; // silent
		}

		Scanner reader = new Scanner(foundFile);
		List<String> templateLines = new ArrayList<>();

		while (reader.hasNextLine()) {
			templateLines.add(reader.nextLine());
		}

		parse(templateLines);
	}

	private void parse(List<String> lines) {
		List<String> bodyLines = new ArrayList<>();

		for(String line : lines) {
			parseCommandArgumentsFromComment(line);

			String cleaned = line.replaceAll(HTML_COMMENTS, "");

			// allow to specify Subject-line at the beginning of the template
			if (bodyLines.isEmpty() && cleaned.trim().startsWith("Subject:")) {
				subject = cleaned.replaceFirst(".*?:", "").trim();
			}

			// blank lines are allowed after non-blank
			if (bodyLines.isEmpty() && cleaned.isEmpty()) {
				continue;
			}

			// add <br> at end of line if options is enabled and <br> is not already added
			if (vars.containsKey(HTML_NEWLINE_TO_BR) && !cleaned.toLowerCase().endsWith("<br>")) {
				bodyLines.add(cleaned + "<br>" + "\n");
			} else {
				bodyLines.add(cleaned + "\n");
			}
		}

		String joined = String.join("", bodyLines).replaceAll(HTML_COMMENTS, ""); // drop comments
		body = body == null ? joined : body + joined; // concat bodies from all included templates
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
						LOG.info("SendGrid sender is not configured: " + request.getBody());
						return error();
					}
				};
				LOG.warn("SendGrid sender is not configured");
			}
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

			return sendWithSendGrid(); // fallback
		}

		private Response sendWithSendGrid() throws IOException {
			Request request = new Request();
			request.setMethod(Method.POST);
			request.setEndpoint("mail/send");
			request.setBody(mail.build());
			return sendGridClient.api(request);
		}

		private Response sendWithSmtp() {
			if (smtpServer == null) {
				LOG.warn("SMTP_SERVER is not configured");
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
				String to = getTo();

				smtp.setFrom(from, name);
				smtp.setSubject(subject);
				smtp.setHtmlMsg(body);
				smtp.addTo(to);
				smtp.send();

			} catch(EmailException e) {
				LOG.warn("SMTP error: " + e.getMessage() + " (" + e.getCause().getMessage() + ")");
				return error();
			}

			return new Response(STATUS_CODE_SENT, "", null);
		}
	}
}
