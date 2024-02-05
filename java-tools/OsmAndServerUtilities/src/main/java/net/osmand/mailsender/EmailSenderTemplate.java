package net.osmand.mailsender;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// new EmailSenderTemplate().template("...").to("osmand@t-online.de").set("TOKEN", "777").send();
// new EmailSenderTemplate().load("cloud/web", "ru").to("osmand@t-online.de").set("TOKEN", "777").send();

public class EmailSenderTemplate {
	public String dir = "/opt/osmand/web-server-config/templates/email";

	private int totalEmails, sentEmails;
	private List<String> toList = new ArrayList<>(); // added by to()
	private HashMap<String, String> vars = new HashMap<>(); // set by set()
	private String fromEmail, fromName, subject, body; // read from the template

	public static void main(String[] args) throws FileNotFoundException {
		EmailSenderTemplate sender = new EmailSenderTemplate()
//				.load("cloud/web", "ru")
				.template(
"<!--Set: DEFAULT_MAIL_FROM=contactus@osmand.net-->\n"+
"<!-- Set NOREPLY_MAIL_FROM = noreply@osmand.net -->\n"+
"<!--Set HTML_NEWLINE_TO_BR=true-->\n"+
"<!--From: @NOREPLY_MAIL_FROM@-->\n"+
"<!--Name: OsmAnd-->\n"+
"<!--Subject: hello-->\n"+
"Hello, world!\n"
				)
				.to("osmand@t-online.de")
				.to("copy@t-online.de")
				.set("ACTION", "REGISTER")
				.set("TOKEN", "777")
				.send();
		System.err.printf("%s\n", sender);
	}

	public String toString() {
		return String.format("From: %s (%s)\nSubject: %s\n\n%s\n",
				fill(fromEmail), fill(fromName), fill(subject), fill(body));
	}

	public EmailSenderTemplate() {
		// TODO settings, environment
	}

	// send out email(s)
	public EmailSenderTemplate send() {
		// TODO smtp/sendgrid support
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

	// load template from file (default lang)
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

	// add To from string
	public EmailSenderTemplate to(String email) {
		toList.add(email);
		totalEmails++;
		return this;
	}

	// bulk add To(s) from list
	public EmailSenderTemplate to(List<String> emails) {
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

	private void setVarsByTo(String to) throws UnsupportedEncodingException {
		String to64 = URLEncoder.encode(Base64.getEncoder().encodeToString(to.getBytes()), "UTF-8");
		set("TO_BASE64", to64); // @TO_BASE64@
		set("TO", to); // @TO@
	}

	private void validateLoadedTemplates(String template) {
		if (fromEmail == null || fromEmail.isEmpty()) {
			throw new IllegalStateException(template + " fromEmail is not found (try <!--From: from@email-->)");
		}
		if (fromName == null || fromName.isEmpty()) {
			throw new IllegalStateException(template + " fromName is not found (try <!--Name: CompanyName-->)");
		}
		if (subject == null || subject.isEmpty()) {
			throw new IllegalStateException(template + " subject is not found (try <!--Subject: hello-->)");
		}
		if (body == null || body.isEmpty()) {
			throw new IllegalStateException(template + " body is not found (please fill in template)");
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
				dir + "/" + template + "/" + lang + ".html", // dir/template/name/lang.html
				dir + "/" + template + "/" + "en" + ".html", // dir/template/name/en.html
				dir + "/" + template + "/" + "index" + ".html", // dir/template/name/index.html
				dir + "/" + template + ".html" // dir/template/name.html
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
				throw new IllegalStateException(template + " template is not found");
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

			if (bodyLines.isEmpty() && cleaned.isEmpty()) {
				continue;
			}

			if (vars.containsKey(HTML_NEWLINE_TO_BR) && !cleaned.toLowerCase().endsWith("<br>")) {
				bodyLines.add(cleaned + "<br>" + "\n");
			} else {
				bodyLines.add(cleaned + "\n");
			}
		}

		String joined = String.join("", bodyLines).replaceAll(HTML_COMMENTS, ""); // drop comments
		body = body == null ? joined : body + joined; // concat bodies from all included templates
	}
}
