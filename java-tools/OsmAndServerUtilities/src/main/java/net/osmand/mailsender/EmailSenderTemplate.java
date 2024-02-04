package net.osmand.mailsender;

import javax.swing.text.html.HTML;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// new EmailSenderTemplate("cloud/web", "ru").to("osmand@t-online.de").set("TOKEN", "777").send();

public class EmailSenderTemplate {
	public String dir = "/opt/osmand/web-server-config/templates/email";

	private List<String> toList = new ArrayList<>(); // added by to()
	private HashMap<String, String> vars = new HashMap<>(); // set by set()
	private String fromEmail, fromName, subject, body; // read from the template

	public static void main(String[] args) throws FileNotFoundException {
		new EmailSenderTemplate("cloud/web", "ru").to("osmand@t-online.de").set("TOKEN", "777").send();
	}

	public EmailSenderTemplate(String template, String lang) throws FileNotFoundException {
		include("defaults", lang, false); // settings (email-headers, vars, etc)
		include("header", lang, false); // optional
		include(template, lang, true); // template required
		include("footer", lang, false); // optional
		include("unsubscribe", lang, false); // optional
		validateLoadedTemplates(template);
	}

	public EmailSenderTemplate EmailSenderTemplate(String template) throws FileNotFoundException {
		return new EmailSenderTemplate(template, "en");
	}

	public EmailSenderTemplate to(String email) {
		toList.add(email);
		return this;
	}

	public EmailSenderTemplate to(List<String> emails) {
		toList.addAll(emails);
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

	public EmailSenderTemplate send() {
		// TODO
		return this;
	}

	private String fill(String in) {
		String filled = in;
		for (String key : vars.keySet()) {
			filled = filled.replace("@" + key + "@", vars.get(key));
		}
		return filled;
	}

	private void setVarsByTo(String to) {
		// TODO
		set("TO", to); // @TO@
		// set("TO_HASH", ...); // UNSUBHASH
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

	private final String HTML_COMMENTS = "(?s)<!--.*?-->"; // (?s) for Pattern.DOTALL multiline mode
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
		List<String> bodyLines = new ArrayList<>();

		while (reader.hasNextLine()) {
			String line = reader.nextLine();
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
