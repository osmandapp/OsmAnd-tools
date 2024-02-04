package net.osmand.server.api.services;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.mail.EmailException;
import org.springframework.stereotype.Service;
import org.apache.commons.mail.HtmlEmail;

import com.sendgrid.Content;
import com.sendgrid.Email;
import com.sendgrid.FooterSetting;
import com.sendgrid.Mail;
import com.sendgrid.MailSettings;
import com.sendgrid.Method;
import com.sendgrid.Personalization;
import com.sendgrid.Request;
import com.sendgrid.Response;
import com.sendgrid.SendGrid;

import net.osmand.util.Algorithms;

@Service
public class EmailSenderService {
//	public static void main(String[] args) {
//		EmailSenderService sender = new EmailSenderService();
//		String email = "osmand@t-online.de";
//		sender.sendOsmRecipientsDeleteEmail(email);
//		sender.sendOsmAndCloudPromoEmail(email, "promo");
//		sender.sendOsmAndCloudWebEmail(email, "token", "action");
//		sender.sendOsmAndCloudRegistrationEmail(email, "token", true);
//		sender.sendPromocodesEmails(email, "59f7ce12-f28d-4d35-9b28-a21e22f00450", "promocodes");
//	}

    private static final Log LOGGER = LogFactory.getLog(EmailSenderService.class);

	private static final String DEFAULT_MAIL_FROM = "contactus@osmand.net";
	private static final String NOREPLY_MAIL_FROM = "noreply@osmand.net";

	private SmtpSendGridSender wrapper;

	private class SmtpSendGridSender {
		private String smtpServer;
		private SendGrid sendGridClient;
		private final int FAKE_SUCCESS_STATUS_CODE = 200;
		private final int SENDGRID_SUCCESS_STATUS_CODE = 202;

		private SmtpSendGridSender(String smtpServer, String apiKeySendGrid) {
			this.smtpServer = smtpServer;
			if (apiKeySendGrid != null) {
				sendGridClient = new SendGrid(apiKeySendGrid);
			} else {
				sendGridClient = new SendGrid(null) {
					@Override
					public Response api(Request request) throws IOException {
						LOGGER.info("Send grid emails is not configured: " + request.getBody());
						return fakeSuccess();
					}
				};
				LOGGER.warn("Send grid emails is not configured");
			}
		}

		private Mail mail;

		private Response fakeSuccess() {
			return new Response(FAKE_SUCCESS_STATUS_CODE, "", null);
		}

		private String getTo() {
			return mail.getPersonalization().get(0).getTos().get(0).getEmail();
		}

		private boolean isSmtpFirst() {
			if (mail.getTemplateId() != null) {
				return false; // templates are allowed with SendGrid only
			}
			if (getTo().contains("t-online") || getTo().contains("osmand.net") || getTo().contains("victor.shcherb")) {
				return true;
			}
			return true; // use SMTP by default
//			return Math.random() < 0.1; // try SMTP_SERVER on 10% of outgoing emails
		}

		private Response send(Mail mail) throws IOException {
			this.mail = mail;

			Response first = isSmtpFirst() ? sendWithSmtp() : sendWithSendGrid(); // first try

			if (first.getStatusCode() == SENDGRID_SUCCESS_STATUS_CODE) {
				return first;
			}

			return isSmtpFirst() ? sendWithSendGrid() : sendWithSmtp(); // second try (vice versa)
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
				LOGGER.warn("SMTP_SERVER is not configured");
				return fakeSuccess();
			}

			try {
				org.apache.commons.mail.Email smtp = new HtmlEmail();

				// server self-signed SSL cert requires fine tuning of mail.smtp.ssl.trust
				System.setProperty("mail.smtp.ssl.trust","*"); // required
				// smtp.setSSLCheckServerIdentity(false); // optional
				// smtp.setDebug(true); // optional

				smtp.setHostName(smtpServer);

				smtp.setSSLOnConnect(true); smtp.setSslSmtpPort("465"); // pure SMTPS (SSL)
				// smtp.setStartTLSEnabled(true); smtp.setSmtpPort(587); // plain SMTP + STARTTLS

				// smtp.setAuthentication("username", "password"); // optional SMTP auth

				String name = mail.getFrom().getName();
				String from = mail.getFrom().getEmail();
				String subject = mail.getSubject();
				String body = mail.getContent().get(0).getValue(); // html content
				String to = getTo();

				smtp.setFrom(from, name);
				smtp.setSubject(subject);
				smtp.setMsg(body);
				smtp.addTo(to);
				smtp.send();

			} catch(EmailException e) {
				LOGGER.warn("SMTP error: " + e.getMessage() + " (" + e.getCause().getMessage() + ")");
				return fakeSuccess();
			}

			return new Response(SENDGRID_SUCCESS_STATUS_CODE, "", null);
		}
	}

	private EmailSenderService() {
		final String smtpServer = System.getenv("SMTP_SERVER");
		final String apiKey = System.getenv("SENDGRID_KEY");
		wrapper = new SmtpSendGridSender(smtpServer, apiKey);
	}

    public void sendOsmAndCloudPromoEmail(String email, String promo) { // TODO cloud/promo
		LOGGER.info("Sending mail to: " + email);
		Email from = new Email(NOREPLY_MAIL_FROM);
		from.setName("OsmAnd");
		Email to = new Email(email);
		String topic = "Congratulations and welcome to OsmAnd Cloud!";
		StringBuilder contentStr = new StringBuilder();
		contentStr.append("Dear OsmAnd User!");
		contentStr.append("<br><br>");
		contentStr.append("You have been selected for OsmAnd Cloud promo subscription. Your promo OsmAnd Pro is <b>"+promo+"</b><br>");
		contentStr.append("Now you can open OsmAnd Settings -&gt; Backup and Restore and Login with this email to OsmAnd Cloud and get all features enabled.<br>");
		contentStr.append("<br><br>");
		contentStr.append("Best Regards, <br>OsmAnd Team");

		Content content = new Content("text/html", contentStr.toString());
		Mail mail = new Mail(from, topic, to, content);
		mail.from = from;
		try {
			Response response = wrapper.send(mail);
			LOGGER.info("Response code: " + response.getStatusCode());
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}
	}
    
    public void sendOsmAndCloudWebEmail(String email, String token, String action) { // TODO cloud/web
		LOGGER.info("Sending mail to: " + email);
		Email from = new Email(NOREPLY_MAIL_FROM);
		from.setName("OsmAnd");
		Email to = new Email(email);
		String topic = "Register web access to OsmAnd Cloud";
		StringBuilder contentStr = new StringBuilder();
		contentStr.append("Dear OsmAnd User!");
		contentStr.append("<br><br>");
		contentStr.append("Please use the following verification code to <b>" + action + "</b> your OsmAnd Cloud account. Your verification code is <b>"+token+"</b><br>");
		contentStr.append("<br><br>");
		contentStr.append("Best Regards, <br>OsmAnd Team");

		Content content = new Content("text/html", contentStr.toString());
		Mail mail = new Mail(from, topic, to, content);
		mail.from = from;
		try {
			Response response = wrapper.send(mail);
			LOGGER.info("Response code: " + response.getStatusCode());
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}
	}
    
    public void sendOsmAndCloudRegistrationEmail(String email, String token, boolean newUser) { // TODO cloud/register
		LOGGER.info("Sending mail to: " + email);
		Email from = new Email(NOREPLY_MAIL_FROM);
		from.setName("OsmAnd");
		Email to = new Email(email);
		String topic = newUser? "Welcome to OsmAnd Cloud" : "Register new device to OsmAnd Cloud";
		StringBuilder contentStr = new StringBuilder();
		contentStr.append("Dear OsmAnd User!");
		contentStr.append("<br><br>");
		contentStr.append("New device has been connected to OsmAnd Cloud. Your activation code is <b>"+token+"</b><br>");
		contentStr.append("You can also use <a clicktracking=\"off\" href='https://osmand.net/premium/device-registration?token=" + token
				+ "'>following link</a> to open it with OsmAnd.");
		contentStr.append("<br><br>");
		contentStr.append("Best Regards, <br>OsmAnd Team");

		Content content = new Content("text/html", contentStr.toString());
		Mail mail = new Mail(from, topic, to, content);
		mail.from = from;
		try {
			Response response = wrapper.send(mail);
			LOGGER.info("Response code: " + response.getStatusCode());
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}
	}
    
    // 6503d80a-4574-461d-95f1-2bd0c43392b8
    public boolean sendPromocodesEmails(String mailTo, String templateId, String promocodes) { // TODO promocode/{ios,android}
    	LOGGER.info("Sending mail to: " + mailTo);
        Email from = new Email(DEFAULT_MAIL_FROM);
        from.setName("OsmAnd");
        Mail mail = new Mail();
        mail.from = from;
        Personalization personalization = new Personalization();
        for(String mt : mailTo.split(",")) {
        	if(!Algorithms.isEmpty(mt)) {
        		personalization.addTo(new Email(mt));
        	}
        }
        mail.addPersonalization(personalization);
        mail.setTemplateId(templateId);
        MailSettings mailSettings = new MailSettings();
        FooterSetting footerSetting = new FooterSetting();
        footerSetting.setEnable(true);
        String footer = String.format("<center style='margin:5px 0px 5px 0px'>"
        		+ "<h2>Your promocode is '%s'.</h2></center>", promocodes) ;
        footerSetting.setHtml("<html>"+footer+"</html>");
        mailSettings.setFooterSetting(footerSetting);
        mail.setMailSettings(mailSettings);
        try {
            Response response = wrapper.send(mail);
            LOGGER.info("Response code: " + response.getStatusCode());
            return true;
        } catch (Exception e) {
        	LOGGER.warn(e.getMessage(), e);
        	return false;
		}
    }
	
	public void sendOsmRecipientsDeleteEmail(String email) { // TODO obsolete
		LOGGER.info("Sending mail to: " + email);
		Email from = new Email(DEFAULT_MAIL_FROM);
		from.setName("OSM BTC");
		Email to = new Email(email);
		String topic = "Important message from OSM BTC!";
		String contentStr = "Dear OpenStreetMap Editor!" +
				"<br><br>" +
				"You received this message because OsmAnd suspect that you didn't follow OSM BTC guidelines and your account was suspended." +
                "Please contact our support if you believe it was done by mistake.<br>" +
				"<br><br>" +
				"Best Regards, <br>OsmAnd Team";
		Content content = new Content("text/html", contentStr);
		Mail mail = new Mail(from, topic, to, content);
		mail.from = from;
		try {
			Response response = wrapper.send(mail);
			LOGGER.info("Response code: " + response.getStatusCode());
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}
	}

	public boolean isEmail(String comment) {
		if (comment.contains(" ")) {
			return false;
		}
		if (!comment.contains("@")) {
			return false;
		}
		String[] twoParts = comment.split("@");
		if(twoParts.length != 2) {
			return false;
		}
		if (twoParts[0].trim().isEmpty()) {
			return false;
		}
		// validate domain
		if (!twoParts[1].contains(".") || twoParts[1].trim().isEmpty()) {
			return false;
		}
		return true;
	}
 }
