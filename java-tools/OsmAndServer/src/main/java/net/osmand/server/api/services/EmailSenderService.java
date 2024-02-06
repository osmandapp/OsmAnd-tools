package net.osmand.server.api.services;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import net.osmand.mailsender.EmailSenderTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

@Service
public class EmailSenderService {
	public static void test(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		EmailSenderService sender = new EmailSenderService();
		String email = "dmarc-reports@osmand.net";
//		sender.sendOsmRecipientsDeleteEmail(email); // obsolete
//		sender.sendOsmAndCloudPromoEmail(email, "promo"); // test ok
//		sender.sendOsmAndCloudWebEmail(email, "token", "action"); // test ok
//		sender.sendOsmAndCloudRegistrationEmail(email, "token", true); // test ok
//		sender.sendPromocodesEmails(email, "promocode/android", "ANDROID"); // test ok
//		sender.sendPromocodesEmails(email, "promocode/ios", "IOS"); // test ok
	}

    private static final Log LOGGER = LogFactory.getLog(EmailSenderService.class);

    public void sendOsmAndCloudPromoEmail(String email, String promo) throws FileNotFoundException, UnsupportedEncodingException {
		boolean ok = new EmailSenderTemplate()
				.load("cloud/promo")
				.set("PROMO", promo)
				.to(email)
				.send()
				.isSuccess();
	    LOGGER.info("sendOsmAndCloudPromoEmail to: " + email + " (" + ok + ")");
	}
    
    public void sendOsmAndCloudWebEmail(String email, String token, String action) throws FileNotFoundException, UnsupportedEncodingException {
	    boolean ok = new EmailSenderTemplate()
			    .load("cloud/web")
			    .set("ACTION", action)
			    .set("TOKEN", token)
			    .to(email)
			    .send()
			    .isSuccess();
	    LOGGER.info("sendOsmAndCloudWebEmail to: " + email + " (" + ok + ")");
	}
    
    public void sendOsmAndCloudRegistrationEmail(String email, String token, boolean newUser) throws FileNotFoundException, UnsupportedEncodingException {
		String subject = newUser ? "Welcome to OsmAnd Cloud" : "Register new device to OsmAnd Cloud";
	    boolean ok = new EmailSenderTemplate()
			    .load("cloud/register")
			    .set("SUBJECT", subject)
			    .set("TOKEN", token)
			    .to(email)
			    .send()
			    .isSuccess();
	    LOGGER.info("sendOsmAndCloudRegistrationEmail to: " + email + " (" + ok + ")");
	}
    
    public boolean sendPromocodesEmails(String mailTo, String templateId, String promocodes) throws FileNotFoundException, UnsupportedEncodingException {
	    boolean ok = new EmailSenderTemplate()
			    .load(templateId) // should be "promocode/ios" or "promocode/anroid"
			    .to(Arrays.asList(mailTo.split(",")))
			    .set("PROMOCODE", promocodes)
			    .send()
			    .isSuccess();
	    LOGGER.info("sendPromocodesEmails to: " + mailTo + " (" + ok + ")");
		return ok;
    }
	
	public void sendOsmRecipientsDeleteEmail(String email) {
		throw new IllegalStateException("sendOsmRecipientsDeleteEmail() is obsolete");
//		LOGGER.info("Sending mail to: " + email);
//		Email from = new Email(DEFAULT_MAIL_FROM);
//		from.setName("OSM BTC");
//		Email to = new Email(email);
//		String topic = "Important message from OSM BTC!";
//		String contentStr = "Dear OpenStreetMap Editor!" +
//				"<br><br>" +
//				"You received this message because OsmAnd suspect that you didn't follow OSM BTC guidelines and your account was suspended." +
//                "Please contact our support if you believe it was done by mistake.<br>" +
//				"<br><br>" +
//				"Best Regards, <br>OsmAnd Team";
//		Content content = new Content("text/html", contentStr);
//		Mail mail = new Mail(from, topic, to, content);
//		mail.from = from;
//		try {
//			Response response = wrapper.send(mail);
//			LOGGER.info("Response code: " + response.getStatusCode());
//		} catch (Exception e) {
//			LOGGER.warn(e.getMessage(), e);
//		}
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
