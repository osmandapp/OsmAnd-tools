package net.osmand.server.api.services;

import java.util.Arrays;

import net.osmand.mailsender.EmailSenderTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

@Service
public class EmailSenderService {
	public static void test(String[] args) {
		EmailSenderService sender = new EmailSenderService();
		String email = "dmarc-reports@osmand.net";
		sender.sendOsmAndCloudPromoEmail(email, "promo");
		sender.sendOsmAndCloudWebEmail(email, "token", "en", "delete");
		sender.sendOsmAndCloudRegistrationEmail(email, "token", "en", true);
		sender.sendPromocodesEmails(email, "promocode/android", "ANDROID");
		sender.sendPromocodesEmails(email, "promocode/ios", "IOS");
	}

    private static final Log LOGGER = LogFactory.getLog(EmailSenderService.class);

    public void sendOsmAndCloudPromoEmail(String email, String promo) {
		boolean ok = new EmailSenderTemplate()
				.load("cloud/promo")
				.set("PROMO", promo)
				.to(email)
				.send()
				.isSuccess();
	    LOGGER.info("sendOsmAndCloudPromoEmail to: " + shorten(email) + " (" + ok + ")");
	}
    
    public void sendOsmAndCloudWebEmail(String email, String token, String action, String lang) {
		String templateAction = action;
		if ("setup".equals(action)) {
			templateAction = "@ACTION_SETUP@";
		} else if("change".equals(action)) {
			templateAction = "@ACTION_CHANGE@";
		} else if("delete".equals(action)) {
			templateAction = "@ACTION_DELETE@";
		}
	    boolean ok = new EmailSenderTemplate()
			    .load("cloud/web", lang)
			    .set("ACTION", templateAction)
			    .set("TOKEN", token)
			    .to(email)
			    .send()
			    .isSuccess();
	    LOGGER.info("sendOsmAndCloudWebEmail to: " + shorten(email) + " (" + ok + ") [" + lang + "]");
	}
    
    public void sendOsmAndCloudRegistrationEmail(String email, String token, String lang, boolean newUser) {
		String subject = newUser ? "@SUBJECT_NEW@" : "@SUBJECT_OLD@";
	    boolean ok = new EmailSenderTemplate()
			    .load("cloud/register", lang)
			    .set("SUBJECT", subject)
			    .set("TOKEN", token)
			    .to(email)
			    .send()
			    .isSuccess();
	    LOGGER.info("sendOsmAndCloudRegistrationEmail to: " + shorten(email) + " (" + ok + ") [" + lang + "]");
	}
    
    public boolean sendPromocodesEmails(String mailTo, String templateId, String promocodes) {
	    boolean ok = new EmailSenderTemplate()
			    .load(templateId) // should be "promocode/ios" or "promocode/anroid"
			    .to(Arrays.asList(mailTo.split(",")))
			    .set("PROMOCODE", promocodes)
			    .send()
			    .isSuccess();
	    LOGGER.info("sendPromocodesEmails to: " + shorten(mailTo) + " (" + ok + ")");
		return ok;
    }

	public void sendOsmAndSpecialGiftEmail(String email) {
		boolean ok = new EmailSenderTemplate()
				.load("birthday/XV")
				.to(email)
				.send()
				.isSuccess();

		LOGGER.info("sendOsmAndSpecialGiftEmail to: " + shorten(email) + " (" + ok + ")");
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

	// hide full email from logs
	private String shorten(String full) {
		return full.replaceFirst(".....", "....."); // osmand@t-online.de -> .....d@t-online.de
	}
 }
