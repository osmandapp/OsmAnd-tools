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
		sender.sendOsmAndCloudWebEmail(email, "token", "delete", "en");
		sender.sendOsmAndCloudRegistrationEmail(email, "token", "en", true);
		for (CloudAccountAction action : CloudAccountAction.values()) {
			sender.sendOsmAndCloudAccountEmail(email, "token", "en", action, "new@osmand.net");
		}
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
		sendOsmAndCloudAccountEmail(email, token, lang, newUser ? CloudAccountAction.SETUP : CloudAccountAction.LOGIN);
	}

	public enum CloudAccountAction {
		SETUP("cloud/account/setup"),
		LOGIN("cloud/account/login"),
		PASSWORD("cloud/account/password"),
		EMAIL_CHANGE_REQUEST("cloud/account/change-request"),
		EMAIL_CHANGE("cloud/account/change"),
		DELETE("cloud/account/delete"),
		EMAIL_CHANGED("cloud/account/email-changed");

		public final String template;

		CloudAccountAction(String template) {
			this.template = template;
		}
	}

	public void sendOsmAndCloudAccountEmail(String email, String token, String lang, CloudAccountAction action) {
		sendOsmAndCloudAccountEmail(email, token, lang, action, null);
	}

	public void sendShareFileAccessEmail(String email, boolean approved, String ownerName, String fileName,
			String fileExt, String fileMeta, String fileUrl) {
		boolean ok = new EmailSenderTemplate()
				.load(approved ? "cloud/share/approved" : "cloud/share/declined")
				.set("OWNER_NAME", htmlText(ownerName))
				.set("FILE_NAME", htmlText(fileName))
				.set("FILE_NAME_PLAIN", plainText(fileName))
				.set("FILE_EXT", htmlText(fileExt))
				.set("FILE_META", htmlText(fileMeta))
				.set("FILE_URL", htmlText(fileUrl))
				.to(email)
				.send()
				.isSuccess();
		LOGGER.info("sendShareFileAccessEmail approved=" + approved + " to: " + shorten(email) + " (" + ok + ")");
	}

	// User-supplied text placed into an HTML template: escape markup, and '@' as &#64;, so the value can neither
	// inject HTML into an OsmAnd-branded email nor be expanded (or rejected) by the template's @VAR@ substitution.
	static String htmlText(String s) {
		return s == null ? "" : org.springframework.web.util.HtmlUtils.htmlEscape(s).replace("@", "&#64;");
	}

	// The same for a plain-text header such as Subject, where entities would show literally: no line breaks,
	// and '@' replaced with the full-width look-alike so it cannot form an @VAR@ token.
	static String plainText(String s) {
		return s == null ? "" : s.replaceAll("[\\r\\n]+", " ").replace('@', '＠');
	}

	// cloud/purchase/receipt. renewalLabel/renewalDate == null means a lifetime purchase: the Renews/Expires row is hidden.
	public void sendPurchaseReceiptEmail(String email, String orderId, String orderDate, String orderTotal,
			String productName, String planName, String renewalLabel, String renewalDate) {
		String productShort = productName.startsWith("OsmAnd ") ? productName.substring("OsmAnd ".length()) : productName;
		EmailSenderTemplate sender = new EmailSenderTemplate()
				.load("cloud/purchase/receipt")
				.set("EMAIL", email)
				.set("ORDER_ID", orderId == null ? "" : orderId)
				.set("ORDER_DATE", orderDate)
				.set("ORDER_TOTAL", orderTotal)
				.set("PRODUCT_NAME", productName)
				.set("PRODUCT_SHORT", productShort)
				.set("PLAN_NAME", planName);
		if (renewalLabel != null && renewalDate != null) {
			sender.set("RENEWAL_ROW", "@RENEWAL_ROW_T@")
					.set("RENEWAL_LABEL", renewalLabel)
					.set("RENEWAL_DATE", renewalDate);
		}
		boolean ok = sender.to(email).send().isSuccess();
		LOGGER.info("sendPurchaseReceiptEmail order " + orderId + " to: " + shorten(email) + " (" + ok + ")");
	}

	public void sendOsmAndCloudAccountEmail(String email, String token, String lang, CloudAccountAction action,
			String newEmail) {
		EmailSenderTemplate sender = new EmailSenderTemplate()
				.load(action.template, lang)
				.set("TOKEN", token == null ? "" : token);
		if (newEmail != null) {
			sender.set("NEW_EMAIL", htmlText(newEmail));
		}
		boolean ok = sender.to(email).send().isSuccess();
		LOGGER.info("sendOsmAndCloudAccountEmail " + action.name() + " to: " + shorten(email)
				+ " (" + ok + ") [" + lang + "]");
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

	// n***@example.com - enough for the owner to recognise the address without exposing it in full
	public static String maskEmail(String email) {
		int at = email == null ? -1 : email.indexOf('@');
		if (at <= 0) {
			return "***";
		}
		return email.charAt(0) + "***" + email.substring(at);
	}

	// hide full email from logs
	public static String shorten(String full) {
		return full == null ? null : full.replaceFirst(".....", "....."); // osmand@t-online.de -> .....d@t-online.de
	}
 }
