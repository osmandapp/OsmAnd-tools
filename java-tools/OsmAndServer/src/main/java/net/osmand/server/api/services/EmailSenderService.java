package net.osmand.server.api.services;

import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

import net.osmand.mailsender.EmailSenderTemplate;
import net.osmand.server.PurchasesDataLoader;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.utils.FileSizeFormatter;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EmailSenderService {

	private static final String SHARE_LINK_PREFIX = "https://osmand.net/map/share/join/";

	@Autowired
	protected PurchasesDataLoader purchasesDataLoader;

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

		// action names the clients send to /send-code; null for an unknown action
		public static CloudAccountAction fromCodeRequest(String action, boolean toNewEmail) {
			if (action == null) {
				return null;
			}
			switch (action) {
				case "setup":
					return SETUP;
				case "change":
					return toNewEmail ? EMAIL_CHANGE : EMAIL_CHANGE_REQUEST;
				case "delete":
					return DELETE;
				default:
					return null;
			}
		}
	}

	public void sendOsmAndCloudAccountEmail(String email, String token, String lang, CloudAccountAction action) {
		sendOsmAndCloudAccountEmail(email, token, lang, action, null);
	}

	public void sendShareFileAccessEmail(String email, boolean approved, String ownerNickname, String fileName,
			String fileType, long fileSize, UUID fileUuid) {
		String name = fileName == null ? "" : fileName;
		int dotIdx = name.lastIndexOf('.');
		String ext = dotIdx > 0 && dotIdx < name.length() - 1
				? name.substring(dotIdx + 1).toUpperCase(Locale.ROOT) : "FILE";
		String meta = fileType == null ? ext : fileType;
		if (fileSize > 0) {
			meta = meta + " · " + FileSizeFormatter.format(fileSize);
		}
		String url = fileUuid == null ? "https://osmand.net/map" : SHARE_LINK_PREFIX + fileUuid;
		boolean ok = new EmailSenderTemplate()
				.load(approved ? "cloud/share/approved" : "cloud/share/declined")
				.set("OWNER_NAME", Algorithms.isEmpty(ownerNickname)
						? "@OWNER_DEFAULT@" : htmlText(ownerNickname))
				.set("FILE_NAME", htmlText(name))
				.set("FILE_NAME_PLAIN", plainText(name))
				.set("FILE_EXT", htmlText(ext))
				.set("FILE_META", htmlText(meta))
				.set("FILE_URL", htmlText(url))
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
	// and only an '@' that starts an @VAR@ token is replaced with the full-width look-alike (a@b.gpx stays as is).
	static String plainText(String s) {
		return s == null ? "" : TEMPLATE_TOKEN_START.matcher(s.replaceAll("[\\r\\n]+", " ")).replaceAll("＠");
	}

	private static final Pattern TEMPLATE_TOKEN_START = Pattern.compile("@(?=[A-Z0-9_]+@)");

	// cloud/purchase/receipt. The Renews/Expires row is hidden for a lifetime purchase (no renewal date).
	public void sendPurchaseReceiptEmail(String email, String lang, String orderId, Date orderDate, String orderTotal,
			List<DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase> purchases,
			List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions) {
		String productName;
		String planName;
		String planCount = "";
		String renewalLabel;
		Date renewalDate;
		if (subscriptions != null && !subscriptions.isEmpty()) {
			DeviceSubscriptionsRepository.SupporterDeviceSubscription sub = subscriptions.get(0);
			PurchasesDataLoader.Subscription skuData = purchasesDataLoader.getSubscriptions().get(sub.sku);
			productName = skuData != null ? skuData.name() : sub.sku;
			int months = skuData == null ? 0
					: "year".equals(skuData.durationUnit()) ? skuData.duration() * 12 : skuData.duration();
			if (months == 1) {
				planName = "@RECEIPT_PLAN_MONTHLY@";
			} else if (months == 12) {
				planName = "@RECEIPT_PLAN_ANNUAL@";
			} else if (months > 0 && months % 12 == 0) {
				planName = "@RECEIPT_PLAN_YEARS@";
				planCount = String.valueOf(months / 12);
			} else if (months > 0) {
				planName = "@RECEIPT_PLAN_MONTHS@";
				planCount = String.valueOf(months);
			} else {
				planName = "@RECEIPT_PLAN_SUBSCRIPTION@";
			}
			renewalLabel = Boolean.TRUE.equals(sub.autorenewing) ? "@RECEIPT_RENEWS_ON@" : "@RECEIPT_EXPIRES_ON@";
			renewalDate = sub.expiretime;
		} else if (purchases != null && !purchases.isEmpty()) {
			DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase iap = purchases.get(0);
			PurchasesDataLoader.InApp skuData = purchasesDataLoader.getInApps().get(iap.sku);
			productName = skuData != null ? skuData.name() : iap.sku;
			planName = "@RECEIPT_PLAN_ONETIME@";
			renewalLabel = "@RECEIPT_EXPIRES_ON@";
			renewalDate = skuData != null ? skuData.getExpireDate(iap.purchaseTime) : null;
		} else {
			return;
		}
		String productShort = productName.startsWith("OsmAnd ") ? productName.substring("OsmAnd ".length()) : productName;
		EmailSenderTemplate sender = new EmailSenderTemplate()
				.load("cloud/purchase/receipt", lang)
				.set("EMAIL", htmlText(email))
				.set("ORDER_ID", htmlText(orderId))
				.set("ORDER_DATE", htmlText(formatReceiptDate(orderDate, lang)))
				.set("ORDER_TOTAL", orderTotal == null ? "&mdash;" : htmlText(orderTotal))
				.set("PRODUCT_NAME", htmlText(productName))
				.set("PRODUCT_SHORT", htmlText(productShort))
				.set("PLAN_NAME", planName)
				.set("RECEIPT_PLAN_COUNT", planCount);
		if (renewalDate != null) {
			sender.set("RENEWAL_ROW", "@RENEWAL_ROW_T@")
					.set("RENEWAL_LABEL", renewalLabel)
					.set("RENEWAL_DATE", htmlText(formatReceiptDate(renewalDate, lang)));
		}
		boolean ok = sender.to(email).send().isSuccess();
		LOGGER.info("sendPurchaseReceiptEmail order " + orderId + " to: " + shorten(email) + " (" + ok + ") [" + lang + "]");
	}

	private static String formatReceiptDate(Date date, String lang) {
		Locale locale = lang == null || lang.isEmpty() ? Locale.ENGLISH : Locale.forLanguageTag(lang.replace('_', '-'));
		if (locale.getLanguage().isEmpty()) {
			locale = Locale.ENGLISH;
		}
		return DateFormat.getDateInstance(DateFormat.MEDIUM, locale).format(date);
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
