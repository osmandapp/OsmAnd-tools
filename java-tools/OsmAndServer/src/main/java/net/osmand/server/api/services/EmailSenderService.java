package net.osmand.server.api.services;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import com.sendgrid.Content;
import com.sendgrid.Email;
import com.sendgrid.FooterSetting;
import com.sendgrid.Mail;
import com.sendgrid.MailSettings;
import com.sendgrid.Method;
import com.sendgrid.OpenTrackingSetting;
import com.sendgrid.Personalization;
import com.sendgrid.Request;
import com.sendgrid.Response;
import com.sendgrid.SendGrid;
import com.sendgrid.TrackingSettings;

import net.osmand.util.Algorithms;

@Service
public class EmailSenderService {

    private static final Log LOGGER = LogFactory.getLog(EmailSenderService.class);
	private SendGrid sendGridClient;
	
	private static final String DEFAULT_MAIL_FROM = "contactus@osmand.net";
	private static final String NOREPLY_MAIL_FROM = "noreply@osmand.net";

	private EmailSenderService() {
		final String apiKey = System.getenv("SENDGRID_KEY");
		if (apiKey != null) {
			sendGridClient = new SendGrid(apiKey);
		} else {
			sendGridClient = new SendGrid(null) {
				@Override
				public Response api(Request request) throws IOException {
					LOGGER.info("Send grid emails is not configured: " + request.getBody());
					return new Response(200, "", null);
				}
			};
			LOGGER.warn("Send grid emails is not configured");
		}
	}
    
    
    public void sendOsmAndCloudPromoEmail(String email, String promo) {
		LOGGER.info("Sending mail to: " + email);
		Email from = new Email(NOREPLY_MAIL_FROM);
		from.setName("OsmAnd");
		Email to = new Email(email);
		String topic = "Congratulations and welcome to OsmAnd Cloud!";
		StringBuilder contentStr = new StringBuilder();
		contentStr.append("Dear OsmAnd User!");
		contentStr.append("<br><br>");
		contentStr.append("You have been selected for OsmAnd Cloud promo subscription. Your promo OsmAnd Pro is <b>"+promo+"</b><br>");
		contentStr.append("Now you can open OsmAnd Settings -> Backup and Restore and Login with this email to OsmAnd Cloud and get all features enabled.<br>");
		contentStr.append("<br><br>");
		contentStr.append("Best Regards, <br>OsmAnd Team");
		
		
		Content content = new Content("text/html", contentStr.toString());
		Mail mail = new Mail(from, topic, to, content);
		mail.from = from;
		Request request = new Request();
		try {
			request.setMethod(Method.POST);
			request.setEndpoint("mail/send");
			String body = mail.build();
			request.setBody(body);
			Response response = sendGridClient.api(request);
			LOGGER.info("Response code: " + response.getStatusCode());
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}
	}
    
    public void sendOsmAndCloudWebEmail(String email, String token, String action) {
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
		Request request = new Request();
		try {
			request.setMethod(Method.POST);
			request.setEndpoint("mail/send");
			String body = mail.build();
			request.setBody(body);
			Response response = sendGridClient.api(request);
			LOGGER.info("Response code: " + response.getStatusCode());
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}
		
	}
    
    public void sendOsmAndCloudRegistrationEmail(String email, String token, boolean newUser) {
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
				+ "'>following link<a> to open it with OsmAnd.");
		contentStr.append("<br><br>");
		contentStr.append("Best Regards, <br>OsmAnd Team");
		
		
		Content content = new Content("text/html", contentStr.toString());
		Mail mail = new Mail(from, topic, to, content);
		mail.from = from;
		Request request = new Request();
		try {
			request.setMethod(Method.POST);
			request.setEndpoint("mail/send");
			String body = mail.build();
			request.setBody(body);
			Response response = sendGridClient.api(request);
			LOGGER.info("Response code: " + response.getStatusCode());
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}
		
	}
    
    // 6503d80a-4574-461d-95f1-2bd0c43392b8
    public boolean sendPromocodesEmails(String mailTo, String templateId, String promocodes) {
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
        Request request = new Request();
        try {
            request.setMethod(Method.POST);
            request.setEndpoint("mail/send");
            String body = mail.build();
            request.setBody(body);
            Response response = sendGridClient.api(request);
            LOGGER.info("Response code: " + response.getStatusCode());
            return true;
        } catch (Exception e) {
        	LOGGER.warn(e.getMessage(), e);
        	return false;
		}
    }
	
	public void sendOsmRecipientsDeleteEmail(String email) {
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
		Request request = new Request();
		try {
			request.setMethod(Method.POST);
			request.setEndpoint("mail/send");
			String body = mail.build();
			request.setBody(body);
			Response response = sendGridClient.api(request);
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
