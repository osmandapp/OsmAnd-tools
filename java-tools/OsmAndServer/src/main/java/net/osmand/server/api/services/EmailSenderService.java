package net.osmand.server.api.services;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

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

    private static final Log LOGGER = LogFactory.getLog(EmailSenderService.class);
	private SendGrid sendGridClient;
	
	private static final String DEFAULT_MAIL_FROM = "contactus@osmand.net";
	private static final String NOREPLY_MAIL_FROM = "noreply@osmand.net";

    private EmailSenderService() {
    	final String apiKey = System.getenv("SENDGRID_KEY");
    	if(apiKey != null) {
    		sendGridClient = new SendGrid(apiKey);
    	} else {
    		LOGGER.warn("Send grid emails is not configured");
    	}
    }
    
    
    public void sendRegistrationEmail(String email, String token, boolean newUser) {
		LOGGER.info("Sending mail to: " + email);
		Email from = new Email(NOREPLY_MAIL_FROM);
		from.setName("OsmAnd");
		Email to = new Email(email);
		String topic = newUser? "Welcome to OsmAnd Premium" : "Register new device to OsmAnd Premium";
		StringBuilder contentStr = new StringBuilder();
		contentStr.append("Hello OsmAnd User!");
		contentStr.append("\n\n");
		contentStr.append("New device has been connected to OsmAnd Premium. You activation code is <b>"+token+"</b>.\n");
		contentStr.append("You can also use <a href='https://osmand.net/premium/device-registration?token=" + token
				+ "'>following link<a> to open it with OsmAnd.");
		contentStr.append("\n\n");
		contentStr.append("Best Regards, \nOsmAnd Team");
		
		
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


 }
