package net.osmand.mailsender;

import net.osmand.mailsender.authorization.DataProvider;
import com.sendgrid.*;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

// Uses SendGrid's Java Library
// https://github.com/sendgrid/sendgrid-java

public class EmailSenderMain {

    private static String mailFrom;
    private static SendGrid sendGridClient;

    public static void main(String[] args) throws IOException, GeneralSecurityException {
        String apiKey = null;
        String id = null;
        String range = null;
        if (args.length > 3) {
            id = args[0];
            range = args[1];
            apiKey = args[2];
            mailFrom = args[3];
        } else {
            System.out.println("Usage: <sheet_id> <data_range> <sendgrid_api_key> <mail_from>");
            System.exit(1);
        }
        sendGridClient = new SendGrid(apiKey);
        List<List<Object>> data = DataProvider.getValues(id, range);
        if (data != null) {
            for (List<Object> row : data) {
                String address = (String) row.get(0);
                System.out.println("Sending mail to: " + address);
                sendMail(address);
            }
        }
    }

    private static void sendMail(String mailTo) {
        Email from = new Email(mailFrom);
        Email to = new Email(mailTo);
        String subject = "Sending with SendGrid is Fun";
        Content content = new Content("text/plain", "Test mail from the Google sheet!!!");
        Mail mail = new Mail(from, subject, to, content);
        Request request = new Request();
        try {
            request.setMethod(Method.POST);
            request.setEndpoint("mail/send");
            request.setBody(mail.build());
            Response response = sendGridClient.api(request);
            System.out.println(response.getStatusCode());
            System.out.println(response.getBody());
            System.out.println(response.getHeaders());
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        }
    }
}
