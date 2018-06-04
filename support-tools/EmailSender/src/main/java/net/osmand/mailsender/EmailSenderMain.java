package net.osmand.mailsender;

import com.sendgrid.*;

import javax.annotation.Nullable;
import java.io.IOException;

import java.sql.*;
import java.util.logging.Logger;


// Uses SendGrid's Java Library
// https://github.com/sendgrid/sendgrid-java

public class EmailSenderMain {

    private final static Logger LOGGER = Logger.getLogger(EmailSenderMain.class.getName());

    private static String mailFrom;
    private static SendGrid sendGridClient;

    public static void main(String[] args) throws SQLException {
        String tableName = null;
        String templateId = null;
        boolean dryRun = false;
        if (args.length > 2) {
            templateId = args[0];
            tableName = args[1];
            mailFrom = args[2];
        } else {
            LOGGER.info("Usage: <template_id> <table_name> <mail_from> [-d]<dry_run>(optional)");
            System.exit(1);
        }
        if (args.length > 3) {
            dryRun = args[3].equals("-d");
        }

        final String apiKey = System.getenv("SENDGRID_KEY");
        sendGridClient = new SendGrid(apiKey);

        Connection conn = getConnection();
        if (conn == null) {
            LOGGER.info("Can't connect to the database");
            System.exit(1);
        }
        PreparedStatement ps = conn.prepareStatement("SELECT" +
                (tableName.equals("supporters") ? "useremail" : "email") + " FROM " + tableName);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String address = resultSet.getString(1);
            if (dryRun) {
                LOGGER.info("Address to send email: " + address);
            } else {
                LOGGER.info("Sending mail to: " + address);
                sendMail(address, templateId);
            }

        }
    }

    @Nullable
    private static Connection getConnection() {
        String url = "jdbc:postgresql://localhost:5433/changeset";
        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PWD");
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private static void sendMail(String mailTo, String templateId) {
        Email from = new Email(mailFrom);
        Email to = new Email(mailTo);
        Mail mail = new Mail();
        mail.from = from;
        Personalization personalization = new Personalization();
        personalization.addTo(to);
        mail.addPersonalization(personalization);
        mail.setTemplateId(templateId);
        Request request = new Request();
        try {
            request.setMethod(Method.POST);
            request.setEndpoint("mail/send");
            request.setBody(mail.build());
            Response response = sendGridClient.api(request);
            System.out.println(response.getStatusCode());
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        }
    }
}
