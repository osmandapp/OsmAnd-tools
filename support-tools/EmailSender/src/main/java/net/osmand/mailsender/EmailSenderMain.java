package net.osmand.mailsender;

import com.sendgrid.*;

import javax.annotation.Nullable;
import java.io.IOException;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.*;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;


// Uses SendGrid's Java Library
// https://github.com/sendgrid/sendgrid-java

public class EmailSenderMain {

    private final static Logger LOGGER = Logger.getLogger(EmailSenderMain.class.getName());

    private static String mailFrom;
    private static SendGrid sendGridClient;

    public static void main(String[] args) throws SQLException {

        String templateId = null;
        String mailingGroups = null;
        String topic = null;
        String runMode = null;
        String testAddresses = null;
        if (args.length < 5) {
            printUsage();
        }
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String val = arg.substring(arg.indexOf("="));
            if (arg.startsWith("id=")) {
                templateId = val;
            } else if (arg.startsWith("groups=")) {
                mailingGroups = val;
            } else if (arg.startsWith("sneder_mail=")) {
                mailFrom = val;
            } else if (arg.startsWith("topic=")) {
                topic = val;
            } else if (arg.startsWith("run_opt=")) {
                runMode = val;
            } else if (arg.startsWith("test_addr=")) {
                testAddresses = val;
            }
        }
        checkValidity(templateId, mailingGroups, mailFrom, topic, runMode, testAddresses);

        final String apiKey = System.getenv("SENDGRID_KEY");
        sendGridClient = new SendGrid(apiKey);

        Connection conn = getConnection();
        if (conn == null) {
            LOGGER.info("Can't connect to the database");
            System.exit(1);
        }

        Set<String> unsubscribed = getUnsubscribed(conn, topic);
        switch (runMode) {
            case "send_to_test_email_group":
                sendTestEmails(testAddresses, topic, templateId, unsubscribed);
                break;
            case "print_statistics":
                printStats(conn, mailingGroups, topic, unsubscribed);
                break;
            case "send_to_production":
                sendProductionEmails(conn, templateId, topic, mailingGroups, unsubscribed);
                break;
        }
        conn.close();
    }

    private static void sendProductionEmails(Connection conn, String templateId,
                                             String topic, String mailingGroups, Set<String> unsubscribed) throws SQLException {
        String query = buildQuery(mailingGroups);
        PreparedStatement ps = conn.prepareStatement(query);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String address = resultSet.getString(1);
            if (!unsubscribed.contains(address)) {
                sendMail(address, templateId, topic);
            }
        }
    }

    private static String buildQuery(String mailingGroups) {
        String[] groups = mailingGroups.split(",");
        if (groups.length == 1) {
            return "SELECT " +
                    (mailingGroups.equals("supporters") ? "useremail" : "email") + " FROM " + mailingGroups;
        } else {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < groups.length - 2; i++) {
                sb.append("SELECT ");
                sb.append((groups[i].equals("supporters") ? "useremail" : "email"));
                sb.append(" FROM ");
                sb.append(groups[i]);
                sb.append(" UNION ");
            }
            sb.append("SELECT ");
            sb.append((groups[groups.length - 1].equals("supporters") ? "useremail" : "email"));
            sb.append(" FROM ");
            sb.append(groups[groups.length - 1]);
            return sb.toString();
        }
    }

    private static void printStats(Connection conn, String mailingGroups, String topic, Set<String> unsubscribed) throws SQLException {
        LOGGER.info("TEST SQL query for the databases: " + buildQuery(mailingGroups));
        String[] groups = mailingGroups.split(",");
        for (String group : groups) {
            LOGGER.info("Outputting addresses of the group " + group);
            PreparedStatement prep = conn.prepareStatement("SELECT " +
                    (group.equals("supporters") ? "useremail" : "email") + " FROM " + group);
            ResultSet rs = prep.executeQuery();
            while (rs.next()) {
                LOGGER.info(rs.getString(1));
            }
            prep.close();
            prep = conn.prepareStatement("SELECT COUNT(*) FROM " + group);
            rs.close();
            rs = prep.executeQuery();
            LOGGER.info("Total in the group: " + rs.getInt(1));
            rs.close();
            prep.close();
        }
        LOGGER.info("Selecting unsubscribed users for topic: " + topic);
        LOGGER.info("Unsubscribed from this topic: " + unsubscribed.size());
        LOGGER.info("Printing unsubscribed...");
        for (String s : unsubscribed) {
            LOGGER.info(s);
        }
    }

    private static void sendTestEmails(String testAddresses, String topic,
                                       String templateId, Set<String> unsubscribed) {
        LOGGER.info("Sending test messages...");
        String[] testRecipients = testAddresses.split(",");
        for (String recipient : testRecipients) {
            sendMail(recipient, templateId, topic);
        }
    }

    private static void checkValidity(String templateId, String mailingGroups, String mailFrom,
                                      String topic, String runMode, String testAddresses) {
        if (templateId == null || mailingGroups == null
                || mailFrom == null || topic == null || runMode == null || templateId.isEmpty()
                || mailingGroups.isEmpty() || mailFrom.isEmpty()
                || topic.isEmpty() || runMode.isEmpty()) {
            printUsage();
            throw new RuntimeException("Correct arguments weren't supplied");
        }
        if (runMode.equals("send_to_test_email_group") &&
                (testAddresses == null || testAddresses.isEmpty())) {
            throw new RuntimeException("Test email group wasn't specified in test sending mode.");
        }
    }

    private static Set<String> getUnsubscribed(Connection conn, String topic) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("SELECT email FROM email_unsubscribed WHERE channel=?");
        ps.setString(1, topic);
        ResultSet rs = ps.executeQuery();
        Set<String> res = new HashSet<>();
        while (rs.next()) {
            res.add(rs.getString(1));
        }
        return res;
    }

    private static void printUsage() {
        LOGGER.info("Usage: id=$TEMPLATE_ID groups=$GROUP_NAMES sneder_mail=$SENDER_EMAIL topic=$TOPIC run_opt=$RUN_OPTIONS test_addr=$TEST_EMAIL_GROUP");
        System.exit(1);
    }

    @Nullable
    private static Connection getConnection() {
        String url = "jdbc:postgresql://localhost:5433/changeset";
        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PWD");

        try {
            return  DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private static void sendMail(String mailTo, String templateId, String topic) {
        LOGGER.info("Sending mail to: " + mailTo);
        String userHash;
        try {
            userHash = URLEncoder.encode(Base64.getEncoder().encodeToString(mailTo.getBytes()), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Shouldn't happen
            LOGGER.info(e.getMessage());
            return;
        }
        Email from = new Email(mailFrom);
        Email to = new Email(mailTo);
        Mail mail = new Mail();
        mail.from = from;
        Personalization personalization = new Personalization();
        personalization.addTo(to);
        mail.addPersonalization(personalization);
        mail.setTemplateId(templateId);
        MailSettings mailSettings = new MailSettings();
        FooterSetting footerSetting = new FooterSetting();
        footerSetting.setEnable(true);
        footerSetting.setHtml("<html><center><a href=\"https://osmand.net/unsubscribe?id=" + userHash + "&group=" + topic
                + "\">Unsubscribe</a></center></html>");
        mailSettings.setFooterSetting(footerSetting);
        mail.setMailSettings(mailSettings);
        Request request = new Request();
        try {
            request.setMethod(Method.POST);
            request.setEndpoint("mail/send");
            request.setBody(mail.build());
            Response response = sendGridClient.api(request);
            LOGGER.info("Response code: " + response.getStatusCode());
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        }
    }
}
