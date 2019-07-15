package net.osmand.mailsender;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import net.osmand.mailsender.data.BlockedUser;

import com.google.gson.Gson;
import com.sendgrid.Email;
import com.sendgrid.FooterSetting;
import com.sendgrid.Mail;
import com.sendgrid.MailSettings;
import com.sendgrid.Method;
import com.sendgrid.Personalization;
import com.sendgrid.Request;
import com.sendgrid.Response;
import com.sendgrid.SendGrid;


// Uses SendGrid's Java Library
// https://github.com/sendgrid/sendgrid-java

public class EmailSenderMain {

    private final static Logger LOGGER = Logger.getLogger(EmailSenderMain.class.getName());
    private static SendGrid sendGridClient;
    
    private static class EmailParams {
    	String templateId = null;
        String mailingGroups = null;
        String topic = null;
        String runMode = null;
        String testAddresses = null;
        String subject = null;
        String mailFrom;
        int sentSuccess = 0;
        int sentFailed = 0;
		public String giveawaySeries;
    }

    public static void main(String[] args) throws SQLException {
    	System.out.println("Send email utility");
        EmailParams p = new EmailParams(); 
        boolean updateBlockList = false;
        for (String arg : args) {
            String val = arg.substring(arg.indexOf("=") + 1);
            if (arg.startsWith("--id=")) {
                p.templateId = val;
            } else if (arg.startsWith("--groups=")) {
            	p.mailingGroups = val;
            } else if (arg.startsWith("--sender_mail=")) {
            	p.mailFrom = val;
            } else if (arg.startsWith("--giveaway-series=")) {
            	p.giveawaySeries = val;
            } else if (arg.startsWith("--subject=")) {
            	p.subject = val;
            } else if (arg.startsWith("--topic=")) {
            	p.topic = val;
            } else if (arg.startsWith("--run_opt=")) {
            	p.runMode = val;
            } else if (arg.startsWith("--test_addr=")) {
            	p.testAddresses = val;
            } else if (arg.equals("--update_block_list")) {
                updateBlockList = true;
            }
        }
        final String apiKey = System.getenv("SENDGRID_KEY");
        sendGridClient = new SendGrid(apiKey);

        Connection conn = getConnection();
        if (updateBlockList) {
            updateUnsubscribed(conn);
            updateBlockList(conn);
            conn.close();
            return;
        }
        checkValidity(p);


        if (conn == null) {
            LOGGER.info("Can't connect to the database");
            System.exit(1);
        }

        Set<String> unsubscribedAndBlocked = getUnsubscribedAndBlocked(conn, p.topic);
        switch (p.runMode) {
            case "send_to_test_email_group":
                sendTestEmails(p, unsubscribedAndBlocked);
                break;
            case "print_statistics":
                printStats(conn, p, unsubscribedAndBlocked);
                break;
            case "send_to_production":
                sendProductionEmails(conn, p, unsubscribedAndBlocked);
                break;
        }
        conn.close();
    }

    private static void updateUnsubscribed(Connection conn) {
        Request request = new Request();
        request.setMethod(Method.GET);
        request.setEndpoint("suppression/unsubscribes");
        try {
            updateUnsubscribeDbFromResponse(sendGridClient.api(request), conn);
        } catch (Exception e) {
            LOGGER.info(e.getMessage());
        }
    }

    private static void updateUnsubscribeDbFromResponse(Response queryResponse, Connection conn) throws SQLException {
        String response = queryResponse.getBody();
        System.out.println(response);
        PreparedStatement ps = conn.prepareStatement("INSERT INTO email_unsubscribed(email, channel, timestamp) " +
                "SELECT ?, ?, ? " +
                "WHERE NOT EXISTS (SELECT email FROM email_unsubscribed WHERE email=? AND channel=?)");
        Gson gson = new Gson();
        System.out.println(response);
        BlockedUser[] users = gson.fromJson(response, BlockedUser[].class);
        int batchSize = 0;
        for (BlockedUser user : users) {
            ps.setString(1, user.getEmail());
            ps.setString(2, "all");
            ps.setTimestamp(3, new Timestamp(Long.parseLong(user.getCreated())));
            ps.setString(4, user.getEmail());
            ps.setString(5, "all");
            ps.addBatch();
            if (++batchSize > 500) {
                ps.executeBatch();
            }
        }
        ps.executeBatch();
        ps.close();
    }

    private static void updateBlockList(Connection conn)  {
        String[] blockList = new String[] {"suppression/blocks", "suppression/bounces",
                "suppression/spam_reports", "suppression/invalid_emails"};
        for (String blocked : blockList) {
            Request request = new Request();
            request.setMethod(Method.GET);
            request.setEndpoint(blocked);
            try {
                updateBlockDbFromResponse(sendGridClient.api(request), conn);
            } catch (Exception e) {
                LOGGER.info(e.getMessage());
            }
        }

    }

    private static void updateBlockDbFromResponse(Response queryResponse, Connection conn) throws SQLException {
        String response = queryResponse.getBody();
        PreparedStatement ps = conn.prepareStatement("INSERT INTO email_blocked(email, reason, timestamp) " +
                "SELECT ?, ?, ? " +
                "WHERE NOT EXISTS (SELECT email FROM email_blocked WHERE email=?)");
        Gson gson = new Gson();
        System.out.println(response);
        BlockedUser[] users = gson.fromJson(response, BlockedUser[].class);
        int batchSize = 0;
        for (BlockedUser user : users) {
            ps.setString(1, user.getEmail());
            ps.setString(2, user.getReason());
            ps.setTimestamp(3, new Timestamp(Long.parseLong(user.getCreated())));
            ps.setString(4, user.getEmail());
            ps.addBatch();
            if (++batchSize > 500) {
                ps.executeBatch();
            }
        }
        ps.executeBatch();
        ps.close();
    }

    private static void sendProductionEmails(Connection conn, EmailParams p, Set<String> unsubscribed) throws SQLException {
        String query = buildQuery(p.mailingGroups);
        LOGGER.info("SQL query is " + query);
        PreparedStatement ps = conn.prepareStatement(query);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String address = resultSet.getString(1);
            if (!unsubscribed.contains(address) && address != null) {
                sendMail(address, p);
            }
        }
        LOGGER.warning(String.format("Sending mails finished: %d success, %d failed", p.sentSuccess, p.sentFailed));
    }

    private static String buildQuery(String mailingGroups) {
        String[] groups = mailingGroups.split(",");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < groups.length; i++) {
			if (i > 0) {
				sb.append(" UNION ");
			}
            sb.append("SELECT ");
            String table = groups[i].trim();
            sb.append((table.equals("supporters") ? "useremail" : "email"));
            sb.append(" FROM ");
            if(table.startsWith("email_free")) {
            	sb.append(" email_free_users ");
            	if(!table.contains("ios")) {
            		sb.append(" WHERE os <> 'ios' or os is null");
            	} else {
            		sb.append(" WHERE os = 'ios' ");
            	}
            } else {
            	sb.append(table);	
            }
        }
        return sb.toString();
    }

    private static void printStats(Connection conn, EmailParams p, Set<String> unsubscribed ) throws SQLException {
        LOGGER.info("TEST SQL query for the databases: " + buildQuery(p.mailingGroups));
        String[] groups = p.mailingGroups.split(",");
        for (String group : groups) {
            group = group.trim();
            PreparedStatement prep = conn.prepareStatement("SELECT count(*) FROM " + group);
            ResultSet rs = prep.executeQuery();
            int total = 0;
            while (rs.next()) {
                total = rs.getInt(1);
            }
            LOGGER.info("Total in the group " + group +": " + total);
            rs.close();
            prep.close();
        }
        PreparedStatement prep = conn.prepareStatement("SELECT count(*) FROM email_unsubscribed");
        ResultSet rs = prep.executeQuery();
        int count = 0;
        while (rs.next()) {
            count = rs.getInt(1);
        }
        LOGGER.info("Unsubscribed in total: " + count);
        LOGGER.info("Blocked/unsubscribed on db: " + unsubscribed.size());
        LOGGER.info("Fetching the invalid/blocked emails...");
        prep.close();
        rs.close();
        count = 0;
        prep = conn.prepareStatement("SELECT count(*) FROM email_blocked");
        rs = prep.executeQuery();
        while (rs.next()) {
            count = rs.getInt(1);
        }
        LOGGER.info("Total blocked: " + count);
    }

    private static void sendTestEmails(EmailParams p, Set<String> unsubscribed) {
        LOGGER.info("Sending test messages...");
        String[] testRecipients = p.testAddresses.split(",");
        for (String recipient : testRecipients) {
            sendMail(recipient.trim(), p);
        }
    }

    private static void checkValidity(EmailParams p) {
        if (p.templateId == null || p.mailingGroups == null
                || p.mailFrom == null || p.topic == null || p.runMode == null || p.templateId.isEmpty()
                || p.mailFrom.isEmpty()
                || p.topic.isEmpty() || p.runMode.isEmpty()) {
            printUsage();
            throw new RuntimeException("Correct arguments weren't supplied");
        }
        if((p.giveawaySeries == null || p.giveawaySeries.length() == 0) && "giveaway".equals(p.topic)) {
        	throw new RuntimeException("Giveaway series is required");
        }
        if (p.runMode.equals("send_to_test_email_group") &&
                (p.testAddresses == null || p.testAddresses.isEmpty())) {
            throw new RuntimeException("Test email group wasn't specified in test sending mode.");
        }
    }

    private static Set<String> getUnsubscribedAndBlocked(Connection conn, String topic) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("SELECT email FROM email_unsubscribed WHERE channel=? OR channel='all' UNION " +
                "SELECT email FROM email_blocked");
        ps.setString(1, topic);
        ResultSet rs = ps.executeQuery();
        Set<String> res = new HashSet<>();
        while (rs.next()) {
            res.add(rs.getString(1));
        }
        ps.close();
        rs.close();
        return res;
    }

    private static void printUsage() {
        LOGGER.info("Usage: id=$TEMPLATE_ID groups=$GROUP_NAMES sender_mail=$SENDER_EMAIL topic=$TOPIC run_opt=$RUN_OPTIONS test_addr=$TEST_EMAIL_GROUP");
        System.exit(1);
    }

    @Nullable
    private static Connection getConnection() {
        String url = System.getenv("DB_CONN");
        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PWD");

        try {
            return  DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private static void sendMail(String mailTo, EmailParams p) {
    	if(mailTo == null || mailTo.isEmpty()) {
    		return;
    	}
    	String mailHsh = mailTo;
    	if(mailTo.length() > 5) {
    		mailHsh = "....." + mailTo.substring(5);
    	}
        LOGGER.info("Sending mail to: " + mailHsh);
        String userHash;
        try {
            userHash = URLEncoder.encode(Base64.getEncoder().encodeToString(mailTo.getBytes()), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Shouldn't happen
            LOGGER.info(e.getMessage());
            return;
        }
        Email from = new Email(p.mailFrom);
        from.setName("OsmAnd");
        Email to = new Email(mailTo);
        Mail mail = new Mail();
        mail.from = from;
        Personalization personalization = new Personalization();
        personalization.addTo(to);
        mail.addPersonalization(personalization);
        mail.setTemplateId(p.templateId);
        if(p.subject != null) {
        	mail.setSubject(p.subject);
        }
        MailSettings mailSettings = new MailSettings();
        FooterSetting footerSetting = new FooterSetting();
        footerSetting.setEnable(true);
        String footer = "<center style='margin:5px 0px 5px 0px'><a href=\"https://osmand.net/api/email/unsubscribe?id=" + 
        		userHash + "&group=" + p.topic + "\">Unsubscribe</a></center>";
        if("giveaway".equals(p.topic)) {
        	String giv = String.format(
        			"<table border='0' cellPadding='0' cellSpacing='0' class='module' data-role='module-button' data-type='button' role='module' "
        			+ "style='table-layout:fixed' width='100%%'><tbody><tr><td align='center' class='outer-td' style='padding:0px 0px 0px 0px'>"
        			+ "<table border='0' cellPadding='0' cellSpacing='0' class='button-css__deep-table___2OZyb wrapper-mobile' style='text-align:center'>"
        			+ "<tbody><tr><td align='center' bgcolor='#333333' class='inner-td' style='border-radius:6px;font-size:16px;text-align:center;background-color:inherit'>"
        			+ "<a href='%s' style='background-color:#333333;border:1px solid #333333;border-color:#333333;border-radius:6px;border-width:1px;color:#ffffff;display:inline-block;font-family:arial,helvetica,sans-serif;font-size:16px;font-weight:normal;letter-spacing:0px;line-height:16px;padding:12px 18px 12px 18px;text-align:center;text-decoration:none' "
        			+ "target='_blank'>%s</a></td></tr></tbody>"
        			+ "</table></td></tr></tbody></table></td></tr></table>",
							"https://osmand.net/giveaway?series=" + URLEncoder.encode(p.giveawaySeries) + "&email="
									+ URLEncoder.encode(mailTo), "Participate in a Giveaway!");
        	footer = giv + footer;
        }
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
            p.sentSuccess++;
        } catch (IOException ex) {
        	p.sentFailed++;
            System.err.println(ex.getMessage());
        }
    }
}
