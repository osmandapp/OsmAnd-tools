package net.osmand.mailsender;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import net.osmand.util.Algorithms;

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

public class GiveAwaySenderMain {

    private final static Logger LOGGER = Logger.getLogger(GiveAwaySenderMain.class.getName());
    private static SendGrid sendGridClient;
    
    private static class EmailParams {
    	String templateId = null;
        String testAddress = null;
        String[] promocodes = new String[0];
        Set<String> dbPromocodes = new TreeSet<>();
        
        String month;
        PreparedStatement updateStatement;
        
        String mailFrom;
        int sentSuccess = 0;
        int sentFailed = 0;
		boolean testSent;
    }

    public static void main(String[] args) throws SQLException {
    	System.out.println("Send email utility");
        EmailParams p = new EmailParams(); 
        for (String arg : args) {
            String val = arg.substring(arg.indexOf("=") + 1);
            if(Algorithms.isEmpty(val)) {
            	continue;
            }
            if (arg.startsWith("--id=")) {
                p.templateId = val;
            } else if (arg.startsWith("--sender_mail=")) {
            	p.mailFrom = val;
            } else if (arg.startsWith("--test_addr=")) {
            	p.testAddress = val;
            } else if (arg.startsWith("--promocodes=")) {
            	p.promocodes = val.split(",");
            }
        }
        final String apiKey = System.getenv("SENDGRID_KEY");
        sendGridClient = new SendGrid(apiKey);
        p.month = String.format("%1$tY-%1$tm", new Date());
        Connection conn = getConnection();
        checkValidity(p);

        if (conn == null) {
            LOGGER.info("Can't connect to the database");
            System.exit(1);
        }

        ResultSet rs = conn.createStatement().executeQuery("select distinct promo from lottery_promocodes where month='"+p.month+"'" );
        int total = 0;
        while(rs.next()) {
        	if(rs.getString(1) != null) {
        		total++;
        		p.dbPromocodes.add(rs.getString(1));
        	}
        }
        if(p.promocodes.length > 0) {
        	PreparedStatement ps = conn.prepareStatement("insert into lottery_promocodes(month, promo) values (?, ?)");
        	for(String prom : p.promocodes) {
        		if(!p.dbPromocodes.contains(prom)) {
        			ps.setString(1, p.month);
        			ps.setString(2, prom);
        			ps.execute();
        			p.dbPromocodes.add(prom);
        		}
        	}
        }
        rs = conn.createStatement().executeQuery("select distinct promocode from lottery_users where month='"+p.month+"'" );
        while(rs.next()) {
        	if(rs.getString(1) != null) {
        		p.dbPromocodes.remove(rs.getString(1));
        	}
        }
        sendProductionEmails(conn, p);
        LOGGER.info(String.format("Promocodes: %d left, %d used, %d total.", p.dbPromocodes.size(), total - p.dbPromocodes.size(), total));
        conn.close();
    }


    private static void sendProductionEmails(Connection conn, EmailParams p) throws SQLException {
        String query = "select email from lottery_users where month='"+p.month+"' and promocode = 'WON'";
        p.updateStatement = conn.prepareStatement(
        		"update lottery_users set promocode = ? where email = ? and month = '"+p.month+"'");
        PreparedStatement ps = conn.prepareStatement(query);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String address = resultSet.getString(1);
            sendMail(address, p);
        }
        LOGGER.warning(String.format(
        		"Sending mails finished: %d success, %d failed", 
        		p.sentSuccess, p.sentFailed));
    }


    private static void checkValidity(EmailParams p) {
        if (p.templateId == null ||  p.mailFrom == null || p.templateId.isEmpty()
                || p.mailFrom.isEmpty()) {
            printUsage();
            throw new RuntimeException("Correct arguments weren't supplied");
        }
    }

    private static void printUsage() {
        LOGGER.info("Usage: --id=$TEMPLATE_ID --promocodes=$PROMOCODES --sender_mail=$SENDER_EMAIL --test_addr=$TEST_EMAIL_GROUP");
        System.exit(1);
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

    private static void sendMail(String mailTo, EmailParams p) {
    	if(mailTo == null || mailTo.isEmpty()) {
    		return;
    	}
    	String mailHsh = mailTo;
    	if(mailTo.length() > 5) {
    		mailHsh = "....." + mailTo.substring(5);
    	}
        LOGGER.info("Sending mail to: " + mailHsh);
        
        Email from = new Email(p.mailFrom);
        from.setName("OsmAnd");
        Email to = new Email(mailTo);
        if(p.testAddress != null) {
        	to = new Email(p.testAddress);
        }
        Mail mail = new Mail();
        mail.from = from;
        Personalization personalization = new Personalization();
        personalization.addTo(to);
        mail.addPersonalization(personalization);
        mail.setTemplateId(p.templateId);
        if(p.testSent) {
        	return;
        }
        String promo = p.dbPromocodes.iterator().next();
        MailSettings mailSettings = new MailSettings();
        FooterSetting footerSetting = new FooterSetting();
        footerSetting.setEnable(true);
        String footer = String.format("<center style='margin:5px 0px 5px 0px'>"
        		+ "<h2>Your promocode is '%s' %s.</h2></center>",
        		promo, p.testAddress != null ? mailTo : "") ;
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
            if(p.testAddress == null) {
            	p.dbPromocodes.remove(promo);
            	p.updateStatement.setString(1, promo);
            	p.updateStatement.setString(2, mailTo);
            	p.updateStatement.execute();
            } else {
            	p.testSent = true;
            }
        } catch (IOException ex) {
        	p.sentFailed++;
            System.err.println(ex.getMessage());
        } catch (SQLException e) {
        	p.sentFailed++;
        	System.err.println(e.getMessage());
		}
    }
}
