package net.osmand.mailsender;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.gson.Gson;
import com.sendgrid.Method;
import com.sendgrid.Request;
import com.sendgrid.Response;
import com.sendgrid.SendGrid;

import net.osmand.mailsender.data.BlockedUser;
import net.osmand.util.Algorithms;

public class EmailSenderMain {

    private final static Logger LOGGER = Logger.getLogger(EmailSenderMain.class.getName());
    private static final int LIMIT_SENDGRID_API = 500; // probably paging is needed use "offset": ..
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
        int daySince;
		String giveawaySeries;
    }

    public static void main(String[] args) throws SQLException, IOException {
    	System.out.println("Send email utility");
        EmailParams p = new EmailParams();
        boolean updateBlockList = false;
	    boolean updatePostfixBounced = false;
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
            } else if (arg.startsWith("--since-days-ago=")) {
            	if(val.length() > 0) {
            		p.daySince = Integer.parseInt(val);
            	}
            } else if (arg.equals("--update_block_list")) {
                updateBlockList = true;
            } else if (arg.equals("--update_postfix_bounced")) {
              updatePostfixBounced = true;
            }
        }
        final String apiKey = System.getenv("SENDGRID_KEY");
        sendGridClient = new SendGrid(apiKey);

        Connection conn = getConnection();
		if (conn == null) {
			throw new RuntimeException("Please setup DB connection");
		}
	    if (updatePostfixBounced) {
			updatePostfixBounced(conn);
		    conn.close();
			return;
	    }
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

	private static void updatePostfixBounced(Connection conn) throws IOException, SQLException {
		String url = System.getenv("URL_POSTFIX_BOUNCED_CSV");
		String file = System.getenv("FILE_POSTFIX_BOUNCED_CSV");
		HashMap<String, String> bouncedEmailReason = new HashMap<>();
		List<String> csvLines = new ArrayList<>();

		if (url != null) {
			HttpURLConnection http = (HttpURLConnection) new URL(url).openConnection();
			if (http.getResponseCode() == HttpURLConnection.HTTP_OK) {
				BufferedReader in = new BufferedReader(new InputStreamReader(http.getInputStream()));
				String line;
				while ((line = in.readLine()) != null) {
					csvLines.add(line);
				}
				in.close();
			}
			http.disconnect();
		}

		if (file != null) {
			Scanner reader = new Scanner(new File(file));
			while (reader.hasNextLine()) {
				csvLines.add(reader.nextLine());
			}
			reader.close();
		}

		if (!csvLines.isEmpty()) {
			for (String line : csvLines) {
				String[] split = line.split("\",\""); // the format is: "email","reason"
				if (split.length == 2) {
					String email = split[0].replaceFirst("^\"", "");
					String reason = split[1].replaceFirst("\"$", "");
					bouncedEmailReason.put(email, reason);
				}
			}
		}

		if (!bouncedEmailReason.isEmpty()) {
			updateBlockDbWithEmailReason(conn, bouncedEmailReason);
			return; // success
		}

		throw new RuntimeException("Empty CSV or no URL_POSTFIX_BOUNCED_CSV/FILE_POSTFIX_BOUNCED_CSV specified");
	}

	private static void updateBlockDbWithEmailReason(Connection conn, HashMap<String, String> bouncedEmailReason) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("INSERT INTO email_blocked(email, reason, timestamp) " +
				"SELECT ?, ?, ? " +
				"WHERE NOT EXISTS (SELECT email FROM email_blocked WHERE email=?)");
		for (String email : bouncedEmailReason.keySet()) {
			String reason = bouncedEmailReason.get(email);
			ps.setString(1, email);
			ps.setString(2, reason);
			ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
			ps.setString(4, email);
			ps.addBatch();
		}
		int[] batch = ps.executeBatch();
		ps.close();

		LOGGER.info(String.format("Got bounced logs from postfix: inserted %d of %d emails",
				sumBatch(batch), bouncedEmailReason.size()));
	}

	private static void updateUnsubscribed(Connection conn) {
    	int offset = 0;
    	boolean repeat = true;
		while (repeat) {
			repeat = false;
			Request request = new Request();
			request.setMethod(Method.GET);
			request.setEndpoint("suppression/unsubscribes");
			request.addQueryParam("limit", LIMIT_SENDGRID_API + "");
			request.addQueryParam("offset", offset + "");
			try {
				int fetched = updateUnsubscribeDbFromResponse(sendGridClient.api(request), conn);
				repeat = fetched == LIMIT_SENDGRID_API;
				offset += LIMIT_SENDGRID_API;
			} catch (Exception e) {
				LOGGER.info(e.getMessage());
			}
		}
    }

    private static int updateUnsubscribeDbFromResponse(Response queryResponse, Connection conn) throws SQLException {
        String response = queryResponse.getBody();
        PreparedStatement ps = conn.prepareStatement("INSERT INTO email_unsubscribed(email, channel, timestamp) " +
                "SELECT ?, ?, ? " +
                "WHERE NOT EXISTS (SELECT email FROM email_unsubscribed WHERE email=? AND channel=?)");
        Gson gson = new Gson();
        BlockedUser[] users = gson.fromJson(response, BlockedUser[].class);
        int batchSize = 0;
        int updated = 0;
        for (BlockedUser user : users) {
            ps.setString(1, user.getEmail());
            ps.setString(2, "all");
            ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            ps.setString(4, user.getEmail());
            ps.setString(5, "all");
            ps.addBatch();
            if (++batchSize > 500) {
                int[] batch = ps.executeBatch();
                updated += sumBatch(batch);
            }
        }
        int[] batch = ps.executeBatch();
        updated += sumBatch(batch);
        ps.close();
        LOGGER.info(String.format("Updated unsubscribed from sendgrid: %d unsubscribed sendgrid, %d updated in db.", users.length, updated));
        return users.length;
    }

    private static void updateBlockList(Connection conn)  {
        String[] blockList = new String[] {"suppression/blocks", "suppression/bounces",
                "suppression/spam_reports", "suppression/invalid_emails"};
        for (String blockGroup : blockList) {
        	int offset = 0;
        	boolean repeat = true;
			while (repeat) {
				repeat = false;
				Request request = new Request();
				request.setMethod(Method.GET);
				request.addQueryParam("limit", LIMIT_SENDGRID_API + "");
				request.addQueryParam("offset", offset + "");
				request.setEndpoint(blockGroup);
				try {
					int fetched = updateBlockDbFromResponse(sendGridClient.api(request), conn, blockGroup);
					repeat = fetched == LIMIT_SENDGRID_API;
					offset += LIMIT_SENDGRID_API;
				} catch (Exception e) {
					LOGGER.info(e.getMessage());
				}
			}
        }

    }

    private static int updateBlockDbFromResponse(Response queryResponse, Connection conn, String blockGroup) throws SQLException {
        String response = queryResponse.getBody();
        PreparedStatement ps = conn.prepareStatement("INSERT INTO email_blocked(email, reason, timestamp) " +
                "SELECT ?, ?, ? " +
                "WHERE NOT EXISTS (SELECT email FROM email_blocked WHERE email=?)");
        Gson gson = new Gson();
        BlockedUser[] users = gson.fromJson(response, BlockedUser[].class);
        int batchSize = 0;
        int updated = 0;
        for (BlockedUser user : users) {
            ps.setString(1, user.getEmail());
            ps.setString(2, user.getReason());
            ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            ps.setString(4, user.getEmail());
            ps.addBatch();
            if (++batchSize > 500) {
                int[] batch = ps.executeBatch();
                updated += sumBatch(batch);
            }
        }
        int[] batch = ps.executeBatch();
        updated += sumBatch(batch);
        ps.close();
        LOGGER.info(String.format("Updated blocked %s from sendgrid: %d blocked sendgrid, %d updated in db.", blockGroup, users.length, updated));
        return users.length;
    }

	private static int sumBatch(int[] batch) {
		int s = 0;
		for (int i = 0; i < batch.length; i++) {
			s += batch[i];
		}
		return s;
	}

	private static void sendProductionEmails(Connection conn, EmailParams p, Set<String> unsubscribed) throws SQLException {
        String query = buildQuery(false, p.mailingGroups, p.daySince);
        LOGGER.info("SQL query is " + query);
        PreparedStatement ps = conn.prepareStatement(query);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String address = resultSet.getString(1);
            if (!Algorithms.isEmpty(address)) {
                if (!unsubscribed.contains(address)) {
                    sendMail(address, p);
                } else {
                    LOGGER.info("Skip unsubscribed email: " + address.replaceFirst(".....", "....."));
                }
            }
        }
        LOGGER.warning(String.format("Sending mails finished: %d success, %d failed", p.sentSuccess, p.sentFailed));
    }

    // 	email_free_users_android, email_free_users_ios, supporters, osm_recipients (deprecated)
	private static String buildQuery(boolean count, String mailingGroups, int daysSince) {
		String[] groups = mailingGroups.split(",");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < groups.length; i++) {
			if (i > 0) {
				sb.append(" UNION ");
			}
			sb.append("SELECT ");
			String group = groups[i].trim();
			if(count) {
				sb.append(String.format(" '%s', count(*) ", group));
			} else {
				sb.append((group.equals("supporters") ? "useremail" : "email"));
			}
			sb.append(" FROM ");
			if (group.startsWith("email_free")) {
				sb.append(" email_free_users ");
				if (!group.contains("ios")) {
					sb.append(" WHERE (os <> 'ios' or os is null) ");
				} else {
					sb.append(" WHERE (os = 'ios') ");
				}
				if(daysSince > 0 && !group.equals("supporters")) {
					sb.append(" and updatetime > now() - interval '" + daysSince + "' day ");
				}
			} else {
				sb.append(group);
			}
		}
		return sb.toString();
	}

    private static void printStats(Connection conn, EmailParams p, Set<String> unsubscribed ) throws SQLException {
    	String query = buildQuery(true, p.mailingGroups, p.daySince);
        LOGGER.info("TEST SQL query for the databases: " + query);
        PreparedStatement prep = conn.prepareStatement(query);
        ResultSet rs = prep.executeQuery();
        int total = 0;
        while (rs.next()) {
            total += rs.getInt(2);
            LOGGER.info("Total in the group '" + rs.getString(1) +"' : " + rs.getInt(2));
        }
        rs.close();
        prep.close();
        LOGGER.info("Total: " + total);

        LOGGER.info("Blocked/unsubscribed for selected topic: " + unsubscribed.size());

        prep = conn.prepareStatement("SELECT count(*) FROM email_unsubscribed");
        rs = prep.executeQuery();
        int count = 0;
        while (rs.next()) {
            count = rs.getInt(1);
        }
        LOGGER.info("Total Unsubscribed: " + count);
        prep.close();
        rs.close();
        count = 0;

        prep = conn.prepareStatement("SELECT count(*) FROM email_blocked");
        rs = prep.executeQuery();
        while (rs.next()) {
            count = rs.getInt(1);
        }
        LOGGER.info("Total Blocked: " + count);
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
		if (mailTo == null || mailTo.isEmpty()) {
			return;
		}

		EmailSenderTemplate sender = new EmailSenderTemplate()
				.load(p.templateId) // should be "giveaway" for OsmAnd giveaway campaign
				.set("UNSUBGROUP", p.topic)
				.from(p.mailFrom)
				.name("OsmAnd")
				.to(mailTo);

		if (p.subject != null) {
			sender.subject(p.subject);
		}

		if ("giveaway".equals(p.topic)) {
			sender.set("GIVEAWAY_SERIES", p.giveawaySeries);
		}

		boolean ok = sender.send().isSuccess();

		if (ok) {
			p.sentSuccess++;
		} else {
			p.sentFailed++;
		}

		LOGGER.info("Sending mail to: " + mailTo.replaceFirst(".....", ".....") + " (" + ok + ") " +
				String.format("[%d success, %d failed]", p.sentSuccess, p.sentFailed));
	}
}
