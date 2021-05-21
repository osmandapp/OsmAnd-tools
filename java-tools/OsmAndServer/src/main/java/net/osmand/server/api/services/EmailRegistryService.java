package net.osmand.server.api.services;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

@Service
public class EmailRegistryService {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public List<EmailId> searchEmails(String emailPart) {
		List<EmailId> searchEmails = new ArrayList<EmailId>();
		searchEmails.addAll(parseEmails(emailPart, "email", "updatetime", null, "email_free_users", "Free users (extra maps)"));
		searchEmails.addAll(parseEmails(emailPart, "email", "updatetime", null, "osm_recipients", "OSM editors (OsmAnd Live)"));
		searchEmails.addAll(parseEmails(emailPart, "useremail", null, null, "supporters", "OsmAnd Live subscriptions (Supporters)"));
		searchEmails.addAll(parseEmails(emailPart, "email", "timestamp", null, "email_blocked", "Blocked (No emails sent!)"));
		searchEmails.addAll(parseEmails(emailPart, "email", "timestamp", "channel", "email_unsubscribed","Unsubscribed (No emails sent by channel!)"));
		return searchEmails;
	}

	private List<EmailId> parseEmails(String emailPart, String emailCol, String dateCol, String channelCol,
			String table, String category) {
		List<EmailId> emails = jdbcTemplate.query("select " + emailCol + (dateCol != null ? ", " + dateCol : "")
				+ (channelCol != null ? ", " + channelCol : "") + " from " + table + " where " + emailCol + " like ?",
				new String[] { "%" + emailPart + "%" }, new RowMapper<EmailId>() {

					@Override
					public EmailId mapRow(ResultSet rs, int rowNum) throws SQLException {
						EmailId emailId = new EmailId();
						emailId.email = rs.getString(1);
						if (dateCol == null) {
							emailId.date = new Date();
						} else {
							emailId.date = new Date(rs.getDate(dateCol).getTime());
						}
						if (channelCol == null) {
							emailId.channel = "";
						} else {
							emailId.channel = rs.getString(channelCol);
						}
						emailId.table = table;
						emailId.source = category;
						return emailId;
					}

				});
		return emails;
	}

	public List<EmailReport> getEmailsDBReport() {
		List<EmailReport> er = new ArrayList<EmailReport>();
		addEmailReport(er, "Free users with 3 maps", "email_free_users", "email_free_users", "email");
		addEmailReport(er, "OSM editors (OsmAnd Live)", "osm_recipients", "osm_recipients", "email");
		addEmailReport(er, "OsmAnd Live subscriptions", "supporters", "supporters", "useremail");
		return er;
	}

	private void addEmailReport(List<EmailReport> er, String category, String categoryId, String table,
			String mailCol) {
		final EmailReport re = new EmailReport();
		re.category = category;
		re.categoryId = categoryId;
		jdbcTemplate.query("select count(distinct A." + mailCol + "), U.channel from " + table + " A "
				+ " left join email_unsubscribed U on A." + mailCol + " = U.email " + " where A." + mailCol
				+ " not in (select email from email_blocked ) group by U.channel", new RowCallbackHandler() {

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						re.addChannel(rs.getString(2), rs.getInt(1));
					}
				});

		er.add(re.calculateActive());
	}

	public static class EmailId {
		public String channel;
		public String table;
		public String email;
		public String source;
		public Date date;
	}

	public static class EmailReport {
		public String category;
		public String categoryId;
		public int totalCount;
		public int activeMarketing;
		public int activeOsmAndLive;
		public int activeNews;

		public int filterMarketing;
		public int filterOsmAndLive;
		public int filterNews;
		public int filterAll;

		public void addChannel(String channel, int total) {
			totalCount += total;
			if (channel == null || channel.isEmpty()) {
				// skip
			} else if ("marketing".equals(channel)) {
				filterMarketing += total;
			} else if ("all".equals(channel)) {
				filterAll += total;
			} else if ("osmand_live".equals(channel)) {
				filterOsmAndLive += total;
			} else if ("news".equals(channel)) {
				filterNews += total;
			} else {
				filterNews += total;
			}
		}

		public EmailReport calculateActive() {
			activeMarketing = totalCount - filterAll - filterMarketing;
			activeOsmAndLive = totalCount - filterAll - filterOsmAndLive;
			activeNews = totalCount - filterAll - filterNews;
			return this;
		}
	}

}
