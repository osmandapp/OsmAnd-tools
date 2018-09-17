package net.osmand.data.changeset;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.osmand.PlatformUtil;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.json.JSONObject;

import com.google.gson.Gson;

public class OsmAndLiveReports {

	private static final int BATCH_SIZE = 1000;
	private static final Log LOG = PlatformUtil.getLog(OsmAndLiveReports.class);
	
	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/changeset",
				isEmpty(System.getenv("DB_USER")) ? "test" : System.getenv("DB_USER"),
						isEmpty(System.getenv("DB_PWD")) ? "test" : System.getenv("DB_PWD"));
		PreparedStatement ps = conn.prepareStatement("select report, time, accesstime from final_reports where month = ? and name = ? and region = ?");
		
		for (int y = 2016; y <= 2018; y++) {
			int si = y == 2015 ? 8 : 1;
			int ei = y == 2018 ? 8 : 12;
			for (int i = si; i <= ei; i++) {
				int s = 0;
				String m = i < 10 ? "0" + i : i + "";
				String month = y + "-" + m;
				System.out.println("TEST " + month);
				OsmAndLiveReports reports = new OsmAndLiveReports(conn, month);
				CountriesReport cntrs = reports.getReport(OsmAndLiveReportType.COUNTRIES, null, CountriesReport.class);
				checkReport(ps, month, OsmAndLiveReportType.COUNTRIES, null);
				checkReport(ps, month, OsmAndLiveReportType.SUPPORTERS, null);
				checkReport(ps, month, OsmAndLiveReportType.PAYOUTS, null);
				checkReport(ps, month, OsmAndLiveReportType.RANKING, null);
				checkReport(ps, month, OsmAndLiveReportType.TOTAL_CHANGES, null);
				checkReport(ps, month, OsmAndLiveReportType.USERS_RANKING, null);
				checkReport(ps, month, OsmAndLiveReportType.RECIPIENTS, null);
				s+=7;
				for (Country reg : cntrs.rows) {
					if(reg.map.equals("1")) {
						checkReport(ps, month, OsmAndLiveReportType.RANKING, reg.downloadname);
						checkReport(ps, month, OsmAndLiveReportType.TOTAL_CHANGES, reg.downloadname);
						checkReport(ps, month, OsmAndLiveReportType.USERS_RANKING, reg.downloadname);
						checkReport(ps, month, OsmAndLiveReportType.RECIPIENTS, reg.downloadname);
					}
					s+=4;
				}
				checkReport(ps, month, OsmAndLiveReportType.REGION_RANKING_RANGE, null);
				checkReport(ps, month, OsmAndLiveReportType.RANKING_RANGE, null);
				checkReport(ps, month, OsmAndLiveReportType.MIN_CHANGES, null);
				checkReport(ps, month, OsmAndLiveReportType.BTC_VALUE, null);
				checkReport(ps, month, OsmAndLiveReportType.EUR_VALUE, null);
				if(!checkReport(ps, month, OsmAndLiveReportType.EUR_BTC_RATE, null)){
					Number eur = reports.getNumberReport(OsmAndLiveReportType.EUR_VALUE);
					Number btc = reports.getNumberReport(OsmAndLiveReportType.BTC_VALUE);
					reports.saveReport(eur.doubleValue() / btc.doubleValue(), OsmAndLiveReportType.EUR_BTC_RATE, null, 0);
				}
				s+=6;
				System.out.println(String.format("TESTED %d reports", s));
			}
		}
//		migrateData(conn);
		
//		String cond = String.format("starttime < '%s' and expiretime >= '%s' ", "2017-01-01", "2017-01-01");
//		System.out.println(reports.supportersQuery(cond));
//		OsmAndLiveReports reports = new OsmAndLiveReports(conn, "2018-08");
////		System.out.println(reports.getJsonReport(OsmAndLiveReportType.COUNTRIES, null));
//		System.out.println(reports.getJsonReport(OsmAndLiveReportType.TOTAL_CHANGES, null));
//		System.out.println(reports.getJsonReport(OsmAndLiveReportType.RANKING, null));
//		System.out.println(reports.getJsonReport(OsmAndLiveReportType.SUPPORTERS, null));
//		System.out.println(reports.getJsonReport(OsmAndLiveReportType.USERS_RANKING, "belarus_europe"));
//		System.out.println(reports.getJsonReport(OsmAndLiveReportType.RECIPIENTS, null));
//		System.out.println(reports.getJsonReport(OsmAndLiveReportType.PAYOUTS, null));
//		reports.buildReports(conn);
	}

	private static boolean checkReport(PreparedStatement ps, String mnth, OsmAndLiveReportType tp, String reg) throws SQLException {
		ps.setString(1, mnth);
		ps.setString(2, tp.getSqlName());
		String r = isEmpty(reg) ? "" : reg;
		ps.setString(3, r);
		ResultSet rs = ps.executeQuery();
		if(rs.next()) {
			String report = rs.getString(1);
			if(isEmpty(report)) {
				System.out.println(String.format("EMPTY REPORT '%s' for '%s' in '%s'", tp.getSqlName(), r, mnth) );
				return false;
			}
		} else {
			System.out.println(String.format("MISSING '%s' for '%s' in '%s'", tp.getSqlName(), r, mnth) );
			return false;
		}
		return true;
	}

	protected static void migrateData(Connection conn) throws SQLException, ParseException {
		PreparedStatement ins = conn.prepareStatement(
				"insert into final_reports(month, region, name, report, time, accesstime) values (?, ?, ?, ?, ?, ?)");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		for(int y= 2015; y <= 2018; y++) {
			int si = y == 2015 ? 8 : 1;
			int ei = y == 2018 ? 8 : 12;
			
			
			for (int i = si; i <= ei; i++) {
				String m = i < 10 ? "0" + i : i + "";
				String month = y + "-" + m;
				Connection connFrom = DriverManager.getConnection("jdbc:postgresql://localhost:5433/changeset_" + y
						+ "_" + m, isEmpty(System.getenv("DB_USER")) ? "test" : System.getenv("DB_USER"),
						isEmpty(System.getenv("DB_PWD")) ? "test" : System.getenv("DB_PWD"));
				String r = "select report,region,name from final_reports where month = ?";
				PreparedStatement p = connFrom.prepareStatement(r);
				p.setString(1, month);
				int reports = 0;
				ResultSet rs = p.executeQuery();
				Date dt = sdf.parse(month+"-01");
				while (rs.next()) {
					ins.setString(1, month);
					ins.setString(2, rs.getString("region"));
					ins.setString(3, rs.getString("name"));
					ins.setString(4, rs.getString("report"));
					ins.setLong(5, dt.getTime() / 1000l);
					ins.setLong(6, dt.getTime() / 1000l);
					ins.addBatch();
					reports++;

				}
				ins.executeBatch();
				System.out.println("Processs " + month + " " + reports + " reports");
				connFrom.close();

			}
		}
		ins.close();
	}
	
	
	private String month;
	private boolean thisMonth;
	private Connection conn;

	public OsmAndLiveReports(Connection conn, String month) {
		this.conn = conn;
		this.month = month;
		String currentMonth = String.format("%1$tY-%1$tm", new Date());
		if(isEmpty(month)) {
			this.month = currentMonth;
		}
		thisMonth = currentMonth.equals(this.month);
		
	}
	
	// FUNCTION LIST
	// 1st step:
	// getTotalChanges - region - {use report cache}
	// calculateRanking - region - {use report cache}
	// calculateUsersRanking - region - {use report cache}
	// [getSupporters , getCountries] - {use report cache}
	// [getRegionRankingRange, getRankingRange, getMinChanges ]
	// 2nd step:
	// ! [getBTCEurRate, getBTCValue, getEurValue] !
	// FINAL step: 
	// ! getRecipients - region - {use report cache}


	public int getRankingRange() {
		return 20;
	}
	
	public int getRegionRankingRange() {
		return 7;
	}

	public int getMinChanges() {
		return 3;
	}
	
	private double getEurBTCRate() throws IOException {
		URLConnection un = new URL("https://blockchain.info/ticker").openConnection();
		StringBuilder bld = Algorithms.readFromInputStream(un.getInputStream());
		JSONObject obj = new JSONObject(bld.toString());
		return obj.getJSONObject("EUR").getDouble("sell");
	}

	private double getBtcValue() throws IOException, SQLException {
		double rt = getNumberReport(OsmAndLiveReportType.EUR_BTC_RATE).doubleValue();
		double eur = getNumberReport(OsmAndLiveReportType.EUR_VALUE).doubleValue();
		if (rt != 0) {
			return eur / rt;
		}
		return 0;
	}


	private double getEurValue() throws SQLException, IOException {
		SupportersReport supporters = getReport(OsmAndLiveReportType.SUPPORTERS, null, SupportersReport.class);
		return supporters.activeCount * 0.4 ; // 1 EUR - 20% (GPlay) - 50% (OsmAnd)
	}
	
	
	public CountriesReport getCountries() throws SQLException{
		PreparedStatement ctrs = conn.prepareStatement( "select id, parentid, downloadname, name, map from countries;");
		ResultSet rs = ctrs.executeQuery();
		CountriesReport countriesReport = new CountriesReport();
		countriesReport.month = month;
		countriesReport.date = reportTime();
		while(rs.next()) {
			Country c = new Country();
			c.id = rs.getString("id");
			c.parentid = rs.getString("parentid");
			c.name = rs.getString("name");
			c.map = rs.getString("map");
			countriesReport.mapId.put(c.id, c);
			if("World".equals(c.name)) {
				c.downloadname = "";
			} else if("0".equals(c.map)) {
				continue;
//				c.downloadname = "<none>";
			} else {
				c.downloadname = rs.getString("downloadname");
			}
			countriesReport.rows.add(c);
		}
		for(Country c : countriesReport.rows) {
			String name = c.name;
			if("1".equals(c.map)) {
				int depth = countriesReport.depth(c);
				Country parent = countriesReport.parent(c);
				Country grandParent = countriesReport.parent(parent);
				if(parent.name.equalsIgnoreCase("russia")) {
					name = parent.name + " " + name;
				} else if(grandParent.name.equalsIgnoreCase("russia")) {
					name = grandParent.name + " " + name;
				} else {
					if(depth == 2) {
						// country name 
					} else if(depth == 3) {
						// county name
						if(!"gb_england_europe".equals(parent.downloadname)) {
							name = parent.name + " " + name;
						}
					} else if(depth == 4) {
						if("gb_england_europe".equals(grandParent.downloadname)) {
							name = parent.name + " " + name;
						} else {
							name = grandParent.name + " " + parent.name + " " + name;
						}
					}
					
				}
				c.visiblename = name;
				countriesReport.map.put(c.downloadname, name);
			}
		}
		return countriesReport;
	}
	
	
	private long reportTime() {
		return System.currentTimeMillis() / 1000;
		//return reportTime.format(new Date());
	}

	private static String supportersQuery(String cond) {
		return "select s.userid uid, s.visiblename visiblename, s.preferred_region region, s.useremail email, "+
           " t.sku sku, t.checktime checktime, t.starttime starttime, t.expiretime expiretime from supporters s " +
           " join (select userid, sku, max(checktime) checktime, max(starttime) starttime, max(expiretime) expiretime " +
           "  from supporters_device_sub where expiretime is not null and " + cond +" group by userid, sku) t " +  
           " on s.userid = t.userid where s.preferred_region is not null and s.preferred_region <> 'none' order by s.userid;";
	}

	public SupportersReport getSupporters() throws SQLException, IOException {
		CountriesReport countries = getReport(OsmAndLiveReportType.COUNTRIES, null, CountriesReport.class);
		SupportersReport supportersReport = new SupportersReport();
		supportersReport.month = month;
		supportersReport.date = reportTime();
		SupportersRegion worldwide = new SupportersRegion();
		worldwide.id = "";
		worldwide.name = "Worldwide";
		supportersReport.regions.put("", worldwide);
		String cond = "expiretime > now()";
		if(!thisMonth) {
			String dt = this.month +"-01";
			cond = String.format("starttime < '%s' and expiretime >= '%s' ", dt, dt);
		}
		PreparedStatement q = conn.prepareStatement(supportersQuery(cond));
		ResultSet rs = q.executeQuery();
		while (rs.next()) {
			Supporter s = new Supporter();
			s.user = rs.getString("visiblename");
			if (isEmpty(s.user)) {
				s.user = "User " + rs.getString("uid");
			}
			s.status = "Active";
			s.region = rs.getString("region");
			s.sku = rs.getString("sku");
			if (countries.map.containsKey(s.region)) {
				s.regionName = countries.map.get(s.region);
			} else {
				s.regionName = Algorithms.capitalizeFirstLetter(s.region.replace('_', ' '));
			}
			if (isEmpty(s.region)) {
				worldwide.count += 2;
				supportersReport.activeCount++;
			} else if (s.region.equals("none")) {
			} else {
				if (!supportersReport.regions.containsKey(s.region)) {
					SupportersRegion r = new SupportersRegion();
					r.id = s.region;
					r.name = s.regionName;
					supportersReport.regions.put(s.region, r);
				}
				supportersReport.regions.get(s.region).count++;
				worldwide.count++;
				supportersReport.activeCount++;
			}
			supportersReport.rows.add(s);
			supportersReport.count++;
		}
		for (String s : supportersReport.regions.keySet()) {
			SupportersRegion r = supportersReport.regions.get(s);
			if (supportersReport.activeCount > 0) {
				r.percent = ((float) r.count) / (2 * supportersReport.activeCount);
			} else {
				r.percent = 0;
			}
		}
		return supportersReport;
	}

	public TotalChangesReport getTotalChanges(String region) throws SQLException {
		TotalChangesReport report = new TotalChangesReport();
		report.month = month;
		report.region = region;
		report.date = reportTime();
		ResultSet rs;
		if(!isEmpty(region)) {
			String r = "select count(distinct ch.username) users, count(distinct ch.id) changes" + 
					  " from changesets_view ch, changeset_country_view cc where ch.id = cc.changesetid"+ 
					  " and cc.countryid = (select id from countries where downloadname= ?)"+
					  " and substr(ch.closed_at_day, 0, 8) = ?";
			PreparedStatement ps = conn.prepareStatement(r);
			ps.setString(1, region);
			ps.setString(2, month);
			rs = ps.executeQuery();
		} else {
			String r = "select count ( distinct username) users, count(*) changes from changesets_view"+
						" where substr(closed_at_day, 0, 8) = ?";
			PreparedStatement ps = conn.prepareStatement(r);
			ps.setString(1, month);
			rs = ps.executeQuery();
		}
		rs.next();
		report.users = rs.getInt("users");
		report.changes = rs.getInt("changes");
		return report;
	}
	
	public RankingReport getRanking(String region) throws SQLException, IOException {
		RankingReport report = new RankingReport();
		report.month = month;
		report.region = region;
		report.date = reportTime();
		int minChanges = getNumberReport(OsmAndLiveReportType.MIN_CHANGES).intValue();
		int rankingRange;
		ResultSet rs;
		if(!isEmpty(region)) {
			rankingRange = getNumberReport(OsmAndLiveReportType.REGION_RANKING_RANGE).intValue();
		    String r =  " SELECT data.cnt changes, count(*) group_size FROM ("+
		    			"  		SELECT username, count(*) cnt FROM changesets_view ch, changeset_country_view cc " + 
		    			"		WHERE substr(ch.closed_at_day, 0, 8) = ? and ch.id = cc.changesetid  "+
		    			"  			and cc.countryid = (SELECT id from countries where downloadname = ? )" +
		    			" 		GROUP by ch.username HAVING count(*) >= ? ORDER by count(*) desc )" +
		    			" data GROUP by data.cnt ORDER by changes desc";
			PreparedStatement ps = conn.prepareStatement(r);
			ps.setString(1, month);
			ps.setString(2, region);
			ps.setInt(3, minChanges);
			rs = ps.executeQuery();
		} else {
			rankingRange = getNumberReport(OsmAndLiveReportType.RANKING_RANGE).intValue();
			String r = "SELECT data.cnt changes, count(*) group_size FROM ( "+
					   "	SELECT username, count(*) cnt FROM changesets_view ch " +
					   "    WHERE substr(ch.closed_at_day, 0, 8) = ? " +
					   " 	GROUP by  ch.username HAVING count(*) >= ? ORDER by count(*) desc) " +
					   " data GROUP by data.cnt ORDER by changes desc";
			PreparedStatement ps = conn.prepareStatement(r);
			ps.setString(1, month);
			ps.setInt(2, minChanges);
			rs = ps.executeQuery();
		}
		List<RankingRange> lst = report.rows;
		while(rs.next()) {
			RankingRange r = new RankingRange();
			r.minChanges = rs.getInt(1);
			r.maxChanges = rs.getInt(1);
			r.countUsers = rs.getInt(2);
			r.totalChanges = r.minChanges * r.countUsers;
			lst.add(r);
		}
		while(lst.size() > rankingRange && lst.size() > 1) {
			int minind = 0;
			int minsum = lst.get(0).countUsers + lst.get(1).countUsers;
			for(int i = 1 ; i < lst.size() - 1; i++){
				int sum = lst.get(i).countUsers + lst.get(i+1).countUsers;
				if(sum < minsum) {
					minind = i;
					minsum = sum;
				}
			}
			int min = Math.min(lst.get(minind).minChanges, lst.get(minind+1).minChanges);
			int max = Math.max(lst.get(minind).maxChanges, lst.get(minind+1).maxChanges);
			int changes = lst.get(minind).totalChanges + lst.get(minind + 1).totalChanges;
			lst.remove(minind);
			lst.get(minind).minChanges = min;
			lst.get(minind).maxChanges = max;
			lst.get(minind).countUsers = minsum;
			lst.get(minind).totalChanges = changes;
		}
		for(int i = 0; i < lst.size(); i++) {
			RankingRange r = lst.get(i);
			r.rank = i + 1;
			if(r.countUsers > 0) {
				r.avgChanges = r.totalChanges / ((float) r.countUsers);
			}
		}
		return report;
	}
	
	public UserRankingReport getUsersRanking(String region) throws SQLException, IOException {
		UserRankingReport report = new UserRankingReport();
		
		RankingReport ranking = getReport(OsmAndLiveReportType.RANKING, region, RankingReport.class); 
		RankingReport granking = getReport(OsmAndLiveReportType.RANKING, null, RankingReport.class);
		int minChanges = getNumberReport(OsmAndLiveReportType.RANKING_RANGE).intValue();
		report.month = month;
		report.region = region;
		report.date = reportTime();
		String q = "SELECT  t.username username, t.size changes , s.size gchanges FROM "+ 
				 	" ( SELECT username, count(*) size from changesets_view ch, changeset_country_view cc "+
				 	" 	WHERE ch.id = cc.changesetid and substr(ch.closed_at_day, 0, 8) = ? " +
				 	"   and cc.countryid = (select id from countries where downloadname= ?)"+
				 	" GROUP by ch.username HAVING count(*) >= ? " +
				 	" ORDER by count(*) desc ) t JOIN " +
				 	" (SELECT username, count(*) size from changesets_view ch " +
				 	"	WHERE substr(ch.closed_at_day, 0, 8) = ? GROUP by ch.username " +
				 	" ) s on s.username = t.username ORDER by t.size desc";
		PreparedStatement ps = conn.prepareStatement(q);
		ps.setString(1, month);
		ps.setString(2, region);
		ps.setInt(3, minChanges);
		ps.setString(4, month);
		ResultSet rs = ps.executeQuery();

		while(rs.next()) {
			UserRanking r = new UserRanking();
			
			r.user = rs.getString("username");
			r.changes = rs.getInt("changes");
			r.globalchanges= rs.getInt("gchanges");
			r.rank = 0;
			r.grank = 0;
			for (int i = 0; i < granking.rows.size(); i++) {
				RankingRange range = granking.rows.get(i);
				if (range.minChanges <= r.globalchanges && r.globalchanges <= range.maxChanges) {
					r.grank = range.rank;
				}
			}
			for (int i = 0; i < ranking.rows.size(); i++) {
				RankingRange range = ranking.rows.get(i);
				if (range.minChanges <= r.changes && r.changes <= range.maxChanges) {
					r.rank = range.rank;
				}
			}
			if(r.rank > 0) {
				report.rows.add(r);
			}
		}
		return report;
	}
	
	public RecipientsReport getRecipients(String region) throws SQLException, IOException {
		RecipientsReport report = new RecipientsReport();
		
		SupportersReport supporters = getReport(OsmAndLiveReportType.SUPPORTERS, null, SupportersReport.class);
		RankingReport ranking = getReport(OsmAndLiveReportType.RANKING, null, RankingReport.class);
		double eurValue = getNumberReport(OsmAndLiveReportType.EUR_VALUE).doubleValue();
		double btcValue = getNumberReport(OsmAndLiveReportType.BTC_VALUE).doubleValue();
		double eurBTCRate = getNumberReport(OsmAndLiveReportType.EUR_BTC_RATE).doubleValue();
		report.month = month;
		report.region = region;
		report.notReadyToPay = true;
		report.date = reportTime();
		boolean eregion = isEmpty(region);
		String q = " SELECT distinct s.osmid osmid, t.size changes," + 
					" first_value(s.btcaddr) over (partition by osmid order by updatetime desc) btcaddr " + 
					" FROM osm_recipients s left join " + 
					" 	(SELECT count(*) size, ch.username " +
					" 	 FROM changesets_view ch ";
		if(eregion) {
				q += "   WHERE substr(ch.closed_at_day, 0, 8) = ? " +
		
					 "	 GROUP by username) "+
					 "t on t.username = s.osmid ORDER by changes desc"; 
		} else {
				q += "   						 , changeset_country_view cc " +
					 "   WHERE ch.id = cc.changesetid  and substr(ch.closed_at_day, 0, 8) = ? "+
					 " 		   and cc.countryid = (select id from countries where downloadname = ?) " +
					 
					 "	 GROUP by username) "+
					 "t on t.username = s.osmid WHERE t.size is not null ORDER by changes desc";
		}
		PreparedStatement ps = conn.prepareStatement(q);
		ps.setString(1, month);
		if(!eregion) {
			ps.setString(2, region);
		}
		ResultSet rs = ps.executeQuery();
		
		report.regionPercentage = 0;
		SupportersRegion sr = supporters.regions.get(isEmpty(region) ? "" : region);
		if(sr != null) {
			report.regionPercentage = sr.percent;
		}
		report.regionCount = 0;
		report.regionTotalWeight = 0;
		while(rs.next()) {
			Recipient recipient = new Recipient();
			recipient.osmid = rs.getString("osmid");
			recipient.changes = rs.getInt("changes");
			recipient.btcaddress = rs.getString("btcaddr");
			if(isEmpty(recipient.btcaddress)) {
				continue;
			}
			report.regionCount++;
			for (int i = 0; i < ranking.rows.size() ; ++i) {
				RankingRange range = ranking.rows.get(i);
				if(recipient.changes >= range.minChanges && recipient.changes <= range.maxChanges) {
					recipient.rank = range.rank;
					if(eregion) {
						recipient.weight = getNumberReport(OsmAndLiveReportType.RANKING_RANGE).intValue() + 1 - recipient.rank; 
					} else {
						recipient.weight = getNumberReport(OsmAndLiveReportType.REGION_RANKING_RANGE).intValue() + 1 - recipient.rank;
					}
					report.regionTotalWeight += recipient.weight;
					break;
				}
			}
			report.rows.add(recipient);
		}
		report.eur = (float) eurValue;
		report.rate = (float) eurBTCRate;
		report.btc = (float) btcValue;
		report.regionBtc = report.regionPercentage * report.btc;
		report.notReadyToPay = !thisMonth;
		for(int i = 0; i < report.rows.size(); i++) {
			Recipient r = report.rows.get(i);
			if (report.regionTotalWeight > 0) {
				r.btc = report.regionBtc * r.weight / report.regionTotalWeight;
			} else {
				r.btc = 0f;
			}
		}
		return report;
	}
	
	
	public PayoutsReport getPayouts() throws IOException, SQLException {
		PayoutsReport report = new PayoutsReport();
		CountriesReport countries = getReport(OsmAndLiveReportType.COUNTRIES, null, CountriesReport.class);
		report.payoutTotal = 0d;
		report.payoutBTCAvailable = getNumberReport(OsmAndLiveReportType.BTC_VALUE).doubleValue();
		report.payoutEurAvailable = getNumberReport(OsmAndLiveReportType.EUR_VALUE).doubleValue();
		if (report.payoutBTCAvailable > 0) {
			report.rate = report.payoutEurAvailable / report.payoutBTCAvailable;
		}
		for(int i = 0; i < countries.rows.size(); i++) {
			Country c = countries.rows.get(i);
			String reg = null;
			if(!"World".equals(c.name)) {
				if("0".equals(c.map)) {
					continue;
				}
				reg = c.downloadname;
			}
			RecipientsReport recipients = getReport(OsmAndLiveReportType.RECIPIENTS, reg, RecipientsReport.class);
			for(int j = 0; j < recipients.rows.size(); j++) {
				Recipient recipient = recipients.rows.get(j);
				Payout p = new Payout();
				p.btc = recipient.btc;
				p.osmid = recipient.osmid;
				p.btcaddress = recipient.btcaddress;
				report.payoutTotal += p.btc;
				report.payouts.add(p);
			}
		}
		return report;
	}
	
	public Map<String, Object> getTotal() throws SQLException {
		Map<String, Object> res = new HashMap<>();
		List<Map<String, Object>> reports = new ArrayList<>();
		String r = "select report,region,name from final_reports where month = ?";
		PreparedStatement ps = conn.prepareStatement(r);
		ps.setString(1, month);
		ResultSet rs = ps.executeQuery();
		Gson gson = getJsonFormatter();
		while (rs.next()) {
			Map<String, Object> rt = new HashMap<>();
			rt.put("month", month);
			rt.put("region", rs.getString("region"));
			rt.put("name", rs.getString("name"));
			rt.put("report", gson.fromJson(rs.getString("report"), rt.getClass()));
			reports.add(rt);
		}
		res.put("reports", reports);
		return res;
	}
	
	
	
	private void saveReport(Object report, OsmAndLiveReportType type, String region, 
			long accessTime) throws SQLException, ParseException {
		
		String r = isEmpty(region) ? "" : region;
		PreparedStatement p = conn.prepareStatement(
				"delete from final_reports where month = ? and region = ? and name = ?");
		p.setString(1, month);
		p.setString(2, r);
		p.setString(3, type.getSqlName());
		p.executeUpdate();
		
		p = conn.prepareStatement(
				"insert into final_reports(month, region, name, report, time, accesstime) values (?, ?, ?, ?, ?, ?)");
		p.setString(1, month);
		p.setString(2, r);
		p.setString(3, type.getSqlName());
		
		if (report instanceof Number) {
			Number nm = (Number) report;
			p.setString(4, (nm.intValue() == nm.doubleValue() ? nm.intValue() : nm.doubleValue()) + "");
		} else {
			Map<String, Object> rt = new HashMap<>();
			Gson gson = getJsonFormatter();
			rt.put("name", type.getSqlName());
			rt.put("report", report);
			rt.put("month", month);
			rt.put("region", r);
			p.setString(4, gson.toJson(rt));			
		}
		long time = System.currentTimeMillis() / 1000;
		if(!thisMonth) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			time = sdf.parse(month+"-01").getTime() / 1000;
			
		}
		p.setLong(5, time);
		if(accessTime == 0) {
			p.setLong(6, time);
		}  else {
			p.setLong(6, accessTime);
		}
		p.executeUpdate();
	}
	
	public <T> T getReport(OsmAndLiveReportType type, String region, Class<T> cl) throws SQLException, IOException {
		return getJsonFormatter().fromJson(getJsonReport(type, region), cl);
	}
	
	public String getJsonReport(OsmAndLiveReportType type, String region) throws SQLException, IOException {
		Object report = null;
		Gson gson = getJsonFormatter();
		PreparedStatement ps = conn.prepareStatement("select report, time, accesstime from final_reports where month = ? and name = ? and region = ?");
		ps.setString(1, month);
		ps.setString(2, type.getSqlName());
		ps.setString(3, isEmpty(region) ? "": region);
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			if (thisMonth) {
				// TODO
				// set current time if a report was not accessed more than X minutes
				// if ((time() - $row[2]) > Constants::REFRESH_ACCESSTIME) {
				// pg_query($dbconn, "update final_reports set accesstime=".time()." where month = '${month}'
				// and region = '${rregion}' and name='${name}';");
				// }
			}
			return rs.getString("report");
		}
		
		if(type == OsmAndLiveReportType.COUNTRIES) {
			report = getCountries();
		} else if(type == OsmAndLiveReportType.SUPPORTERS) {
			report = getSupporters();
		} else if(type == OsmAndLiveReportType.TOTAL_CHANGES) {
			report = getTotalChanges(region);
		} else if(type == OsmAndLiveReportType.RANKING) {
			report = getRanking(region);
		} else if(type == OsmAndLiveReportType.USERS_RANKING) {
			report = getUsersRanking(region);
		} else if(type == OsmAndLiveReportType.RECIPIENTS) {
			report = getRecipients(region);
		} else if(type == OsmAndLiveReportType.PAYOUTS) {
			report = getPayouts();
		} else if(type == OsmAndLiveReportType.TOTAL) {
			report = getTotal();
		} else {
			throw new UnsupportedOperationException();
		}
		// TODO save report
//		if(thisMonth) {
//			saveReport(report, type, region);
//		}
		return gson.toJson(report);
	}
	
	

	public Gson getJsonFormatter() {
		return new Gson();
	}
	
	
	public Number getNumberReport(OsmAndLiveReportType type) throws IOException, SQLException {
		if (!thisMonth) {
			PreparedStatement ps = conn
					.prepareStatement("select report from final_reports where month = ? and name = ? ");
			ps.setString(1, month);
			ps.setString(2, type.getSqlName());
			ResultSet q = ps.executeQuery();
			if (q.next()) {
				return Double.parseDouble(q.getString(1));
			}
		}
		
		if(type == OsmAndLiveReportType.MIN_CHANGES) {
			return getMinChanges();
		} else if(type == OsmAndLiveReportType.REGION_RANKING_RANGE) {
			return getRegionRankingRange();
		} else if(type == OsmAndLiveReportType.RANKING_RANGE) {
			return getRankingRange();
		} else if(type == OsmAndLiveReportType.EUR_BTC_RATE) {
			return getEurBTCRate();
		} else if(type == OsmAndLiveReportType.BTC_VALUE) {
			return getBtcValue();
		} else if(type == OsmAndLiveReportType.EUR_VALUE) {
			return getEurValue();
		} else {
			throw new UnsupportedOperationException();
		}
	}


	private static boolean isEmpty(String vl) {
		return vl == null || vl.equals("");
	}
	
	
	protected static class CountriesReport {
		public String month;
		public long date;
		public List<Country> rows = new ArrayList<Country>();
		public Map<String, Country> mapId = new HashMap<String, Country>();
		public Map<String, String> map = new TreeMap<String, String>();
		
		public Country parent(Country c) {
			if(c != null && mapId.containsKey(c.parentid)) {
				return mapId.get(c.parentid);
			}
			return null;
		}
		
		public int depth(Country c) {
			// 0 is World, 1 - continents
			Country parent = parent(c);
			if(parent != null) {
				return depth(parent) + 1;
			}
			return 0;
		}
	}
	
	protected static class Country {
		public String map;
		public String id;
		public String parentid;
		public String downloadname;
		public String name;
		public String visiblename;
	}
	
	protected static class SupportersReport {
		public String month;
		public long date;
		public Map<String, SupportersRegion> regions = new HashMap<String, SupportersRegion>();
		public List<Supporter> rows = new ArrayList<OsmAndLiveReports.Supporter>();
		public int count;
		public int activeCount;
	}
	
	protected static class SupportersRegion {
		public int count;
		public String id;
		public String name;
		public float percent;
	}
	
	protected class TotalChangesReport {
		public String month;
		public String region;		
		public long date;
		public int users;
		public int changes;
	}
	
	protected static class Supporter {
		public int count;
		public String user;
		public String status;
		public String region;
		public String regionName;
		public String sku;
		
	}
	
	
	protected static class RankingReport {
		public String month;
		public String region;		
		public long date;
		public int users;
		public int changes;
		List<RankingRange> rows = new ArrayList<RankingRange>();
	}
	
	protected static class RankingRange {
		public int minChanges;
		public int maxChanges;
		public int countUsers;
		public int rank;
		public int totalChanges;
		public float avgChanges;
	}
	
	

	protected static class UserRankingReport {
		public String month;
		public long date;
		public String region;		
		List<UserRanking> rows = new ArrayList<UserRanking>();
	}
	
	protected static class UserRanking {
		public String user;
		public String name;		
		public int rank;
		public int grank;
		public int globalchanges;
		public int changes;
	}
	
	
	protected static class Recipient {

		public float btc;
		public int weight;
		public int rank;
		public String btcaddress;
		public String osmid;
		public int changes;
		
	}
	
	public static class RecipientsReport {
		public String region;
		public String month;
		public long date;
		
		public int regionTotalWeight;
		public int regionCount;
		public float regionPercentage;
		
		public float regionBtc;
		public float btc;
		public float rate;
		public float eur;
		public boolean notReadyToPay;
		
		public List<Recipient> rows = new ArrayList<Recipient>();
	}
	
	protected static class PayoutsReport {
		public long date;
		public double rate;
		public double payoutBTCAvailable;
		public double payoutEurAvailable;
		public double payoutTotal;
		public String month;
		public List<Payout> payouts = new ArrayList<Payout>();
		
	}
	
	protected class Payout {
		public String osmid;
		public String btcaddress;
		public float btc;
	}
	
	
	

}
