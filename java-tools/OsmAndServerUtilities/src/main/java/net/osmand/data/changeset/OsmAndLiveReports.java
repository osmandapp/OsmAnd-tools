package net.osmand.data.changeset;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
		OsmAndLiveReports reports = new OsmAndLiveReports();
		reports.conn = conn;
		reports.month = "2018-08";
		
//		System.out.println(reports.getJsonReport(OsmAndLiveReportType.COUNTRIES, null));
		System.out.println(reports.getJsonReport(OsmAndLiveReportType.TOTAL_CHANGES, null));
		System.out.println(reports.getJsonReport(OsmAndLiveReportType.RANKING, null));
		System.out.println(reports.getJsonReport(OsmAndLiveReportType.SUPPORTERS, null));
		System.out.println(reports.getJsonReport(OsmAndLiveReportType.USERS_RANKING, "belarus_europe"));
		System.out.println(reports.getJsonReport(OsmAndLiveReportType.RECIPIENTS, null));
//		System.out.println(reports.getJsonReport(OsmAndLiveReportType.PAYOUTS, null));
		
//		reports.buildReports(conn);
	}
	
	private String month;
	private Connection conn;
	private CountriesReport countriesReport;
	private SupportersReport supportersReport;
	
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
		double rt = getEurBTCRate();
		double eur = getEurValue();
		if (rt != 0) {
			return eur / rt;
		}
		return 0;
	}


	private double getEurValue() throws SQLException {
		SupportersReport supporters = getSupporters();
		return supporters.activeCount * 0.4 ; // 1 EUR - 20% (GPlay) - 50% (OsmAnd)
	}
	
	
	public CountriesReport getCountries() throws SQLException{
		if(countriesReport != null) {
			return countriesReport;
		}
		PreparedStatement ctrs = conn.prepareStatement( "select id, parentid, downloadname, name, map from countries;");
		ResultSet rs = ctrs.executeQuery();
		countriesReport = new CountriesReport();
		countriesReport.month = month;
		while(rs.next()) {
			Country c = new Country();
			c.id = rs.getString("id");
			c.parentid = rs.getString("parentid");
			c.name = rs.getString("name");
			c.map = rs.getString("map");
			if("World".equals(c.name)) {
				c.downloadname = "";
			} else if("0".equals(c.map)) {
				continue;
			} else {
				c.downloadname = rs.getString("downloadname");
			}
			countriesReport.map.put(c.downloadname, c.name);
			countriesReport.rows.add(c);
		}
		return countriesReport;
	}
	
	
	private static String supportersQuery() {
		return "select s.userid uid, s.visiblename visiblename, s.preferred_region region, s.useremail email, "+
           " t.sku sku, t.checktime checktime, t.starttime starttime, t.expiretime expiretime from supporters s " +
           " join (select userid, sku, max(checktime) checktime, max(starttime) starttime, max(expiretime) expiretime " +
           "  from supporters_device_sub where expiretime is not null and expiretime > now() group by userid, sku) t " +  
           " on s.userid = t.userid where s.preferred_region is not null and s.preferred_region <> 'none' order by s.userid;";
	}

	public SupportersReport getSupporters() throws SQLException {
		if (supportersReport != null) {
			return supportersReport;
		}
		CountriesReport countries = getCountries();
		supportersReport = new SupportersReport();
		supportersReport.month = month;
		SupportersRegion worldwide = new SupportersRegion();
		worldwide.id = "";
		worldwide.name = "Worldwide";
		supportersReport.regions.put("", worldwide);
		PreparedStatement q = conn.prepareStatement(supportersQuery());
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
	
	public RankingReport getRanking(String region) throws SQLException {
		RankingReport report = new RankingReport();
		report.month = month;
		report.region = region;
		int minChanges = getMinChanges();
		int rankingRange;
		ResultSet rs;
		if(!isEmpty(region)) {
			rankingRange = getRegionRankingRange();
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
			rankingRange = getRankingRange();
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
	
	public UserRankingReport getUserWorldRanking(String region) throws SQLException {
		UserRankingReport report = new UserRankingReport();
		
		RankingReport ranking = getRanking(region);
		RankingReport granking = getRanking(null);
		report.month = month;
		report.region = region;
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
		ps.setInt(3, getMinChanges());
		ps.setString(4, month);
		ResultSet rs = ps.executeQuery();

		while(rs.next()) {
			UserRanking r = new UserRanking();
			
			r.name = rs.getString("username");
			r.changes= rs.getInt("changes");
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
				if (range.minChanges <= r.globalchanges && r.globalchanges <= range.maxChanges) {
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
		SupportersReport supporters = getSupporters();
		RankingReport ranking = getRanking(null);
		double eurValue = getEurValue();
		double btcValue = getBtcValue();
		double eurBTCRate = getEurBTCRate();
		RecipientsReport report = new RecipientsReport();
		report.month = month;
		report.region = region;
		boolean eregion = isEmpty(region);
		String q = " SELECT distinct s.osmid osmid, t.size changes," + 
					" first_value(s.btcaddr) over (partition by osmid order by updatetime desc) btcaddr " + 
					" FROM osm_recipients s left join " + 
					" 	(SELECT count(*) size, ch.username " +
					" 	 FROM changesets_view ch ";
		if(eregion) {
				q += "   WHERE substr(ch.closed_at_day, 0, 8) = ? " +
		
					 "	 GROUP by username) "+
					 "t on t.username = s.osmid WHERE t.size is not null ORDER by changes desc"; 
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
						recipient.weight = getRankingRange() + 1 - recipient.rank; 
					} else {
						recipient.weight = getRankingRange() + 1 - recipient.rank;
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
		for(int i = 0; i < report.rows.size(); i++) {
			Recipient r = report.rows.get(i);
			if(report.regionTotalWeight > 0) {
				r.btc = report.btc  * r.weight / report.regionTotalWeight; 
			} else {
				r.btc = 0f;
			}
		}
		return report;
	}
	
	
	public PayoutsReport getPayouts() throws IOException, SQLException {
		PayoutsReport report = new PayoutsReport();
		CountriesReport countries = getCountries();
		report.payoutTotal = 0d;
		report.payoutBTCAvailable = getBtcValue();
		report.payoutEurAvailable = getEurValue();
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
			RecipientsReport recipients = getRecipients(reg);
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

	public String getJsonReport(OsmAndLiveReportType type, String region) throws SQLException, IOException {
		Gson gson = new Gson();
		if(type == OsmAndLiveReportType.COUNTRIES) {
			return gson.toJson(getCountries());
		} else if(type == OsmAndLiveReportType.SUPPORTERS) {
			return gson.toJson(getSupporters());
		} else if(type == OsmAndLiveReportType.TOTAL_CHANGES) {
			return gson.toJson(getTotalChanges(region));
		} else if(type == OsmAndLiveReportType.RANKING) {
			return gson.toJson(getRanking(region));
		} else if(type == OsmAndLiveReportType.USERS_RANKING) {
			return gson.toJson(getUserWorldRanking(region));
		} else if(type == OsmAndLiveReportType.RECIPIENTS) {
			return gson.toJson(getRecipients(region));
		} else if(type == OsmAndLiveReportType.PAYOUTS) {
			return gson.toJson(getPayouts());
		} else {
			throw new UnsupportedOperationException();
		}
	}
	
	public Number getNumberReport(OsmAndLiveReportType type) throws IOException, SQLException {
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
		public List<Country> rows = new ArrayList<Country>();
		public Map<String, String> map = new TreeMap<String, String>();
	}
	
	protected static class Country {
		public String map;
		public String id;
		public String parentid;
		public String downloadname;
		public String name;
	}
	
	protected static class SupportersReport {
		public String month;
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
	
	
	protected class RankingReport {
		public String month;
		public String region;		
		public int users;
		public int changes;
		List<RankingRange> rows = new ArrayList<RankingRange>();
	}
	
	protected class RankingRange{
		public int minChanges;
		public int maxChanges;
		public int countUsers;
		public int rank;
		public int totalChanges;
		public float avgChanges;
	}
	
	

	protected class UserRankingReport {
		public String month;
		public String region;		
		List<UserRanking> rows = new ArrayList<UserRanking>();
	}
	
	protected class UserRanking {
		public String name;		
		public int rank;
		public int grank;
		public int globalchanges;
		public int changes;
	}
	
	
	protected class Recipient {

		public float btc;
		public int weight;
		public int rank;
		public String btcaddress;
		public String osmid;
		public int changes;
		
	}
	
	protected class RecipientsReport {
		public String region;
		public String month;
		
		public int regionTotalWeight;
		public int regionCount;
		public float regionPercentage;
		
		public float regionBtc;
		public float btc;
		public float rate;
		public float eur;
		
		public List<Recipient> rows = new ArrayList<Recipient>();
	}
	
	protected class PayoutsReport {
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
