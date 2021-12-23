package net.osmand.data.changeset;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.osmand.PlatformUtil;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.json.JSONObject;

import com.google.gson.Gson;

public class OsmAndLiveReports {

	
	public static final double DEFAULT_APPROXIMATION_DONATION = 500; // eur
	public static final double PAYOUT_PERCENT = 0.03; // 3%
	
	private static final Log LOG = PlatformUtil.getLog(OsmAndLiveReports.class);
	
	// changesets_view (quick) or changesets (if we need to generate older > 3 months)
	public static final String CHANGESETS_VIEW = "changesets_view";
	// changeset_country_view (quick) or changeset_country (if we need to generate older > 3 months)
	public static final String CHANGESET_COUNTRY_VIEW = "changeset_country_view"; 
	
	public static final long MINUTE = 60 * 1000l; // 15 minutes
	public static final long HOUR = 60 * MINUTE; // 15 minutes
	
	public static final long REFRESH_ACCESSTIME = 15 * MINUTE;
	public static final long REPORTS_DELETE_DEPRECATED = 6 * HOUR;
	
	public static final String MONTH_START_COUNT_CHANGES = "2021-12";

	 
	
	
	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection(System.getenv("DB_CONN"),
				isEmpty(System.getenv("DB_USER")) ? "test" : System.getenv("DB_USER"),
						isEmpty(System.getenv("DB_PWD")) ? "test" : System.getenv("DB_PWD"));
		if(args != null && args.length > 0) {
			Date firstDayThisMonth = 
					new SimpleDateFormat("yyyy-MM-dd").parse(String.format("%1$tY-%1$tm-01", new Date()));
			String prevMonth = new SimpleDateFormat("yyyy-MM").format(firstDayThisMonth.getTime() - 5 * 24 * HOUR);
			if(args[0].equals("check-missing-reports")) {
				checkMissingReports(conn);
			} else if(args[0].equals("refresh-current-month")) {
				refreshCurrentMonth(conn);
			} else if(args[0].equals("set-btc-donation-current-month")) {
				double btcDonation = 0;
				OsmAndLiveReports reports = new OsmAndLiveReports(conn, null);
				for(int i = 1; i < args.length; i++) {
					String value = args[i];
					String key = "";
					if(value.contains("=")) {
						key = value.substring(0, value.indexOf('='));
						value = value.substring(value.indexOf('=') + 1);
					}
					if(key.equals("--btcDonation")) {
						btcDonation = Double.parseDouble(value);
					}
				}
				reports.saveReport(btcDonation +"", OsmAndLiveReportType.BTC_DONATION_VALUE, null, null);
			} else if(args[0].equals("finalize-previous-month-total")) {
				System.out.println("Previous month is " + prevMonth);
				OsmAndLiveReports reports = new OsmAndLiveReports(conn, prevMonth);
//				CountriesReport cntrs = reports.getReport(OsmAndLiveReportType.COUNTRIES, null, CountriesReport.class);
//				for (Country reg : cntrs.rows) {
//					if(reg.map.equals("1")) {
//						reports.getJsonReport(OsmAndLiveReportType.RECIPIENTS, reg.downloadname, false, true);
//					}
//				}
				reports.getJsonReport(OsmAndLiveReportType.RECIPIENTS, null, false, true);
				reports.getJsonReport(OsmAndLiveReportType.PAYOUTS, null, false, true);
				reports.getJsonReport(OsmAndLiveReportType.TOTAL, null, false, true);
			} else if(args[0].equals("finalize-previous-month")) {
				double btc = Double.NaN;
				double eur = Double.NaN;
				double btcEurRate = Double.NaN;
				double btcDonation = 0;
				System.out.println("Previous month is " + prevMonth);
//				String currentMonth = String.format("%1$tY-%1$tm", new Date());
				String month = prevMonth;
				System.out.println("Processing month is " + month);
				for(int i = 1; i < args.length; i++) {
					String value = args[i];
					String key = "";
					if(value.contains("=")) {
						key = value.substring(0, value.indexOf('='));
						value = value.substring(value.indexOf('=') + 1);
					}
					if(key.equals("--btc")) {
						btc = Double.parseDouble(value);
					} else if(key.equals("--eur")) {
						eur = Double.parseDouble(value);
					} else if(key.equals("--btcEurRate")) {
						btcEurRate = Double.parseDouble(value);
					} else if(key.equals("--btcDonation")) {
						btcDonation = Double.parseDouble(value);
					}
				}
				if(Double.isNaN(btc)) {
					throw new IllegalArgumentException("You didn't specify amount btc to pay");
				}
				if(Double.isNaN(btcEurRate)) {
					throw new IllegalArgumentException("You didn't specify amount btc to eur rate");
				}
				if(Double.isNaN(eur)) {
					eur = btc * btcEurRate;
				}
				finalizeMonth(conn, month, btc, eur, btcEurRate, btcDonation);
			} else {
				throw new UnsupportedOperationException();
			}
		} else {
			System.out.println("Please specify parameter: check-missing-reports (checks reports in past months and generates), "
					+ "refresh-current-month (refreshes views and generated reports for this month)");
		}
	}
	
	private static void finalizeMonth(Connection conn, String month, double btc, double eur, double actualRate, double btcDonation) throws SQLException, IOException, ParseException {
		LOG.info("Refreshing materialized views ");
		conn.createStatement().execute("REFRESH MATERIALIZED VIEW  changesets_view");
		LOG.info("changesets_view refreshed");
		conn.createStatement().execute("REFRESH MATERIALIZED VIEW  changeset_country_view");
		LOG.info("changeset_country_view refreshed");
		OsmAndLiveReports reports = new OsmAndLiveReports(conn, month);
		reports.getJsonReport(OsmAndLiveReportType.COUNTRIES, null, false, true);

		reports.saveReport(btc +"", OsmAndLiveReportType.BTC_VALUE, null, null);
		reports.saveReport(eur +"", OsmAndLiveReportType.EUR_VALUE, null, null);
		if (btc > 0) {
			reports.saveReport(((float) eur / btc) + "", OsmAndLiveReportType.EUR_BTC_RATE, null, null);
		} else {
			reports.saveReport(actualRate + "", OsmAndLiveReportType.EUR_BTC_RATE, null, null);
		}
		reports.saveReport(actualRate + "", OsmAndLiveReportType.EUR_BTC_ACTUAL_RATE, null, null);
		reports.saveReport(btcDonation + "", OsmAndLiveReportType.BTC_DONATION_VALUE, null, null);
		reports.saveReport(reports.getRankingRange() + "", OsmAndLiveReportType.RANKING_RANGE, null, null);
		reports.saveReport(reports.getMinChanges() + "", OsmAndLiveReportType.MIN_CHANGES, null, null);
		
		reports.getJsonReport(OsmAndLiveReportType.COUNTRIES, null, false, true);
		
		reports.getJsonReport(OsmAndLiveReportType.TOTAL_CHANGES, null, false, true);
		reports.getJsonReport(OsmAndLiveReportType.RANKING, null, false, true);
		reports.getJsonReport(OsmAndLiveReportType.USERS_RANKING, null, false, true);
		reports.getJsonReport(OsmAndLiveReportType.RECIPIENTS, null, false, true);
		reports.getJsonReport(OsmAndLiveReportType.PAYOUTS, null, false, true);
		
		CountriesReport cntrs = reports.getReport(OsmAndLiveReportType.COUNTRIES, null, CountriesReport.class);
		for (Country reg : cntrs.rows) {
			if (reg.map.equals("1")) {
				reports.getJsonReport(OsmAndLiveReportType.TOTAL_CHANGES, reg.downloadname, false, true);
				reports.getJsonReport(OsmAndLiveReportType.RANKING, reg.downloadname, false, true);
				reports.getJsonReport(OsmAndLiveReportType.USERS_RANKING, reg.downloadname, false, true);
			}
		}
		
		reports.getJsonReport(OsmAndLiveReportType.TOTAL, null, false, true);
		
	}

	private static void refreshCurrentMonth(Connection conn) throws SQLException, IOException {
		LOG.info("Refreshing materialized views ");
		conn.createStatement().execute("REFRESH MATERIALIZED VIEW  changesets_view");
		LOG.info("changesets_view refreshed");
		conn.createStatement().execute("REFRESH MATERIALIZED VIEW  changeset_country_view");
		LOG.info("changeset_country_view refreshed");
		OsmAndLiveReports reports = new OsmAndLiveReports(conn, null);
		reports.getJsonReport(OsmAndLiveReportType.COUNTRIES, null, false, true);
		reports.getJsonReport(OsmAndLiveReportType.TOTAL_CHANGES, null, false, true);
		reports.getJsonReport(OsmAndLiveReportType.RANKING, null, false, true);
		reports.getJsonReport(OsmAndLiveReportType.USERS_RANKING, null, false, true);
		reports.getJsonReport(OsmAndLiveReportType.RECIPIENTS, null, false, true);
		
		PreparedStatement dl = conn.prepareStatement("delete from final_reports where month = ? and region = ? and name = ?");
		PreparedStatement ps = conn.prepareStatement("select name, accesstime, region, time from final_reports where month = ?");
		ps.setString(1, reports.month);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			String name = rs.getString("name");
			String region = rs.getString("region");
			Timestamp accesstime = rs.getTimestamp("accesstime");
			if (isEmpty(region)) {
				continue;
			}
			if (accesstime == null || System.currentTimeMillis() - accesstime.getTime() > REPORTS_DELETE_DEPRECATED) {
				dl.setString(1, reports.month);
				dl.setString(2, region);
				dl.setString(3, name);
				dl.execute();
				LOG.info(String.format("Deleting report '%s' '%s' region '%s' outdated ", name, reports.month, region));
			} else {
				reports.getJsonReport(OsmAndLiveReportType.fromSqlName(name), region, false, true);
			}
		}
		dl.close();
		ps.close();

	}

	protected static void checkMissingReports(Connection conn) throws SQLException, IOException,
			ParseException {
		PreparedStatement ps = conn.prepareStatement("select report, time, accesstime from final_reports where month = ? and name = ? and region = ?");
		Calendar cd = Calendar.getInstance();
		for (int y = 2016; y <= cd.get(Calendar.YEAR); y++) {
			int si = 1;
			// don't include current month
			int ei = y == cd.get(Calendar.YEAR) ? cd.get(Calendar.MONTH) : 12;
			for (int i = si; i <= ei; i++) {
				int s = 0;
				String m = i < 10 ? "0" + i : i + "";
				String month = y + "-" + m;
				System.out.println("TEST " + month);
				OsmAndLiveReports reports = new OsmAndLiveReports(conn, month);
				CountriesReport cntrs = reports.getReport(OsmAndLiveReportType.COUNTRIES, null, CountriesReport.class);
				checkReport(ps, month, OsmAndLiveReportType.COUNTRIES, null);
				checkReport(ps, month, OsmAndLiveReportType.PAYOUTS, null);
				
				s+=3;
				for (Country reg : cntrs.rows) {
					if(reg.map.equals("1") || isEmpty(reg.downloadname)) {
						if(!checkReport(ps, month, OsmAndLiveReportType.RANKING, reg.downloadname)) {
							reports.getJsonReport(OsmAndLiveReportType.RANKING, reg.downloadname, false, true);
						}
						if(!checkReport(ps, month, OsmAndLiveReportType.TOTAL_CHANGES, reg.downloadname)) {
							reports.getJsonReport(OsmAndLiveReportType.TOTAL_CHANGES, reg.downloadname, false, true);
						}
						if(!checkReport(ps, month, OsmAndLiveReportType.USERS_RANKING, reg.downloadname)) {
							reports.getJsonReport(OsmAndLiveReportType.USERS_RANKING, reg.downloadname, false, true);
						}
						checkReport(ps, month, OsmAndLiveReportType.RECIPIENTS, reg.downloadname);
					}
					s+=4;
				}
				if (!checkReport(ps, month, OsmAndLiveReportType.RANKING_RANGE, null)) {
					reports.saveReport(reports.getRankingRange() + "", OsmAndLiveReportType.RANKING_RANGE, null, null);
				}
				if (!checkReport(ps, month, OsmAndLiveReportType.MIN_CHANGES, null)) {
					reports.saveReport(reports.getMinChanges() + "", OsmAndLiveReportType.MIN_CHANGES, null, null);
				}
				checkReport(ps, month, OsmAndLiveReportType.BTC_VALUE, null);
				if (!checkReport(ps, month, OsmAndLiveReportType.BTC_DONATION_VALUE, null)) {
					reports.saveReport(0 + "", OsmAndLiveReportType.BTC_DONATION_VALUE, null, null);
				}
				if (!checkReport(ps, month, OsmAndLiveReportType.EUR_BTC_RATE, null)) {
					Number eur = reports.getNumberReport(OsmAndLiveReportType.EUR_VALUE);
					Number btc = reports.getNumberReport(OsmAndLiveReportType.BTC_VALUE);
					if(!Double.isNaN(eur.doubleValue()) && !Double.isNaN(btc.doubleValue()) ) {
						reports.saveReport(((float) eur.doubleValue() / btc.doubleValue()) + "",
								OsmAndLiveReportType.EUR_BTC_RATE, null, null);
					}
				}
				s+=6;
				System.out.println(String.format("TESTED %d reports", s));
			}
		}
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

	public int getRankingRange() {
		return 20;
	}
	
	public int getMinChanges() {
		return 300;
	}
	
	private double getEurBTCRate() throws IOException {
		URLConnection un = new URL("https://blockchain.info/ticker").openConnection();
		StringBuilder bld = Algorithms.readFromInputStream(un.getInputStream());
		JSONObject obj = new JSONObject(bld.toString());
		return obj.getJSONObject("EUR").getDouble("sell");
	}

	private double getBtcValue() throws IOException, SQLException {
		double rt = getNumberReport(OsmAndLiveReportType.EUR_BTC_RATE).doubleValue();
		double eur = DEFAULT_APPROXIMATION_DONATION;
		if (rt != 0) {
			return eur / rt;
		}
		return 0;
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
           " t.sku sku, t.orderid orderid, t.checktime checktime, t.starttime starttime, t.expiretime expiretime from supporters s " +
           " join (select orderid, sku, checktime, starttime, expiretime " +
           "  from supporters_device_sub where expiretime is not null and " + cond +" ) t " +  
           " on s.orderid = t.orderid where s.preferred_region is not null and s.preferred_region <> 'none' order by s.userid;";
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
		Set<String> orderIds = new TreeSet<>();
		ResultSet rs = q.executeQuery();
		while (rs.next()) {
			// Don't count same purchase twice
			String orderId = rs.getString("orderid");
			if (orderIds.contains(orderId)) {
				continue;
			}
			orderIds.add(orderId);
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
			// since 2021-03 don't calculate region percentage
			if (month != null && month.compareTo("2021-03") < 0) {
				if (supportersReport.activeCount > 0) {
					r.percent = ((float) r.count) / (2 * supportersReport.activeCount);
				} else {
					r.percent = 0;
				}
			} else {
				r.percent = isEmpty(s) ? 1 : 0;
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
			String r = "select count(distinct ch.username) users, count(distinct ch.id) changes, sum(distinct ch.changes_count) achanges " + 
					  " from "+CHANGESETS_VIEW + " ch, " + CHANGESET_COUNTRY_VIEW+" cc where ch.id = cc.changesetid"+ 
					  " and cc.countryid = (select id from countries where downloadname= ?)"+
					  " and substr(ch.closed_at_day, 0, 8) = ?";
			PreparedStatement ps = conn.prepareStatement(r);
			ps.setString(1, region);
			ps.setString(2, month);
			rs = ps.executeQuery();
		} else {
			String r = "select count ( distinct username) users, count(*) changes, sum(changes_count) achanges from "+CHANGESETS_VIEW+
						" where substr(closed_at_day, 0, 8) = ?";
			PreparedStatement ps = conn.prepareStatement(r);
			ps.setString(1, month);
			rs = ps.executeQuery();
		}
		rs.next();
		report.users = rs.getInt("users");
		report.changes = rs.getInt("changes");
		report.allchanges = rs.getInt("achanges");
		return report;
	}
	
	public RankingReport getRanking(String region) throws SQLException, IOException {
		RankingReport report = new RankingReport();
		report.month = month;
		report.region = region;
		report.date = reportTime();
		boolean startCountingChanges = this.month.compareTo(MONTH_START_COUNT_CHANGES) >= 0;
		int minChanges;
		int rankingRange;
		ResultSet rs;
		String cntChanges = startCountingChanges ? "sum(changes_count)" : "count(*)";
		if(!isEmpty(region)) {
			minChanges = getNumberReport(OsmAndLiveReportType.REGION_MIN_CHANGES).intValue();
			rankingRange = getNumberReport(OsmAndLiveReportType.REGION_RANKING_RANGE).intValue();
		    String r =  " SELECT data.cnt changes, count(*) group_size, sum(cnt_changes) achanges, sum(cnt_changesets) achangesets FROM ("+
		    			"  		SELECT username, " + cntChanges + " cnt, sum(ch.changes_count) cnt_changes, count(*) cnt_changesets  FROM " +CHANGESETS_VIEW + " ch, " + CHANGESET_COUNTRY_VIEW + " cc " + 
		    			"		WHERE substr(ch.closed_at_day, 0, 8) = ? and ch.id = cc.changesetid  "+
		    			"  			and cc.countryid = (SELECT id from countries where downloadname = ? )" +
		    			" 		GROUP by ch.username HAVING " + cntChanges + " >= ? ORDER by " + cntChanges + " desc) " +
		    			" data GROUP by data.cnt ORDER by changes desc";
			PreparedStatement ps = conn.prepareStatement(r);
			ps.setString(1, month);
			ps.setString(2, region);
			ps.setInt(3, minChanges);
			rs = ps.executeQuery();
		} else {
			minChanges = getNumberReport(OsmAndLiveReportType.MIN_CHANGES).intValue();
			rankingRange = getNumberReport(OsmAndLiveReportType.RANKING_RANGE).intValue();
			String r = "SELECT data.cnt changes, count(*) group_size, sum(cnt_changes) achanges, sum(cnt_changesets) achangesets FROM ( "+
					   "	SELECT username, " + cntChanges + " cnt, sum(changes_count) cnt_changes, count(*) cnt_changesets FROM " + CHANGESETS_VIEW +" ch " +
					   "    WHERE substr(ch.closed_at_day, 0, 8) = ? " +
					   " 	GROUP by  ch.username HAVING " + cntChanges + " >= ? ORDER by " + cntChanges + " desc) " +
					   " data GROUP by data.cnt ORDER by changes desc";
			PreparedStatement ps = conn.prepareStatement(r);
			ps.setString(1, month);
			ps.setInt(2, minChanges);
			rs = ps.executeQuery();
		}
		List<RankingRange> lst = report.rows;
		while (rs.next()) {
			RankingRange r = new RankingRange();
			r.minChanges = rs.getInt(1);
			r.maxChanges = rs.getInt(1);
			r.countUsers = rs.getInt(2);
			r.atomTotalChanges = rs.getInt(3);
			r.totalChanges = rs.getInt(4);
			lst.add(r);
		}
		while (lst.size() > rankingRange && lst.size() > 1) {
			int minind = 0;
			int minsum = lst.get(0).countUsers + lst.get(1).countUsers;
			for (int i = 1; i < lst.size() - 1; i++) {
				int sum = lst.get(i).countUsers + lst.get(i + 1).countUsers;
				if (sum < minsum) {
					minind = i;
					minsum = sum;
				}
			}
			int min = Math.min(lst.get(minind).minChanges, lst.get(minind + 1).minChanges);
			int max = Math.max(lst.get(minind).maxChanges, lst.get(minind + 1).maxChanges);
			int changes = lst.get(minind).totalChanges + lst.get(minind + 1).totalChanges;
			int atomTotalChanges = lst.get(minind).atomTotalChanges + lst.get(minind + 1).atomTotalChanges;
			lst.remove(minind);
			lst.get(minind).minChanges = min;
			lst.get(minind).maxChanges = max;
			lst.get(minind).countUsers = minsum;
			lst.get(minind).totalChanges = changes;
			lst.get(minind).atomTotalChanges = atomTotalChanges;
		}
		for(int i = 0; i < lst.size(); i++) {
			RankingRange r = lst.get(i);
			r.rank = i + 1;
			if (r.countUsers > 0) {
				r.avgChanges = r.totalChanges / ((float) r.countUsers);
			}
		}
		return report;
	}
	
	public UserRankingReport getUsersRanking(String region) throws SQLException, IOException {
		UserRankingReport report = new UserRankingReport();
		
		RankingReport ranking = getReport(OsmAndLiveReportType.RANKING, region, RankingReport.class); 
		RankingReport granking = getReport(OsmAndLiveReportType.RANKING, null, RankingReport.class);
		int minChanges = getNumberReport(OsmAndLiveReportType.MIN_CHANGES).intValue();
		report.month = month;
		report.region = region;
		report.date = reportTime();
		String q = "SELECT  t.username username, t.size changes, s.size gchanges, t.asize achanges, s.asize agchanges FROM "+ 
				 	" ( SELECT username, count(*) size, sum(changes_count) asize from "+CHANGESETS_VIEW+" ch, "+CHANGESET_COUNTRY_VIEW+" cc "+
				 	" 	WHERE ch.id = cc.changesetid and substr(ch.closed_at_day, 0, 8) = ? " +
				 	"   and cc.countryid = (select id from countries where downloadname= ?)"+
				 	" GROUP by ch.username HAVING count(*) >= ? " +
				 	" ORDER by count(*) desc ) t JOIN " +
				 	" (SELECT username, count(*) size, sum(changes_count) asize from "+CHANGESETS_VIEW+" ch " +
				 	"	WHERE substr(ch.closed_at_day, 0, 8) = ? GROUP by ch.username " +
				 	" ) s on s.username = t.username ORDER by t.size desc";
		PreparedStatement ps = conn.prepareStatement(q);
		ps.setString(1, month);
		ps.setString(2, region);
		ps.setInt(3, minChanges);
		ps.setString(4, month);
		ResultSet rs = ps.executeQuery();
		boolean startCountingChanges = this.month.compareTo(MONTH_START_COUNT_CHANGES) >= 0;
		while(rs.next()) {
			UserRanking r = new UserRanking();
			
			r.user = rs.getString("username");
			r.changes = rs.getInt("changes");
			r.globalchanges = rs.getInt("gchanges");
			r.atomglobalchanges = rs.getInt("achanges"); // SWAP:INCORRECT but already stored in reports
			r.atomchanges = rs.getInt("agchanges"); // SWAP:INCORRECT but already stored in reports
			r.rank = 0;
			r.grank = 0;
			for (int i = 0; i < granking.rows.size(); i++) {
				RankingRange range = granking.rows.get(i);
				int ch = startCountingChanges ? r.atomchanges : r.globalchanges;
				if (range.minChanges <= ch && ch <= range.maxChanges) {
					r.grank = range.rank;
				}
			}
			for (int i = 0; i < ranking.rows.size(); i++) {
				RankingRange range = ranking.rows.get(i);
				int ch = startCountingChanges ? r.atomglobalchanges : r.changes;
				if (range.minChanges <= ch && ch <= range.maxChanges) {
					r.rank = range.rank;
				}
			}
			if(r.rank > 0) {
				report.rows.add(r);
			}
		}
		return report;
	}
	
	public RecipientsReport getRecipients() throws SQLException, IOException {
		RecipientsReport report = new RecipientsReport();
		RankingReport ranking = getReport(OsmAndLiveReportType.RANKING, null, RankingReport.class);
		double btcDonationValue = getNumberReport(OsmAndLiveReportType.BTC_DONATION_VALUE).doubleValue();
		double btcValueApproximate = getNumberReport(OsmAndLiveReportType.BTC_VALUE).doubleValue();
		double btcValue = btcValueApproximate + btcDonationValue;
		report.month = month;
		report.region = null;
		report.date = reportTime();
		String q = " SELECT distinct s.osmid osmid, t.size changes, t.objsize objchanges," + 
					" first_value(s.btcaddr) over (partition by osmid order by updatetime desc) btcaddr " + 
					" FROM osm_recipients s left join " + 
					" 	(SELECT count(*) size, sum(changes_count) objsize, ch.username " +
					" 	 FROM " + CHANGESETS_VIEW + " ch " +
					"   WHERE substr(ch.closed_at_day, 0, 8) = ? " +
					"	 GROUP by username) " +
					" t on t.username = s.osmid ORDER by changes desc"; 
		// old query for region 
//				q += "   						 , " + CHANGESET_COUNTRY_VIEW + " cc " +
//					 "   WHERE ch.id = cc.changesetid  and substr(ch.closed_at_day, 0, 8) = ? "+
//					 " 		   and cc.countryid = (select id from countries where downloadname = ?) " +
//					 
//					 "	 GROUP by username) "+
//					 "t on t.username = s.osmid WHERE t.size is not null ORDER by changes desc";
		PreparedStatement ps = conn.prepareStatement(q);
		ps.setString(1, month);
		ResultSet rs = ps.executeQuery();
		int rankingNum = getNumberReport(OsmAndLiveReportType.RANKING_RANGE).intValue();
		boolean startCountingChanges = this.month.compareTo(MONTH_START_COUNT_CHANGES) >= 0;
		while (rs.next()) {
			Recipient recipient = new Recipient();
			recipient.osmid = rs.getString("osmid");
			recipient.changes = rs.getInt("changes");
			recipient.objchanges = rs.getInt("objchanges");
			recipient.btcaddress = rs.getString("btcaddr");
			if (isEmpty(recipient.btcaddress)) {
				continue;
			}
			report.regionCount++;
			for (int i = 0; i < ranking.rows.size(); ++i) {
				RankingRange range = ranking.rows.get(i);
				int ch = startCountingChanges ? recipient.objchanges : recipient.changes; 
				if (ch >= range.minChanges && ch <= range.maxChanges) {
					recipient.rank = range.rank;
					recipient.weight = rankingNum + 1 - recipient.rank;
					report.regionTotalWeight += recipient.weight;
					break;
				}
			}
			report.rows.add(recipient);
		}
		report.btc = btcValue;
		report.btcPayoutComission = (btcValue * PAYOUT_PERCENT);
		report.regionBtc = report.btc - report.btcPayoutComission;
		report.notReadyToPay = Double.isNaN(loadNumberReport(OsmAndLiveReportType.BTC_VALUE));
		for (int i = 0; i < report.rows.size(); i++) {
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
		RecipientsReport recipients = getReport(OsmAndLiveReportType.RECIPIENTS, null, RecipientsReport.class);
		report.payoutTotal = 0d;
		report.payoutBTCAvailable = recipients.btc;
		report.payoutBTCCollected = recipients.btc;
		report.payoutBTCComission = recipients.btcPayoutComission;
		for (int j = 0; j < recipients.rows.size(); j++) {
			Recipient recipient = recipients.rows.get(j);
			Payout p = new Payout();
			p.btc = recipient.btc;
			p.osmid = recipient.osmid;
			p.btcaddress = recipient.btcaddress;
			if (p.btc > 0) {
				report.payoutTotal += p.btc;
				report.payments.add(p);
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
			try {
				if (OsmAndLiveReportType.fromSqlName(rs.getString("name")).isNumberReport()) {
					rt.put("report", rs.getDouble("report"));
				} else {
					rt.put("report", gson.fromJson(rs.getString("report"), rt.getClass()));
				}
				reports.add(rt);
			} catch(IllegalArgumentException e) {
				// don't add report if it is too unknown type
				
			}
		}
		res.put("reports", reports);
		return res;
	}
	
	
	
	private void saveReport(String report, OsmAndLiveReportType type, String region, Timestamp accessTime) throws SQLException, ParseException {
		String r = isEmpty(region) ? "" : region;
		LOG.info(String.format("Saving report '%s' '%s' region '%s' ", type.getSqlName(), month, region));
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
		p.setString(4, report);			
		Timestamp time = new Timestamp(System.currentTimeMillis());
		if(!thisMonth) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			time = new Timestamp(sdf.parse(month+"-01").getTime());
		}
		p.setTimestamp(5, time);
		if(accessTime == null) {
			p.setTimestamp(6, time);
		}  else {
			p.setTimestamp(6, accessTime);
		}
		p.executeUpdate();
	}
	
	public <T> T getReport(OsmAndLiveReportType type, String region, Class<T> cl) throws SQLException, IOException {
		return getJsonFormatter().fromJson(getJsonReport(type, region), cl);
	}
	
	public String getJsonReport(OsmAndLiveReportType type, String region) throws SQLException, IOException {
		return getJsonReport(type, region, true, false);
	}

	public String getJsonReport(OsmAndLiveReportType type, String region, boolean useCache, boolean forceSave)
			throws SQLException, IOException {
		Object report = null;
		Gson gson = getJsonFormatter();
		PreparedStatement ps = conn
				.prepareStatement("select report, time, accesstime from final_reports where month = ? and name = ? and region = ?");
		ps.setString(1, month);
		ps.setString(2, type.getSqlName());
		ps.setString(3, isEmpty(region) ? "" : region);
		ResultSet rs = ps.executeQuery();
		Timestamp accesstime = null;
		if (rs.next()) {
			accesstime = rs.getTimestamp("accesstime");
			String retReport = rs.getString("report");
			rs.close();
			ps.close();
			if (useCache) {
				if (thisMonth) {
					long time = System.currentTimeMillis();
					// set current time if a report was not accessed more than X minutes
					if (accesstime == null || time - accesstime.getTime() > REFRESH_ACCESSTIME) {
						PreparedStatement upd = conn
								.prepareStatement("update final_reports set accesstime = ? where month = ? and region = ? and name = ?");
						upd.setTimestamp(1, new Timestamp(time));
						upd.setString(2, month);
						upd.setString(3, isEmpty(region) ? "" : region);
						upd.setString(4, type.getSqlName());
						upd.executeUpdate();
						upd.close();
					}
				}
				return retReport;
			}
		}

		if (type == OsmAndLiveReportType.COUNTRIES) {
			report = getCountries();
		} else if (type == OsmAndLiveReportType.SUPPORTERS) {
			report = getSupporters();
		} else if (type == OsmAndLiveReportType.TOTAL_CHANGES) {
			report = getTotalChanges(region);
		} else if (type == OsmAndLiveReportType.RANKING) {
			report = getRanking(region);
		} else if (type == OsmAndLiveReportType.USERS_RANKING) {
			report = getUsersRanking(region);
		} else if (type == OsmAndLiveReportType.RECIPIENTS) {
			if (!Algorithms.isEmpty(region)) {
				throw new UnsupportedOperationException("Report is deprecated");
			}
			report = getRecipients();
		} else if (type == OsmAndLiveReportType.PAYOUTS) {
			report = getPayouts();
		} else if (type == OsmAndLiveReportType.TOTAL) {
			report = getTotal();
		} else {
			throw new UnsupportedOperationException();
		}
		String jsonReport = gson.toJson(report);
		if (thisMonth || forceSave) {
			try {
				saveReport(jsonReport, type, region, accesstime);
			} catch (ParseException e) {
				throw new IOException(e);
			}
		}
		return jsonReport;
	}
	
	

	public Gson getJsonFormatter() {
		return new Gson();
	}
	
	
	public Number getNumberReport(OsmAndLiveReportType type) throws IOException, SQLException {
		if (!thisMonth || type == OsmAndLiveReportType.BTC_DONATION_VALUE) {
			double ret = loadNumberReport(type);
			if(!Double.isNaN(ret)) {
				return ret;
			}
		}
		
		if(type == OsmAndLiveReportType.MIN_CHANGES) {
			return getMinChanges();
		} else if(type == OsmAndLiveReportType.REGION_RANKING_RANGE) {
			// deprecated 
			return 7;
		} else if(type == OsmAndLiveReportType.REGION_MIN_CHANGES) {
			// deprecated 
			return 1;
		} else if(type == OsmAndLiveReportType.RANKING_RANGE) {
			return getRankingRange();
		} else if(type == OsmAndLiveReportType.EUR_BTC_RATE) {
			return getEurBTCRate();
		} else if(type == OsmAndLiveReportType.EUR_BTC_ACTUAL_RATE) {
			return getEurBTCRate();
		} else if (type == OsmAndLiveReportType.BTC_VALUE) {
			return getBtcValue();
		} else if (type == OsmAndLiveReportType.BTC_DONATION_VALUE) {
			// enabled only by report
			return 0;
		} else if (type == OsmAndLiveReportType.EUR_VALUE) {
			// deprecated
			throw new UnsupportedOperationException();
		} else {
			throw new UnsupportedOperationException();
		}
	}

	private double loadNumberReport(OsmAndLiveReportType type) throws SQLException {
		PreparedStatement ps = conn
				.prepareStatement("select report from final_reports where month = ? and name = ? ");
		ps.setString(1, month);
		ps.setString(2, type.getSqlName());
		ResultSet q = ps.executeQuery();
		if (q.next()) {
			return Double.parseDouble(q.getString(1));
		}
		return Double.NaN;
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
		public int allchanges;
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
		public int atomTotalChanges;
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
		public int atomchanges;
		public int atomglobalchanges;
	}
	
	
	protected static class Recipient {

		public double btc;
		public int weight;
		public int rank;
		public String btcaddress;
		public String osmid;
		public int changes;
		public int objchanges;
		
	}
	
	public static class RecipientsReport {
		public String region;
		public String month;
		public long date;
		
		public int regionTotalWeight;
		public int regionCount;
		public float regionPercentage;
		
		public double regionBtc;
		public double btcPayoutComission;
		public double btc;
		public float rate;
		public float eur;
		public boolean notReadyToPay;
		
		public List<Recipient> rows = new ArrayList<Recipient>();
		// UI Fields
		public String payouts;
		public String regionCollectedMessage;
		public String worldCollectedMessage;
		public String reports;
	}
	
	protected static class PayoutsReport {
		public long date;
		public double payoutBTCAvailable;
		public double payoutBTCCollected;
		public double payoutBTCComission;
		public double payoutTotal;
		public String month;
		public List<Payout> payments = new ArrayList<Payout>();
		
	}
	
	protected class Payout {
		public String osmid;
		public String btcaddress;
		public double btc;
	}
	
	
	

}
