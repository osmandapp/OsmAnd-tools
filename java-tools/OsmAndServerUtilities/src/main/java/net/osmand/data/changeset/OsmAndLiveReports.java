package net.osmand.data.changeset;

import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.X509TrustManager;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kxml2.io.KXmlParser;
import org.xmlpull.v1.XmlPullParser;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.stream.JsonReader;

public class OsmAndLiveReports {

	private static final int BATCH_SIZE = 1000;
	private static final Log LOG = PlatformUtil.getLog(OsmAndLiveReports.class);
	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/changeset",
				isEmpty(System.getenv("DB_USER")) ? "test" : System.getenv("DB_USER"),
				isEmpty(System.getenv("DB_PWD")) ? "test" : System.getenv("DB_PWD"));
		OsmAndLiveReports reports = new OsmAndLiveReports();
		reports.conn = conn;
		reports.month = "2018-07";
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
	
	private Number getEurBTCRate() throws IOException {
		URLConnection un = new URL("https://blockchain.info/ticker").openConnection();
		StringBuilder bld = Algorithms.readFromInputStream(un.getInputStream());
		JSONObject obj = new JSONObject(bld.toString());
		return obj.getJSONObject("EUR").getDouble("sell");
	}

	public int getRegionRankingRange() {
		return 7;
	}

	public int getMinChanges() {
		return 3;
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
			c.downloadname = rs.getString("downloadname");
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

			supportersReport.count++;
		}
		for (String s : supportersReport.regions.keySet()) {
			SupportersRegion r = supportersReport.regions.get(s);
			if (supportersReport.activeCount > 0) {
				r.percent = r.count / (2 * supportersReport.activeCount);
			} else {
				r.percent = 0;
			}
		}
		return supportersReport;
	}

	

	
	private String getJsonReport(OsmAndLiveReportType type) throws SQLException {
		Gson gson = new Gson();
		if(type == OsmAndLiveReportType.COUNTRIES) {
			return gson.toJson(getCountries());
		} else if(type == OsmAndLiveReportType.SUPPORTERS) {
			return gson.toJson(getSupporters());
		} else if(type == OsmAndLiveReportType.TOTAL_CHANGES) {
			throw new UnsupportedOperationException();
		} else if(type == OsmAndLiveReportType.USERS_RANKING) {
			throw new UnsupportedOperationException();
		} else if(type == OsmAndLiveReportType.USER_RANKING) {
			throw new UnsupportedOperationException();
		} else if(type == OsmAndLiveReportType.RANKING) {
			throw new UnsupportedOperationException();
		} else if(type == OsmAndLiveReportType.RECIPIENTS) {
			throw new UnsupportedOperationException();
		} else {
			throw new UnsupportedOperationException();
		}
	}
	
	private Number getNumberReport(Connection conn, OsmAndLiveReportType type) throws IOException {
		if(type == OsmAndLiveReportType.MIN_CHANGES) {
			return getMinChanges();
		} else if(type == OsmAndLiveReportType.REGION_RANKING_RANGE) {
			return getRegionRankingRange();
		} else if(type == OsmAndLiveReportType.RANKING_RANGE) {
			return getRankingRange();
		} else if(type == OsmAndLiveReportType.EUR_BTC_RATE) {
			return getEurBTCRate();
		} else if(type == OsmAndLiveReportType.BTC_VALUE) {
			Number eur = getNumberReport(conn, OsmAndLiveReportType.EUR_VALUE);
			Number rt = getNumberReport(conn, OsmAndLiveReportType.EUR_BTC_RATE);
			if (rt.doubleValue() != 0) {
				return eur.doubleValue() / rt.doubleValue();
			}
		} else if(type == OsmAndLiveReportType.EUR_VALUE) {
			return getRankingRange();
		} else {
			throw new UnsupportedOperationException();
		}
		return 0;
	}


	private static boolean isEmpty(String vl) {
		return vl == null || vl.equals("");
	}
	
	
	private static class CountriesReport {
		public String month;
		public List<Country> rows = new ArrayList<Country>();
		public Map<String, String> map = new TreeMap<String, String>();
	}
	
	private static class Country {
		public String map;
		public String id;
		public String parentid;
		public String downloadname;
		public String name;
	}
	
	private static class SupportersReport {
		public String month;
		public Map<String, SupportersRegion> regions = new HashMap<String, SupportersRegion>();
		public List<Supporter> rows = new ArrayList<OsmAndLiveReports.Supporter>();
		public int count;
		public int activeCount;
	}
	
	private static class SupportersRegion {
		public int count;
		public String id;
		public String name;
		public float percent;
	}
	
	private static class Supporter {
		public int count;
		public String user;
		public String status;
		public String region;
		public String regionName;
		public String sku;
		
		public float percent;
	}
}
