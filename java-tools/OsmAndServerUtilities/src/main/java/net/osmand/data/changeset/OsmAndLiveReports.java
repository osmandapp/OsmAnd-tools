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

	private double getBtcValue() throws IOException {
		double rt = getEurBTCRate();
		double eur = getEurValue();
		if (rt != 0) {
			return eur / rt;
		}
		return 0;
	}


	private double getEurValue() {
		throw new UnsupportedOperationException();
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

	public TotalChangesReport getTotalChanges(String region) throws SQLException {
		TotalChangesReport report = new TotalChangesReport();
		report.month = month;
		report.region = region;
		ResultSet rs;
		if(!isEmpty(region)) {
			String r = "select count(distinct ch.username) users, count(distinct ch.id) changes" + 
					  " from changesets_view ch, changeset_country_view cc where ch.id = cc.changesetid"+ 
					  " and cc.countryid = (select id from countries where downloadname= '?')"+
					  " and substr(ch.closed_at_day, 0, 8) = '?'";
			PreparedStatement ps = conn.prepareStatement(r);
			ps.setString(1, region);
			ps.setString(2, month);
			rs = ps.executeQuery();
		} else {
			String r = "select count ( distinct username) users, count(*) changes from changesets_view"+
						" where substr(closed_at_day, 0, 8) = '?'";
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
		    String r =  " SELECT data.cnt changes, count(*) group_size from ("+
		    			"  		SELECT username, count(*) cnt from changesets_view ch, changeset_country_view cc " + 
		    			"		WHERE substr(ch.closed_at_day, 0, 8) = '?' and ch.id = cc.changesetid  "+
		    			"  			and cc.countryid = (select id from countries where downloadname= '?' )" +
		    			" 		GROUP BY ch.username having count(*) >= ? order by count(*) desc )" +
		    			" data group by data.cnt order by changes desc";
			PreparedStatement ps = conn.prepareStatement(r);
			ps.setString(1, month);
			ps.setString(2, region);
			ps.setInt(3, minChanges);
			rs = ps.executeQuery();
		} else {
			rankingRange = getRankingRange();
			String r = "SELECT data.cnt changes, count(*) group_size from ( "+
					   "	SELECT username, count(*) cnt from changesets_view ch where substr(ch.closed_at_day, 0, 8) = ? " +
					   " 	GROUP BY by ch.username having count(*) >= ? order by count(*) desc) " +
					   " data group by data.cnt order by changes desc";
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

//		function calculateUserRanking($useReport = true, $saveReport = 0 ) {
//		  global $iregion, $imonth, $iuser, $month, $dbconn;
//		  $finalReport = getReport('calculateUserRanking', $iregion);
//		  if($finalReport != NULL && $useReport ) {
//		    return $finalReport;
//		  }
//		  $gar = calculateRanking('', $useReport, -1)->rows;
//		  $ar = calculateRanking(NULL, $useReport, -1)->rows;
//		  $region =  pg_escape_string($iregion);
//		  $user =  pg_escape_string($iuser);
//		  
//		    
//		  $result = pg_query($dbconn, "
//		    SELECT  t.username, t.size changes , s.size gchanges FROM
//		     ( SELECT username, count(*) size 
//		        from changesets_view ch, changeset_country_view cc where ch.id = cc.changesetid 
//		        and substr(ch.closed_at_day, 0, 8) = '{$month}'
//		        and cc.countryid = (select id from countries where downloadname= '${region}')
//		        and username= '${user}'
//		        group by ch.username) t join 
//		     (SELECT username, count(*) size from changesets_view ch where 
//		      substr(ch.closed_at_day, 0, 8) = '{$month}'
//		      and username= '${user}'
//		      group by ch.username
//		      ) s on s.username = t.username order by t.size desc;
//		        ");
//		  if (!$result) {
//		    $res = new stdClass();
//		    $res->error ='No result';
//		    return $res;
//		  }
//		  
//		  
//		  $res = new stdClass();
//		  $res->month = $month;
//		  $res->rows = array();
//		  while ($row = pg_fetch_row($result)) {
//		    $rw = new stdClass();
//		    array_push($res->rows, $rw);
//		    $rw->user = $row[0];
//		    $rw->changes = $row[1];
//		    $rw->globalchanges = $row[2];
//		    $rw->rank = '';
//		    for($i = 0; $i < count($ar); $i++) {
//		      if($ar[$i]->minChanges <= $row[1]  && $ar[$i]->maxChanges >= $row[1] ){
//		        $rw->rank = $ar[$i]->rank;
//		        // $rw->min = $ar[$i]->minChanges ;
//		        // $rw->max = $ar[$i]->maxChanges ;
//		        break;
//		      }
//		    }
//		    $rw->grank = '';
//		    for($i = 0; $i < count($gar); $i++) {
//		      if($gar[$i]->minChanges <= $row[2]  && $gar[$i]->maxChanges >= $row[2] ){
//		        $rw->grank = $gar[$i]->rank;
//		        // $rw->gmin = $gar[$i]->minChanges ;
//		        // $rw->gmax = $gar[$i]->maxChanges ;
//
//		        break;
//		      }
//		    }
//		  }
//		  if($saveReport >= 0) {
//		    saveReport('calculateUserRanking', $res, $imonth, $iregion, $timeReport);
//		  }
//		  return $res;
//
//		}
//
//
//		function calculateUsersRanking($useReport = true, $saveReport = 0) {
//		  global $iregion, $imonth, $month, $dbconn;
//		  $finalReport = getReport('calculateUsersRanking', $iregion);
//		  if($finalReport != NULL && $useReport) {
//		    return $finalReport;
//		  }
//		  $gar = calculateRanking('', true, -1)->rows;
//		  $ar = calculateRanking(NULL, true, -1)->rows;
//		  $region =  pg_escape_string($iregion);
//		  $min_changes = getMinChanges();
//		  
//		    
//		  $result = pg_query($dbconn, "
//		    SELECT  t.username, t.size changes , s.size gchanges FROM
//		     ( SELECT username, count(*) size 
//		        from changesets_view ch, changeset_country_view cc where ch.id = cc.changesetid 
//		        and substr(ch.closed_at_day, 0, 8) = '{$month}'
//		        and cc.countryid = (select id from countries where downloadname= '${region}')
//		        group by ch.username
//		        having count(*) >= {$min_changes}
//		        order by count(*) desc ) t join 
//		     (SELECT username, count(*) size from changesets_view ch where 
//		      substr(ch.closed_at_day, 0, 8) = '{$month}'
//		      group by ch.username
//		      ) s on s.username = t.username order by t.size desc;
//		        ");
//		  if (!$result) {
//		    $res = new stdClass();
//		    $res->error ='No result';
//		    return $res;
//		  }
//		  
//		  
//		  $res = new stdClass();
//		  $res->month = $month;
//		  $res->rows = array();
//		  while ($row = pg_fetch_row($result)) {
//		    $rw = new stdClass();
//		    array_push($res->rows, $rw);
//		    $rw->user = $row[0];
//		    $rw->changes = $row[1];
//		    $rw->globalchanges = $row[2];
//		    $rw->rank = '';
//		    for($i = 0; $i < count($ar); $i++) {
//		      if($ar[$i]->minChanges <= $row[1]  && $ar[$i]->maxChanges >= $row[1] ){
//		        $rw->rank = $ar[$i]->rank;
//		        // $rw->min = $ar[$i]->minChanges ;
//		        // $rw->max = $ar[$i]->maxChanges ;
//		        break;
//		      }
//		    }
//		    $rw->grank = '';
//		    for($i = 0; $i < count($gar); $i++) {
//		      if($gar[$i]->minChanges <= $row[2]  && $gar[$i]->maxChanges >= $row[2] ){
//		        $rw->grank = $gar[$i]->rank;
//		        // $rw->gmin = $gar[$i]->minChanges ;
//		        // $rw->gmax = $gar[$i]->maxChanges ;
//
//		        break;
//		      }
//		    }
//		    
//		  }
//		  if($saveReport >= 0) {
//		    saveReport('calculateUsersRanking', $res, $imonth, $iregion, $saveReport);
//		  }
//		  return $res;
//
//		}


	
	public String getJsonReport(OsmAndLiveReportType type, String region) throws SQLException {
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
			throw new UnsupportedOperationException();
		} else if(type == OsmAndLiveReportType.USER_RANKING) {
			throw new UnsupportedOperationException();
		} else if(type == OsmAndLiveReportType.RECIPIENTS) {
			throw new UnsupportedOperationException();
		} else {
			throw new UnsupportedOperationException();
		}
	}
	
	public Number getNumberReport(OsmAndLiveReportType type) throws IOException {
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
		
		public float percent;
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
	

}
