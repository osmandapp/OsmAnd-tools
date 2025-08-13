package net.osmand.server.monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.kxml2.io.KXmlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.PlatformUtil;
import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.TelegramBotManager;
import net.osmand.server.monitor.OsmAndServerMonitoringBot.Sender;
import net.osmand.util.Algorithms;

@Component
public class OsmAndServerMonitorTasks {

	private static final Log LOG = LogFactory.getLog(TelegramBotManager.class);

	private static final int SECOND = 1000;
	private static final int MINUTE = 60 * SECOND;
	private static final int HOUR = 60 * MINUTE;
	private static final int LIVE_STATUS_MINUTES = 2;
	private static final int DOWNLOAD_MAPS_MINITES = 5;
	private static final int DOWNLOAD_TILE_MINUTES = 10;
	private static final int TILE_ZOOM = 19;
	private static final int NEXT_TILE = 16;
	private static final int TILEX_NUMBER = 268660;
	private static final int TILEY_NUMBER = 175100;
	private static final long INITIAL_TIMESTAMP_S = 1530840000;

	private static final int MAPS_COUNT_THRESHOLD = 700;

	private static final String[] HOSTS_TO_TEST = new String[] { "download.osmand.net",
			 "dl2.osmand.net", "dl3.osmand.net",  "dl4.osmand.net",  "dl8.osmand.net",  "dl9.osmand.net", 
			 "maptile.osmand.net", "data.osmand.net"};
	private static final String[] JAVA_HOSTS_TO_TEST = new String[] { "test.osmand.net", "download.osmand.net",
			"maptile.osmand.net" };
	private static final String[] JAVA_HOSTS_TO_RESTART = new String[] {
			"https://creator.osmand.net:8080/view/WebSite/job/WebSite_OsmAndServer/", // TODO add builder
			"https://osmand.net:8095/job/WebSite_OsmAndServer/",
			"https://maptile.osmand.net:8080/job/UpdateOsmAndServer/" };

	private static final String TILE_SERVER = "https://tile.osmand.net/hd/";
	
	// Build Server
	List<BuildServerCheckInfo> buildServers = new ArrayList<>();
	{
//		buildServers.add(new BuildServerCheckInfo("https://builder.osmand.net:8080", "builder")); // TODO no-anonymous
		buildServers.add(new BuildServerCheckInfo("https://creator.osmand.net:8080", "creator"));
		buildServers.add(new BuildServerCheckInfo("https://dl2.osmand.net:8080", "jenkins-dl2"));
		buildServers.add(new BuildServerCheckInfo("https://osmand.net:8095", "jenkins-main"));
		buildServers.add(new BuildServerCheckInfo("https://maptile.osmand.net:8080", "jenkins-maptile"));
		buildServers.add(new BuildServerCheckInfo("https://data.osmand.net:8080", "jenkins-data"));
		buildServers.add(new BuildServerCheckInfo("https://veles.osmand.net:8080", "jenkins-veles"));
	}
	

	private static final double PERC = 95;
	private static final double PERC_SMALL = 100 - PERC;


	private static final String RED_MAIN_SERVER = "main";
	private static final String RED_MAPTILE_SERVER = "maptile";
	private static final String RED_STAT_PREFIX = "stat.";
	private static final String RED_KEY_OSMAND_LIVE = "live_delay_time";
	private static final String RED_KEY_TILE = "tile_time";
	private static final String RED_KEY_DOWNLOAD = "download_map";

	private static final SimpleDateFormat TIME_FORMAT_UTC = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	static {
		TIME_FORMAT_UTC.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	private static final SimpleDateFormat TIMESTAMP_FORMAT_OPR = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
	static {
		TIMESTAMP_FORMAT_OPR.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

//  private final static SimpleDateFormat XML_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
	// Formatter for the pattern "2025-08-09 12:21:49 -0700"
	private static final SimpleDateFormat FORMAT_RFC822 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
	// Formatter for the pattern "2025-08-09 18:20:24 UTC"
	private static final SimpleDateFormat FORMAT_ZONE_ID = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");

	private static Date parseDate(String s) throws ParseException {
		try {
			return FORMAT_RFC822.parse(s);
		} catch (ParseException e) {
			return FORMAT_ZONE_ID.parse(s);
		}
	}

	@Value("${monitoring.enabled}")
	private boolean enabled;
	
	@Value("${monitoring.changes.feed}")
	private String urlChangesFeed;
	
	@Value("${monitoring.changes.publish.channel}")
	private String publishChannel;
	
	private long lastFeedCheckTimestamp; 
	
	private List<FeedEntry> feed = new ArrayList<>();

	@Autowired
	@Qualifier("monitorJdbcTemplate")
	JdbcTemplate jdbcTemplate;
	
	@Autowired
	DatasourceConfiguration config;

	// OsmAnd Live validation
	LiveCheckInfo live = new LiveCheckInfo();
	
	// Java servers check
	JavaServerCheckInfo javaChecks = new JavaServerCheckInfo();
	// Tile validation
	double lastResponseTime;
	// Index map validation
	boolean mapIndexPrevValidation = true;
	Map<String, DownloadTestResult> downloadTests = new TreeMap<>();

	OsmAndServerMonitoringBot.Sender telegram;

	static class FeedEntry {
		Date published;
		Date updated;
		String id;
		String content;
		String title;
		String author;
		String link;
	}


	@Scheduled(fixedRate = LIVE_STATUS_MINUTES * MINUTE)
	public void checkOsmAndLiveStatus() {
		checkOsmAndLiveStatus(true);
	}

	public void checkOsmAndLiveStatus(boolean updateStats) {
		if(!enabled) {
			return;
		}
		try {
			URL url = new URL("https://osmand.net/api/osmlive_status");
			InputStream is = url.openConnection().getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String osmlivetime = br.readLine();
			Date dt = TIME_FORMAT_UTC.parse(osmlivetime);
			br.close();
			is.close();
			long currentDelay = System.currentTimeMillis() - dt.getTime();
			if (currentDelay - live.previousOsmAndLiveDelay > 30 * MINUTE && currentDelay > HOUR ) {
				sendBroadcastMessage(getLiveDelayedMessage(currentDelay));
				live.previousOsmAndLiveDelay = currentDelay;
			}
			live.lastCheckTimestamp = System.currentTimeMillis();
			live.lastOsmAndLiveDelay = currentDelay;
			if (updateStats) {
				addStat(RED_KEY_OSMAND_LIVE, RED_MAIN_SERVER, currentDelay);
			}
		} catch (Exception e) {
			sendBroadcastMessage("Exception while checking the server live status.");
			LOG.error(e.getMessage(), e);
		}
	}
	
	private void sendBroadcastMessage(String string) {
		if (telegram != null) {
			telegram.sendBroadcastMessage(string);
			telegram.sendChannelMessage(publishChannel, EmojiConstants.ROBOT_EMOJI + string);
		}
	}

	@Scheduled(fixedRate = MINUTE)
	public void checkOsmAndJavaServers() {
		if (!enabled) {
			return;
		}
		try {
			Map<String, String> failed = new LinkedHashMap<String, String>();
			for (int i = 0; i < JAVA_HOSTS_TO_TEST.length; i++) {
				boolean error = false;
				try {
					URL url = new URL(urlToTestJavaServer(JAVA_HOSTS_TO_TEST[i]));
					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
					conn.connect();
					if (conn.getResponseCode() >= 300) {
						// error
						error = true;
					}
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
					error = true;
				}
				if (error) {
					failed.put(JAVA_HOSTS_TO_TEST[i], JAVA_HOSTS_TO_RESTART[i]);
				}
			}
			boolean stateChanged = failed.size() != javaChecks.failed.size();
			javaChecks.failed = failed;
			javaChecks.lastCheckTimestamp = System.currentTimeMillis();
			if (stateChanged) {
				if (failed.size() == 0) {
					sendBroadcastMessage("All java servers are up.");
				} else {
					StringBuilder sb = new StringBuilder();
					Iterator<Entry<String, String>> it = failed.entrySet().iterator();
					while (it.hasNext()) {
						Entry<String, String> e = it.next();
						sb.append(String.format(
								"Java server <a href='%s'>%s</a> is down - <a href='%s'>restart it</a>.\n",
								urlToTestJavaServer(e.getKey()), e.getKey(), e.getValue()));
					}
					sendBroadcastMessage(sb.toString());
				}
			}
		} catch (Exception e) {
			sendBroadcastMessage("Exception while checking the java server status.");
			LOG.error(e.getMessage(), e);
		}
	}

	private String urlToTestJavaServer(String u) {
		return "https://" + u + "/api/giveaway-series";
	}

	@Scheduled(fixedRate = MINUTE)
	public void checkOsmAndBuildServer() {
		if (!enabled) {
			return;
		}
		for (BuildServerCheckInfo buildServer : buildServers) {
			try {
				Set<String> jobsFailed = new TreeSet<String>();
				URL url = new URL(buildServer.serverUrl + "/api/json");
				InputStream is = url.openConnection().getInputStream();
				JSONObject object = new JSONObject(new JSONTokener(is));
				JSONArray jsonArray = object.getJSONArray("jobs");
				for (int i = 0; i < jsonArray.length(); i++) {
					JSONObject jb = jsonArray.getJSONObject(i);
					String name = jb.getString("name");
					String color = jb.getString("color");
					if (!color.equals("blue") && !color.equals("disabled") && !color.equals("notbuilt")
							&& !color.equals("blue_anime")) {
						jobsFailed.add(name);
					}
				}
				is.close();
				if (buildServer.jobsFailed == null) {
					buildServer.jobsFailed = jobsFailed;
				} else if (!buildServer.jobsFailed.equals(jobsFailed)) {
					Set<String> jobsFailedCopy = new TreeSet<String>(jobsFailed);
					jobsFailedCopy.removeAll(buildServer.jobsFailed);
					if (!jobsFailedCopy.isEmpty()) {
						sendBroadcastMessage(
								"There are new failures on Build Server: " + formatJobNamesAsHref(buildServer, jobsFailedCopy));
					}
					Set<String> jobsRecoveredCopy = new TreeSet<String>(buildServer.jobsFailed);
					jobsRecoveredCopy.removeAll(jobsFailed);
					if (!jobsRecoveredCopy.isEmpty()) {
						sendBroadcastMessage(
								"There are recovered jobs on Build Server: " + formatJobNamesAsHref(buildServer, jobsRecoveredCopy));
					}
					buildServer.jobsFailed = jobsFailed;
				}
				buildServer.lastCheckTimestamp = System.currentTimeMillis();
			} catch (Exception e) {
				sendBroadcastMessage("Exception while checking the build server status: " + buildServer.serverUrl);
				LOG.error(buildServer.serverUrl + "\n" + e.getMessage(), e);
			}
		}
	}

	@Scheduled(fixedRate = DOWNLOAD_MAPS_MINITES * MINUTE)
	public void checkIndexesValidity() {
		if(!enabled) {
			return;
		}
		System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
		GZIPInputStream gis = null;
		try {
			URL url = new URL("https://osmand.net/get_indexes?gzip=true");
			URLConnection conn = url.openConnection();
			InputStream is = conn.getInputStream();
			gis = new GZIPInputStream(is);
			validateAndReport(gis);
			is.close();
		} catch (IOException ioex) {
			sendBroadcastMessage("Exception while checking the map index validity.");
			LOG.error(ioex.getMessage(), ioex);
		} finally {
			if (gis != null) {
				close(gis);
			}
		}
		for (String host : HOSTS_TO_TEST) {
			URL url = null;
			DownloadTestResult res = downloadTests.get(host);
			if(res == null) {
				res = new DownloadTestResult(host);
				downloadTests.put(host, res);
			}
			try {
				url = new URL("https://" + host + "/download?standard=yes&file=Angola_africa_2.obf.zip");
				// on download servers there is a glitch that randomly it starts downloading very slow, so let's take 3 measurements
				double spd1 = downloadSpeed(url);
				double spd2 = downloadSpeed(url);
				res.addSpeedMeasurement(Math.max(spd1, spd2));
				if (!res.lastSuccess) {
					sendBroadcastMessage(host + " OK. Maps download works fine");
				}
				res.lastSuccess = true;

			} catch (IOException ex) {
				if(res.lastSuccess ) {
					sendBroadcastMessage(host + " FAILURE: problems downloading maps " + ex.getMessage());
					LOG.error(ex.getMessage(), ex);
				}
				res.lastSuccess = false;
			}
		}
	}

	private double downloadSpeed(URL url) throws IOException {
		long startedAt = System.currentTimeMillis();
		long contentLength = 0;
		URLConnection conn = url.openConnection();
		try (InputStream is = conn.getInputStream()) {
			int read = 0;
			byte[] buf = new byte[1024 * 1024];
			while ((read = is.read(buf)) != -1) {
				contentLength += read;
				if(System.currentTimeMillis() - startedAt > 7000) {
					break;
				}
			}
		}
		long finishedAt = System.currentTimeMillis();
		double downloadTimeInSec = (finishedAt - startedAt) / 1000d;
		double downloadSpeedMBPerSec = (contentLength / downloadTimeInSec) / (1024 * 1024);
		return downloadSpeedMBPerSec;
	}

	private int countMapsInMapIndex(InputStream is) throws IOException, XmlPullParserException {
		int mapCounter = 0;
		XmlPullParser xpp = new KXmlParser();
		xpp.setInput(new InputStreamReader(is));
		int eventType = xpp.getEventType();
		while (eventType != XmlPullParser.END_DOCUMENT) {
			if (eventType == XmlPullParser.START_TAG) {
				if (xpp.getAttributeValue(0).equals("map")) {
					mapCounter++;
				}
			}
			if (eventType == XmlPullParser.TEXT) {
				if (!xpp.isWhitespace()) {
					throw new XmlPullParserException("Text in document");
				}
			}
			eventType = xpp.next();
		}
		return mapCounter;
	}

	private void validateAndReport(InputStream is) throws IOException {
		boolean mapIndexCurrValidation = true;
		int mapsInMapIndex = 0;
		try {
			mapsInMapIndex = countMapsInMapIndex(is);
			if (!mapIndexPrevValidation ) {
				sendBroadcastMessage(String.format("download.osmand.net: Map index is correct and serves %d maps.", mapsInMapIndex));
			}

			if (mapsInMapIndex < MAPS_COUNT_THRESHOLD) {
				mapIndexCurrValidation = false;
				if (mapIndexPrevValidation) {
					sendBroadcastMessage(String.format("download.osmand.net: Maps index is not correct and serves only of %d maps.", mapsInMapIndex));
				}
			}
		} catch (XmlPullParserException xmlex) {
			mapIndexCurrValidation = false;
			if (mapIndexPrevValidation) {
				sendBroadcastMessage("download.osmand.net: problems with parsing map index.");
				LOG.error(xmlex.getMessage(), xmlex);
			}

		}
		this.mapIndexPrevValidation = mapIndexCurrValidation;
	}

	private void close(InputStream is) {
		try {
			is.close();
		} catch (IOException ex) {
			LOG.error(ex.getMessage(), ex);
		}
	}



	@Scheduled(fixedRate = DOWNLOAD_TILE_MINUTES * MINUTE)
	public void tileDownloadTest() {
		if(!enabled) {
			return;
		}
		double respTimeSum = 0;
		int count = 4;
		int retryCount = 3;
		long now = System.currentTimeMillis();
		long period = (((now / 1000) - INITIAL_TIMESTAMP_S) / 60 ) / DOWNLOAD_TILE_MINUTES;
		long yShift = period % 14400;
		long xShift = period / 14400;
		List<Float> times = new ArrayList<Float>();
		int failed = 0;
		int retry = retryCount;
		boolean succeed = false;
		while (!succeed) {
			failed = 0;
			for (int i = 0; i < count; i++) {
				String tileUrl = new StringBuilder().append(TILE_SERVER).append(TILE_ZOOM).append("/")
						.append(TILEX_NUMBER + (i + count * xShift) * NEXT_TILE).append("/")
						.append(TILEY_NUMBER + yShift * NEXT_TILE).append(".png").toString();
				double tileDownload = estimateResponse(tileUrl);
				LOG.info("Downloaded " + tileUrl + " " + tileDownload + " seconds.");
				times.add((float) tileDownload);
				if (tileDownload > 0) {
					respTimeSum += tileDownload;
				} else {
					failed++;
				}
			}
			if (failed >= count / 2) {
				if(retry-- < 0) {
					String txStatus = getTirexStatus();
					sendBroadcastMessage(String.format("tile.osmand.net: problems with downloading tiles: %s (%d) - %s", times, retryCount, txStatus));
					return;
				} else {
					try {
						Thread.sleep(20000);
					} catch (Exception e) {
						throw new IllegalStateException(e);
					}
				}
			} else {
				succeed = true;
				break;
			}
		}
		lastResponseTime = respTimeSum / count;
		if (lastResponseTime > 0) {
			addStat(RED_KEY_TILE, RED_MAPTILE_SERVER, lastResponseTime);
		}
	}

	protected String getRoutingStatus() {
		try {
			String res = Algorithms.readFromInputStream(new URL("https://maptile.osmand.net/web-route-stats.txt").openStream()).toString();
			res = prepareAccessStats(res);
			return res;
		} catch (Exception e) {
			LOG.warn(e.getMessage(), e);
			return "Error: " + e.getMessage();
		}
	}
	
	protected  String getTirexStatus() {
		try {
			String res = Algorithms.readFromInputStream(new URL("https://maptile.osmand.net/access_stats.txt").openStream()).toString();
			res = prepareAccessStats(res);

			StringBuilder rs = Algorithms.readFromInputStream(new URL("https://maptile.osmand.net/renderd.stats").openStream());
			res += prepareRenderdResult(rs.toString());
			StringBuilder date = Algorithms.readFromInputStream(new URL("https://maptile.osmand.net/osmupdate/state.txt").openStream());
			long t = TIMESTAMP_FORMAT_OPR.parse(date.toString()).getTime();
			res += String.format("\n<a href='https://maptile.osmand.net:8080/'>Tile Postgis DB</a>: %s", timeAgo(t));
			return res;
		} catch (Exception e) {
			LOG.warn(e.getMessage(), e);
			return "Error: " + e.getMessage();
		}
	}

	private static String prepareAccessStats(String lns) {
		String[] spl = lns.split("\n");
		if (spl.length >= 2) {
			String result = "\n";
			String[] percents = spl[spl.length - 2].split("\\s+");
			String[] timings = spl[spl.length - 1].split("\\s+");
			for (int i = 1; i < timings.length && i < percents.length; i++) {
				double d = Double.parseDouble(timings[i].trim());
				result += String.format("%.2fs (%s) ", d, percents[i]);
			}
			return result;
		}
		return "";
	}

	private String prepareRenderdResult(String res) {
		String result = "\n<b>Tile queue</b>: ";
		String[] lns = res.split("\n");
		for (String ln : lns) {
			if (ln.startsWith("ReqQueueLength:")) {
				result = addToResult("Queue", result, "ReqQueueLength:", ln);
			} else if (ln.startsWith("ReqPrioQueueLength:")) {
				result = addToResult("Priority", result, "ReqPrioQueueLength:", ln);
			} else if (ln.startsWith("ReqLowQueueLength:")) {
				result = addToResult("Low", result, "ReqLowQueueLength:", ln);
			} else if (ln.startsWith("ReqBulkQueueLength:")) {
				result = addToResult("Bulk", result, "ReqBulkQueueLength:", ln);
			} else if (ln.startsWith("DirtQueueLength:")) {
				result = addToResult("Ddirty", result, "DirtQueueLength:", ln);
			}
		}
		return result;
	}

	private String addToResult(String pr, String result, String suf, String ln) {
		String vl = ln.substring(suf.length()).trim();
		if (!vl.equals("0")) {
			result += pr + "-" + vl + " ";
		}
		return result;
	}

	protected String getOwnTirexStatus() {
		String txStatus = "tirex-status -r";
		String res = runCmd(txStatus, new File("."), null);
		return prepareTirexResult(res);
	}

	private String prepareTirexResult(String res) {
		if(res != null) {
			JSONObject jsonObject = null;
			try {
				jsonObject = new JSONObject(res);
				JSONObject tls = jsonObject.getJSONObject("rm");
				String msg = String.format("(Now: %d tiles, load %s. ",
						tls.getInt("num_rendering"), tls.getDouble("load") +"");
				JSONArray queue = jsonObject.getJSONObject("queue").getJSONArray("prioqueues");
				for (int i = 0; i < queue.length(); i++) {
					JSONObject o = queue.getJSONObject(i);
					if (o.getInt("size") > 0) {
						msg += String.format(", queue-%d - %d", o.getInt("prio"), o.getInt("size"));
					}
				}
				msg +=")";
				return msg;
			} catch (JSONException e) {
				LOG.warn("Error reading json from tirex " + (jsonObject != null ? jsonObject.toString() : res) + " " + e.getMessage());
			}
		}
		return "Rendering service (tirex) is down!";
	}

	private void addStat(String key, String server, double score) {
		if (!config.monitorInitialized()) {
			return;
		}
		jdbcTemplate.execute("INSERT INTO servers.metrics(timestamp, server, name, value) VALUES(?,?,?,?)",
				new PreparedStatementCallback<Boolean>() {

					@Override
					public Boolean doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
						ps.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
						ps.setString(2, convertServer(server));
						ps.setString(3, RED_STAT_PREFIX + key);
						ps.setDouble(4, score);
						return ps.execute();
					}
				});
	}

	private double estimateResponse(String tileUrl) {
		double respTime = -1d;
		HttpURLConnection conn = null;
		InputStream is = null;
		long startedAt = System.currentTimeMillis();
		try {
			URL url = new URL(tileUrl);
			conn = (HttpURLConnection) url.openConnection();
			conn.setConnectTimeout(15 * MINUTE);
			is = conn.getInputStream();
			while(is.read() != -1) {
				// read input stream
			}
			long finishedAt = System.currentTimeMillis();
			respTime = (finishedAt - startedAt) / 1000d;
		} catch (IOException ex) {
			LOG.error(ex.getMessage(), ex);
		} finally {
			if (is != null) {
				close(is);
			}
			if (conn != null) {
				conn.disconnect();
			}
			if(respTime < 0) {
				long finishedAt = System.currentTimeMillis();
				respTime = -(finishedAt - startedAt) / 1000d;
			}
		}
		return respTime;
	}

	private String getTileServerMessage() {
		DescriptiveStatistics tile24Hours = readStats(RED_KEY_TILE, RED_MAPTILE_SERVER, 24);
		String msg = String.format("<a href='https://tile.osmand.net/hd/3/4/2.png'>tile</a>: "
				+ "<b>%s</b>. Response time: 24h — %.1f sec · 95th 24h — %.1f sec.",
				lastResponseTime < 60 ? "OK" : "FAILED", tile24Hours.getMean(), tile24Hours.getPercentile(PERC));
		String url ="https://maptile.osmand.net/routing/route?routeMode=car&points=51.063218,6.211030&points=51.179417,8.490871&maxDist=100";
		String routingStatus = "OK";
		long time = System.nanoTime();
		try {
			Algorithms.readFromInputStream(new URL(url).openStream()).toString();
		} catch (Exception e) {
			LOG.info(e.getMessage(), e);
			routingStatus = "FAILED";
		}
		msg += String.format("\n<a href='" + url + "'>routing</a>: " + "<b>%s</b>. Response time: %.1f sec.",
				routingStatus, (System.nanoTime() - time) / 1.0e9);
		return msg;
	}

	private DescriptiveStatistics readStats(String key, String server, int hour) {
		
		DescriptiveStatistics stats = new DescriptiveStatistics();
		jdbcTemplate.execute("SELECT value FROM servers.metrics WHERE name = ? and server = ? and timestamp >= now() - interval ? hour "
						+ " ORDER BY timestamp asc", new PreparedStatementCallback<Boolean>() {

							@Override
							public Boolean doInPreparedStatement(PreparedStatement ps)
									throws SQLException, DataAccessException {
								ps.setString(1, RED_STAT_PREFIX + key);
								ps.setString(2, convertServer(server));
								ps.setInt(3, hour);
								ResultSet rs = ps.executeQuery();
								while (rs.next()) {
									stats.addValue(rs.getDouble(1));
								}
								return true;
							}
						});
		return stats;
	}

	protected String convertServer(String server) {
		server = server.replace(".osmand.net", "");
		if (server.equals("download")) {
			return RED_MAIN_SERVER;
		}
		if (server.equals("tile")) {
			return RED_MAPTILE_SERVER;
		}
		return server;
	}

	public String refreshAll() {
		checkOsmAndLiveStatus(false);
		checkOsmAndBuildServer();
		checkIndexesValidity();
		checkOsmAndJavaServers();
		tileDownloadTest();

		return getStatusMessage();
	}

	public String getStatusMessage() {
		String msg = getLiveDelayedMessage(live.lastOsmAndLiveDelay) + "\n";
		int failed = 0;
		for (BuildServerCheckInfo buildServer : buildServers) {
			if (buildServer.jobsFailed != null && !buildServer.jobsFailed.isEmpty()) {
				failed++;
				msg += String.format("<a href='%s'>%s</a>: <b>FAILED</b>. Jobs: %s\n", buildServer.serverUrl,
						buildServer.serverName, formatJobNamesAsHref(buildServer, buildServer.jobsFailed));
			}
		}
		if (failed == 0) {
			msg += "<a href='https://creator.osmand.net:8080'>jenkins</a>: <b>OK</b>.\n"; // TODO builder
		}
		for (String host: downloadTests.keySet()) {
			DownloadTestResult r = downloadTests.get(host);
			String urlToRestart = javaChecks.failed.get(host);
			msg += r.fullString(urlToRestart) + "\n";
		}
		msg += getTileServerMessage();
		return msg;
	}

	private String timeAgo(long tm) {
		float hr = (float) ((System.currentTimeMillis() - tm) / (60 * 60 * 1000.0));
		int h = (int) hr;
		int m = (int) ((hr * 60) - 60 * h);
		return String.format("%d:%d ago", h, m);
	}

	private Set<String> formatJobNamesAsHref(BuildServerCheckInfo buildServer, Set<String> jobNames) {
		Set<String> formatted = new TreeSet<>();
		for (String jobName : jobNames) {
			formatted.add(String.format("<a href='%s/job/%s/'>%s</a>", buildServer.serverUrl, jobName, jobName));
		}
		return formatted;
	}

	private String getLiveDelayedMessage(long delay) {
		DescriptiveStatistics live3Hours = readStats(RED_KEY_OSMAND_LIVE, RED_MAIN_SERVER, 3);
		DescriptiveStatistics live24Hours = readStats(RED_KEY_OSMAND_LIVE, RED_MAIN_SERVER, 24);
		DescriptiveStatistics live3Days = readStats(RED_KEY_OSMAND_LIVE, RED_MAIN_SERVER, 24 * 3);
		DescriptiveStatistics live7Days = readStats(RED_KEY_OSMAND_LIVE, RED_MAIN_SERVER, 24 * 7);
		DescriptiveStatistics live30Days = readStats(RED_KEY_OSMAND_LIVE, RED_MAIN_SERVER, 24 * 30);
		return String.format("<a href='https://builder.osmand.net/osm_live/'>live</a>: <b>%s</b>. Delayed by: %s h · 3h — %s h · 24h — %s (%s) h\n"
				+ "Day stats: 3d %s (%s) h  · 7d — %s (%s) h · 30d — %s (%s) h",
				delay < HOUR ? "OK" : "FAILED",
						formatTime(delay), formatTime(live3Hours.getPercentile(PERC)),
						formatTime(live24Hours.getPercentile(PERC)), formatTime(live24Hours.getMean()),
						formatTime(live3Days.getPercentile(PERC)), formatTime(live3Days.getMean()),
						formatTime(live7Days.getPercentile(PERC)), formatTime(live7Days.getMean()),
						formatTime(live30Days.getPercentile(PERC)), formatTime(live30Days.getMean()));
	}
	
	@Scheduled(fixedRate = 5 * MINUTE)
	public void checkOsmAndChangesFeed() throws IOException, XmlPullParserException, ParseException {
		if (!Algorithms.isEmpty(urlChangesFeed)) {
			List<FeedEntry> newFeed = new ArrayList<>();
			long timestampNow = System.currentTimeMillis();
			for (FeedEntry e : parseFeed(new URL(urlChangesFeed).openStream())) {
				FeedEntry old = null;
				for (FeedEntry o : feed) {
					if (Algorithms.objectEquals(o.id, e.id)) {
						old = o;
						break;
					}
				}
				if ((old == null || old.updated.getTime() != e.updated.getTime())
						&& e.updated.getTime() > lastFeedCheckTimestamp) {
					newFeed.add(e);
				}
			}
			if (lastFeedCheckTimestamp == 0) {
				if (newFeed.size() > 0) {
					FeedEntry last = newFeed.get(newFeed.size() - 1);
					telegram.sendChannelMessage(publishChannel, formatGithubMsg(last));
				}
			} else {
				for (FeedEntry n : newFeed) {
					telegram.sendChannelMessage(publishChannel, formatGithubMsg(n));
				}
			}
			feed.addAll(newFeed);
			while (feed.size() > 100) {
				feed.remove(0);
			}
			
			lastFeedCheckTimestamp = timestampNow;
		}
	}

	

	static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(" EEE - dd MMM yyyy, HH:mm");
	
	/**
	 * This method now creates a plain text message with a raw link
	 * to allow for Telegram's link preview functionality.
	 */
	private String formatGithubMsg(FeedEntry n) {
		String emoji = EmojiConstants.GITHUB_EMOJI;
		String tags = "";

		String title = n.title.toLowerCase();

		
		if (title.contains("opened a pull request")) {
			emoji = EmojiConstants.OPEN_EMOJI;
			tags = "#pullrequest #open";
		} else if (title.contains("reopened")) {
			emoji = EmojiConstants.REOPEN_EMOJI;
			tags = "#reopen";
		} else if (title.contains("opened an issue")) {
			emoji = EmojiConstants.OPEN_EMOJI;
			tags = "#issue #open";
		} else if (title.contains("closed")) {
			emoji = EmojiConstants.CLOSED_EMOJI;
			tags = "#close";
		} else if (title.contains("pushed")) {
			emoji = EmojiConstants.PUSHED_EMOJI;
			tags = "#push #branch";
		} else if (title.contains("commented on")) {
			emoji = EmojiConstants.COMMENT_EMOJI;
			tags = "#comment";
		} else if (title.contains("merged")) {
			emoji = EmojiConstants.MERGE_EMOJI;
			tags = "#merge";
		} else if (title.contains("created a branch")) {
			emoji = EmojiConstants.CREATE_EMOJI;
			tags = "#create #branch";
		} else if (title.contains("deleted a branch")) {
			emoji = EmojiConstants.DELETE_EMOJI;
			tags = "#delete #branch";
		}

		// Build the main message body
		StringBuilder bld = new StringBuilder();
		bld.append(emoji).append(" <b>#").append(n.author).append("</b> ").append(title.toLowerCase().replace(n.author.toLowerCase(), ""));
		

		if (n.link != null) {
			bld.append("\n").append(loadPreviewTitleAndDescription(n.link));
			
		}

		// Add timestamp, tags, and the raw link on separate lines
		bld.append("\n").append(DATE_FORMAT.format(n.updated).trim());
		bld.append("\n").append(tags.trim());
		bld.append("\n").append(n.link);
		
		return bld.toString();
	}
	
	public String loadPreviewTitleAndDescription(String urlString) {
		String title = "";
		String description = "";
		try {
			URL url = new URL(urlString);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestProperty("User-Agent", "Mozilla/5.0"); // Act like a browser
			// Request only the first 4KB of the page, which should contain the <head> tag.
			connection.setInstanceFollowRedirects(true);
			int responseCode = connection.getResponseCode();
			if (responseCode != HttpURLConnection.HTTP_OK) {
				return "";
			}
			Pattern titleM = Pattern.compile("property=\"og:title\" content=\"([^\"]*)\"");
			Pattern descM = Pattern.compile("property=\"og:description\" content=\"([^\"]*)\"");
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
				String line;
				while ((line = reader.readLine()) != null) {
					Matcher matcher = titleM.matcher(line);
					if (matcher.find()) {
						title = matcher.group(1);
					}
					matcher = descM.matcher(line);
					if (matcher.find()) {
						description = matcher.group(1);
					}
					// Stop early if both are found
					if (!title.isEmpty() && !description.isEmpty()) {
						break;
					}
				}
			}
		} catch (IOException e) {
			System.err.println("Error fetching or parsing URL: " + e.getMessage());
			return ""; // Return empty string on error
		}

		//return (title + " " + description).trim();
//		System.out.println("TITLE: " + title);
//		System.out.println("DESC: " + description);
		return title.trim();
	}


	public List<FeedEntry> parseFeed(InputStream stream) throws IOException, XmlPullParserException, ParseException {
		XmlPullParser parser = PlatformUtil.newXMLPullParser();
		parser.setInput(new InputStreamReader(stream));
		int token;
		StringBuilder content = new StringBuilder();
		FeedEntry lastEntry = null;
		List<FeedEntry> lst = new ArrayList<FeedEntry>();
		while((token = parser.nextToken()) != XmlPullParser.END_DOCUMENT) {
			if(token == XmlPullParser.START_TAG) {
				switch (parser.getName()) {
				case "entry": lastEntry = new FeedEntry(); break;
				case "link": if (lastEntry != null) lastEntry.link = parser.getAttributeValue("", "href"); break;
				}
				content.setLength(0);
			} else if(token == XmlPullParser.TEXT) {
				content.append(parser.getText());
			} else if(token == XmlPullParser.END_TAG) {
				switch (parser.getName()) {
				case "entry":
					if (lastEntry != null) {
						if (lastEntry.published == null) {
							lastEntry.published = lastEntry.updated;
						} else if (lastEntry.updated == null) {
							lastEntry.updated = lastEntry.published;
						}
						if (lastEntry.updated != null) {
							lst.add(lastEntry);
						}
					}
					break;
				case "name":
					if (lastEntry != null) lastEntry.author = content.toString(); break;
				case "published":
					if (lastEntry != null) lastEntry.published = parseDate(content.toString()); break;
				case "updated":
					if (lastEntry != null) lastEntry.updated = parseDate(content.toString()); break;
				case "id":
					if (lastEntry != null) lastEntry.id = content.toString(); break;
				case "title":
					if (lastEntry != null) lastEntry.title = content.toString(); break;
				case "content":
					if (lastEntry != null) {
						lastEntry.content = content.toString();
						// no title could be parsed
					}
					break;
				}
			}
		};
		Collections.sort(lst, new Comparator<FeedEntry>() {

			@Override
			public int compare(FeedEntry o1, FeedEntry o2) {
				return Long.compare(o1.updated.getTime(), o2.updated.getTime());
			}
		});
		return lst;
	}

	private String formatTime(double i) {
		double f = i / HOUR;
		int d = (int) f;
		int min = (int) ((f - d) * 60);
		if (min < 10) {
			return d + ":0" + min;
		}
		return d + ":" + min;
	}

	protected static class LiveCheckInfo {
		long previousOsmAndLiveDelay = 0;
		long lastOsmAndLiveDelay = 0;
		long lastCheckTimestamp = 0;
	}
	
	protected static class JavaServerCheckInfo {
		Map<String, String> failed = new LinkedHashMap<String, String>();
		long lastCheckTimestamp = 0;
	}

	protected static class BuildServerCheckInfo {
		String serverUrl;
		String serverName;
		Set<String> jobsFailed;
		long lastCheckTimestamp = 0;
		
		public BuildServerCheckInfo(String serverUrl, String serverName) {
			this.serverName = serverName;
			this.serverUrl = serverUrl;
		}
	}

	protected class DownloadTestResult {
		private final String host;
		boolean lastSuccess = true;
		double lastSpeed;

		public DownloadTestResult(String host) {
			this.host = host;
		}

		public void addSpeedMeasurement(double spdMBPerSec) {
			lastSpeed = spdMBPerSec;
			addStat(RED_KEY_DOWNLOAD, host, spdMBPerSec);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			DownloadTestResult that = (DownloadTestResult) o;
			return host != null ? host.equals(that.host) : that.host == null;
		}

		public String fullString(String urlFailedJava) {
			DescriptiveStatistics last = readStats(RED_KEY_DOWNLOAD, host, 1);
			DescriptiveStatistics speed3Hours = readStats(RED_KEY_DOWNLOAD, host, 3);
			DescriptiveStatistics speed24Hours = readStats(RED_KEY_DOWNLOAD, host, 24);
			String name = host.substring(0, host.indexOf('.'));
			String status = (lastSuccess ? "OK" : "FAILED");
			if (urlFailedJava != null) {
				status = String.format("FAILED. Server - <a href='%s'>restart</a>", urlFailedJava);
			}
			return String.format("<a href='%s'>%s</a>: <b>%s</b>. Speed: %5.2f, 3h — %5.2f MBs · 24h — %5.2f MBs",
					host, name, status,
					last.getMean(), speed3Hours.getPercentile(PERC_SMALL),
					speed24Hours.getPercentile(PERC_SMALL));
		}

		@Override
		public String toString() {
			return host + ": <b>" + (lastSuccess ? "OK" : "FAILED") + "</b>. "
					+ String.format("last - %5.2f MBs ", lastSpeed);
		}

		@Override
		public int hashCode() {
			return host != null ? host.hashCode() : 0;
		}
	}

	private String runCmd(String cmd, File loc, List<String> errors) {
		try {
			Process p = Runtime.getRuntime().exec(cmd.split(" "), new String[0], loc);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String s, commit = "";
			// read the output from the command
			while ((s = stdInput.readLine()) != null) {
				commit += s +"\n";
			}

			p.waitFor();
			return commit.trim();
		} catch (Exception e) {
			String fmt = String.format("Error running %s: %s", cmd, e.getMessage());
			LOG.warn(fmt);
			if(errors != null) {
				errors.add(fmt);
			}
			return null;
		}
	}

	public void setSender(Sender sender) {
		this.telegram = sender;
	}
	
	public static void main(String[] args) throws IOException, XmlPullParserException, ParseException {
		OsmAndServerMonitorTasks t = new OsmAndServerMonitorTasks();
		for (FeedEntry e : t.parseFeed(OsmAndServerMonitorTasks.class.getResourceAsStream("/test-feed.xml"))) {
			System.out.println(t.formatGithubMsg(e));
			System.out.println("----------");
		}
	}
}
