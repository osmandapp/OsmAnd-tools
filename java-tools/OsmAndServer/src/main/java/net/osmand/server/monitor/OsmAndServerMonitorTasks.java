package net.osmand.server.monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.server.TelegramBotManager;
import net.osmand.util.Algorithms;

@Component
public class OsmAndServerMonitorTasks {

	private static final Log LOG = LogFactory.getLog(TelegramBotManager.class);

	private static final int SECOND = 1000;
	private static final int MINUTE = 60 * SECOND;
	private static final int HOUR = 60 * MINUTE;
	private static final long DAY = 24l * HOUR;
	private static final int LIVE_STATUS_MINUTES = 2;
	private static final int DOWNLOAD_MAPS_MINITES = 5;
	private static final int DOWNLOAD_TILE_MINUTES = 10;
	private static final int TILE_ZOOM = 19;
	private static final int NEXT_TILE = 16;
	private static final int TILEX_NUMBER = 268660;
	private static final int TILEY_NUMBER = 175100;
	private static final long INITIAL_TIMESTAMP_S = 1530840000;
	private static final long METRICS_EXPIRE = 30l * DAY;

	private static final int MAPS_COUNT_THRESHOLD = 700;

	private static final String[] HOSTS_TO_TEST = new String[] { "download.osmand.net",
			 "dl2.osmand.net","dl7.osmand.net",  "dl8.osmand.net",  "dl9.osmand.net", "maptile.osmand.net"};
	private static final String TILE_SERVER = "https://tile.osmand.net/hd/";

	private static final double PERC = 95;
	private static final double PERC_SMALL = 100 - PERC;


	private static final String RED_KEY_OSMAND_LIVE = "live_delay_time";
	private static final String RED_KEY_TILE = "tile_time";
	private static final String RED_KEY_DOWNLOAD = "download_map.";

	private static final SimpleDateFormat TIME_FORMAT_UTC = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	static {
		TIME_FORMAT_UTC.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	private static final SimpleDateFormat TIMESTAMP_FORMAT_OPR = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
	static {
		TIMESTAMP_FORMAT_OPR.setTimeZone(TimeZone.getTimeZone("UTC"));
	}


	@Value("${monitoring.enabled}")
	private boolean enabled;

	@Autowired
	private StringRedisTemplate redisTemplate;

	// OsmAnd Live validation
	LiveCheckInfo live = new LiveCheckInfo();
	// Build Server
	BuildServerCheckInfo buildServer = new BuildServerCheckInfo();
	// Tile validation
	double lastResponseTime;
	// Index map validation
	boolean mapIndexPrevValidation = true;
	Map<String, DownloadTestResult> downloadTests = new TreeMap<>();

	@Autowired
	OsmAndServerMonitoringBot telegram;



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
			if (currentDelay - live.previousOsmAndLiveDelay > 30 * MINUTE && currentDelay > HOUR) {
				telegram.sendMonitoringAlertMessage(getLiveDelayedMessage(currentDelay));
				live.previousOsmAndLiveDelay = currentDelay;
			}
			live.lastCheckTimestamp = System.currentTimeMillis();
			live.lastOsmAndLiveDelay = currentDelay;
			if (updateStats) {
				addStat(RED_KEY_OSMAND_LIVE, currentDelay);
			}
		} catch (Exception e) {
			telegram.sendMonitoringAlertMessage("Exception while checking the server live status.");
			LOG.error(e.getMessage(), e);
		}
	}

	@Scheduled(fixedRate = MINUTE)
	public void checkOsmAndBuildServer() {
		if (!enabled) {
			return;
		}
		try {
			Set<String> jobsFailed = new TreeSet<String>();
			URL url = new URL("https://builder.osmand.net:8080/api/json");
			InputStream is = url.openConnection().getInputStream();
			JSONObject object = new JSONObject(new JSONTokener(is));
			JSONArray jsonArray = object.getJSONArray("jobs");
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject jb = jsonArray.getJSONObject(i);
				String name = jb.getString("name");
				String color = jb.getString("color");
				if (!color.equals("blue") && !color.equals("disabled") &&
						!color.equals("notbuilt") && !color.equals("blue_anime")) {
					jobsFailed.add(name);
				}
			}
			is.close();
			if(buildServer.jobsFailed == null) {
				buildServer.jobsFailed = jobsFailed;
			} else if (!buildServer.jobsFailed.equals(jobsFailed)) {
				Set<String> jobsFailedCopy = new TreeSet<String>(jobsFailed);
				jobsFailedCopy.removeAll(buildServer.jobsFailed);
				if (!jobsFailedCopy.isEmpty()) {
					telegram.sendMonitoringAlertMessage("There are new failures on Build Server: " + formatJobNamesAsHref(jobsFailedCopy));
				}
				Set<String> jobsRecoveredCopy = new TreeSet<String>(buildServer.jobsFailed);
				jobsRecoveredCopy.removeAll(jobsFailed);
				if (!jobsRecoveredCopy.isEmpty()) {
					telegram.sendMonitoringAlertMessage("There are recovered jobs on Build Server: " + formatJobNamesAsHref(jobsRecoveredCopy));
				}
				buildServer.jobsFailed = jobsFailed;
			}
			buildServer.lastCheckTimestamp = System.currentTimeMillis();
		} catch (Exception e) {
			telegram.sendMonitoringAlertMessage("Exception while checking the build server status.");
			LOG.error(e.getMessage(), e);
		}
	}

	@Scheduled(fixedRate = DOWNLOAD_MAPS_MINITES * MINUTE)
	public void checkIndexesValidity() {
		if(!enabled) {
			return;
		}
		GZIPInputStream gis = null;
		try {
			URL url = new URL("https://osmand.net/get_indexes?gzip=true");
			URLConnection conn = url.openConnection();
			InputStream is = conn.getInputStream();
			gis = new GZIPInputStream(is);
			validateAndReport(gis);
			is.close();
		} catch (IOException ioex) {
			LOG.error(ioex.getMessage(), ioex);
			telegram.sendMonitoringAlertMessage("Exception while checking the map index validity.");
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
					telegram.sendMonitoringAlertMessage(host + " OK. Maps download works fine");
				}
				res.lastSuccess = true;

			} catch (IOException ex) {
				if(res.lastSuccess ) {
					telegram.sendMonitoringAlertMessage(
							host + " FAILURE: problems downloading maps " + ex.getMessage());
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
			if (!mapIndexPrevValidation) {
				telegram.sendMonitoringAlertMessage(
						String.format("download.osmand.net: Map index is correct and serves %d maps.", mapsInMapIndex));
			}

			if (mapsInMapIndex < MAPS_COUNT_THRESHOLD) {
				mapIndexCurrValidation = false;
				if (mapIndexPrevValidation) {
					telegram.sendMonitoringAlertMessage(String.format(
							"download.osmand.net: Maps index is not correct and serves only of %d maps.", mapsInMapIndex));
				}
			}
		} catch (XmlPullParserException xmlex) {
			mapIndexCurrValidation = false;
			if (mapIndexPrevValidation) {
				telegram.sendMonitoringAlertMessage("download.osmand.net: problems with parsing map index.");
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
					telegram.sendMonitoringAlertMessage(
							String.format("tile.osmand.net: problems with downloading tiles: %s (%d) - %s", times, retryCount, txStatus));
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
			addStat(RED_KEY_TILE, lastResponseTime);
		}
	}

	private String getTirexStatus() {
		try {
			String res = Algorithms.readFromInputStream(new URL("https://maptile.osmand.net/access_stats.txt").openStream()).toString();
			res = prepareAccessStats(res);

			StringBuilder rs = Algorithms.readFromInputStream(new URL("https://maptile.osmand.net/renderd.stats").openStream());
			res += prepareRenderdResult(rs.toString());
			StringBuilder date = Algorithms.readFromInputStream(new URL("https://maptile.osmand.net/osmupdate/state.txt").openStream());
			long t = TIMESTAMP_FORMAT_OPR.parse(date.toString()).getTime();
			res += String.format("\n<a href='https://tile.osmand.net:8080/'>Tile Postgis DB</a>: %s", timeAgo(t));
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
			String[] percents = spl[0].split("\\s+");
			String[] timings = spl[1].split("\\s+");
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

	private void addStat(String key, double score) {
		long now = System.currentTimeMillis();
		redisTemplate.opsForZSet().add(key, score + ":" + now, now);
		// Long removed =
		redisTemplate.opsForZSet().removeRangeByScore(key, 0, now - METRICS_EXPIRE);
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
		DescriptiveStatistics tile24Hours = readStats(RED_KEY_TILE, 24);
		return String.format("<a href='https://tile.osmand.net/hd/3/4/2.png'>tile</a>: "
				+ "<b>%s</b>. Response time: 24h — %.1f sec · 95th 24h — %.1f sec. %s",
				lastResponseTime < 60 ? "OK" : "FAILED", tile24Hours.getMean(), tile24Hours.getPercentile(PERC), getTirexStatus());
	}

	private DescriptiveStatistics readStats(String key, double hour) {
		long now = System.currentTimeMillis();
		DescriptiveStatistics stats = new DescriptiveStatistics();
		Set<String> ls = redisTemplate.opsForZSet().rangeByScore(key, now - hour * HOUR, now);
		for(String k : ls) {
			double v = Double.parseDouble(k.substring(0, k.indexOf(':')));
			stats.addValue(v);
		}
		return stats;
	}

	public String refreshAll() {
		checkOsmAndLiveStatus(false);
		checkOsmAndBuildServer();
		checkIndexesValidity();
		tileDownloadTest();

		return getStatusMessage();
	}

	public String getStatusMessage() {
		String msg = getLiveDelayedMessage(live.lastOsmAndLiveDelay) + "\n";
		if (buildServer.jobsFailed == null || buildServer.jobsFailed.isEmpty()) {
			msg += "<a href='https://builder.osmand.net:8080'>builder</a>: <b>OK</b>.\n";
		} else {
			msg += "<a href='https://builder.osmand.net:8080'>builder</a>: <b>FAILED</b>. Jobs: " + formatJobNamesAsHref(buildServer.jobsFailed) + "\n";
		}
		for (DownloadTestResult r : downloadTests.values()) {
			msg += r.fullString() + "\n";
		}
		msg += getTileServerMessage();
		msg += getOpenPlaceReviewsMessage();
		return msg;
	}

	private String getOpenPlaceReviewsMessage() {
		return String.format("\n<a href='https://r2.openplacereviews.org:890'>OpenPlaceReviews</a>: "
				+ "<a href='https://openplacereviews.org/api/admin'>%s</a> (test <a href='https://test.openplacereviews.org/api/admin'>%s</a>).",
				getOprSyncStatus("openplacereviews.org"),  getOprSyncStatus("test.openplacereviews.org"));
	}

	private String getOprSyncStatus(String url) {
		long minTimestamp = 0;
		try {
			JSONObject object = new JSONObject(new JSONTokener(
					new URL("https://" + url + "/api/objects-by-id?type=sys.bot&key=osm-sync").openStream()));
			JSONArray jsonArray = object.getJSONArray("objects");
			if (jsonArray.length() > 0) {
				JSONObject osmTags = jsonArray.getJSONObject(0).getJSONObject("bot-state").getJSONObject("osm-tags");
				for (String key : osmTags.keySet()) {
					String dt = osmTags.getJSONObject(key).getString("date");
					long tm = TIMESTAMP_FORMAT_OPR.parse(dt).getTime();
					if (minTimestamp == 0 || minTimestamp > tm) {
						minTimestamp = tm;
					}
				}
			}
			return minTimestamp == 0 ? "failed" : timeAgo(minTimestamp);
		} catch (Exception e) {
			LOG.warn(e.getMessage(), e);
			return "Error: " + e.getMessage();
		}
	}

	private String timeAgo(long tm) {
		float hr = (float) ((System.currentTimeMillis() - tm) / (60 * 60 * 1000.0));
		int h = (int) hr;
		int m = (int) ((hr * 60) - 60 * h);
		return String.format("%d:%d ago", h, m);
	}

	private Set<String> formatJobNamesAsHref(Set<String> jobNames) {
		Set<String> formatted = new TreeSet<>();
		for (String jobName : jobNames) {
			formatted.add(String.format("<a href='https://builder.osmand.net:8080/job/%1$s/'>%1$s</a>", jobName));
		}
		return formatted;
	}

	private String getLiveDelayedMessage(long delay) {
		DescriptiveStatistics live3Hours = readStats(RED_KEY_OSMAND_LIVE, 3);
		DescriptiveStatistics live24Hours = readStats(RED_KEY_OSMAND_LIVE, 24);
		DescriptiveStatistics live3Days = readStats(RED_KEY_OSMAND_LIVE, 24 * 3);
		DescriptiveStatistics live7Days = readStats(RED_KEY_OSMAND_LIVE, 24 * 7);
		DescriptiveStatistics live30Days = readStats(RED_KEY_OSMAND_LIVE, 24 * 30);
		return String.format("<a href='osmand.net/osm_live'>live</a>: <b>%s</b>. Delayed by: %s h · 3h — %s h · 24h — %s (%s) h\n"
				+ "Day stats: 3d %s (%s) h  · 7d — %s (%s) h · 30d — %s (%s) h",
				delay < HOUR ? "OK" : "FAILED",
						formatTime(delay), formatTime(live3Hours.getPercentile(PERC)),
						formatTime(live24Hours.getPercentile(PERC)), formatTime(live24Hours.getMean()),
						formatTime(live3Days.getPercentile(PERC)), formatTime(live3Days.getMean()),
						formatTime(live7Days.getPercentile(PERC)), formatTime(live7Days.getMean()),
						formatTime(live30Days.getPercentile(PERC)), formatTime(live30Days.getMean()));
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

	protected static class BuildServerCheckInfo {
		Set<String> jobsFailed;
		long lastCheckTimestamp = 0;
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
			addStat(RED_KEY_DOWNLOAD + host, spdMBPerSec);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			DownloadTestResult that = (DownloadTestResult) o;
			return host != null ? host.equals(that.host) : that.host == null;
		}

		public String fullString() {
			DescriptiveStatistics last = readStats(RED_KEY_DOWNLOAD + host, 0.5);
			DescriptiveStatistics speed3Hours = readStats(RED_KEY_DOWNLOAD + host, 3);
			DescriptiveStatistics speed24Hours = readStats(RED_KEY_DOWNLOAD + host, 24);
			String name = host.substring(0, host.indexOf('.'));
			return String.format("<a href='%s'>%s</a>: <b>%s</b>. Speed: %5.2f, 3h — %5.2f MBs · 24h — %5.2f MBs",
					host, name, (lastSuccess ? "OK" : "FAILED"),
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
//			BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
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
}
