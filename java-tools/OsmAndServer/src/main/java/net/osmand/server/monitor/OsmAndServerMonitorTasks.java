package net.osmand.server.monitor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import net.osmand.server.TelegramBotManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.kxml2.io.KXmlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

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

	private static final String[] HOSTS_TO_TEST = new String[] { "download.osmand.net", "dl4.osmand.net",
			"dl5.osmand.net", "dl6.osmand.net" };
	private static final String TILE_SERVER = "http://tile.osmand.net/hd/";

	private static final double PERCENTILE = 95;
	
	
	private static final String RED_KEY_OSMAND_LIVE = "live_delay_time";
	private static final String RED_KEY_TILE = "tile_time";
	private static final String RED_KEY_DOWNLOAD = "download_map.";
	
	private static final SimpleDateFormat TIME_FORMAT_UTC = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	static {
		TIME_FORMAT_UTC.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	

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
		try {
			URL url = new URL("http://osmand.net/api/osmlive_status");
			InputStream is = url.openConnection().getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String osmlivetime = br.readLine();
			Date dt = TIME_FORMAT_UTC.parse(osmlivetime);
			br.close();
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
		try {
			Set<String> jobsFailed = new TreeSet<String>();
			URL url = new URL("http://builder.osmand.net:8080/api/json");
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
					telegram.sendMonitoringAlertMessage("There are new failures on Build Server: " + jobsFailedCopy);
				}
				Set<String> jobsRecoveredCopy = new TreeSet<String>(buildServer.jobsFailed);
				jobsRecoveredCopy.removeAll(jobsFailed);
				if (!jobsRecoveredCopy.isEmpty()) {
					telegram.sendMonitoringAlertMessage("There are recovered jobs on Build Server: " + jobsRecoveredCopy);
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
		GZIPInputStream gis = null;
		try {
			URL url = new URL("http://osmand.net/get_indexes?gzip=true");
			URLConnection conn = url.openConnection();
			InputStream is = conn.getInputStream();
			gis = new GZIPInputStream(is);
			validateAndReport(gis);
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
				url = new URL("http://"+host+"/download.php?standard=yes&file=Angola_africa_2.obf.zip");
				URLConnection conn = url.openConnection();
				long contentLength = 0;
				try (InputStream is = conn.getInputStream()) {
					int read = 0;
					byte[] buf = new byte[1024 * 1024];
					long startedAt = System.currentTimeMillis();
					while ((read = is.read(buf)) != -1) {
						contentLength += read;
					}
					long finishedAt = System.currentTimeMillis();
					double downloadTimeInSec = (finishedAt - startedAt) / 1000d;
					double downloadSpeedMBPerSec = (contentLength / downloadTimeInSec) / (1024*1024);
					res.addSpeedMeasurement(downloadSpeedMBPerSec);
					if(!res.lastSuccess) {
						telegram.sendMonitoringAlertMessage(
								host + " OK. Maps download works fine");
					}
					res.lastSuccess = true;
				}
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
		double respTimeSum = 0; 
		int count = 4;
		long now = System.currentTimeMillis();
		long period = (((now / 1000) - INITIAL_TIMESTAMP_S) / 60 ) / DOWNLOAD_TILE_MINUTES;
		long yShift = period % 14400;
		long xShift = period / 14400;
		
		for (int i = 0; i < count; i++) {
			String tileUrl = new StringBuilder().append(TILE_SERVER).append(TILE_ZOOM).append("/").
					append(TILEX_NUMBER + (i + count * xShift) * NEXT_TILE).append("/").
					append(TILEY_NUMBER + yShift * NEXT_TILE).append(".png").toString();
			double tileDownload = estimateResponse(tileUrl);
			if(tileDownload < 0) {
				telegram.sendMonitoringAlertMessage("tile.osmand.net: problems with downloading tiles.");
				return;
			}
			LOG.info("Downloaded " + tileUrl + " " + tileDownload + " seconds.");
			respTimeSum += tileDownload;
		}
		lastResponseTime = respTimeSum / count;
		if (lastResponseTime > 0) {
			addStat(RED_KEY_TILE, lastResponseTime);
		}
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
		try {
			long startedAt = System.currentTimeMillis();
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
		}
		return respTime;
	}

	private String getTileServerMessage() {
		DescriptiveStatistics tile24Hours = readStats(RED_KEY_TILE, 24);
		return String.format("<a href='http://tile.osmand.net/hd/3/4/2.png'>tile</a>: "
				+ "<b>%s</b>. Response time: 24h — %.1f sec · max 24h — %.1f sec.",
				lastResponseTime < 60 ? "OK" : "FAILED", tile24Hours.getMean(), tile24Hours.getPercentile(95));
	}

	private DescriptiveStatistics readStats(String key, int hour) {
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
			msg += "<a href='builder.osmand.net'>builder</a>: <b>OK</b>.\n";
		} else {
			msg += "<a href='builder.osmand.net'>builder</a>: <b>FAILED</b>. Jobs: " + buildServer.jobsFailed + "\n";
		}
		for (DownloadTestResult r : downloadTests.values()) {
			msg += r.fullString() + "\n";
		}
		msg += getTileServerMessage();
		return msg;
	}

	private String getLiveDelayedMessage(long delay) {
		DescriptiveStatistics live3Hours = readStats(RED_KEY_OSMAND_LIVE, 3);
		DescriptiveStatistics live24Hours = readStats(RED_KEY_OSMAND_LIVE, 24);
		return String.format("<a href='osmand.net/osm_live'>live</a>: <b>%s</b>. Delayed by: %s h · 3h — %s h · 24h — %s (%s) h",
				delay < HOUR ? "OK" : "FAILED", formatTime(delay), formatTime(live3Hours.getPercentile(PERCENTILE)),
						formatTime(live24Hours.getPercentile(PERCENTILE)), formatTime(live24Hours.getMean()));
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
			DescriptiveStatistics speed3Hours = readStats(RED_KEY_DOWNLOAD + host, 3);
			DescriptiveStatistics speed24Hours = readStats(RED_KEY_DOWNLOAD + host, 24);
			String name = host.substring(0, host.indexOf('.'));
			return String.format("<a href='%s'>%s</a>: <b>%s</b>. Speed: " + "3h — %5.2f MBs · 24h — %5.2f MBs",
					host, name, (lastSuccess ? "OK" : "FAILED"), speed3Hours.getPercentile(PERCENTILE),
					speed24Hours.getPercentile(PERCENTILE));
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
}