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
	private static final int LIVE_STATUS_MINUTES = 2;
	private static final int DOWNLOAD_MAPS_MINITES = 5;
	private static final int DOWNLOAD_TILE_MINUTES = 10;
	private static final int TILE_ZOOM = 19;
	private static final int NEXT_TILE = 64;
	private static final int TILEX_NUMBER = 268660;
	private static final int TILEY_NUMBER = 174100;


	private static final int MAPS_COUNT_THRESHOLD = 700;


	private static final String[] HOSTS_TO_TEST = new String[] { "download.osmand.net", "dl4.osmand.net",
			"dl5.osmand.net", "dl6.osmand.net" };

	private static final String TILE_SERVER = "http://tile.osmand.net/hd/";

	DescriptiveStatistics live3Hours = new DescriptiveStatistics(3 * 60 / LIVE_STATUS_MINUTES);
	DescriptiveStatistics live24Hours = new DescriptiveStatistics(24 * 60 / LIVE_STATUS_MINUTES);

	private int tileX = TILEX_NUMBER;
	private int tileY = TILEY_NUMBER;

	DescriptiveStatistics tile24Hours = new DescriptiveStatistics(24 * 60 / DOWNLOAD_TILE_MINUTES);

	@Autowired
	OsmAndServerMonitoringBot telegram;

	LiveCheckInfo live = new LiveCheckInfo();
	BuildServerCheckInfo buildServer = new BuildServerCheckInfo();
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");

	boolean mapIndexPrevValidation = true;
	Map<String, DownloadTestResult> downloadTests = new TreeMap<>();

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
			format.setTimeZone(TimeZone.getTimeZone("UTC"));
			Date dt = format.parse(osmlivetime);
			br.close();
			long currentDelay = System.currentTimeMillis() - dt.getTime();
			if (currentDelay - live.previousOsmAndLiveDelay > 30 * MINUTE && currentDelay > HOUR) {
				telegram.sendMonitoringAlertMessage(getLiveDelayedMessage(currentDelay));
				live.previousOsmAndLiveDelay = currentDelay;
			}
			live.lastCheckTimestamp = System.currentTimeMillis();
			live.lastOsmAndLiveDelay = currentDelay;
			if (updateStats) {
				live3Hours.addValue(currentDelay);
				live24Hours.addValue(currentDelay);
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
			if (!buildServer.jobsFailed.equals(jobsFailed)) {
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
							"There are no problems any more with host " + host);
					}
					res.lastSuccess = true;
				}
				
			} catch (IOException ex) {
				if(res.lastSuccess ) {
					telegram.sendMonitoringAlertMessage(
							"There are problems with downloading maps from " + host);
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
		} catch (XmlPullParserException xmlex) {
			mapIndexCurrValidation = false;
			LOG.error(xmlex.getMessage(), xmlex);
		}

		if (mapIndexPrevValidation && !mapIndexCurrValidation) {
			telegram.sendMonitoringAlertMessage("Map index is not correctly generated on the website (check).");
		}

		if (!mapIndexPrevValidation && mapIndexCurrValidation) {
			telegram.sendMonitoringAlertMessage(
					String.format("Map index is correct and contains %5d maps.", mapsInMapIndex));
		}

		if (mapsInMapIndex < MAPS_COUNT_THRESHOLD) {
			telegram.sendMonitoringAlertMessage(
					String.format("Maps quantity (%5d) is less than required (%5d).",
							mapsInMapIndex, MAPS_COUNT_THRESHOLD));
		}
		mapIndexPrevValidation = mapIndexCurrValidation;
	}

	private void close(InputStream is) {
		try {
			is.close();
		} catch (IOException ex) {
			LOG.error(ex.getMessage(), ex);
		}
	}

	private String buildMessage(Set<DownloadTestResult> downloadTestResults) {
		StringBuilder sb = new StringBuilder();
		for (DownloadTestResult dtr : downloadTestResults) {
			sb.append(dtr.toString());
		}
		return sb.toString();
	}

	@Scheduled(fixedRate = DOWNLOAD_TILE_MINUTES * MINUTE)
	public void tileDownloadTest() {
		for (int i = 0; i < 3; i++) {
			String tileUrl = new StringBuilder()
					.append(TILE_SERVER).append(TILE_ZOOM)
					.append("/").append(tileX)
					.append("/").append(tileY)
					.append(".png").toString();
			double responseTime = estimateResponse(tileUrl);
			if (responseTime > 0) {
				tile24Hours.addValue(responseTime);
			} else {
				telegram.sendMonitoringAlertMessage("There are problems with downloading tile from " + tileUrl);
			}
			tileY -= NEXT_TILE;
			if (tileY == 0) {
				tileY = TILEY_NUMBER;
			}
		}
	}

	private double estimateResponse(String tileUrl) {
		double respTime = -1d;
		HttpURLConnection conn = null;
		try {
			URL url = new URL(tileUrl);
			conn = (HttpURLConnection) url.openConnection();
			conn.setConnectTimeout(7 * MINUTE);
			long startedAt = System.currentTimeMillis();
			conn.connect();
			long finishedAt = System.currentTimeMillis();
			respTime = (finishedAt - startedAt) / 1000d;
		} catch (IOException ex) {
			LOG.error(ex.getMessage(), ex);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return respTime;
	}

	private String getTileServerMessage() {
		return String.format("Tile server response time: avg24h = %.1f sec, max24h = %.1f sec%n",
				tile24Hours.getMean(), tile24Hours.getMax());
	}

	public String refreshAll() {
		checkOsmAndLiveStatus(false);
		checkOsmAndBuildServer();
		return getStatusMessage();
	}

	public String getStatusMessage() {
		String msg = getLiveDelayedMessage(live.lastOsmAndLiveDelay) + "\n";
		if (buildServer.jobsFailed.isEmpty()) {
			msg += "Build server is OK.";
		} else {
			msg += "Build server has failures: " + buildServer.jobsFailed;
		}
		for (DownloadTestResult r : downloadTests.values()) {
			msg += r.toString() + "\n";
		}
		msg += getTileServerMessage();
		return msg;
	}

	private String getLiveDelayedMessage(long delay) {
		String txt = "OsmAnd Live is delayed by " + formatTime(delay) + " hours ";
		txt += " (avg3h " + formatTime(live3Hours.getMean()) + ", avg24h " + formatTime(live24Hours.getMean())
				+ ", max24h " + formatTime(live24Hours.getMax()) + ")";
		return txt;
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
		Set<String> jobsFailed = new TreeSet<String>();
		long lastCheckTimestamp = 0;
	}

	protected static class DownloadTestResult {
		private final String host;
		boolean lastSuccess = true;
		double lastSpeed;
		
		DescriptiveStatistics speed3Hours = new DescriptiveStatistics(3 * 60 / DOWNLOAD_MAPS_MINITES);
		DescriptiveStatistics speed24Hours = new DescriptiveStatistics(24 * 60 / DOWNLOAD_MAPS_MINITES);

		public DownloadTestResult(String host) {
			this.host = host;
		}

		public void addSpeedMeasurement(double spdMBPerSec) {
			lastSpeed = spdMBPerSec;
			speed24Hours.addValue(spdMBPerSec);
			speed3Hours.addValue(spdMBPerSec);
		}


		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			DownloadTestResult that = (DownloadTestResult) o;
			return host != null ? host.equals(that.host) : that.host == null;
		}
		
		@Override
		public String toString() {
			return host + ": " + (lastSuccess ? "OK" : "FAILED") + 
					" (" + String.format("3h %5.2f MBs, 24h %5.2f MBs", speed3Hours.getMean(), speed24Hours.getMean()) + ")";
		}

		@Override
		public int hashCode() {
			return host != null ? host.hashCode() : 0;
		}
	}
}