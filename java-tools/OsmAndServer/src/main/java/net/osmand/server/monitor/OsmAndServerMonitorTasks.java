package net.osmand.server.monitor;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.*;
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

	DescriptiveStatistics live3Hours = new DescriptiveStatistics(3 * 60 / LIVE_STATUS_MINUTES);
	DescriptiveStatistics live24Hours = new DescriptiveStatistics(24 * 60 / LIVE_STATUS_MINUTES);

	@Autowired
	OsmAndServerMonitoringBot telegram;

	LiveCheckInfo live = new LiveCheckInfo();
	BuildServerCheckInfo buildServer = new BuildServerCheckInfo();
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");

	boolean mapIndexPrevValidation = true;

	int mapsCountThreshold = 700;

	List<String> urlsForDownload =
			Arrays.asList(
					"http://download.osmand.net/download.php?standard=yes&file=Angola_africa_2.obf.zip",
					"http://dl4.osmand.net/download.php?standard=yes&file=Angola_africa_2.obf.zip",
					"http://dl5.osmand.net/download.php?standard=yes&file=Angola_africa_2.obf.zip",
					"http://dl6.osmand.net/download.php?standard=yes&file=Angola_africa_2.obf.zip");

	List<DownloadTestResult> downloadTestResults = new ArrayList<>();

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
					telegram.sendMonitoringAlertMessage("There are new failures on OsmAnd Build Server: " + jobsFailedCopy);
				}

				Set<String> jobsRecoveredCopy = new TreeSet<String>(buildServer.jobsFailed);
				jobsRecoveredCopy.removeAll(jobsFailed);
				if (!jobsRecoveredCopy.isEmpty()) {
					telegram.sendMonitoringAlertMessage("There are recovered jobs on OsmAnd Build Server: " + jobsRecoveredCopy);
				}
				buildServer.jobsFailed = jobsFailed;
			}
			buildServer.lastCheckTimestamp = System.currentTimeMillis();
		} catch (Exception e) {
			telegram.sendMonitoringAlertMessage("Exception while checking the build server status.");
			LOG.error(e.getMessage(), e);
		}
	}

	@Scheduled(fixedRate = 5 * MINUTE)
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

		File tmpDir = new File("/tmp/dlspeedtest");
		if (!tmpDir.exists()) {
			tmpDir.mkdir();
		}

		File tmpFile = new File(tmpDir.getPath() + "/dltestfile.zip");

		downloadTestResults.clear();

		for (String downloadUrl : urlsForDownload) {
			URL url = null;
			String host = null;
			try {
				url = new URL(downloadUrl);
				host = url.getHost();
				URLConnection conn = url.openConnection();
				long contentLength = conn.getContentLengthLong();
				try (InputStream is = conn.getInputStream();
					 FileOutputStream fos = new FileOutputStream(tmpFile)) {
					int read = 0;
					byte[] buf = new byte[1024 * 1024];
					long startedAt = System.currentTimeMillis();
					while ((read = is.read(buf)) != -1) {
						fos.write(buf);
					}
					long finishedAt = System.currentTimeMillis();
					double downloadTimeInSec = (finishedAt - startedAt) / 1000d;
					double downloadSpeedBytesPerSec = contentLength / downloadTimeInSec;
					DownloadTestResult dtr = new DownloadTestResult(host, true, downloadSpeedBytesPerSec);
					downloadTestResults.add(dtr);
				}
			} catch (IOException ex) {
				DownloadTestResult dtr = new DownloadTestResult(host, false, -1);
				downloadTestResults.add(dtr);
				LOG.error(ex.getMessage(), ex);
			}
		}
		tmpFile.delete();
		tmpDir.delete();
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
			if (mapsInMapIndex >= mapsCountThreshold) {
				telegram.sendMonitoringAlertMessage(
						String.format("Map index is correct and contains %5d maps.", mapsInMapIndex));
			} else {
				telegram.sendMonitoringAlertMessage(
						String.format("Map index is correct but maps quantity (%5d) is less than required (%5d)",
								mapsInMapIndex, mapsCountThreshold));
			}

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


	public String refreshAll() {
		checkOsmAndLiveStatus(false);
		checkOsmAndBuildServer();
		return getStatusMessage();
	}

	public String getStatusMessage() {
		String msg = getLiveDelayedMessage(live.lastOsmAndLiveDelay) + "\n";
		if (buildServer.jobsFailed.isEmpty()) {
			msg += "OsmAnd Build server is OK.";
		} else {
			msg += "OsmAnd Build server has failing jobs: " + buildServer.jobsFailed;
		}
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
		private static final List<String> SUFFIXES = Arrays.asList("kb", "Mb");

		private final String host;
		private final boolean success;
		private final double speed;

		public DownloadTestResult(String host, boolean success, double speed) {
			this.host = host;
			this.success = success;
			this.speed = speed;
		}

		private static String formatSpeed(double downloadSpeed) {
			for (String suffix : SUFFIXES) {
				downloadSpeed /= 1000;
				if (downloadSpeed < 1000) {
					return String.format("%5.1f %s", downloadSpeed, suffix);
				}
			}
			throw new IllegalArgumentException();
		}

		public String getHost() {
			return host;
		}

		public boolean isSuccess() {
			return success;
		}

		public double getSpeed() {
			return speed;
		}

		@Override
		public String toString() {
			if (success) {
				return String.format("Download speed from %s - %5.1f%n", host, formatSpeed(speed));
			}
			return String.format("%s is unavailable.%n", host);
		}
	}


}