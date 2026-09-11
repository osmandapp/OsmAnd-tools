package net.osmand.server.traffic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.Normalizer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;

/**
 * One traffic data source. Everything lives under feeds/&lt;id&gt;/:
 * <ul>
 * <li>raw/&lt;yyyy-mm-dd&gt;/ - files downloaded for that local day (removed after {@link TrafficFeeds#RETENTION_DAYS})</li>
 * <li>static/ - site tables and calibration data that are not tied to a day</li>
 * <li>&lt;yyyy-mm-dd&gt;.json.gz - the day in {@link TrafficDayFormat}</li>
 * </ul>
 */
public abstract class TrafficFeed {

	private static final double POINT_MAX_DIST_M = 40;
	private static final double POINT_PIECE_HALF_M = 45;
	private static final HttpClient HTTP = HttpClient.newBuilder()
			.followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(Duration.ofSeconds(30)).build();

	public static class Day {
		public final List<TrafficSensor> sensors = new ArrayList<>();
		public final List<TrafficEvent> events = new ArrayList<>();
		public String source;
		public String stateSource = "occupancy"; // occupancy bands, the source's own level ("city") or speed bands ("speed")
		public Integer defaultHour;              // live feeds: open the page on the latest snapshot
	}

	protected File dir;

	public void init(File feedsDir) {
		dir = new File(feedsDir, id());
	}

	public abstract String id();

	/** Short name for the source selector and the page title. */
	public abstract String name();

	public abstract ZoneId zone();

	/** OBF files (names in the maps folder) that cover the sensors. */
	public abstract List<String> obfFiles();

	public double defaultSpeedKmh() {
		return 50;
	}

	/** How often the source publishes new data. How often it is downloaded follows from this: {@link TrafficFeeds#downloadEvery}. */
	public abstract Duration publishInterval();

	/** How much time one published value covers (a 60-minute count, a 1-minute snapshot, ...). */
	public abstract Duration valueWindow();

	/**
	 * Asked by the master run (cron) on every tick. Default: the last download is older than
	 * {@link TrafficFeeds#downloadEvery}, less half a tick - otherwise a feed that wants 15 minutes and is asked every
	 * 5 minutes would slip to every 20 minutes because the runs start a few seconds late.
	 */
	public boolean wantsDownload(java.time.Instant lastDownload, java.time.Instant now, Duration tick) {
		return lastDownload == null
				|| Duration.between(lastDownload, now).plus(tick.dividedBy(2)).compareTo(TrafficFeeds.downloadEvery(this)) >= 0;
	}

	/** True when the source keeps past data, so a download fetches whole days and nothing is lost between downloads. */
	public boolean keepsHistory() {
		return false;
	}

	/** Downloads what the source publishes now into raw/ and static/. */
	public abstract void download(LocalDate today) throws Exception;

	public abstract Day read(LocalDate day) throws Exception;

	/**
	 * Called after matching (free-flow speed is known): fills estimated speed and returns a note for the page.
	 * Default: per-sensor calibration on the light-traffic hours of the same day.
	 */
	public String estimate(Day day, LoopSpeedEstimator estimator, ObfRoadMatcher matcher) throws Exception {
		int calibrated = calibrateOnOwnHours(day.sensors, estimator);
		return String.format(Locale.US, "Estimated from flow and occupancy: effective length calibrated per sensor on light-traffic hours "
				+ "of this day (%d sensors), assuming free flow runs at the OBF max speed.", calibrated);
	}

	/**
	 * For sources that measure speed: the state (congestion colours) is the speed as a share of the OBF max speed.
	 * @return sensors with a measured speed
	 */
	protected static int statesFromMeasuredSpeed(Day day) {
		int withSpeed = 0;
		for (TrafficSensor s : day.sensors) {
			boolean any = false;
			for (int h = 0; h < TrafficSensor.HOURS; h++) {
				s.state[h] = TrafficDayFormat.speedBand(s.measuredSpeed[h] > 0 ? s.measuredSpeed[h] : Double.NaN, s.freeFlowSpeed);
				any |= s.measuredSpeed[h] > 0;
			}
			withSpeed += any ? 1 : 0;
		}
		return withSpeed;
	}

	/** Text of a file, gunzipped when it ends with .gz. */
	protected static String readText(File file) throws IOException {
		try (InputStream in = file.getName().endsWith(".gz") ? new java.util.zip.GZIPInputStream(new java.io.FileInputStream(file)) : new java.io.FileInputStream(file)) {
			return new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	public File rawDir(LocalDate day) {
		return new File(dir, "raw/" + day);
	}

	public File staticDir() {
		File f = new File(dir, "static");
		f.mkdirs();
		return f;
	}

	public File dayFile(LocalDate day) {
		return new File(dir, day + ".json.gz");
	}

	/**
	 * Snapshot files that can hold values of this day: the day's raw folder and the next day's files before 02:00
	 * (a 60-minute value for 23:00-24:00 is downloaded after midnight). Readers keep only the values of the day.
	 */
	protected List<File> snapshotFiles(LocalDate day, String suffix) {
		List<File> files = new ArrayList<>(filesOf(rawDir(day), suffix));
		for (File f : filesOf(rawDir(day.plusDays(1)), suffix)) {
			if (f.getName().compareTo("0200") < 0) {
				files.add(f);
			}
		}
		return files;
	}

	/** Newest raw file that can change this day. */
	public long newestRaw(LocalDate day) {
		long newest = rawDir(day).lastModified();
		for (File f : snapshotFiles(day, "")) {
			newest = Math.max(newest, f.lastModified());
		}
		return newest;
	}

	public List<LocalDate> rawDays() {
		List<LocalDate> days = new ArrayList<>();
		File[] files = new File(dir, "raw").listFiles();
		if (files != null) {
			for (File f : files) {
				File[] content = f.listFiles();
				if (f.isDirectory() && content != null && content.length > 0 && f.getName().matches("\\d{4}-\\d{2}-\\d{2}")) {
					days.add(LocalDate.parse(f.getName()));
				}
			}
		}
		Collections.sort(days);
		return days;
	}

	protected static List<File> filesOf(File dir, String suffix) {
		List<File> list = new ArrayList<>();
		File[] files = dir.listFiles();
		if (files != null) {
			for (File f : files) {
				if (f.isFile() && f.getName().endsWith(suffix)) {
					list.add(f);
				}
			}
		}
		Collections.sort(list);
		return list;
	}

	/** Matches the sensors to OBF roads and sets the free-flow speed; point sensors without a road are dropped. */
	public static List<TrafficSensor> match(List<TrafficSensor> sensors, ObfRoadMatcher matcher, double defaultSpeed) throws Exception {
		List<TrafficSensor> result = new ArrayList<>();
		for (TrafficSensor s : sensors) {
			if (s.lat.length > 1) {
				s.match = matcher.matchLine(s.lat, s.lon);
				if (s.match != null) {
					// draw the sensor on the OBF roads it was matched to
					s.lat = s.match.pieceLat;
					s.lon = s.match.pieceLon;
				}
			} else {
				s.match = matcher.matchPoint(s.lat[0], s.lon[0], s.heading, s.headingTolerance, longestWord(s.name.split(" - ")[0]),
						POINT_MAX_DIST_M, POINT_PIECE_HALF_M);
				if (s.match == null) {
					continue;
				}
				s.lat = s.match.pieceLat;
				s.lon = s.match.pieceLon;
				s.directional = !Double.isNaN(s.heading);
			}
			double ms = s.match == null ? Double.NaN : s.match.getMaxSpeedKmh();
			s.freeFlowFromMap = !Double.isNaN(ms);
			s.freeFlowSpeed = s.freeFlowFromMap ? ms : defaultSpeed;
			result.add(s);
		}
		return result;
	}

	/**
	 * Calibrates every sensor on its own light-traffic hours. No fallback to a shared length: sensors that never see
	 * light traffic are the congested ones, and there a shared length gives nonsense (Madrid 10 Jun 2026: 75% of
	 * their hours under 5 km/h). Leaves their speed empty.
	 */
	public static int calibrateOnOwnHours(List<TrafficSensor> sensors, LoopSpeedEstimator estimator) {
		int calibrated = 0;
		for (TrafficSensor s : sensors) {
			s.effectiveLength = estimator.calibrate(s, s.freeFlowSpeed);
			s.effectiveLengthFallback = Double.isNaN(s.effectiveLength);
			calibrated += s.effectiveLengthFallback ? 0 : 1;
			estimator.estimate(s);
		}
		return calibrated;
	}

	/**
	 * Downloads url into target, gzipped when gzip is true. Asks for a gzip transfer (Digitraffic answers 406 without
	 * it) and stores a gzip body as is when the target is gzipped, unpacked otherwise.
	 */
	protected static void download(String url, File target, boolean gzip) throws IOException, InterruptedException {
		for (int attempt = 1; ; attempt++) {
			try {
				downloadOnce(url, target, gzip);
				return;
			} catch (IOException e) {
				// Brussels answers 503 now and then
				if (attempt >= 3 || !String.valueOf(e.getMessage()).startsWith("HTTP 5")) {
					throw e;
				}
				TrafficFeeds.log("  %s, retrying", e.getMessage());
				Thread.sleep(15_000L * attempt);
			}
		}
	}

	private static void downloadOnce(String url, File target, boolean gzip) throws IOException, InterruptedException {
		HttpResponse<InputStream> response = HTTP.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMinutes(15))
				.header("User-Agent", "OsmAnd traffic prototype").header("Digitraffic-User", "OsmAnd/traffic-prototype")
				.header("Accept-Encoding", "gzip").build(), HttpResponse.BodyHandlers.ofInputStream());
		if (response.statusCode() != 200) {
			response.body().close();
			throw new IOException("HTTP " + response.statusCode() + " for " + url);
		}
		boolean gzipBody = response.headers().firstValue("Content-Encoding").map(e -> e.equalsIgnoreCase("gzip")).orElse(false);
		target.getParentFile().mkdirs();
		File part = new File(target.getPath() + ".part");
		try (InputStream body = response.body();
		     InputStream in = gzipBody && !gzip ? new java.util.zip.GZIPInputStream(body) : body;
		     OutputStream out = gzip && !gzipBody ? new GZIPOutputStream(new FileOutputStream(part)) : new FileOutputStream(part)) {
			in.transferTo(out);
		}
		Files.move(part.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	protected static void writeGz(File target, String text) throws IOException {
		target.getParentFile().mkdirs();
		File part = new File(target.getPath() + ".part");
		try (OutputStream out = new GZIPOutputStream(new FileOutputStream(part))) {
			out.write(text.getBytes(StandardCharsets.UTF_8));
		}
		Files.move(part.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	protected static String postJson(String url, String json) throws IOException, InterruptedException {
		HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMinutes(5))
				.header("User-Agent", "OsmAnd traffic prototype").header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(json)).build(), HttpResponse.BodyHandlers.ofString());
		if (response.statusCode() != 200) {
			throw new IOException("HTTP " + response.statusCode() + " for " + url + ": " + response.body().substring(0, Math.min(300, response.body().length())));
		}
		return response.body();
	}

	protected static String downloadText(String url) throws IOException, InterruptedException {
		HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMinutes(2))
				.header("User-Agent", "OsmAnd traffic prototype").build(), HttpResponse.BodyHandlers.ofString());
		if (response.statusCode() != 200) {
			throw new IOException("HTTP " + response.statusCode() + " for " + url);
		}
		return response.body();
	}

	protected static String encode(String s) {
		return URLEncoder.encode(s, StandardCharsets.UTF_8);
	}

	protected static boolean olderThanDays(File f, int days) {
		return !f.exists() || System.currentTimeMillis() - f.lastModified() > days * 24L * 3600 * 1000;
	}

	protected static double parse(String s) {
		if (s == null || s.isEmpty()) {
			return Double.NaN;
		}
		try {
			return Double.parseDouble(s.trim().replace(',', '.'));
		} catch (NumberFormatException e) {
			return Double.NaN;
		}
	}

	protected static List<String> splitCsv(String line, char separator) {
		List<String> out = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		boolean quoted = false;
		for (int i = 0; i < line.length(); i++) {
			char c = line.charAt(i);
			if (quoted) {
				if (c == '"') {
					if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
						sb.append('"');
						i++;
					} else {
						quoted = false;
					}
				} else {
					sb.append(c);
				}
			} else if (c == '"') {
				quoted = true;
			} else if (c == separator) {
				out.add(sb.toString());
				sb.setLength(0);
			} else {
				sb.append(c);
			}
		}
		out.add(sb.toString());
		return out;
	}

	// heading in degrees (0 = north, clockwise) from compass vectors
	protected static double heading(double dx, double dy) {
		double h = Math.toDegrees(Math.atan2(dx, dy));
		return h < 0 ? h + 360 : h;
	}

	private static String longestWord(String s) {
		String best = null;
		String n = Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("\\p{M}", "").toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", " ").trim();
		for (String w : n.split(" ")) {
			if (w.length() >= 4 && (best == null || w.length() > best.length())) {
				best = w;
			}
		}
		return best;
	}
}
