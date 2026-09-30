package net.osmand.server.traffic;

import net.osmand.router.RouteResultPreparation;
import net.osmand.server.traffic.feeds.BrusselsFeed;
import net.osmand.server.traffic.feeds.FinlandFeed;
import net.osmand.server.traffic.feeds.HamburgFeed;
import net.osmand.server.traffic.feeds.LithuaniaFeed;
import net.osmand.server.traffic.feeds.MadridFeed;
import net.osmand.server.traffic.feeds.NdwFeed;
import net.osmand.server.traffic.feeds.NorwayFeed;
import net.osmand.server.traffic.feeds.ParisFeed;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Traffic feeds in one folder. On every tick each feed is asked whether it wants a download
 * ({@link TrafficFeed#wantsDownload}); raw days older than RETENTION_DAYS are dropped; changed days are rebuilt into
 * &lt;source&gt;/&lt;yyyy-mm-dd&gt;.json.gz and index.json lists the sources and their days. Downloads are only ever added,
 * day files are derived from them and can be rebuilt at any time.
 */
public class TrafficFeeds {

	private static final Log LOG = LogFactory.getLog(TrafficFeeds.class);

	public static final int RETENTION_DAYS = 3;
	/** Time resolution of the day files. */
	public static final Duration OUTPUT_INTERVAL = Duration.ofHours(1);
	/** Downloads per OUTPUT_INTERVAL for live feeds: at least 2, so a value of a whole interval is never missed. */
	public static final int SAMPLES_PER_INTERVAL = 4;
	public static final String INDEX_FILE = "index.json";
	private static final String LAST_DOWNLOAD = "last-download.txt";

	private final File dir;
	private final File maps;
	private final List<TrafficFeed> feeds = List.of(new ParisFeed(), new MadridFeed(), new BrusselsFeed(), new HamburgFeed(),
			new NdwFeed(), new NorwayFeed(), new LithuaniaFeed(), new FinlandFeed());

	public TrafficFeeds(File dir, File maps) {
		this.dir = dir;
		this.maps = maps;
		for (TrafficFeed feed : feeds) {
			feed.init(dir);
		}
	}

	public List<TrafficFeed> getFeeds() {
		return feeds;
	}

	/**
	 * How often a feed is downloaded, from what it declares. Live feeds: OUTPUT_INTERVAL / SAMPLES_PER_INTERVAL
	 * (15 min), not more often than the source publishes. Feeds that keep history fetch whole days:
	 * publishInterval / SAMPLES_PER_INTERVAL, at least OUTPUT_INTERVAL.
	 */
	public static Duration downloadEvery(TrafficFeed feed) {
		if (feed.keepsHistory()) {
			return max(feed.publishInterval().dividedBy(SAMPLES_PER_INTERVAL), OUTPUT_INTERVAL);
		}
		return max(feed.publishInterval(), OUTPUT_INTERVAL.dividedBy(SAMPLES_PER_INTERVAL));
	}

	/**
	 * One run of the scheduler.
	 * @param tick how often the scheduler calls it
	 */
	public void tick(Duration tick) throws IOException {
		// road matching runs the router for every sensor: no route info in the console for this run
		boolean printRoutes = RouteResultPreparation.PRINT_TO_CONSOLE_ROUTE_INFORMATION;
		RouteResultPreparation.PRINT_TO_CONSOLE_ROUTE_INFORMATION = false;
		try {
			boolean changed = false;
			for (TrafficFeed feed : feeds) {
				try {
					changed |= runFeed(feed, tick);
				} catch (Exception e) {
					LOG.error("Traffic feed " + feed.id() + " failed", e);
				}
			}
			if (changed || !new File(dir, INDEX_FILE).exists()) {
				writeIndex();
			}
		} finally {
			RouteResultPreparation.PRINT_TO_CONSOLE_ROUTE_INFORMATION = printRoutes;
		}
	}

	private boolean runFeed(TrafficFeed feed, Duration tick) throws Exception {
		LocalDate today = ZonedDateTime.now(feed.zone()).toLocalDate();
		File lastDownload = new File(feed.staticDir(), LAST_DOWNLOAD);
		Instant last = lastDownload.exists() ? Instant.parse(Files.readString(lastDownload.toPath()).trim()) : null;
		if (feed.wantsDownload(last, Instant.now(), tick)) {
			try {
				feed.download(today);
				Files.writeString(lastDownload.toPath(), Instant.now().toString());
			} catch (Exception e) {
				LOG.warn("Traffic feed " + feed.id() + " download failed: " + e);
			}
		}
		boolean changed = prune(feed, today);
		for (LocalDate day : feed.rawDays()) {
			File out = feed.dayFile(day);
			if (!out.exists() || out.lastModified() < feed.newestRaw(day)) {
				changed |= build(feed, day);
			}
		}
		return changed;
	}

	private boolean build(TrafficFeed feed, LocalDate day) throws Exception {
		long started = System.currentTimeMillis();
		TrafficFeed.Day d = feed.read(day);
		if (d.sensors.isEmpty() && d.events.isEmpty()) {
			return false;
		}
		for (TrafficSensor s : d.sensors) {
			for (int h = 0; h < TrafficSensor.HOURS; h++) {
				if (s.isFaulty(h)) {
					s.state[h] = TrafficSensor.STATE_UNKNOWN;
				}
			}
		}
		int read = d.sensors.size();
		List<TrafficSensor> matched = new ArrayList<>();
		String speedNote = "";
		if (!d.sensors.isEmpty()) {
			List<File> obfs = new ArrayList<>();
			for (String name : feed.obfFiles()) {
				obfs.add(new File(maps, name));
			}
			try (ObfRoadMatcher matcher = new ObfRoadMatcher(obfs)) {
				matched = TrafficFeed.match(d.sensors, matcher, feed.defaultSpeedKmh());
				d.sensors.clear();
				d.sensors.addAll(matched);
				speedNote = feed.estimate(d, new LoopSpeedEstimator(), matcher);
			}
		}
		JSONObject meta = new JSONObject().put("id", feed.id()).put("name", feed.name()).put("day", day.toString())
				.put("zone", feed.zone().getId()).put("source", d.source).put("speedNote", speedNote).put("stateSource", d.stateSource)
				.put("generated", Instant.now().toString());
		if (d.defaultHour != null) {
			meta.put("defaultHour", d.defaultHour);
		}
		TrafficDayFormat.write(feed.dayFile(day), meta, matched, d.events);
		log("%s %s: %d sensors, %d matched, %d events in %.1f s", feed.id(), day, read, matched.size(), d.events.size(),
				(System.currentTimeMillis() - started) / 1e3);
		return true;
	}

	// raw days older than RETENTION_DAYS go, and so do day files whose raw data is gone
	private static boolean prune(TrafficFeed feed, LocalDate today) throws IOException {
		LocalDate oldest = today.minusDays(RETENTION_DAYS);
		for (LocalDate day : feed.rawDays()) {
			if (day.isBefore(oldest)) {
				deleteRecursively(feed.rawDir(day));
			}
		}
		Set<LocalDate> raw = new HashSet<>(feed.rawDays());
		boolean changed = false;
		for (File f : TrafficFeed.filesOf(feed.dayFile(today).getParentFile(), ".json.gz")) {
			String name = f.getName().replace(".json.gz", "");
			if (name.matches("\\d{4}-\\d{2}-\\d{2}") && !raw.contains(LocalDate.parse(name))) {
				Files.delete(f.toPath());
				changed = true;
			}
		}
		return changed;
	}

	private void writeIndex() throws IOException {
		JSONArray sources = new JSONArray();
		for (TrafficFeed feed : feeds) {
			List<File> files = TrafficFeed.filesOf(feed.dayFile(LocalDate.now()).getParentFile(), ".json.gz");
			JSONArray days = new JSONArray();
			for (int i = files.size() - 1; i >= 0; i--) {
				if (files.get(i).getName().matches("\\d{4}-\\d{2}-\\d{2}\\.json\\.gz")) {
					JSONObject meta = new JSONObject(TrafficFeed.readText(files.get(i))).getJSONObject("meta");
					days.put(new JSONObject().put("day", meta.getString("day")).put("sensors", meta.getInt("sensors"))
							.put("events", meta.optInt("events")).put("hours", meta.getInt("hours")).put("source", meta.getString("source")));
				}
			}
			if (days.length() > 0) {
				sources.put(new JSONObject().put("id", feed.id()).put("name", feed.name()).put("zone", feed.zone().getId()).put("days", days));
			}
		}
		dir.mkdirs();
		Files.writeString(new File(dir, INDEX_FILE).toPath(), new JSONObject().put("generated", Instant.now().toString())
				.put("retentionDays", RETENTION_DAYS).put("sources", sources).toString(1));
	}

	private static Duration max(Duration a, Duration b) {
		return a.compareTo(b) >= 0 ? a : b;
	}

	private static void deleteRecursively(File f) throws IOException {
		File[] children = f.listFiles();
		if (children != null) {
			for (File c : children) {
				deleteRecursively(c);
			}
		}
		Files.deleteIfExists(f.toPath());
	}

	public static void log(String format, Object... args) {
		LOG.info(String.format(Locale.US, format, args));
	}
}
