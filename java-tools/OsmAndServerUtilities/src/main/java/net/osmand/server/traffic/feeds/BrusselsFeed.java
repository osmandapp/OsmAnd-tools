package net.osmand.server.traffic.feeds;

import net.osmand.server.traffic.LoopSpeedEstimator;
import net.osmand.server.traffic.ObfRoadMatcher;
import net.osmand.server.traffic.TrafficFeed;
import net.osmand.server.traffic.TrafficFeeds;
import net.osmand.server.traffic.TrafficSensor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Brussels Mobility traffic counts (data.mobility.brussels/traffic/api/counts): live count, speed and occupancy per
 * counting traverse (a detector across all lanes of one direction), with its location and orientation.
 */
public class BrusselsFeed extends TrafficFeed {

	private static final ZoneId BRUSSELS = ZoneId.of("Europe/Brussels");
	private static final String API = "https://data.mobility.brussels/traffic/api/counts/?outputFormat=json&request=";
	private static final DateTimeFormatter REQUEST_TIME = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
	private static final DateTimeFormatter WINDOW_TIME = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm");
	private static final String[] WINDOWS = {"60m", "15m", "5m", "1m"}; // the longest window that has values
	private static final int[] WINDOW_MINUTES = {60, 15, 5, 1};

	@Override
	public String id() {
		return "brussels";
	}

	@Override
	public String name() {
		return "Brussels";
	}

	@Override
	public ZoneId zone() {
		return BRUSSELS;
	}

	@Override
	public List<String> obfFiles() {
		return List.of("Belgium_flanders_europe_2.obf", "Belgium_wallonia_europe_2.obf");
	}

	@Override
	public Duration publishInterval() {
		return Duration.ofMinutes(1);
	}

	// the 1-minute values are the ones filled in; longer windows are used when they have values
	@Override
	public Duration valueWindow() {
		return Duration.ofMinutes(1);
	}

	@Override
	public void download(LocalDate today) throws Exception {
		File devices = new File(staticDir(), "devices.json.gz");
		if (olderThanDays(devices, 7)) {
			download(API + "devices", devices, true);
		}
		File part = new File(staticDir(), "live.json.gz");
		download(API + "live", part, true);
		LocalDateTime time = LocalDateTime.parse(new JSONObject(readText(part)).getString("requestDate"), REQUEST_TIME);
		File target = new File(rawDir(time.toLocalDate()), time.format(DateTimeFormatter.ofPattern("HHmm")) + ".json.gz");
		target.getParentFile().mkdirs();
		Files.move(part.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
		TrafficFeeds.log("  snapshot %s", time);
	}

	@Override
	public Day read(LocalDate day) throws Exception {
		Map<String, JSONObject> devices = new HashMap<>();
		JSONArray features = new JSONObject(readText(new File(staticDir(), "devices.json.gz"))).getJSONArray("features");
		for (int i = 0; i < features.length(); i++) {
			JSONObject f = features.getJSONObject(i);
			devices.put(f.getJSONObject("properties").getString("traverse_name"), f);
		}
		Map<String, TrafficSensor> sensors = new LinkedHashMap<>();
		List<File> snapshots = snapshotFiles(day, ".json.gz");
		Day result = new Day();
		int lastHour = -1;
		for (File file : snapshots) {
			JSONObject data = new JSONObject(readText(file)).getJSONObject("data");
			for (String key : data.keySet()) {
				JSONObject device = devices.get(key);
				JSONObject results = data.getJSONObject(key).optJSONObject("results");
				if (device == null || results == null) {
					continue;
				}
				for (int w = 0; w < WINDOWS.length; w++) {
					JSONObject window = results.optJSONObject(WINDOWS[w]);
					JSONObject t1 = window == null ? null : window.optJSONObject("t1");
					if (t1 == null || t1.isNull("count")) {
						continue;
					}
					LocalDateTime start = LocalDateTime.parse(t1.getString("start_time"), WINDOW_TIME);
					if (start.toLocalDate().equals(day)) {
						sensors.computeIfAbsent(key, k -> newSensor(k, device)).addSample(start.getHour(),
								t1.getDouble("count") * 60 / WINDOW_MINUTES[w], t1.optDouble("occupancy", Double.NaN), t1.optDouble("speed", -1), 0);
						lastHour = Math.max(lastHour, start.getHour());
					}
					break;
				}
			}
		}
		for (TrafficSensor s : sensors.values()) {
			s.finishSamples(1);
			result.sensors.add(s);
		}
		result.defaultHour = lastHour >= 0 ? lastHour : null;
		result.source = String.format(Locale.US, "Brussels Mobility traffic counts (data.mobility.brussels), live count, speed and occupancy per traverse: %d snapshot files",
				snapshots.size());
		return result;
	}

	@Override
	public String estimate(Day day, LoopSpeedEstimator estimator, ObfRoadMatcher matcher) {
		long withSpeed = day.sensors.stream().filter(TrafficSensor::hasMeasuredSpeed).count();
		return String.format(Locale.US, "Speed measured by the detectors on %d of %d traverses; state from occupancy bands.", withSpeed, day.sensors.size());
	}

	private static TrafficSensor newSensor(String key, JSONObject device) {
		JSONObject p = device.getJSONObject("properties");
		JSONArray c = device.getJSONObject("geometry").getJSONArray("coordinates");
		TrafficSensor s = new TrafficSensor(key);
		s.name = key + " · " + p.optString("descr_en");
		s.type = p.optInt("number_of_lanes") + " lanes";
		s.lat = new double[] {c.getDouble(1)};
		s.lon = new double[] {c.getDouble(0)};
		s.heading = p.isNull("orientation") ? Double.NaN : p.getDouble("orientation");
		return s;
	}
}
