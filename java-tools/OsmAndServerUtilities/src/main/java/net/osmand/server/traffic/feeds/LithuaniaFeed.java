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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Lithuania, eismoinfo.lt traffic intensity service (open JSON): for each station and direction the vehicles of the last
 * 15-minute interval, their average speed and a traffic type (normal, slow, heavy). Live: one snapshot per download.
 */
public class LithuaniaFeed extends TrafficFeed {

	private static final ZoneId VILNIUS = ZoneId.of("Europe/Vilnius");
	private static final String API = "https://eismoinfo.lt/traffic-intensity-service";

	@Override
	public String id() {
		return "lithuania";
	}

	@Override
	public String name() {
		return "Lithuania";
	}

	@Override
	public ZoneId zone() {
		return VILNIUS;
	}

	@Override
	public List<String> obfFiles() {
		return List.of("Lithuania_europe_2.obf");
	}

	@Override
	public double defaultSpeedKmh() {
		return 70;
	}

	@Override
	public Duration publishInterval() {
		return Duration.ofMinutes(15);
	}

	@Override
	public Duration valueWindow() {
		return Duration.ofMinutes(15);
	}

	@Override
	public void download(LocalDate today) throws Exception {
		File part = new File(staticDir(), "intensity.json.gz");
		download(API, part, true);
		JSONArray stations = new JSONArray(readText(part));
		ZonedDateTime newest = null;
		for (int i = 0; i < stations.length(); i++) {
			String date = stations.getJSONObject(i).optString("date", null);
			if (date != null && !date.isEmpty() && !"null".equals(date)) {
				ZonedDateTime t = OffsetDateTime.parse(date).atZoneSameInstant(VILNIUS);
				newest = newest == null || t.isAfter(newest) ? t : newest;
			}
		}
		ZonedDateTime time = newest != null ? newest : ZonedDateTime.now(VILNIUS);
		File target = new File(rawDir(time.toLocalDate()), time.format(DateTimeFormatter.ofPattern("HHmm")) + ".json.gz");
		target.getParentFile().mkdirs();
		Files.move(part.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
		TrafficFeeds.log("  data of %s", time.toLocalDateTime());
	}

	@Override
	public Day read(LocalDate day) throws Exception {
		Map<String, TrafficSensor> sensors = new LinkedHashMap<>();
		List<File> snapshots = snapshotFiles(day, ".json.gz");
		int lastHour = -1, values = 0;
		for (File file : snapshots) {
			JSONArray stations = new JSONArray(readText(file));
			for (int i = 0; i < stations.length(); i++) {
				JSONObject st = stations.getJSONObject(i);
				String date = st.optString("date", null);
				JSONArray segments = st.optJSONArray("roadSegments");
				if (date == null || date.isEmpty() || "null".equals(date) || segments == null) {
					continue;
				}
				int interval = st.optInt("timeInterval", 15);
				// the time is taken as the end of the counted interval
				ZonedDateTime start = OffsetDateTime.parse(date).atZoneSameInstant(VILNIUS).minusMinutes(interval);
				if (!start.toLocalDate().equals(day)) {
					continue;
				}
				for (int j = 0; j < segments.length(); j++) {
					JSONObject seg = segments.getJSONObject(j);
					boolean forward = "FORWARD".equals(seg.optString("direction"));
					String id = st.getInt("id") + (forward ? "-F" : "-B");
					double vehicles = seg.optDouble("numberOfVehicles", Double.NaN);
					int state = switch (seg.optString("trafficType")) {
						case "normal" -> TrafficSensor.STATE_FLUID;
						case "slow" -> TrafficSensor.STATE_PRE_SATURATED;
						case "heavy" -> TrafficSensor.STATE_SATURATED;
						default -> 0;
					};
					sensors.computeIfAbsent(id, k -> newSensor(k, st, seg, forward))
							.addSample(start.getHour(), vehicles * 60 / interval, Double.NaN, seg.optDouble("averageSpeed", -1), state);
					values++;
					lastHour = Math.max(lastHour, start.getHour());
				}
			}
		}
		Day result = new Day();
		for (TrafficSensor s : sensors.values()) {
			s.finishSamples(1);
			result.sensors.add(s);
		}
		result.stateSource = "city";
		result.defaultHour = lastHour >= 0 ? lastHour : null;
		result.source = String.format(Locale.US, "Lithuanian Road Administration, eismoinfo.lt traffic intensity (15-minute vehicles, speed and traffic type per direction): %d values",
				values);
		return result;
	}

	@Override
	public String estimate(Day day, LoopSpeedEstimator estimator, ObfRoadMatcher matcher) {
		long withSpeed = day.sensors.stream().filter(TrafficSensor::hasMeasuredSpeed).count();
		return String.format(Locale.US, "Speed measured on %d of %d directions; state is the service's traffic type (normal, slow, heavy).",
				withSpeed, day.sensors.size());
	}

	private static TrafficSensor newSensor(String id, JSONObject station, JSONObject segment, boolean forward) {
		TrafficSensor s = new TrafficSensor(id);
		s.name = station.optString("name") + " · road " + station.optString("roadNr") + (forward ? " · forward" : " · backward");
		s.type = station.optString("roadName");
		s.lat = new double[] {station.getDouble("x")};
		s.lon = new double[] {station.getDouble("y")};
		// segment ends are in LKS94 (EPSG:3346): x east, y north, grid north within a degree or two of true north
		double dx = segment.optDouble("endX") - segment.optDouble("startX"), dy = segment.optDouble("endY") - segment.optDouble("startY");
		if (Math.hypot(dx, dy) > 50) {
			s.heading = forward ? heading(dx, dy) : heading(-dx, -dy);
		}
		return s;
	}
}
