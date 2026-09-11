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
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Finland, Fintraffic Digitraffic TMS (tie.digitraffic.fi, open JSON): counting stations with fixed 60-minute
 * vehicle counts and mean speeds per direction, so one download per hour covers each hour. Stations in Uusimaa.
 */
public class FinlandFeed extends TrafficFeed {

	private static final ZoneId HELSINKI = ZoneId.of("Europe/Helsinki");
	private static final String STATIONS = "https://tie.digitraffic.fi/api/tms/v1/stations";
	private static final String DATA = "https://tie.digitraffic.fi/api/tms/v1/stations/data";
	private static final double TOP = 60.8, BOTTOM = 59.8, LEFT = 23.3, RIGHT = 26.6; // Uusimaa
	private static final String COUNT = "OHITUKSET_60MIN_KIINTEA_SUUNTA";
	private static final String SPEED = "KESKINOPEUS_60MIN_KIINTEA_SUUNTA";

	@Override
	public String id() {
		return "finland";
	}

	@Override
	public String name() {
		return "Finland · Uusimaa";
	}

	@Override
	public ZoneId zone() {
		return HELSINKI;
	}

	@Override
	public List<String> obfFiles() {
		return List.of("Finland_uusimaa_europe_2.obf");
	}

	@Override
	public double defaultSpeedKmh() {
		return 80;
	}

	@Override
	public Duration publishInterval() {
		return Duration.ofMinutes(1);
	}

	@Override
	public Duration valueWindow() {
		return Duration.ofHours(1);
	}

	@Override
	public void download(LocalDate today) throws Exception {
		File stations = new File(staticDir(), "stations.json.gz");
		if (olderThanDays(stations, 1)) {
			download(STATIONS, stations, true);
		}
		File part = new File(staticDir(), "data.json.gz");
		download(DATA, part, true);
		ZonedDateTime time = Instant.parse(new JSONObject(readText(part)).getString("dataUpdatedTime")).atZone(HELSINKI);
		File target = new File(rawDir(time.toLocalDate()), time.format(DateTimeFormatter.ofPattern("HHmm")) + ".json.gz");
		target.getParentFile().mkdirs();
		Files.move(part.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
		TrafficFeeds.log("  data of %s", time.toLocalDateTime());
	}

	@Override
	public Day read(LocalDate day) throws Exception {
		Map<Integer, JSONObject> stations = new HashMap<>();
		JSONArray features = new JSONObject(readText(new File(staticDir(), "stations.json.gz"))).getJSONArray("features");
		for (int i = 0; i < features.length(); i++) {
			JSONObject f = features.getJSONObject(i);
			JSONObject p = f.getJSONObject("properties");
			JSONArray c = f.getJSONObject("geometry").getJSONArray("coordinates");
			double lon = c.getDouble(0), lat = c.getDouble(1);
			if ("GATHERING".equals(p.optString("collectionStatus")) && !p.isNull("bearing")
					&& lat >= BOTTOM && lat <= TOP && lon >= LEFT && lon <= RIGHT) {
				stations.put(p.getInt("id"), f);
			}
		}
		Map<String, TrafficSensor> sensors = new LinkedHashMap<>();
		List<File> snapshots = snapshotFiles(day, ".json.gz");
		Day result = new Day();
		int lastHour = -1;
		for (File file : snapshots) {
			JSONArray data = new JSONObject(readText(file)).getJSONArray("stations");
			Map<String, double[]> values = new HashMap<>(); // sensor id + hour -> count, speed
			for (int i = 0; i < data.length(); i++) {
				JSONObject st = data.getJSONObject(i);
				JSONObject station = stations.get(st.getInt("id"));
				JSONArray sv = st.optJSONArray("sensorValues");
				if (station == null || sv == null) {
					continue;
				}
				for (int j = 0; j < sv.length(); j++) {
					JSONObject v = sv.getJSONObject(j);
					String name = v.getString("name");
					boolean count = name.startsWith(COUNT), speed = name.startsWith(SPEED);
					if (!count && !speed) {
						continue;
					}
					int direction = name.endsWith("1") ? 1 : 2;
					ZonedDateTime start = Instant.parse(v.getString("timeWindowStart")).atZone(HELSINKI);
					if (!start.toLocalDate().equals(day)) {
						continue;
					}
					String id = st.getInt("id") + "-" + direction;
					sensors.computeIfAbsent(id, k -> newSensor(k, station, direction));
					double[] slot = values.computeIfAbsent(id + "@" + start.getHour(), k -> new double[] {Double.NaN, -1});
					slot[count ? 0 : 1] = v.getDouble("value");
					lastHour = Math.max(lastHour, start.getHour());
				}
			}
			for (Map.Entry<String, double[]> e : values.entrySet()) {
				String[] key = e.getKey().split("@");
				sensors.get(key[0]).addSample(Integer.parseInt(key[1]), e.getValue()[0], Double.NaN, e.getValue()[1], 0);
			}
		}
		for (TrafficSensor s : sensors.values()) {
			s.finishSamples(1);
			result.sensors.add(s);
		}
		result.stateSource = "speed";
		result.defaultHour = lastHour >= 0 ? lastHour : null;
		result.source = String.format(Locale.US, "Fintraffic Digitraffic TMS (tie.digitraffic.fi), fixed 60-minute counts and mean speeds per direction, stations in Uusimaa: %d downloads",
				snapshots.size());
		return result;
	}

	@Override
	public String estimate(Day day, LoopSpeedEstimator estimator, ObfRoadMatcher matcher) {
		int withSpeed = statesFromMeasuredSpeed(day);
		return String.format(Locale.US, "Speed measured (hourly mean per direction) on %d of %d directions; state is the speed as a share of the OBF max speed.",
				withSpeed, day.sensors.size());
	}

	private static TrafficSensor newSensor(String id, JSONObject station, int direction) {
		JSONObject p = station.getJSONObject("properties");
		JSONArray c = station.getJSONObject("geometry").getJSONArray("coordinates");
		TrafficSensor s = new TrafficSensor(id);
		s.name = p.optString("name") + (direction == 1 ? " · direction 1" : " · direction 2");
		s.type = "TMS station " + p.optInt("tmsNumber");
		s.lat = new double[] {c.getDouble(1)};
		s.lon = new double[] {c.getDouble(0)};
		// bearing is the direction 1 heading of the station
		s.heading = (p.getDouble("bearing") + (direction == 1 ? 0 : 180)) % 360;
		return s;
	}
}
