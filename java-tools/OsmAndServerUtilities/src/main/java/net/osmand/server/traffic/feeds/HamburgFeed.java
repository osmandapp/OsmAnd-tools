package net.osmand.server.traffic.feeds;

import net.osmand.server.traffic.LoopSpeedEstimator;
import net.osmand.server.traffic.ObfRoadMatcher;
import net.osmand.server.traffic.TrafficFeed;
import net.osmand.server.traffic.TrafficFeeds;
import net.osmand.server.traffic.TrafficSensor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

/**
 * Hamburg Urban Data Platform (iot.hamburg.de, OGC SensorThings API): hourly vehicle counts of the infrared counting
 * stations (Verkehrszählstellen), one datastream per station. The platform keeps the observations, so whole days are
 * fetched: today on every download, past days until they are complete. Volume only, no speed or occupancy.
 */
public class HamburgFeed extends TrafficFeed {

	private static final ZoneId BERLIN = ZoneId.of("Europe/Berlin");
	private static final String API = "https://iot.hamburg.de/v1.1/Datastreams";
	private static final String FILTER = "properties/layerName eq 'Anzahl_Kfz_Zaehlstelle_1-Stunde' and not substringof('veraltet',name)";
	private static final Pattern DIRECTION = Pattern.compile("(Nordost|Nordwest|Südost|Südwest|Nord|Ost|Süd|West) nach (Nordost|Nordwest|Südost|Südwest|Nord|Ost|Süd|West)");
	private static final String FILE = "counts.json.gz";

	@Override
	public String id() {
		return "hamburg";
	}

	@Override
	public String name() {
		return "Hamburg";
	}

	@Override
	public ZoneId zone() {
		return BERLIN;
	}

	@Override
	public List<String> obfFiles() {
		return List.of("Germany_hamburg_europe_2.obf", "Germany_schleswig-holstein_europe_2.obf");
	}

	@Override
	public Duration publishInterval() {
		return Duration.ofHours(1);
	}

	@Override
	public Duration valueWindow() {
		return Duration.ofHours(1);
	}

	@Override
	public boolean keepsHistory() {
		return true;
	}

	@Override
	public void download(LocalDate today) throws Exception {
		File stations = new File(staticDir(), "stations.json.gz");
		if (olderThanDays(stations, 1)) {
			JSONArray all = fetchAll(API + "?$filter=" + encode(FILTER) + "&$select=" + encode("@iot.id,name,properties")
					+ "&$expand=" + encode("Thing($select=name,properties;$expand=Locations($select=location))") + "&$top=200");
			write(stations, all.toString());
			TrafficFeeds.log("  %d counting stations", all.length());
		}
		for (int i = 0; i <= TrafficFeeds.RETENTION_DAYS; i++) {
			LocalDate day = today.minusDays(i);
			File target = new File(rawDir(day), FILE);
			// complete once downloaded 3 hours after the day ended: the hourly counts arrive about 1.5 hours late
			boolean complete = target.exists()
					&& target.lastModified() > day.plusDays(1).atStartOfDay(BERLIN).plusHours(3).toInstant().toEpochMilli();
			if (i > 0 && complete) {
				continue;
			}
			Instant from = day.atStartOfDay(BERLIN).toInstant(), to = day.plusDays(1).atStartOfDay(BERLIN).toInstant();
			String observations = "Observations($select=phenomenonTime,result;$filter=phenomenonTime ge " + from
					+ " and phenomenonTime lt " + to + ";$orderby=phenomenonTime;$top=50)";
			JSONArray all = fetchAll(API + "?$filter=" + encode(FILTER) + "&$select=" + encode("@iot.id")
					+ "&$expand=" + encode(observations) + "&$top=200");
			int count = observations(all), kept = target.exists() ? observations(new JSONArray(readText(target))) : 0;
			if (count == 0 || count < kept) {
				// never replace data by less of it
				TrafficFeeds.log("  %s: %d counts, %d kept, not replaced", day, count, kept);
				continue;
			}
			write(target, all.toString());
			TrafficFeeds.log("  %s: %d hourly counts", day, count);
		}
	}

	@Override
	public Day read(LocalDate day) throws Exception {
		Map<Integer, JSONObject> stations = new HashMap<>();
		JSONArray stationRows = new JSONArray(readText(new File(staticDir(), "stations.json.gz")));
		for (int i = 0; i < stationRows.length(); i++) {
			stations.put(stationRows.getJSONObject(i).getInt("@iot.id"), stationRows.getJSONObject(i));
		}
		JSONArray data = new JSONArray(readText(new File(rawDir(day), FILE)));
		Map<Integer, TrafficSensor> sensors = new LinkedHashMap<>();
		int counts = 0, lastHour = -1;
		for (int i = 0; i < data.length(); i++) {
			JSONObject ds = data.getJSONObject(i);
			JSONObject station = stations.get(ds.getInt("@iot.id"));
			JSONArray obs = ds.optJSONArray("Observations");
			if (station == null || obs == null) {
				continue;
			}
			for (int j = 0; j < obs.length(); j++) {
				JSONObject o = obs.getJSONObject(j);
				ZonedDateTime start = Instant.parse(o.getString("phenomenonTime").split("/")[0]).atZone(BERLIN);
				double count = o.optDouble("result", Double.NaN);
				if (!start.toLocalDate().equals(day) || !(count >= 0)) {
					continue;
				}
				sensors.computeIfAbsent(station.getInt("@iot.id"), k -> newSensor(station)).addSample(start.getHour(), count, Double.NaN, -1, 0);
				counts++;
				lastHour = Math.max(lastHour, start.getHour());
			}
		}
		Day result = new Day();
		for (TrafficSensor s : sensors.values()) {
			s.finishSamples(1);
			result.sensors.add(s);
		}
		result.stateSource = "volume";
		result.defaultHour = lastHour >= 0 ? lastHour : null;
		result.source = String.format(Locale.US, "Freie und Hansestadt Hamburg, Urban Data Platform (iot.hamburg.de), hourly vehicle counts of infrared counting stations: %d counts",
				counts);
		return result;
	}

	@Override
	public String estimate(Day day, LoopSpeedEstimator estimator, ObfRoadMatcher matcher) {
		return "Vehicle counts only: the stations measure no speed and no occupancy.";
	}

	private static TrafficSensor newSensor(JSONObject station) {
		JSONObject thing = station.getJSONObject("Thing");
		JSONObject location = thing.getJSONArray("Locations").getJSONObject(0).getJSONObject("location");
		JSONArray c = (location.has("geometry") ? location.getJSONObject("geometry") : location).getJSONArray("coordinates");
		TrafficSensor s = new TrafficSensor(String.valueOf(station.getInt("@iot.id")));
		s.name = thing.optString("name");
		s.type = "counting station " + station.getJSONObject("properties").optString("internID");
		s.lat = new double[] {c.getDouble(1)};
		s.lon = new double[] {c.getDouble(0)};
		JSONObject properties = thing.optJSONObject("properties");
		String direction = properties == null ? "" : properties.optString("richtung");
		s.from = direction;
		Matcher m = DIRECTION.matcher(direction);
		if (m.find()) {
			double[] a = compass(m.group(1)), b = compass(m.group(2));
			s.heading = heading(b[0] - a[0], b[1] - a[1]);
			// the direction is an 8-point label for the station, not the local direction of the road
			s.headingTolerance = 89;
		}
		return s;
	}

	private static double[] compass(String word) {
		String w = word.toLowerCase(Locale.ROOT);
		double x = w.endsWith("ost") ? 1 : w.endsWith("west") ? -1 : 0;
		double y = w.startsWith("nord") ? 1 : w.startsWith("süd") ? -1 : 0;
		double l = Math.hypot(x, y);
		return new double[] {x / l, y / l};
	}

	private static JSONArray fetchAll(String url) throws Exception {
		JSONArray all = new JSONArray();
		while (url != null && !url.isEmpty()) {
			JSONObject page = new JSONObject(downloadText(url));
			JSONArray values = page.getJSONArray("value");
			for (int i = 0; i < values.length(); i++) {
				all.put(values.get(i));
			}
			url = page.optString("@iot.nextLink", null);
		}
		return all;
	}

	private static int observations(JSONArray datastreams) {
		int n = 0;
		for (int i = 0; i < datastreams.length(); i++) {
			JSONArray obs = datastreams.getJSONObject(i).optJSONArray("Observations");
			n += obs == null ? 0 : obs.length();
		}
		return n;
	}

	private static void write(File target, String text) throws Exception {
		target.getParentFile().mkdirs();
		File part = new File(target.getPath() + ".part");
		try (OutputStream out = new GZIPOutputStream(new FileOutputStream(part))) {
			out.write(text.getBytes(StandardCharsets.UTF_8));
		}
		Files.move(part.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}
}
