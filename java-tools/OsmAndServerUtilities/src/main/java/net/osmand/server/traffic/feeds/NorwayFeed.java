package net.osmand.server.traffic.feeds;

import net.osmand.server.traffic.LoopSpeedEstimator;
import net.osmand.server.traffic.ObfRoadMatcher;
import net.osmand.server.traffic.TrafficFeed;
import net.osmand.server.traffic.TrafficFeeds;
import net.osmand.server.traffic.TrafficSensor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Norway, Statens vegvesen Trafikkdata (trafikkdata-api.atlas.vegvesen.no, open GraphQL): hourly vehicle volumes of the
 * counting points in Oslo and Akershus. The API keeps history, so whole days are fetched. Volume only.
 */
public class NorwayFeed extends TrafficFeed {

	private static final ZoneId OSLO = ZoneId.of("Europe/Oslo");
	private static final String API = "https://trafikkdata-api.atlas.vegvesen.no/";
	private static final Set<String> COUNTIES = Set.of("Oslo", "Akershus");
	private static final int BATCH = 50;
	private static final double MIN_COVERAGE = 50;
	private static final DateTimeFormatter GRAPHQL_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");
	private static final String FILE = "volumes.json.gz";

	@Override
	public String id() {
		return "norway";
	}

	@Override
	public String name() {
		return "Norway · Oslo and Akershus";
	}

	@Override
	public ZoneId zone() {
		return OSLO;
	}

	@Override
	public List<String> obfFiles() {
		return List.of("Norway_oslo_europe_2.obf", "Norway_akershus_europe_2.obf");
	}

	@Override
	public double defaultSpeedKmh() {
		return 60;
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
		File pointsFile = new File(staticDir(), "points.json.gz");
		if (olderThanDays(pointsFile, 1)) {
			JSONObject data = graphql("{ trafficRegistrationPoints(searchQuery: {isOperational: true, trafficType: VEHICLE}) "
					+ "{ id name location { county { name } coordinates { latLon { lat lon } } roadReference { shortForm } } direction { from to } } }");
			JSONArray all = data.getJSONArray("trafficRegistrationPoints"), points = new JSONArray();
			for (int i = 0; i < all.length(); i++) {
				JSONObject p = all.getJSONObject(i);
				if (COUNTIES.contains(p.getJSONObject("location").getJSONObject("county").optString("name"))) {
					points.put(p);
				}
			}
			writeGz(pointsFile, points.toString());
			TrafficFeeds.log("  %d counting points in %s", points.length(), COUNTIES);
		}
		JSONArray points = new JSONArray(readText(pointsFile));
		for (int i = 0; i <= TrafficFeeds.RETENTION_DAYS; i++) {
			LocalDate day = today.minusDays(i);
			File target = new File(rawDir(day), FILE);
			// complete once downloaded 6 hours after the day ended: hourly volumes arrive a few hours late
			boolean complete = target.exists() && target.lastModified() > day.plusDays(1).atStartOfDay(OSLO).plusHours(6).toInstant().toEpochMilli();
			if (i > 0 && complete) {
				continue;
			}
			String from = GRAPHQL_TIME.format(day.atStartOfDay(OSLO)), to = GRAPHQL_TIME.format(day.plusDays(1).atStartOfDay(OSLO));
			JSONObject volumes = new JSONObject();
			int count = 0;
			for (int start = 0; start < points.length(); start += BATCH) {
				StringBuilder query = new StringBuilder("{");
				for (int j = start; j < Math.min(points.length(), start + BATCH); j++) {
					query.append(" p").append(j).append(": trafficData(trafficRegistrationPointId: \"").append(points.getJSONObject(j).getString("id"))
							.append("\") { volume { byHour(from: \"").append(from).append("\", to: \"").append(to)
							.append("\") { edges { node { from total { volumeNumbers { volume } coverage { percentage } } } } } } }");
				}
				JSONObject data = graphql(query.append(" }").toString());
				for (int j = start; j < Math.min(points.length(), start + BATCH); j++) {
					JSONObject td = data.optJSONObject("p" + j);
					JSONArray edges = td == null ? null : td.getJSONObject("volume").getJSONObject("byHour").getJSONArray("edges");
					if (edges != null && edges.length() > 0) {
						volumes.put(points.getJSONObject(j).getString("id"), edges);
						count += edges.length();
					}
				}
			}
			int kept = target.exists() ? countEdges(new JSONObject(readText(target))) : 0;
			if (count == 0 || count < kept) {
				TrafficFeeds.log("  %s: %d hourly volumes, %d kept, not replaced", day, count, kept);
				continue;
			}
			writeGz(target, volumes.toString());
			TrafficFeeds.log("  %s: %d hourly volumes", day, count);
		}
	}

	private static JSONObject graphql(String query) throws Exception {
		JSONObject response = new JSONObject(postJson(API, new JSONObject().put("query", query).toString()));
		if (!response.has("data") || response.isNull("data")) {
			throw new java.io.IOException("GraphQL error: " + response.optJSONArray("errors"));
		}
		return response.getJSONObject("data");
	}

	private static int countEdges(JSONObject volumes) {
		int n = 0;
		for (String id : volumes.keySet()) {
			n += volumes.getJSONArray(id).length();
		}
		return n;
	}

	@Override
	public Day read(LocalDate day) throws Exception {
		Map<String, JSONObject> points = new HashMap<>();
		JSONArray pointRows = new JSONArray(readText(new File(staticDir(), "points.json.gz")));
		for (int i = 0; i < pointRows.length(); i++) {
			points.put(pointRows.getJSONObject(i).getString("id"), pointRows.getJSONObject(i));
		}
		JSONObject volumes = new JSONObject(readText(new File(rawDir(day), FILE)));
		Map<String, TrafficSensor> sensors = new LinkedHashMap<>();
		int count = 0, lastHour = -1;
		for (String id : volumes.keySet()) {
			JSONObject point = points.get(id);
			if (point == null) {
				continue;
			}
			JSONArray edges = volumes.getJSONArray(id);
			for (int i = 0; i < edges.length(); i++) {
				JSONObject node = edges.getJSONObject(i).getJSONObject("node");
				ZonedDateTime from = OffsetDateTime.parse(node.getString("from")).atZoneSameInstant(OSLO);
				JSONObject total = node.optJSONObject("total");
				JSONObject numbers = total == null ? null : total.optJSONObject("volumeNumbers");
				JSONObject coverage = total == null ? null : total.optJSONObject("coverage");
				if (numbers == null || !from.toLocalDate().equals(day) || (coverage != null && coverage.optDouble("percentage", 0) < MIN_COVERAGE)) {
					continue;
				}
				sensors.computeIfAbsent(id, k -> newSensor(point)).addSample(from.getHour(), numbers.optDouble("volume", Double.NaN), Double.NaN, -1, 0);
				count++;
				lastHour = Math.max(lastHour, from.getHour());
			}
		}
		Day result = new Day();
		for (TrafficSensor s : sensors.values()) {
			s.finishSamples(1);
			result.sensors.add(s);
		}
		result.stateSource = "volume";
		result.defaultHour = lastHour >= 0 ? lastHour : null;
		result.source = String.format(Locale.US, "Statens vegvesen Trafikkdata (trafikkdata.no), hourly volumes of counting points in Oslo and Akershus: %d volumes", count);
		return result;
	}

	@Override
	public String estimate(Day day, LoopSpeedEstimator estimator, ObfRoadMatcher matcher) {
		return "Vehicle volumes only (both directions together): the API gives no speed.";
	}

	private static TrafficSensor newSensor(JSONObject point) {
		JSONObject location = point.getJSONObject("location");
		JSONObject latLon = location.getJSONObject("coordinates").getJSONObject("latLon");
		TrafficSensor s = new TrafficSensor(point.getString("id"));
		JSONObject road = location.optJSONObject("roadReference");
		s.name = point.optString("name") + (road == null ? "" : " · " + road.optString("shortForm"));
		JSONObject direction = point.optJSONObject("direction");
		if (direction != null) {
			s.from = direction.optString("from");
			s.to = direction.optString("to");
		}
		s.type = location.getJSONObject("county").optString("name");
		s.lat = new double[] {latLon.getDouble("lat")};
		s.lon = new double[] {latLon.getDouble("lon")};
		return s;
	}
}
