package net.osmand.server.traffic;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Day format: everything a client shows is computed here, the client only draws it.
 * <p>
 * A day file &lt;source&gt;/&lt;yyyy-mm-dd&gt;.json.gz is {@code {meta, hours, sensors, events}}; index.json lists the sources and
 * their days (see {@link TrafficFeeds}).
 * <ul>
 * <li>meta: id, name, day, zone, source, speedNote, stateSource (occupancy | city | speed | volume), defaultHour,
 * sensors, readings, matched, measuredSpeed, hours (hours with data)</li>
 * <li>sensors[]: id, n name, a/b from/to, t type, c flat lon,lat geometry, o 1 = draw on the right-hand side,
 * q volume veh/h, k occupancy %, s state 0-4, x road 0 unknown/1 open/2 closed/3 invalid, v speed km/h,
 * vm 1 = measured speed, sb speed band 0-4, ff free-flow km/h, r OBF road [osmId, highway, maxspeed, lanes,
 * direction, share %, roads under the sensor], dup index of the first sensor with the same readings, shared count</li>
 * <li>hours: 24-slot arrays over distinct readings (duplicates skipped), top lists as sensor indexes</li>
 * <li>events[]: id, k kind, t title, s subtitle, d description, src source, p [lon, lat], g flat lon,lat line, h0/h1 first
 * and last local hour a download saw it, b 1 = road blocked, st/en start and end time when given</li>
 * </ul>
 */
public class TrafficDayFormat {

	public static final int SPEED_NONE = 0;
	public static final int SPEED_FREE = 1;      // >= 80% of free flow
	public static final int SPEED_SLOWED = 2;    // 50-80%
	public static final int SPEED_SLOW = 3;      // 25-50%
	public static final int SPEED_VERY_SLOW = 4; // < 25%

	private static final int HOURS = TrafficSensor.HOURS;
	private static final int TOP_SIZE = 8;
	private static final double TOP_MIN_FLOW = 100;
	private static final int MIN_DEDUP_HOURS = 6;

	public static int speedBand(double speed, double freeFlow) {
		if (Double.isNaN(speed) || !(freeFlow > 0)) {
			return SPEED_NONE;
		}
		double r = speed / freeFlow;
		return r >= 0.8 ? SPEED_FREE : r >= 0.5 ? SPEED_SLOWED : r >= 0.25 ? SPEED_SLOW : SPEED_VERY_SLOW;
	}

	public static void write(File out, JSONObject meta, List<TrafficSensor> sensors, List<TrafficEvent> events) throws IOException {
		int n = sensors.size();
		int[] dup = new int[n];
		int[] shared = new int[n];
		Map<String, Integer> first = new HashMap<>();
		for (int i = 0; i < n; i++) {
			TrafficSensor s = sensors.get(i);
			dup[i] = -1;
			if (!dedupEligible(s)) {
				continue;
			}
			Integer f = first.putIfAbsent(Arrays.toString(s.flow) + Arrays.toString(s.occupancy), i);
			if (f != null) {
				dup[i] = f;
				shared[f] = (shared[f] == 0 ? 1 : shared[f]) + 1;
			}
		}
		List<Integer> distinct = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			if (dup[i] < 0 && hasData(sensors.get(i))) {
				distinct.add(i);
			}
		}

		JSONObject hours = new JSONObject();
		// volume profile only over readings present in every hour, so sensors dropping in and out do not shape it
		long[] volume = new long[HOURS];
		int panel = 0;
		for (int i : distinct) {
			TrafficSensor s = sensors.get(i);
			if (Arrays.stream(s.flow).noneMatch(Double::isNaN)) {
				panel++;
				for (int h = 0; h < HOURS; h++) {
					volume[h] += Math.round(s.flow[h]);
				}
			}
		}
		int[][] states = new int[5][HOURS];
		int[] withOccupancy = new int[HOURS], volumeReadings = new int[HOURS], speedReadings = new int[HOURS],
				speedMeasured = new int[HOURS], closed = new int[HOURS], invalid = new int[HOURS];
		long[] volumeSum = new long[HOURS];
		for (TrafficSensor s : sensors) {
			for (int h = 0; h < HOURS; h++) {
				closed[h] += s.roadState[h] == 2 ? 1 : 0;
				invalid[h] += s.roadState[h] == 3 ? 1 : 0;
			}
		}
		for (int i : distinct) {
			TrafficSensor s = sensors.get(i);
			for (int h = 0; h < HOURS; h++) {
				states[s.state[h]][h]++;
				withOccupancy[h] += Double.isNaN(s.occupancy[h]) ? 0 : 1;
				if (!Double.isNaN(s.flow[h])) {
					volumeReadings[h]++;
					volumeSum[h] += Math.round(s.flow[h]);
				}
				if (!Double.isNaN(s.speed(h))) {
					speedReadings[h]++;
					speedMeasured[h] += s.measuredSpeed[h] > 0 ? 1 : 0;
				}
			}
		}
		// a partial day (live snapshot) has no reading present in every hour: sum what each hour has
		boolean allReadings = panel == 0;
		if (allReadings) {
			volume = volumeSum;
			panel = Arrays.stream(volumeReadings).max().orElse(0);
		}
		int volumeHours = (int) Arrays.stream(volume).filter(v -> v > 0).count();
		hours.put("volumePanel", panel).put("volume", new JSONArray(volume)).put("volumeAllReadings", allReadings).put("volumeHours", volumeHours);
		hours.put("volumePeak", argBest(volume, true)).put("volumeLow", argBest(volume, false));
		hours.put("congestion", new JSONObject().put("preSaturated", new JSONArray(states[2])).put("saturated", new JSONArray(states[3]))
				.put("blocked", new JSONArray(states[4])).put("withOccupancy", new JSONArray(withOccupancy)));
		hours.put("stats", new JSONObject().put("volume", new JSONArray(volumeReadings)).put("occupancy", new JSONArray(withOccupancy))
				.put("volumeSum", new JSONArray(volumeSum)).put("speed", new JSONArray(speedReadings)).put("speedMeasured", new JSONArray(speedMeasured))
				.put("closed", new JSONArray(closed)).put("invalid", new JSONArray(invalid)));
		hours.put("speed", new JSONObject().put("all", speedProfile(sensors, distinct, false)).put("measured", speedProfile(sensors, distinct, true)));

		JSONArray topCongestion = new JSONArray(), topSpeed = new JSONArray(), topSpeedMeasured = new JSONArray();
		for (int h = 0; h < HOURS; h++) {
			final int hour = h;
			topCongestion.put(top(distinct, i -> sensors.get(i).state[hour] >= TrafficSensor.STATE_SATURATED,
					Comparator.comparingDouble(i -> -sensors.get(i).occupancy[hour])));
			Comparator<Integer> slowest = Comparator.comparingDouble(i -> sensors.get(i).speed(hour) / sensors.get(i).freeFlowSpeed);
			topSpeed.put(top(distinct, i -> hasSpeed(sensors.get(i), hour) && sensors.get(i).flow[hour] >= TOP_MIN_FLOW, slowest));
			topSpeedMeasured.put(top(distinct, i -> sensors.get(i).measuredSpeed[hour] > 0 && sensors.get(i).flow[hour] >= TOP_MIN_FLOW, slowest));
		}
		hours.put("top", new JSONObject().put("congestion", topCongestion).put("speed", topSpeed).put("speedMeasured", topSpeedMeasured)
				.put("minFlow", TOP_MIN_FLOW));

		JSONArray arr = new JSONArray();
		int matched = 0, measured = 0;
		for (int i = 0; i < n; i++) {
			TrafficSensor s = sensors.get(i);
			matched += s.match != null ? 1 : 0;
			measured += s.hasMeasuredSpeed() ? 1 : 0;
			arr.put(sensorJson(s, dup[i], shared[i]));
		}
		int hoursWithData = 0;
		for (int h = 0; h < HOURS; h++) {
			final int hour = h;
			boolean eventsSeen = events.stream().anyMatch(e -> e.firstHour <= hour && hour <= e.lastHour);
			hoursWithData += volumeReadings[h] > 0 || withOccupancy[h] > 0 || speedReadings[h] > 0 || eventsSeen ? 1 : 0;
		}
		meta.put("sensors", n).put("readings", distinct.size()).put("matched", matched).put("measuredSpeed", measured).put("hours", hoursWithData).put("events", events.size());
		JSONArray eventsJson = new JSONArray();
		for (TrafficEvent e : events) {
			eventsJson.put(e.toJson());
		}

		TrafficFeed.writeGz(out, new JSONObject().put("meta", meta).put("hours", hours).put("sensors", arr).put("events", eventsJson).toString());
	}

	private static JSONObject sensorJson(TrafficSensor s, int dup, int shared) {
		JSONObject a = new JSONObject();
		a.put("id", s.id).put("n", s.name).put("a", s.from).put("b", s.to);
		if (!s.type.isEmpty()) {
			a.put("t", s.type);
		}
		JSONArray c = new JSONArray();
		for (int i = 0; i < s.lat.length; i++) {
			c.put(round(s.lon[i], 5)).put(round(s.lat[i], 5));
		}
		a.put("c", c).put("o", s.directional ? 1 : 0);
		JSONArray q = new JSONArray(), k = new JSONArray(), v = new JSONArray(), st = new JSONArray(), x = new JSONArray(), sb = new JSONArray();
		for (int h = 0; h < HOURS; h++) {
			q.put(Double.isNaN(s.flow[h]) ? JSONObject.NULL : (Object) Math.round(s.flow[h]));
			k.put(Double.isNaN(s.occupancy[h]) ? JSONObject.NULL : (Object) round(s.occupancy[h], 1));
			double speed = s.speed(h);
			v.put(Double.isNaN(speed) ? JSONObject.NULL : (Object) Math.round(speed));
			sb.put(speedBand(speed, s.freeFlowSpeed));
			st.put(s.state[h]);
			x.put(s.roadState[h]);
		}
		a.put("q", q).put("k", k).put("s", st).put("x", x).put("v", v).put("sb", sb);
		a.put("vm", s.hasMeasuredSpeed() ? 1 : 0).put("ff", Math.round(s.freeFlowSpeed));
		if (s.match != null) {
			int lanes = s.match.road.getLanes();
			double maxSpeed = s.match.getMaxSpeedKmh();
			a.put("r", new JSONArray().put(s.match.getOsmId()).put(s.match.road.getHighway())
					.put(Double.isNaN(maxSpeed) ? JSONObject.NULL : (Object) Math.round(maxSpeed))
					.put(lanes > 0 ? (Object) lanes : JSONObject.NULL).put(s.match.direction)
					.put(Math.round(s.match.share * 100)).put(s.match.osmIds.size()));
		}
		if (dup >= 0) {
			a.put("dup", dup);
		}
		if (shared > 0) {
			a.put("shared", shared);
		}
		return a;
	}

	private static JSONObject speedProfile(List<TrafficSensor> sensors, List<Integer> distinct, boolean onlyMeasured) {
		JSONArray pct = new JSONArray(), kmh = new JSONArray(), count = new JSONArray();
		for (int h = 0; h < HOURS; h++) {
			List<Double> ratio = new ArrayList<>(), speed = new ArrayList<>();
			for (int i : distinct) {
				TrafficSensor s = sensors.get(i);
				double v = onlyMeasured ? s.measuredSpeed[h] : s.speed(h);
				if (v > 0 && s.freeFlowSpeed > 0) {
					ratio.add(100 * v / s.freeFlowSpeed);
					speed.add(v);
				}
			}
			pct.put(ratio.isEmpty() ? JSONObject.NULL : (Object) Math.round(LoopSpeedEstimator.median(ratio)));
			kmh.put(speed.isEmpty() ? JSONObject.NULL : (Object) Math.round(LoopSpeedEstimator.median(speed)));
			count.put(ratio.size());
		}
		return new JSONObject().put("pct", pct).put("kmh", kmh).put("n", count);
	}

	private static JSONArray top(List<Integer> distinct, java.util.function.IntPredicate filter, Comparator<Integer> order) {
		return new JSONArray(distinct.stream().filter(filter::test).sorted(order).limit(TOP_SIZE).toList());
	}

	private static boolean hasSpeed(TrafficSensor s, int h) {
		return !Double.isNaN(s.speed(h)) && s.freeFlowSpeed > 0;
	}

	// Identical series are one reading published twice only when they are long and vary; a one-hour snapshot
	// (Madrid live 11 Sep 2026) made 4,491 detectors look like 1,440 readings.
	private static boolean dedupEligible(TrafficSensor s) {
		int hours = 0;
		boolean varies = false;
		double first = Double.NaN;
		for (double q : s.flow) {
			if (!Double.isNaN(q)) {
				if (hours++ == 0) {
					first = q;
				} else if (q != first) {
					varies = true;
				}
			}
		}
		return hours >= MIN_DEDUP_HOURS && varies;
	}

	private static boolean hasData(TrafficSensor s) {
		for (int h = 0; h < HOURS; h++) {
			// speed alone counts too: a speed-only source would otherwise get empty charts and lists
			if (!Double.isNaN(s.flow[h]) || !Double.isNaN(s.occupancy[h]) || !Double.isNaN(s.speed(h))) {
				return true;
			}
		}
		return false;
	}

	private static int argBest(long[] values, boolean max) {
		int best = -1;
		for (int i = 0; i < values.length; i++) {
			if (values[i] > 0 && (best < 0 || (max ? values[i] > values[best] : values[i] < values[best]))) {
				best = i;
			}
		}
		return Math.max(best, 0);
	}

	private static double round(double v, int digits) {
		double m = Math.pow(10, digits);
		return Math.round(v * m) / m;
	}
}
