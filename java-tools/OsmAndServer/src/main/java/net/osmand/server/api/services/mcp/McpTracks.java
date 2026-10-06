package net.osmand.server.api.services.mcp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.shared.util.KMapUtils;
import net.osmand.util.MapUtils;

/**
 * Track summaries for MCP clients: a long recording does not fit into a model's context,
 * so the server computes what the assistant asks about (speed, stops, profile) and returns points in slices.
 */
public class McpTracks {

	public static final int MAX_PROFILE = 500;
	public static final int MAX_POINTS = 500;
	private static final int MAX_STOPS = 100;
	// speed over at least this time, single GPS jumps would give absurd maxima
	private static final long SPEED_WINDOW_MS = 10_000;
	private static final long SPEED_WINDOW_MAX_MS = 60_000;
	private static final double STOP_RADIUS_M = 50;

	private final List<WptPt> pts;
	private final double[] dist; // cumulative distance, m
	private final boolean timed;

	public McpTracks(GpxFile gpx) {
		pts = new ArrayList<>();
		List<Integer> segmentStarts = new ArrayList<>();
		for (Track t : gpx.getTracks()) {
			if (t.isGeneralTrack()) {
				continue;
			}
			for (TrkSegment seg : t.getSegments()) {
				if (!seg.isGeneralSegment() && !seg.getPoints().isEmpty()) {
					segmentStarts.add(pts.size());
					pts.addAll(seg.getPoints());
				}
			}
		}
		// no distance between segments, as in GpxTrackAnalysis
		dist = new double[pts.size()];
		for (int i = 1, s = 1; i < pts.size(); i++) {
			boolean newSegment = s < segmentStarts.size() && segmentStarts.get(s) == i;
			if (newSegment) {
				s++;
				dist[i] = dist[i - 1];
				continue;
			}
			WptPt a = pts.get(i - 1), b = pts.get(i);
			// ellipsoid like GpxTrackAnalysis, so the profile adds up to its distance
			dist[i] = dist[i - 1] + KMapUtils.INSTANCE.getEllipsoidDistance(a.getLat(), a.getLon(), b.getLat(), b.getLon());
		}
		timed = pts.size() > 1 && pts.get(0).getTime() > 0 && pts.get(pts.size() - 1).getTime() > pts.get(0).getTime();
	}

	public Map<String, Object> analyze(GpxFile gpx, int profileSize, double stopMinutes) {
		GpxTrackAnalysis a = gpx.getAnalysis(0, null, null, null, false);
		Map<String, Object> res = new LinkedHashMap<>();
		Map<String, Object> s = new LinkedHashMap<>();
		s.put("points", pts.size());
		s.put("waypoints", a.getWptPoints());
		s.put("distanceKm", round(a.getTotalDistance() / 1000.0, 3));
		if (timed) {
			s.put("start", iso(a.getStartTime()));
			s.put("end", iso(a.getEndTime()));
			s.put("durationMin", round(a.getTimeSpan() / 60000.0, 1));
			s.put("movingMin", round(a.getTimeMoving() / 60000.0, 1));
			s.put("avgSpeedKmh", round(a.getAvgSpeed() * 3.6, 1));
			s.put("speedRecorded", a.getHasSpeedInTrack());
			if (a.getHasSpeedInTrack()) {
				s.put("maxRecordedSpeedKmh", round(a.getMaxSpeed() * 3.6, 1));
			}
			Map<String, Object> max = maxSpeed();
			if (max != null) {
				s.put("maxSpeed10s", max);
			}
		}
		if (!Double.isNaN(a.getMinElevation()) && a.getMaxElevation() >= a.getMinElevation()) {
			s.put("elevationMinM", round(a.getMinElevation(), 0));
			s.put("elevationMaxM", round(a.getMaxElevation(), 0));
			s.put("ascentM", round(a.getDiffElevationUp(), 0));
			s.put("descentM", round(a.getDiffElevationDown(), 0));
		}
		res.put("summary", s);
		if (timed) {
			res.put("stops", stops((long) (stopMinutes * 60000)));
		}
		res.put("profile", profile(profileSize));
		return res;
	}

	// fastest stretch of at least SPEED_WINDOW_MS (not across recording gaps)
	private Map<String, Object> maxSpeed() {
		double best = 0;
		int bi = -1, bj = -1;
		int j = 0;
		for (int i = 0; i < pts.size(); i++) {
			long ti = pts.get(i).getTime();
			if (j < i) {
				j = i;
			}
			while (j < pts.size() - 1 && pts.get(j).getTime() - ti < SPEED_WINDOW_MS) {
				j++;
			}
			long dt = pts.get(j).getTime() - ti;
			if (dt >= SPEED_WINDOW_MS && dt <= SPEED_WINDOW_MAX_MS) {
				double v = (dist[j] - dist[i]) / (dt / 1000.0);
				if (v > best) {
					best = v;
					bi = i;
					bj = j;
				}
			}
		}
		if (bi < 0) {
			return null;
		}
		Map<String, Object> m = point(bi);
		m.put("kmh", round(best * 3.6, 1));
		m.put("seconds", (pts.get(bj).getTime() - pts.get(bi).getTime()) / 1000);
		return m;
	}

	// staying within STOP_RADIUS_M for at least minMs, or a recording gap of at least minMs
	private List<Map<String, Object>> stops(long minMs) {
		List<Map<String, Object>> res = new ArrayList<>();
		int i = 0;
		while (i < pts.size() - 1 && res.size() < MAX_STOPS) {
			WptPt a = pts.get(i);
			int j = i + 1;
			while (j < pts.size() && MapUtils.getDistance(a.getLat(), a.getLon(), pts.get(j).getLat(),
					pts.get(j).getLon()) <= STOP_RADIUS_M) {
				j++;
			}
			int last = j - 1;
			long stayed = pts.get(last).getTime() - a.getTime();
			long gap = j < pts.size() ? pts.get(j).getTime() - pts.get(last).getTime() : 0;
			if (stayed + gap >= minMs) {
				Map<String, Object> m = point(i);
				m.put("until", iso(j < pts.size() ? pts.get(j).getTime() : pts.get(last).getTime()));
				m.put("minutes", round((stayed + gap) / 60000.0, 1));
				if (gap >= minMs) {
					m.put("recordingGap", true);
				}
				res.add(m);
			}
			i = Math.max(j, i + 1);
		}
		return res;
	}

	// track split into equal distance parts (equal point counts if the track has no distance)
	private List<Map<String, Object>> profile(int parts) {
		List<Map<String, Object>> res = new ArrayList<>();
		int n = pts.size();
		if (n < 2) {
			return res;
		}
		double total = dist[n - 1];
		int from = 0;
		for (int p = 1; p <= parts && from < n - 1; p++) {
			int to = from + 1;
			if (total > 0) {
				double limit = total * p / parts;
				while (to < n - 1 && dist[to] < limit) {
					to++;
				}
			} else {
				to = Math.min(n - 1, Math.max(from + 1, (int) ((long) (n - 1) * p / parts)));
			}
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("fromKm", round(dist[from] / 1000, 3));
			m.put("toKm", round(dist[to] / 1000, 3));
			WptPt a = pts.get(from), b = pts.get(to);
			if (timed && a.getTime() > 0 && b.getTime() > a.getTime()) {
				m.put("time", iso(a.getTime()));
				m.put("minutes", round((b.getTime() - a.getTime()) / 60000.0, 2));
				m.put("avgKmh", round((dist[to] - dist[from]) / ((b.getTime() - a.getTime()) / 1000.0) * 3.6, 1));
			}
			if (!Double.isNaN(b.getEle())) {
				m.put("eleM", round(b.getEle(), 0));
			}
			res.add(m);
			from = to;
		}
		return res;
	}

	// points between indexes or times, every step-th one; step is chosen to fit MAX_POINTS if not given
	public Map<String, Object> slice(Integer fromIndex, Integer toIndex, Long fromTime, Long toTime, Integer step) {
		int n = pts.size();
		int from = fromIndex != null ? Math.max(0, fromIndex) : 0;
		int to = toIndex != null ? Math.min(n - 1, toIndex) : n - 1;
		if (fromTime != null) {
			while (from <= to && pts.get(from).getTime() < fromTime) {
				from++;
			}
		}
		if (toTime != null) {
			while (to >= from && pts.get(to).getTime() > toTime) {
				to--;
			}
		}
		int count = Math.max(0, to - from + 1);
		int st = step != null && step > 0 ? step : Math.max(1, (count + MAX_POINTS - 1) / MAX_POINTS);
		List<Map<String, Object>> list = new ArrayList<>();
		int i = from;
		for (; i <= to && list.size() < MAX_POINTS; i += st) {
			Map<String, Object> m = point(i);
			m.put("km", round(dist[i] / 1000, 3));
			WptPt p = pts.get(i);
			if (p.getSpeed() > 0) {
				m.put("kmh", round(p.getSpeed() * 3.6, 1));
			}
			list.add(m);
		}
		Map<String, Object> res = new LinkedHashMap<>();
		res.put("totalPoints", n);
		res.put("fromIndex", from);
		res.put("toIndex", to);
		res.put("step", st);
		res.put("points", list);
		if (i <= to) {
			res.put("nextFromIndex", i);
		}
		return res;
	}

	private Map<String, Object> point(int i) {
		WptPt p = pts.get(i);
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("index", i);
		if (p.getTime() > 0) {
			m.put("time", iso(p.getTime()));
		}
		m.put("lat", round(p.getLat(), 6));
		m.put("lon", round(p.getLon(), 6));
		if (!Double.isNaN(p.getEle())) {
			m.put("eleM", round(p.getEle(), 1));
		}
		return m;
	}

	static String iso(long time) {
		return time > 0 ? Instant.ofEpochMilli(time).toString() : null;
	}

	static double round(double v, int digits) {
		double k = Math.pow(10, digits);
		return Math.round(v * k) / k;
	}
}
