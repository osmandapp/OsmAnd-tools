package net.osmand.server.traffic;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;

/**
 * A traffic event seen in a source during a day - roadworks, a closure, an accident: a point, optionally a line, some
 * text, and the local hours of the day in which a download saw it.
 */
public class TrafficEvent {

	public static final String ROADWORKS = "roadworks";
	public static final String CLOSURE = "closure";
	public static final String ACCIDENT = "accident";
	public static final String OBSTRUCTION = "obstruction";
	public static final String WARNING = "warning";
	public static final String REROUTING = "rerouting";
	public static final String SPEED = "speed";
	public static final String EVENT = "event";
	public static final String OTHER = "other";

	private static final int MAX_LINE_POINTS = 60;
	private static final int MAX_DESCRIPTION = 1500;

	public final String id;
	public String kind = OTHER;
	public String title = "";
	public String subtitle = "";
	public String description = "";
	public String source = "";
	public double lat = Double.NaN;
	public double lon = Double.NaN;
	public double[] lineLat; // null when the source gives a point only
	public double[] lineLon;
	public String start;     // ISO time when the source gives it
	public String end;
	public boolean blocked;
	public int firstHour = 24;
	public int lastHour = -1;

	public TrafficEvent(String id) {
		this.id = id;
	}

	/**
	 * Adds an event seen by a download in the given hour: the newest content wins, the hours widen.
	 * Downloads must be added in time order.
	 */
	public static void add(Map<String, TrafficEvent> events, TrafficEvent e, int hour) {
		TrafficEvent old = events.get(e.id);
		if (old != null) {
			e.firstHour = Math.min(e.firstHour, old.firstHour);
			e.lastHour = Math.max(e.lastHour, old.lastHour);
		}
		e.firstHour = Math.min(e.firstHour, hour);
		e.lastHour = Math.max(e.lastHour, hour);
		events.put(e.id, e);
	}

	JSONObject toJson() {
		JSONObject o = new JSONObject().put("id", id).put("k", kind).put("t", title).put("s", subtitle).put("src", source)
				.put("d", description.length() > MAX_DESCRIPTION ? description.substring(0, MAX_DESCRIPTION) + "…" : description)
				.put("p", new JSONArray().put(round(lon)).put(round(lat))).put("h0", firstHour).put("h1", lastHour).put("b", blocked ? 1 : 0);
		if (start != null) {
			o.put("st", start);
		}
		if (end != null) {
			o.put("en", end);
		}
		if (lineLat != null && lineLat.length > 1) {
			JSONArray g = new JSONArray();
			int step = Math.max(1, (lineLat.length + MAX_LINE_POINTS - 1) / MAX_LINE_POINTS);
			for (int i = 0; i < lineLat.length; i += step) {
				g.put(round(lineLon[i])).put(round(lineLat[i]));
			}
			if ((lineLat.length - 1) % step != 0) {
				g.put(round(lineLon[lineLat.length - 1])).put(round(lineLat[lineLat.length - 1]));
			}
			o.put("g", g);
		}
		return o;
	}

	private static double round(double v) {
		return Math.round(v * 1e5) / 1e5;
	}
}
