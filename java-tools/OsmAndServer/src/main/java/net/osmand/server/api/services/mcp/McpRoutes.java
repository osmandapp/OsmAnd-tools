package net.osmand.server.api.services.mcp;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import net.osmand.data.LatLon;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.RoutingService;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.util.MapUtils;

/**
 * Routes for MCP clients with the server router, the same way the web Plan route builds them:
 * every point carries the profile of the leg that starts at it (a router profile with parameters, line or gap).
 */
public class McpRoutes {

	static final String LINE = WebGpxParser.LINE_PROFILE_TYPE;
	static final String GAP = "gap";
	public static final int MAX_POINTS = 50;
	private static final int MAX_GEOMETRY = 300;

	private final RoutingService routingService;
	private final OsmAndMapsService mapsService;
	private final WebGpxParser webGpxParser;
	// profile -> parameter -> description, from /routing/routing-modes without the (devel) parameters
	private final Map<String, Map<String, Object>> profiles;
	private final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
	// the web map's routing server (maptile on osmand.net); empty = route with this server's own maps
	private final String routingSite;
	private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

	public static class RouteException extends Exception {
		private static final long serialVersionUID = 1L;

		RouteException(String message) {
			super(message);
		}
	}

	public record Leg(String profile, double distanceM, boolean routed) {
	}

	public record Built(List<WebGpxParser.Point> points, List<Leg> legs) {
	}

	public McpRoutes(RoutingService routingService, OsmAndMapsService mapsService, WebGpxParser webGpxParser,
	          String routingModesJson, String routingSite) {
		this.routingService = routingService;
		this.routingSite = routingSite == null ? "" : routingSite.trim().replaceAll("/+$", "");
		this.mapsService = mapsService;
		this.webGpxParser = webGpxParser;
		this.profiles = new LinkedHashMap<>();
		JsonObject modes = new JsonParser().parse(routingModesJson).getAsJsonObject();
		for (String key : modes.keySet()) {
			Map<String, Object> params = new LinkedHashMap<>();
			JsonObject ps = modes.getAsJsonObject(key).getAsJsonObject("params");
			for (String p : ps.keySet()) {
				JsonObject o = ps.getAsJsonObject(p);
				String section = o.has("section") && !o.get("section").isJsonNull() ? o.get("section").getAsString() : null;
				if (section != null && section.contains("(devel)")) {
					continue;
				}
				Map<String, Object> d = new LinkedHashMap<>();
				d.put("label", str(o, "label"));
				d.put("type", str(o, "type"));
				if (section != null) {
					d.put("group", section);
				}
				if (o.has("value") && !o.get("value").isJsonNull()) {
					d.put("default", gson.fromJson(o.get("value"), Object.class));
				}
				if (o.has("values") && o.get("values").isJsonArray()) {
					d.put("values", gson.fromJson(o.get("values"), Object.class));
				}
				params.put(p, d);
			}
			profiles.put(key, params);
		}
	}

	public Map<String, Object> describe() {
		Map<String, Object> res = new LinkedHashMap<>();
		res.put("profiles", profiles);
		res.put("special", Map.of(LINE, "straight line to the next point", GAP,
				"no connection to the next point (a new track segment starts)"));
		res.put("usage", "A profile is a key of profiles, optionally with parameters: \"bicycle\", "
				+ "\"car,avoid_motorway,short_way\", \"truck,width=2.5\". Boolean parameters are on when listed; "
				+ "parameters of one group (e.g. driving style) are alternatives, list at most one.");
		return res;
	}

	// "car,avoid_motorway" / {"profile":"car","params":{...}} parts -> checked router mode string
	String mode(String profile, Map<?, ?> params) throws RouteException {
		if (profile == null || profile.isBlank()) {
			throw new RouteException("profile is required, see get_routing_profiles");
		}
		String[] parts = profile.trim().split(",");
		String key = parts[0].trim();
		if (key.equals(LINE) || key.equals(GAP)) {
			return key;
		}
		Map<String, Object> allowed = profiles.get(key);
		if (allowed == null) {
			throw new RouteException("Unknown profile " + key + ", use one of " + profiles.keySet() + ", line, gap");
		}
		StringBuilder sb = new StringBuilder(key);
		List<String> list = new ArrayList<>();
		for (int i = 1; i < parts.length; i++) {
			list.add(parts[i].trim());
		}
		if (params != null) {
			for (Map.Entry<?, ?> e : params.entrySet()) {
				Object v = e.getValue();
				if (Boolean.FALSE.equals(v)) {
					continue;
				}
				String val = v instanceof Number n && n.doubleValue() == Math.rint(n.doubleValue())
						? String.valueOf(n.longValue()) : String.valueOf(v);
				list.add(Boolean.TRUE.equals(v) ? e.getKey().toString() : e.getKey() + "=" + val);
			}
		}
		for (String p : list) {
			if (p.isEmpty()) {
				continue;
			}
			String name = p.contains("=") ? p.substring(0, p.indexOf('=')) : p;
			if (!allowed.containsKey(name) || !p.matches("[A-Za-z0-9_]+(=[A-Za-z0-9_.\\-]+)?")) {
				throw new RouteException("Unknown parameter " + p + " for " + key + ", see get_routing_profiles");
			}
			sb.append(',').append(p);
		}
		return sb.toString();
	}

	// points: [{lat, lon, profile?, params?}], the profile of a point is used for the leg that starts at it
	public Built build(List<?> points, String defProfile, Map<?, ?> defParams) throws Exception {
		if (points == null || points.size() < 2) {
			throw new RouteException("At least 2 points are required");
		}
		if (points.size() > MAX_POINTS) {
			throw new RouteException("At most " + MAX_POINTS + " points");
		}
		String defMode = defProfile != null ? mode(defProfile, defParams) : null;
		List<WebGpxParser.Point> res = new ArrayList<>();
		List<Leg> legs = new ArrayList<>();
		long hhOnlyLimitM = mapsService.getRoutingConfig().hhOnlyLimit * 1000L;
		for (int i = 0; i < points.size(); i++) {
			if (!(points.get(i) instanceof Map<?, ?> m) || !(m.get("lat") instanceof Number lat)
					|| !(m.get("lon") instanceof Number lon) || Math.abs(lat.doubleValue()) > 90
					|| Math.abs(lon.doubleValue()) > 180) {
				throw new RouteException("Point " + i + " needs numeric lat and lon");
			}
			WebGpxParser.Point p = new WebGpxParser.Point();
			p.lat = lat.doubleValue();
			p.lng = lon.doubleValue();
			Object pp = m.get("profile");
			p.profile = pp != null ? mode(pp.toString(), m.get("params") instanceof Map<?, ?> x ? x : null) : defMode;
			if (p.profile == null && i < points.size() - 1) {
				throw new RouteException("Point " + i + " has no profile and no default profile is given");
			}
			p.geometry = new ArrayList<>();
			if (i > 0) {
				WebGpxParser.Point prev = res.get(i - 1);
				double d = 0;
				boolean routed = false;
				if (prev.profile.equals(GAP)) {
					// empty geometry starts a new route; the track segment ends at the last point before the gap
					// (marked like the web Plan route does, the GPX writer splits <trkseg> there)
					if (!prev.geometry.isEmpty()) {
						prev.geometry.get(prev.geometry.size() - 1).profile = GAP;
					}
				} else if (prev.profile.equals(LINE)) {
					p.geometry = straight(prev, p);
				} else {
					LatLon a = new LatLon(prev.lat, prev.lng), b = new LatLon(p.lat, p.lng);
					p.geometry = routeLeg(a, b, prev.profile, MapUtils.getDistance(a, b) > hhOnlyLimitM);
					routed = p.geometry.size() > 2;
				}
				for (int k = 1; k < p.geometry.size(); k++) {
					WebGpxParser.Point g0 = p.geometry.get(k - 1), g1 = p.geometry.get(k);
					d += MapUtils.getDistance(g0.lat, g0.lng, g1.lat, g1.lng);
				}
				legs.add(new Leg(prev.profile, d, routed || prev.profile.equals(LINE) || prev.profile.equals(GAP)));
			}
			res.add(p);
		}
		// the last point has no leg; keep the profile of the leg that ends there (the GPX writer needs one)
		WebGpxParser.Point last = res.get(res.size() - 1);
		last.profile = res.get(res.size() - 2).profile;
		return new Built(res, legs);
	}

	private List<WebGpxParser.Point> routeLeg(LatLon a, LatLon b, String mode, boolean hhOnly) throws RouteException {
		try {
			if (routingSite.isEmpty()) {
				return routingService.updateRouteBetweenPoints(a, b, mode, false, hhOnly, null).points;
			}
			// the same call the web map makes to its routing server
			String form = "start=" + enc(gson.toJson(a)) + "&end=" + enc(gson.toJson(b)) + "&routeMode=" + enc(mode)
					+ "&hasRouting=false";
			HttpRequest req = HttpRequest.newBuilder(URI.create(routingSite + "/routing/update-route-between-points"))
					.timeout(Duration.ofMinutes(2)).header("Content-Type", "application/x-www-form-urlencoded")
					.POST(HttpRequest.BodyPublishers.ofString(form)).build();
			HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
			if (resp.statusCode() != 200) {
				throw new RouteException("Routing server error " + resp.statusCode());
			}
			JsonObject o = new JsonParser().parse(resp.body()).getAsJsonObject();
			return gson.fromJson(o.get("points"), new TypeToken<List<WebGpxParser.Point>>() {}.getType());
		} catch (IOException e) {
			throw new RouteException("Routing server is not available: " + e.getMessage());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RouteException("Routing interrupted");
		}
	}

	private static String enc(String s) {
		return URLEncoder.encode(s, StandardCharsets.UTF_8);
	}

	public Map<String, Object> summary(Built b, boolean withGeometry) {
		Map<String, Object> res = new LinkedHashMap<>();
		double total = 0;
		List<Map<String, Object>> legs = new ArrayList<>();
		List<String> notRouted = new ArrayList<>();
		for (int i = 0; i < b.legs.size(); i++) {
			Leg l = b.legs.get(i);
			total += l.distanceM;
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("from", i);
			m.put("to", i + 1);
			m.put("profile", l.profile);
			m.put("distanceKm", McpTracks.round(l.distanceM / 1000, 3));
			if (!l.routed) {
				m.put("routed", false);
				notRouted.add(String.valueOf(i));
			}
			legs.add(m);
		}
		res.put("distanceKm", McpTracks.round(total / 1000, 3));
		res.put("legs", legs);
		if (!notRouted.isEmpty()) {
			res.put("warning", "No route found for legs " + String.join(", ", notRouted)
					+ ": a straight line is used (no map data there or the points are not reachable with this profile)");
		}
		if (withGeometry) {
			List<double[]> all = new ArrayList<>();
			for (WebGpxParser.Point p : b.points) {
				for (WebGpxParser.Point g : p.geometry) {
					all.add(new double[] { McpTracks.round(g.lat, 6), McpTracks.round(g.lng, 6) });
				}
			}
			int step = Math.max(1, (all.size() + MAX_GEOMETRY - 1) / MAX_GEOMETRY);
			List<double[]> sampled = new ArrayList<>();
			for (int i = 0; i < all.size(); i += step) {
				sampled.add(all.get(i));
			}
			if (!all.isEmpty() && (all.size() - 1) % step != 0) {
				sampled.add(all.get(all.size() - 1));
			}
			res.put("geometry", sampled);
			res.put("geometryStep", step);
		}
		return res;
	}

	// GPX as the web Plan route saves it: route points with the leg profiles + track geometry
	public String toGpx(Built b, String trackName, String description) {
		Map<String, Object> track = new LinkedHashMap<>();
		track.put("points", b.points);
		Map<String, Object> data = new LinkedHashMap<>();
		Map<String, Object> meta = new LinkedHashMap<>();
		meta.put("name", trackName);
		meta.put("desc", description);
		data.put("metaData", meta);
		data.put("tracks", List.of(track));
		data.put("routeTypes", List.of());
		WebGpxParser.TrackData td = gson.fromJson(gson.toJson(data), WebGpxParser.TrackData.class);
		GpxFile gpx = webGpxParser.createGpxFileFromTrackData(td);
		// not GpxUtilities.asString: it returns okio Buffer.toString() ("[text=...]"), not the XML
		okio.Buffer buf = new okio.Buffer();
		Exception e = GpxUtilities.INSTANCE.writeGpx(null, buf, gpx, null);
		if (e != null) {
			throw new IllegalStateException("GPX write failed: " + e.getMessage(), e);
		}
		return buf.readUtf8();
	}

	private static List<WebGpxParser.Point> straight(WebGpxParser.Point a, WebGpxParser.Point b) {
		List<WebGpxParser.Point> l = new ArrayList<>();
		for (WebGpxParser.Point p : List.of(a, b)) {
			WebGpxParser.Point c = new WebGpxParser.Point();
			c.lat = p.lat;
			c.lng = p.lng;
			l.add(c);
		}
		return l;
	}

	private static String str(JsonObject o, String key) {
		return o.has(key) && !o.get(key).isJsonNull() ? o.get(key).getAsString() : null;
	}
}
