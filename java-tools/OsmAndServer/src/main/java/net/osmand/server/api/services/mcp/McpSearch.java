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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.osmand.server.api.services.WikiService;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.Link;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.MapUtils;

// search tools of the MCP server: the web map's search (on the map data server) and its Explore layer (wiki places)
public class McpSearch {

	public static final int MAX_RESULTS = 100;
	public static final int MAX_WAYPOINTS = 100;
	public static final String DEFAULT_ICON = "special_star";
	// the web map's POI icons, mx_<name>.svg; a missing one comes back as the web app page (text/html)
	private static final String ICONS_PATH = "/map/images/poi-icons-svg/mx_";
	private final Map<String, Boolean> icons = new ConcurrentHashMap<>();
	private volatile Boolean iconsAvailable;
	private static final String[] OSM_TYPES = { null, "node", "way", "relation" };
	public static final double MAX_RADIUS_KM = 50;
	private static final int MAX_DESCRIPTION = 300;
	// web map resources/wiki_data_filters.json
	public static final Map<String, String> TOPICS = topics();

	private final String serverApi;
	private final String localApi;
	private final WikiService wikiService;
	private final Gson gson = new Gson();
	private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

	public static class SearchException extends Exception {
		private static final long serialVersionUID = 1L;

		SearchException(String message) {
			super(message);
		}
	}

	// serverApi: the map data server (maptile on osmand.net), empty = localApi (this server)
	public McpSearch(String serverApi, String localApi, WikiService wikiService) {
		String s = serverApi == null ? "" : serverApi.trim().replaceAll("/+$", "");
		this.serverApi = s.isEmpty() ? localApi : s;
		this.localApi = localApi;
		this.wikiService = wikiService;
	}

	private static Map<String, String> topics() {
		Map<String, String> m = new LinkedHashMap<>();
		m.put("0", "All");
		m.put("1", "Cultural and historical heritage");
		m.put("2", "Arts, Entertainment & Leisure");
		m.put("3", "Nature & Outdoors");
		m.put("4", "Food & Drink");
		m.put("5", "Utilities & Essential Services");
		m.put("6", "Administrative Divisions");
		m.put("7", "Other");
		return m;
	}

	// text search near a point: places, addresses and POIs, as the web map search box
	public Map<String, Object> search(String text, double lat, double lon, String locale, int limit, Double radiusKm)
			throws SearchException {
		if (text == null || text.isBlank()) {
			throw new SearchException("text is required");
		}
		String lang = lang(locale);
		JsonObject res = get("/search/search?lat=" + lat + "&lon=" + lon + "&text=" + enc(text.trim()) + "&locale="
				+ enc(lang));
		List<Map<String, Object>> places = new ArrayList<>();
		List<Map<String, Object>> categories = new ArrayList<>();
		for (JsonElement e : features(res)) {
			JsonObject f = e.getAsJsonObject();
			JsonObject p = f.getAsJsonObject("properties");
			String type = str(p, "web_type");
			if ("POI_TYPE".equals(type)) {
				Map<String, Object> c = new LinkedHashMap<>();
				c.put("name", str(p, "web_name"));
				c.put("category", str(p, "web_categoryKeyName"));
				c.put("type", str(p, "web_keyName"));
				categories.add(c);
				continue;
			}
			double[] ll = coords(f);
			if (ll == null) {
				continue;
			}
			double d = MapUtils.getDistance(lat, lon, ll[0], ll[1]) / 1000;
			if (radiusKm != null && d > radiusKm) {
				continue;
			}
			Map<String, Object> m = new LinkedHashMap<>();
			String name = first(p, "web_poi_name:" + lang, "web_poi_name", "web_name", "amenity_name");
			m.put("name", name);
			m.put("kind", type);
			put(m, "category", str(p, "web_poi_type"));
			put(m, "type", str(p, "web_poi_subType"));
			m.put("lat", McpTracks.round(ll[0], 6));
			m.put("lon", McpTracks.round(ll[1], 6));
			m.put("distanceKm", McpTracks.round(d, 2));
			put(m, "icon", icon(p));
			put(m, "osm", osmRef(str(p, "web_poi_osmUrl")));
			put(m, "address", str(p, "web_city"));
			put(m, "openingHours", str(p, "amenity_opening_hours"));
			put(m, "phone", first(p, "phone", "osm_tag_phone"));
			put(m, "website", first(p, "website", "osm_tag_website"));
			put(m, "cuisine", str(p, "osm_tag_cuisine"));
			put(m, "wikidata", str(p, "osm_tag_wikidata"));
			String wp = str(p, "osm_tag_wikipedia");
			put(m, "wikipedia", wp != null && wp.startsWith("http") ? wp.replace(' ', '_') : null);
			places.add(m);
		}
		if (radiusKm != null) {
			places.sort(Comparator.comparingDouble(m -> (Double) m.get("distanceKm")));
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("results", places.size() > limit ? places.subList(0, limit) : places);
		if (!categories.isEmpty()) {
			out.put("matchingCategories", categories);
		}
		return out;
	}

	// POI categories -> types, the names search understands (e.g. "cafe", "fuel")
	public Object categories(String locale) throws SearchException {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("categories", gson.fromJson(getRaw("/search/get-poi-categories?locale=" + enc(lang(locale))), Object.class));
		out.put("popularPlaceTopics", TOPICS);
		return out;
	}

	// the web map's Explore layer: Wikipedia / Wikidata places ranked by popularity (elo)
	public Map<String, Object> popular(double lat, double lon, double radiusKm, Set<String> topics, String locale,
			int limit) throws SearchException {
		for (String t : topics) {
			if (!TOPICS.containsKey(t)) {
				throw new SearchException("Unknown topic " + t + ", use " + TOPICS);
			}
		}
		double r = Math.min(Math.max(radiusKm, 0.1), MAX_RADIUS_KM);
		double dLat = r / 111.2, dLon = r / (111.2 * Math.max(Math.cos(Math.toRadians(lat)), 0.01));
		String nw = (lat + dLat) + "," + (lon - dLon), se = (lat - dLat) + "," + (lon + dLon);
		String lang = lang(locale);
		JsonObject res;
		try {
			res = gson.toJsonTree(wikiService.getWikidataData(nw, se, lang, topics.isEmpty() ? Set.of("0") : topics,
					null)).getAsJsonObject();
		} catch (RuntimeException e) {
			throw new SearchException("Popular places are not available on this server");
		}
		List<Map<String, Object>> places = new ArrayList<>();
		for (JsonElement e : features(res)) {
			JsonObject f = e.getAsJsonObject();
			JsonObject p = f.getAsJsonObject("properties");
			double[] ll = coords(f);
			if (ll == null) {
				continue;
			}
			double d = MapUtils.getDistance(lat, lon, ll[0], ll[1]) / 1000;
			if (d > r) {
				continue;
			}
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("name", label(p, lang));
			put(m, "type", str(p, "poisubtype"));
			put(m, "topic", TOPICS.get(str(p, "topic")));
			put(m, "categories", str(p, "categories"));
			m.put("lat", McpTracks.round(ll[0], 6));
			m.put("lon", McpTracks.round(ll[1], 6));
			m.put("distanceKm", McpTracks.round(d, 2));
			put(m, "icon", icon(p));
			String osmType = str(p, "osmtype"), osmId = str(p, "osmid");
			if (osmType != null && osmType.matches("[123]") && osmId != null && osmId.matches("\\d+")) {
				m.put("osm", OSM_TYPES[Integer.parseInt(osmType)] + "/" + osmId);
			}
			String elo = str(p, "elo");
			m.put("popularity", elo != null ? (int) Double.parseDouble(elo) : 0);
			String desc = str(p, "wikiDesc");
			if (desc != null) {
				desc = desc.split("\n")[0].trim(); // the lead paragraph, later sections carry markup
				m.put("description", desc.length() > MAX_DESCRIPTION ? desc.substring(0, MAX_DESCRIPTION) + "…" : desc);
			}
			put(m, "wikipedia", wiki(str(p, "wikiLang"), str(p, "wikiTitle")));
			String id = str(p, "id");
			put(m, "wikidata", id != null ? "Q" + id : null);
			String photo = str(p, "photoTitle");
			put(m, "photo", photo != null ? "https://commons.wikimedia.org/wiki/File:" + enc(photo.replace(' ', '_')) : null);
			places.add(m);
		}
		places.sort(Comparator.comparingInt((Map<String, Object> m) -> (Integer) m.get("popularity")).reversed());
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("radiusKm", r);
		out.put("found", places.size());
		out.put("places", places.size() > limit ? places.subList(0, limit) : places);
		return out;
	}

	// a track waypoint as OsmAnd saves a POI: amenity_origin, amenity_* / osm_tag_* tags (the app shows the POI
	// details from them and finds the POI on its maps), the POI icon; w: {lat, lon, osm, wikidata, name,
	// description, icon, color, background, group, link}
	public WptPt waypoint(Map<?, ?> w, String locale) throws SearchException {
		WptPt pt = new WptPt();
		Double lat = w.get("lat") instanceof Number n ? n.doubleValue() : null;
		Double lon = w.get("lon") instanceof Number n ? n.doubleValue() : null;
		String osm = w.get("osm") instanceof String o ? o.trim() : null;
		String name = w.get("name") instanceof String x && !x.isBlank() ? x : null;
		String icon = w.get("icon") instanceof String x && !x.isBlank() ? x.replaceFirst("^mx_", "") : null;
		if (osm != null) {
			if (!osm.matches("(node|way|relation)/\\d+")) {
				throw new SearchException("osm must look like node/123, way/123 or relation/123: " + osm);
			}
			if (lat == null || lon == null) {
				throw new SearchException("lat and lon are required with osm (the POI is searched near them)");
			}
			JsonObject f = get("/search/get-poi-by-osmid?lat=" + lat + "&lon=" + lon + "&osmid="
					+ osm.substring(osm.indexOf('/') + 1) + "&type=" + osm.substring(0, osm.indexOf('/')));
			JsonObject p = f.has("properties") && f.get("properties").isJsonObject() ? f.getAsJsonObject("properties")
					: null;
			if (p == null || str(p, "amenity_type") == null) {
				throw new SearchException("POI " + osm + " is not found near " + lat + ", " + lon);
			}
			for (String k : p.keySet()) {
				if (k.startsWith(GpxUtilities.AMENITY_PREFIX) || k.startsWith(GpxUtilities.OSM_PREFIX)
						|| k.startsWith("collapsable_")) {
					String v = str(p, k);
					if (v != null) {
						pt.getExtensionsToWrite().put(k, v);
					}
				}
			}
			String en = first(p, "web_poi_name:en", "amenity_name");
			pt.setAmenityOriginName("Amenity:" + (en != null ? en : "") + ": " + str(p, "amenity_type") + ":"
					+ str(p, "amenity_subtype"));
			double[] ll = coords(f);
			if (ll != null) {
				lat = ll[0];
				lon = ll[1];
			}
			if (name == null) {
				name = first(p, "web_poi_name:" + lang(locale), "web_poi_name", "amenity_name");
			}
			if (icon == null) {
				icon = icon(p);
			}
		}
		if (lat == null || lon == null || Math.abs(lat) > 90 || Math.abs(lon) > 180) {
			throw new SearchException("waypoint needs lat and lon");
		}
		// fields of a search / search_popular_places result, as OsmAnd stores POI tags (amenity_*, osm_tag_*);
		// the POI's own tags win
		Map<String, String> ext = pt.getExtensionsToWrite();
		if (w.get("wikidata") instanceof String q && q.matches("Q\\d+")) {
			ext.putIfAbsent(GpxUtilities.OSM_PREFIX + "wikidata", q);
		}
		String wikipedia = w.get("wikipedia") instanceof String x && x.matches("https?://\\S+") ? x : null;
		if (wikipedia != null) {
			ext.putIfAbsent(GpxUtilities.OSM_PREFIX + "wikipedia", wikipedia);
		}
		if (w.get("photo") instanceof String ph && ph.contains("File:")) {
			ext.putIfAbsent(GpxUtilities.OSM_PREFIX + "wikimedia_commons",
					java.net.URLDecoder.decode(ph.substring(ph.indexOf("File:")), StandardCharsets.UTF_8));
		}
		putText(ext, GpxUtilities.AMENITY_PREFIX + "opening_hours", w.get("openingHours"));
		putText(ext, GpxUtilities.OSM_PREFIX + "website", w.get("website"));
		putText(ext, GpxUtilities.OSM_PREFIX + "phone", w.get("phone"));
		List<Link> links = new ArrayList<>();
		if (w.get("link") instanceof String l && l.matches("https?://\\S+")) {
			links.add(new Link(l));
		}
		if (wikipedia != null && links.stream().noneMatch(x -> wikipedia.equals(x.getHref()))) {
			links.add(new Link(wikipedia, "Wikipedia"));
		}
		if (osm != null) {
			links.add(new Link("https://www.openstreetmap.org/" + osm, "OpenStreetMap"));
		}
		if (icon != null && !iconExists(icon)) {
			throw new SearchException("Unknown icon " + icon + ": use the icon of a search result or e.g. " + DEFAULT_ICON);
		}
		pt.setLat(lat);
		pt.setLon(lon);
		pt.setName(name);
		if (w.get("description") instanceof String d && !d.isBlank()) {
			pt.setDesc(d);
		}
		pt.setIconName(icon != null ? icon : DEFAULT_ICON);
		String bg = w.get("background") instanceof String b ? b : "circle";
		if (!Set.of("circle", "octagon", "square").contains(bg)) {
			throw new SearchException("background is circle, octagon or square");
		}
		pt.setBackgroundType(bg);
		if (w.get("color") instanceof String c) {
			if (!c.matches("#([0-9a-fA-F]{6}|[0-9a-fA-F]{8})")) {
				throw new SearchException("color is #RRGGBB");
			}
			pt.setColor(c);
		}
		if (w.get("group") instanceof String g && !g.isBlank()) {
			pt.setCategory(g);
		}
		if (!links.isEmpty()) {
			pt.setLinks(links);
		}
		return pt;
	}

	// the icon the web map shows for a POI (PoiManager.getIconNameForPoiType)
	String icon(JsonObject p) {
		String tag = str(p, "web_typeOsmTag"), value = str(p, "web_typeOsmValue"), key = str(p, "web_iconKeyName");
		for (String c : new String[] { tag != null && value != null ? tag + "_" + value : null, key,
				key != null ? "topo_" + key : null, str(p, "web_poi_iconName") }) {
			if (c != null && iconExists(c)) {
				return c;
			}
		}
		return null;
	}

	// OsmAnd icon names valid in osmand:icon, checked against the web map's icon files (cached)
	public boolean iconExists(String name) {
		if (name == null || !name.matches("[a-z0-9_]+")) {
			return false;
		}
		if (iconsAvailable == null) {
			if (!fetchIcon(DEFAULT_ICON)) {
				return true; // no icon files to check against (local server, network): trust the name
			}
			iconsAvailable = true;
		}
		return icons.computeIfAbsent(name, this::fetchIcon);
	}

	private boolean fetchIcon(String name) {
		try {
			HttpRequest req = HttpRequest.newBuilder(URI.create(serverApi + ICONS_PATH + name + ".svg"))
					.timeout(Duration.ofSeconds(10)).method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
			HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
			return resp.statusCode() == 200
					&& resp.headers().firstValue("Content-Type").orElse("").startsWith("image/svg");
		} catch (IOException e) {
			return false;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	private static void putText(Map<String, String> ext, String key, Object v) {
		if (v instanceof String t && !t.isBlank() && t.length() < 1000) {
			ext.putIfAbsent(key, t.trim());
		}
	}

	private static String osmRef(String url) {
		if (url == null) {
			return null;
		}
		java.util.regex.Matcher m = java.util.regex.Pattern.compile("/(node|way|relation)/(\\d+)").matcher(url);
		return m.find() ? m.group(1) + "/" + m.group(2) : null;
	}


	private String label(JsonObject p, String lang) {
		String labels = str(p, "labelsJson");
		if (labels != null) {
			try {
				JsonObject l = new JsonParser().parse(labels).getAsJsonObject();
				if (l.has(lang) && !l.get(lang).isJsonNull()) {
					return l.get(lang).getAsString();
				}
			} catch (RuntimeException e) {
				// keep the article title
			}
		}
		return str(p, "wikiTitle");
	}

	private static String wiki(String lang, String title) {
		return lang == null || title == null ? null
				: "https://" + lang + ".wikipedia.org/wiki/" + enc(title.replace(' ', '_')).replace("+", "_");
	}

	private JsonObject get(String path) throws SearchException {
		JsonElement e = new JsonParser().parse(getRaw(path));
		if (!e.isJsonObject()) {
			throw new SearchException("Unexpected answer of the search server");
		}
		return e.getAsJsonObject();
	}

	private String getRaw(String path) throws SearchException {
		try {
			HttpRequest req = HttpRequest.newBuilder(URI.create(serverApi + path)).timeout(Duration.ofMinutes(1)).GET()
					.build();
			HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
			if (resp.statusCode() != 200) {
				throw new SearchException("Search server error " + resp.statusCode());
			}
			return resp.body();
		} catch (IOException e) {
			throw new SearchException("Search server is not available: " + e.getMessage());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new SearchException("Search interrupted");
		}
	}

	private static List<JsonElement> features(JsonObject res) {
		List<JsonElement> l = new ArrayList<>();
		if (res != null && res.has("features") && res.get("features").isJsonArray()) {
			res.getAsJsonArray("features").forEach(l::add);
		}
		return l;
	}

	private static double[] coords(JsonObject f) {
		if (!f.has("geometry") || !f.get("geometry").isJsonObject()) {
			return null;
		}
		JsonElement c = f.getAsJsonObject("geometry").get("coordinates");
		if (c == null || !c.isJsonArray() || c.getAsJsonArray().size() < 2) {
			return null;
		}
		double lon = c.getAsJsonArray().get(0).getAsDouble(), lat = c.getAsJsonArray().get(1).getAsDouble();
		return lat == 0 && lon == 0 ? null : new double[] { lat, lon };
	}

	private static String lang(String locale) {
		return locale == null || !locale.matches("[a-zA-Z]{2,3}([-_][a-zA-Z0-9]{2,8})?") ? "en" : locale;
	}

	private static String str(JsonObject o, String key) {
		JsonElement e = o == null ? null : o.get(key);
		if (e == null || e.isJsonNull()) {
			return null;
		}
		String s = e.isJsonPrimitive() ? e.getAsString() : e.toString();
		return s.isEmpty() ? null : s;
	}

	private static String first(JsonObject o, String... keys) {
		for (String k : keys) {
			String s = str(o, k);
			if (s != null) {
				return s;
			}
		}
		return null;
	}

	private static void put(Map<String, Object> m, String key, Object v) {
		if (v != null) {
			m.put(key, v);
		}
	}

	private static String enc(String s) {
		return URLEncoder.encode(s, StandardCharsets.UTF_8);
	}
}
