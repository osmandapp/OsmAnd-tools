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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.osmand.server.api.services.WikiService;
import net.osmand.util.MapUtils;

// search tools of the MCP server: the web map's search (on the map data server) and its Explore layer (wiki places)
public class McpSearch {

	public static final int MAX_RESULTS = 100;
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
	public Map<String, Object> search(String text, double lat, double lon, String locale, int limit) throws SearchException {
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
			if (places.size() >= limit) {
				continue;
			}
			double[] ll = coords(f);
			if (ll == null) {
				continue;
			}
			Map<String, Object> m = new LinkedHashMap<>();
			String name = first(p, "web_poi_name:" + lang, "web_poi_name", "web_name", "amenity_name");
			m.put("name", name);
			m.put("kind", type);
			put(m, "category", str(p, "web_poi_type"));
			put(m, "type", str(p, "web_poi_subType"));
			m.put("lat", ll[0]);
			m.put("lon", ll[1]);
			m.put("distanceKm", McpTracks.round(MapUtils.getDistance(lat, lon, ll[0], ll[1]) / 1000, 2));
			put(m, "address", str(p, "web_city"));
			put(m, "openingHours", str(p, "amenity_opening_hours"));
			put(m, "phone", first(p, "phone", "osm_tag_phone"));
			put(m, "website", first(p, "website", "osm_tag_website"));
			put(m, "cuisine", str(p, "osm_tag_cuisine"));
			put(m, "wikidata", str(p, "osm_tag_wikidata"));
			put(m, "osmUrl", str(p, "web_poi_osmUrl"));
			places.add(m);
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("results", places);
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
			m.put("lat", ll[0]);
			m.put("lon", ll[1]);
			m.put("distanceKm", McpTracks.round(d, 2));
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
