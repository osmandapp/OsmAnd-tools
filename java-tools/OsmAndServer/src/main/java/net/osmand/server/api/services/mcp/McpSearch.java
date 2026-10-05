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

import net.osmand.data.Amenity;
import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.osm.PoiFilter;
import net.osmand.osm.PoiType;
import net.osmand.server.api.services.WikiService;
import net.osmand.server.api.services.search.AmenityTagsService;
import net.osmand.server.api.services.search.SearchResultConverter;
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
	private final AmenityTagsService tagsService;
	private final Gson gson = new Gson();
	private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

	public static class SearchException extends Exception {
		private static final long serialVersionUID = 1L;

		SearchException(String message) {
			super(message);
		}
	}

	// serverApi: the map data server (maptile on osmand.net), empty = localApi (this server)
	public McpSearch(String serverApi, String localApi, WikiService wikiService, AmenityTagsService tagsService) {
		String s = serverApi == null ? "" : serverApi.trim().replaceAll("/+$", "");
		this.serverApi = s.isEmpty() ? localApi : s;
		this.localApi = localApi;
		this.wikiService = wikiService;
		this.tagsService = tagsService;
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
			places.add(place(p, type, ll, d, lang));
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

	// POIs of OsmAnd filters around a point, as the web map's POI layer and the app's POI filters: any of categories
	// (categories, types, top filters: "sustenance", "park", "cafe_and_restaurant"), each of filters (additional
	// values: "fuel_diesel"), open now by opening_hours in timeZone; unnamed ones (e.g. small parks) only with withUnnamed
	public Map<String, Object> searchCategories(List<String> categories, List<String> filters, boolean openNow,
			String timeZone, double lat, double lon, double radiusKm, String locale, int limit, boolean withUnnamed)
			throws SearchException {
		if (categories.isEmpty()) {
			throw new SearchException("categories are required, e.g. [\"park\"] (see get_poi_categories)");
		}
		if (openNow && (timeZone == null || java.time.ZoneId.getAvailableZoneIds().stream().noneMatch(timeZone::equals))) {
			throw new SearchException("open_now needs time_zone of the place, e.g. Europe/Kyiv");
		}
		double r = Math.min(Math.max(radiusKm, 0.05), MAX_RADIUS_KM);
		String lang = lang(locale);
		boolean[] truncated = { false };
		List<JsonElement> found = poiFeatures(categories, lat, lon, r, lang, timeZone, truncated);
		for (String filter : filters) {
			Set<String> ids = new java.util.HashSet<>();
			for (JsonElement e : poiFeatures(List.of(filter), lat, lon, r, lang, null, truncated)) {
				ids.add(str(e.getAsJsonObject().getAsJsonObject("properties"), "web_poi_id"));
			}
			found.removeIf(e -> !ids.contains(str(e.getAsJsonObject().getAsJsonObject("properties"), "web_poi_id")));
		}
		List<Map<String, Object>> places = new ArrayList<>();
		int unnamed = 0;
		for (JsonElement e : found) {
			JsonObject f = e.getAsJsonObject();
			JsonObject p = f.getAsJsonObject("properties");
			double[] ll = coords(f);
			if (ll == null) {
				continue;
			}
			double d = MapUtils.getDistance(lat, lon, ll[0], ll[1]) / 1000;
			String hours = str(p, "amenity_opening_hours" + SearchResultConverter.OPENING_HOURS_INFO_SUFFIX);
			boolean open = hours != null && hours.startsWith(SearchResultConverter.IS_OPENED_PREFIX);
			if (d > r || (openNow && !open)) {
				continue;
			}
			Map<String, Object> m = place(p, null, ll, d, lang);
			if (hours != null) {
				m.put("openNow", open);
				m.put("openingHoursNow", hours.replace(SearchResultConverter.IS_OPENED_PREFIX, ""));
			}
			if (m.get("name") == null) {
				unnamed++;
				if (!withUnnamed) {
					continue;
				}
			}
			places.add(m);
		}
		places.sort(Comparator.comparingDouble(m -> (Double) m.get("distanceKm")));
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("radiusKm", r);
		out.put("found", places.size());
		if (!withUnnamed && unnamed > 0) {
			out.put("unnamedSkipped", unnamed);
		}
		if (truncated[0]) {
			out.put("truncated", "too many POIs, use a smaller radius");
		}
		out.put("results", places.size() > limit ? places.subList(0, limit) : places);
		return out;
	}

	// /search/search-poi (the web map's POI layer) in a square of radius km around the point
	private List<JsonElement> poiFeatures(List<String> keys, double lat, double lon, double r, String lang,
			String timeZone, boolean[] truncated) throws SearchException {
		double dLat = r / 111.2, dLon = r / (111.2 * Math.max(Math.cos(Math.toRadians(lat)), 0.01));
		List<Map<String, String>> cats = new ArrayList<>();
		for (String k : keys) {
			cats.add(Map.of("category", k.trim(), "lang", lang));
		}
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("categories", cats);
		body.put("northWest", (lat + dLat) + "," + (lon - dLon));
		body.put("southEast", (lat - dLat) + "," + (lon + dLon));
		JsonElement res = new JsonParser().parse(post("/search/search-poi?locale=" + enc(lang) + "&lat=" + lat
				+ "&lon=" + lon + (timeZone != null ? "&timeZone=" + enc(timeZone) : ""), gson.toJson(body)));
		if (!res.isJsonObject()) {
			return new ArrayList<>();
		}
		JsonElement useLimit = res.getAsJsonObject().get("useLimit");
		if (useLimit != null && useLimit.isJsonPrimitive() && useLimit.getAsBoolean()) {
			truncated[0] = true;
		}
		JsonElement fc = res.getAsJsonObject().get("features");
		return features(fc != null && fc.isJsonObject() ? fc.getAsJsonObject() : null);
	}

	// a POI of the web map search as a tool result
	private Map<String, Object> place(JsonObject p, String kind, double[] ll, double d, String lang) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("name", first(p, "web_poi_name:" + lang, "web_poi_name", "web_name", "amenity_name"));
		put(m, "kind", kind);
		put(m, "category", str(p, "web_poi_type"));
		put(m, "type", str(p, "web_poi_subType"));
		m.put("lat", McpTracks.round(ll[0], 6));
		m.put("lon", McpTracks.round(ll[1], 6));
		m.put("distanceKm", McpTracks.round(d, 2));
		put(m, "icon", icon(p));
		String osm = osmRef(str(p, "web_poi_osmUrl"));
		put(m, "osm", osm);
		put(m, "osmandLink", osmandUrl(str(p, "web_poi_subType"), ll, osm));
		put(m, "address", str(p, "web_city"));
		put(m, "tags", tags(p, lang));
		return m;
	}

	// the OsmAnd top POI filters and categories -> types, the names search and search_by_category understand
	// (e.g. "cafe", "fuel")
	public Object categories(String locale, String filter) throws SearchException {
		if (filter != null) {
			return filters(filter.trim());
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("topFilters", gson.fromJson(getRaw("/search/get-top-filters"), Object.class));
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
				put(m, "osmandLink", osmandUrl(str(p, "poisubtype"), ll, (String) m.get("osm")));
			}
			String elo = str(p, "elo");
			m.put("popularity", elo != null ? (int) Double.parseDouble(elo) : 0);
			String desc = str(p, "wikiDesc");
			if (desc != null) {
				desc = desc.split("\n")[0].trim(); // the lead paragraph, later sections carry markup
				m.put("description", desc.length() > MAX_DESCRIPTION ? desc.substring(0, MAX_DESCRIPTION) + "…" : desc);
			}
			// OSM tags, as a POI has them
			Map<String, String> tags = new LinkedHashMap<>();
			String id = str(p, "id");
			if (id != null) {
				tags.put("wikidata", "Q" + id);
			}
			String wp = wiki(str(p, "wikiLang"), str(p, "wikiTitle"));
			if (wp != null) {
				tags.put("wikipedia", wp);
			}
			String photo = str(p, "photoTitle");
			if (photo != null) {
				tags.put("wikimedia_commons", "File:" + photo.replace('_', ' '));
			}
			put(m, "tags", tags.isEmpty() ? null : tags);
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
	// details from them and finds the POI on its maps), the POI icon; w: {lat, lon, osm, tags, name,
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
			// the POI properties are Amenity.getAmenityExtensions (SearchResultConverter), as WptPtEditor stores them
			for (String k : p.keySet()) {
				String v = str(p, k);
				if (v != null && !k.startsWith("web_") && !k.endsWith(SearchResultConverter.OPENING_HOURS_INFO_SUFFIX)) {
					pt.getExtensionsToWrite().put(k, v);
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
		// OSM tags of a place (search_popular_places tags: wikidata, wikipedia...) as the app stores a POI's tags;
		// the POI's own tags win
		Map<String, String> ext = pt.getExtensionsToWrite();
		Map<String, String> tags = new LinkedHashMap<>();
		if (w.get("tags") instanceof Map<?, ?> t) {
			t.forEach((k, v) -> {
				if (k instanceof String key && v != null && !key.startsWith(Amenity.COLLAPSABLE_PREFIX)
						&& String.valueOf(v).length() < 1000) {
					tags.put(key, String.valueOf(v).trim());
				}
			});
		}
		if (!tags.isEmpty()) {
			Amenity a = new Amenity();
			a.setAdditionalInfo(tags);
			a.getAmenityExtensions(MapPoiTypes.getDefault(), true, true, lang(locale)).forEach(ext::putIfAbsent);
		}
		String wikipedia = tags.get("wikipedia") != null && tags.get("wikipedia").matches("https?://\\S+")
				? tags.get("wikipedia") : null;
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

	// the additional filters of a category, top filter or type, grouped as the POI filter screen of the app shows
	// them (PoiUIFilter.fillPoiAdditionals): fuel -> fuel_type: fuel_diesel, fuel_octane_95...
	private Map<String, Object> filters(String key) throws SearchException {
		AbstractPoiType pt = MapPoiTypes.getDefault().getAnyPoiTypeByKey(key, false);
		if (pt instanceof PoiType t && t.isAdditional() && t.getParentType() != null) {
			pt = t.getParentType();
		}
		if (pt == null) {
			throw new SearchException("Unknown filter " + key + ", see get_poi_categories");
		}
		Map<String, PoiType> adds = new LinkedHashMap<>();
		fillPoiAdditionals(pt, true, adds);
		Map<String, Map<String, String>> groups = new java.util.TreeMap<>();
		for (PoiType a : adds.values()) {
			if (!a.isText() && !a.isHidden()) {
				String g = a.getPoiAdditionalCategory() != null ? a.getPoiAdditionalCategory() : "other";
				groups.computeIfAbsent(g, x -> new java.util.TreeMap<>()).put(a.getKeyName(), a.getTranslation());
			}
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("filter", pt.getKeyName());
		out.put("name", pt.getTranslation());
		out.put("filters", groups);
		return out;
	}

	private static void fillPoiAdditionals(AbstractPoiType pt, boolean allFromCategory, Map<String, PoiType> adds) {
		for (PoiType a : pt.getPoiAdditionals()) {
			adds.putIfAbsent(a.getKeyName(), a);
		}
		if (pt instanceof PoiCategory c && allFromCategory) {
			for (PoiFilter pf : c.getPoiFilters()) {
				fillPoiAdditionals(pf, true, adds);
			}
			for (PoiType ps : c.getPoiTypes()) {
				fillPoiAdditionals(ps, false, adds);
			}
		} else if (pt instanceof PoiFilter pf && !(pt instanceof PoiCategory)) {
			for (PoiType ps : pf.getPoiTypes()) {
				fillPoiAdditionals(ps, false, adds);
			}
		}
	}

	// the POI tags OsmAnd shows (opening hours, phone, cuisine, payment...), as the POI card of the apps and the web
	private Map<String, String> tags(JsonObject p, String lang) {
		Map<String, String> raw = new LinkedHashMap<>();
		for (String k : p.keySet()) {
			String v = k.startsWith("web_") ? null : str(p, k);
			if (v != null) {
				raw.put(k, v);
			}
		}
		Map<String, String> tags = new LinkedHashMap<>();
		for (AmenityTagsService.VisibleTag t : tagsService.convertToVisibleTags(raw, lang)) {
			if (t.value() != null && !t.key().equals("name")) {
				tags.put(t.key(), t.value());
			}
		}
		return tags.isEmpty() ? null : tags;
	}

	// the web map's link to a POI (PoiManager.getPoiParams: /map/poi/?type&pin&osmId), opens it with its details
	private String osmandUrl(String type, double[] ll, String osm) {
		if (osm == null || ll == null || type == null) {
			return null; // the web map finds the POI by its type
		}
		String t = type.split(";")[0];
		String pin = String.format(java.util.Locale.US, "%.6f,%.6f", ll[0], ll[1]);
		return localApi + "/map/poi/?type=" + enc(t) + "&pin=" + pin + "&osmId=" + osm.substring(osm.indexOf('/') + 1)
				+ "#17/" + String.format(java.util.Locale.US, "%.5f/%.5f", ll[0], ll[1]);
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

	private String post(String path, String json) throws SearchException {
		try {
			HttpRequest req = HttpRequest.newBuilder(URI.create(serverApi + path)).timeout(Duration.ofMinutes(1))
					.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(json)).build();
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
