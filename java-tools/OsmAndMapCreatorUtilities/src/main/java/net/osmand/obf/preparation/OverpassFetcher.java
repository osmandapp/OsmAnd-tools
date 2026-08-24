package net.osmand.obf.preparation;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import net.osmand.data.QuadRect;
import net.osmand.data.MultipolygonBuilder;
import net.osmand.binary.ObfConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;

public class OverpassFetcher {

	private static final Log log = LogFactory.getLog(OverpassFetcher.class);
	private static OverpassFetcher instance;
	private final String overpassUrl;

	private OverpassFetcher() {
		this.overpassUrl = System.getenv("OVERPASS_URL");
		if (this.overpassUrl == null || this.overpassUrl.isEmpty()) {
			log.warn("OVERPASS_URL environment variable is not set.");
		} else {
			log.warn("OVERPASS_URL is configured: " + overpassUrl);
		}
	}

	public static synchronized OverpassFetcher getInstance() {
		if (instance == null) {
			instance = new OverpassFetcher();
		}
		return instance;
	}

	public boolean isOverpassConfigured() {
		return overpassUrl != null && !overpassUrl.isEmpty();
	}

	public void fetchCompleteGeometryRelation(Relation relation, OsmDbAccessorContext ctx, Long lastModifiedDate) {
		List<Long> wayIdsToFetch = getIncompleteWayIdsForRelation(relation);
		if (wayIdsToFetch.isEmpty()) {
			return;
		}
		if (!isOverpassConfigured()) {
//            log.error("Overpass URL is not configured to fetch incomplete data.");
			return;
		}

		long startTime = System.currentTimeMillis();
		String wayIds = String.join(",", wayIdsToFetch.stream().map(String::valueOf).toArray(String[]::new));

		// Construct the Overpass QL query
		String query = "[out:json];way(id:" + wayIds + "); out geom;";
		String formattedDate = "";
		if (lastModifiedDate != null) {
			Instant instant = Instant.ofEpochMilli(lastModifiedDate);
			DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
			formattedDate = formatter.format(instant);
			query = "[out:json][date:\"" + formattedDate + "\"];way(id:" + wayIds + "); out geom;";
		}
		String urlString = overpassUrl + "/api/interpreter";

		try {
			URL url = new URL(urlString);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			connection.setDoOutput(true);

			// Write the query to the request body
			String body = "data=" + URLEncoder.encode(query, StandardCharsets.UTF_8.toString());
			try (OutputStream os = connection.getOutputStream()) {
				byte[] input = body.getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
			}

			int responseCode = connection.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) {
				BufferedReader in = new BufferedReader(
						new InputStreamReader(new GZIPInputStream(connection.getInputStream())));
				String inputLine;
				StringBuilder response = new StringBuilder();

				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();

				// Parse the JSON response
				JSONObject jsonResponse = new JSONObject(response.toString());
				JSONArray elements = jsonResponse.getJSONArray("elements");

				// Map to store fetched entities (ways and nodes)
				Map<EntityId, Entity> fetchedEntities = new HashMap<>();

				// First, parse all nodes and add them to the map
				for (int i = 0; i < elements.length(); i++) {
					JSONObject element = elements.getJSONObject(i);
					if (element.getString("type").equals("node")) {
						long nodeId = element.getLong("id");
						double lat = element.getDouble("lat");
						double lon = element.getDouble("lon");
						Node node = new Node(lat, lon, nodeId);
						fetchedEntities.put(new EntityId(Entity.EntityType.NODE, nodeId), node);
					}
				}

				// Then, parse all ways and add them to the map
				for (int i = 0; i < elements.length(); i++) {
					JSONObject element = elements.getJSONObject(i);
					if (element.getString("type").equals("way")) {
						long wayId = element.getLong("id");
						JSONArray nodeIds = element.getJSONArray("nodes");
						JSONArray geoms = element.getJSONArray("geometry");

						// Create a new Way object
						Way way = new Way(wayId);

						// Fetch nodes for the way
						for (int j = 0; j < nodeIds.length(); j++) {
							long nodeId = nodeIds.getLong(j);
							Node node = (Node) fetchedEntities.get(new EntityId(Entity.EntityType.NODE, nodeId));
							if (node == null) {
								double lat = geoms.getJSONObject(j).getDouble("lat");
								double lon = geoms.getJSONObject(j).getDouble("lon");
								node = new Node(lat, lon, nodeId);
							}
							if (ctx != null) {
								long nid = ctx.convertId(node);
								node = new Node(node.getLatitude(), node.getLongitude(), nid);
							}
							way.addNode(node);
						}
						fetchedEntities.put(new EntityId(Entity.EntityType.WAY, wayId), way);
					}
				}

				// Update the relation with the fetched ways and nodes
				relation.initializeLinks(fetchedEntities);

				log.info(String.format("Fetched members on date \"%s\" for relation %d (%.2f sec)", formattedDate, relation.getId(),
						(System.currentTimeMillis() - startTime) / 1e3, wayIds));
			} else {
				log.error("Failed to fetch data from Overpass API. Response code: " + responseCode);
			}
		} catch (Exception e) {
			log.error("Error fetching data from Overpass API", e);
		}
	}

	public List<Long> getIncompleteWayIdsForRelation(Relation relation) {
		// Collect way IDs with missing nodes
		List<Long> wayIdsToFetch = new ArrayList<>();
		for (Relation.RelationMember member : relation.getMembers()) {
			// Check if the member is a Way
			if (member.getEntity() instanceof Way) {
				long wayId = ((Way) member.getEntity()).getId();
				// Find the corresponding Way object in the relation
				Way way = null;
				for (Relation.RelationMember m : relation.getMembers()) {
					if (m.getEntity() instanceof Way && ((Way) m.getEntity()).getId() == wayId) {
						way = (Way) m.getEntity();
						break;
					}
				}
				boolean hasNullNodes = false;
				if (way == null || way.getNodeIds().isEmpty() || way.getNodes().size() != way.getNodeIds().size()) {
					hasNullNodes = true;
				} else {
					for (Node node : way.getNodes()) {
						if (node == null) {
							hasNullNodes = true;
							break;
						}
					}
				}
				if (hasNullNodes) {
					wayIdsToFetch.add(wayId);
				}
			}
		}
		return wayIdsToFetch;
	}

	public QuadRect fetchBoundaryBox(long mapObjectId) {
		if (!isOverpassConfigured()) {
            log.error("Overpass URL is not configured to fetch incomplete data.");
			return null;
		}

		long startTime = System.currentTimeMillis();
		Entity.EntityType entityType = ObfConstants.getOsmEntityType(mapObjectId);
		if (entityType != Entity.EntityType.NODE && entityType != Entity.EntityType.WAY
				&& entityType != Entity.EntityType.RELATION) {
			log.warn("Unsupported OSM entity type for map object " + mapObjectId + ": " + entityType);
			return null;
		}
		long osmId = ObfConstants.getOsmIdFromMapObjectId(mapObjectId);

		// Construct the Overpass QL query
		String query;
		if (entityType == Entity.EntityType.NODE) {
			query = "[out:json][timeout:25];node(" + osmId + ");out body;";
		} else if (entityType == Entity.EntityType.WAY) {
			query = "[out:json][timeout:25];(way(" + osmId + ");node(w););out body;";
		} else {
			query = "[out:json][timeout:25];(relation(" + osmId + ");way(r);node(w););out body;";
		}
		String urlString = overpassUrl + "/api/interpreter";

		try {
			URL url = new URL(urlString);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			connection.setDoOutput(true);

			// Write the query to the request body
			String body = "data=" + URLEncoder.encode(query, StandardCharsets.UTF_8.toString());
			try (OutputStream os = connection.getOutputStream()) {
				byte[] input = body.getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
			}

			int responseCode = connection.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) {
				InputStream responseStream = connection.getInputStream();
				if ("gzip".equalsIgnoreCase(connection.getContentEncoding())) {
					responseStream = new GZIPInputStream(responseStream);
				}
				BufferedReader in = new BufferedReader(new InputStreamReader(responseStream, StandardCharsets.UTF_8));
				String inputLine;
				StringBuilder response = new StringBuilder();

				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();

				// Parse the JSON response
				JSONObject jsonResponse = new JSONObject(response.toString());
				JSONArray elements = jsonResponse.getJSONArray("elements");
				Map<Long, Node> nodes = new HashMap<>();
				for (int i = 0; i < elements.length(); i++) {
					JSONObject element = elements.getJSONObject(i);
					if ("node".equals(element.optString("type"))) {
						long nodeId = element.getLong("id");
						nodes.put(nodeId, new Node(element.getDouble("lat"), element.getDouble("lon"), nodeId));
					}
				}
				if (entityType == Entity.EntityType.NODE) {
					Node node = nodes.get(osmId);
					if (node == null) {
						log.warn("Overpass returned no node for OSM node " + osmId);
						return null;
					}
					return new QuadRect(node.getLongitude(), node.getLatitude(),
							node.getLongitude(), node.getLatitude());
				}

				Map<Long, Way> ways = new HashMap<>();
				for (int i = 0; i < elements.length(); i++) {
					JSONObject element = elements.getJSONObject(i);
					if (!"way".equals(element.optString("type"))) {
						continue;
					}
					Way way = new Way(element.getLong("id"));
					JSONArray nodeIds = element.getJSONArray("nodes");
					boolean complete = true;
					for (int j = 0; j < nodeIds.length(); j++) {
						Node node = nodes.get(nodeIds.getLong(j));
						if (node == null) {
							complete = false;
							break;
						}
						way.addNode(node);
					}
					if (complete && way.getNodeIds().size() >= 2) {
						ways.put(way.getId(), way);
					}
				}

				MultipolygonBuilder builder = new MultipolygonBuilder();
				builder.setId(osmId);
				if (entityType == Entity.EntityType.WAY) {
					Way way = ways.get(osmId);
					if (way != null) {
						builder.addOuterWay(way);
					}
				} else {
					JSONObject relation = null;
					for (int i = 0; i < elements.length(); i++) {
						JSONObject element = elements.getJSONObject(i);
						if ("relation".equals(element.optString("type")) && element.getLong("id") == osmId) {
							relation = element;
							break;
						}
					}
					if (relation != null) {
						JSONArray members = relation.optJSONArray("members");
						if (members != null) {
							for (int i = 0; i < members.length(); i++) {
								JSONObject member = members.getJSONObject(i);
								if (!"way".equals(member.optString("type"))) {
									continue;
								}
								Way way = ways.get(member.getLong("ref"));
								if (way == null) {
									continue;
								}
								String role = member.optString("role");
								if ("inner".equals(role)) {
									builder.addInnerWay(way);
								} else if (role.isEmpty() || "outer".equals(role)) {
									builder.addOuterWay(way);
								}
							}
						}
					}
				}
				if (builder.getOuterWays().isEmpty()) {
					log.warn("Overpass returned no complete outer ways for OSM "
							+ entityType.name().toLowerCase() + " " + osmId);
					return null;
				}
				QuadRect bbox = builder.build().getLatLonBbox();
				log.info(String.format("Fetched boundary box for OSM %s %d (%.2f sec)",
						entityType.name().toLowerCase(), osmId,
						(System.currentTimeMillis() - startTime) / 1e3));
				return bbox;
			} else {
				log.error("Failed to fetch data from Overpass API. Response code: " + responseCode);
			}
		} catch (Exception e) {
			log.error("Error fetching data from Overpass API", e);
		}
		return null;
	}
}
