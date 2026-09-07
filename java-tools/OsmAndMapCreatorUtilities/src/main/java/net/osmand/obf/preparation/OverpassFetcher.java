package net.osmand.obf.preparation;

import java.io.BufferedReader;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import net.osmand.binary.ObfConstants;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
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
			log.warn("OVERPASS_URL is configured.");
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
	private JSONArray executeOverpassQuery(String query) {
		if (!isOverpassConfigured()) {
			return null;
		}
		String urlString = overpassUrl + "/api/interpreter";
		try {
			URL url = new URL(urlString);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			connection.setDoOutput(true);

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

				JSONObject jsonResponse = new JSONObject(response.toString());
				return jsonResponse.optJSONArray("elements");
			} else {
				log.error("Failed to fetch data from Overpass API. Response code: " + responseCode);
			}
		} catch (Exception e) {
			log.error("Error fetching data from Overpass API", e);
		}
		return null;
	}

	private String buildDateHeader(Long lastModifiedDate) {
		if (lastModifiedDate != null && lastModifiedDate > 0) {
			Instant instant = Instant.ofEpochMilli(lastModifiedDate);
			return "[date:\"" + DateTimeFormatter.ISO_INSTANT.format(instant) + "\"]";
		}
		return "";
	}

	private Map<EntityId, Entity> parseEntities(JSONArray elements, OsmDbAccessorContext ctx) {
		return parseEntities(elements, ctx, null);
	}

	/**
	 * @param internalIdByOsmId maps the id Overpass answers with back to the id the relation refers
	 *                          to, so initializeLinks() can match what was fetched. Null when the
	 *                          caller already works in plain OSM ids.
	 */
	private Map<EntityId, Entity> parseEntities(JSONArray elements, OsmDbAccessorContext ctx,
			Map<Long, Long> internalIdByOsmId) {
		Map<EntityId, Entity> fetchedEntities = new HashMap<>();
		if (elements == null) {
			return fetchedEntities;
		}

		for (int i = 0; i < elements.length(); i++) {
			JSONObject element = elements.getJSONObject(i);
			if ("node".equals(element.optString("type"))) {
				long nodeId = element.getLong("id");
				double lat = element.getDouble("lat");
				double lon = element.getDouble("lon");
				Node node = new Node(lat, lon, nodeId);
				if (ctx != null) {
					long nid = ctx.convertId(node);
					node = new Node(node.getLatitude(), node.getLongitude(), nid);
				}
				fetchedEntities.put(new EntityId(Entity.EntityType.NODE, nodeId), node);
			}
		}

		for (int i = 0; i < elements.length(); i++) {
			JSONObject element = elements.getJSONObject(i);
			if ("way".equals(element.optString("type"))) {
				long wayId = element.getLong("id");
				JSONArray nodeIds = element.optJSONArray("nodes");
				JSONArray geoms = element.optJSONArray("geometry");

				long targetId = internalIdByOsmId == null
						? wayId : internalIdByOsmId.getOrDefault(wayId, wayId);
				Way way = new Way(targetId);
				if (nodeIds != null) {
					for (int j = 0; j < nodeIds.length(); j++) {
						long nodeId = nodeIds.getLong(j);
						Node node = (Node) fetchedEntities.get(new EntityId(Entity.EntityType.NODE, nodeId));
						if (node == null && geoms != null && j < geoms.length()) {
							double lat = geoms.getJSONObject(j).getDouble("lat");
							double lon = geoms.getJSONObject(j).getDouble("lon");
							node = new Node(lat, lon, nodeId);
							if (ctx != null) {
								long nid = ctx.convertId(node);
								node = new Node(node.getLatitude(), node.getLongitude(), nid);
							}
						}
						if (node != null) {
							way.addNode(node);
						}
					}
				}
				fetchedEntities.put(new EntityId(Entity.EntityType.WAY, targetId), way);
			}
		}
		return fetchedEntities;
	}

	public void fetchRelationMembers(Relation relation, OsmDbAccessorContext ctx, Long lastModifiedDate) {
		if (relation == null) {
			return;
		}
		long startTime = System.currentTimeMillis();
		String dateHeader = buildDateHeader(lastModifiedDate);
		String query = "[out:json]" + dateHeader + ";relation(" + relation.getId() + "); out body;";

		JSONArray elements = executeOverpassQuery(query);
		if (elements == null) {
			return;
		}

		for (int i = 0; i < elements.length(); i++) {
			JSONObject el = elements.getJSONObject(i);
			if ("relation".equals(el.optString("type")) && el.getLong("id") == relation.getId()) {
				JSONArray members = el.optJSONArray("members");
				if (members != null) {
					for (int j = 0; j < members.length(); j++) {
						JSONObject m = members.getJSONObject(j);
						String type = m.getString("type");
						long ref = m.getLong("ref");
						String role = m.optString("role", "");

						Entity memberEntity = null;
						if ("way".equals(type)) {
							memberEntity = new Way(ref);
						} else if ("node".equals(type)) {
							memberEntity = new Node(0, 0, ref);
						} else if ("relation".equals(type)) {
							memberEntity = new Relation(ref);
						}

						if (memberEntity != null) {
							relation.addMember(memberEntity.getId(), EntityType.valueOf(memberEntity), role);
						}
					}
				}
				break;
			}
		}
		log.info(String.format("Fetched %d members for relation %d (%.2f sec)",
				relation.getMembers().size(), relation.getId(), (System.currentTimeMillis() - startTime) / 1e3));
	}

	public void fetchCompleteGeometry(Way w, OsmDbAccessorContext ctx, Long lastModifiedDate) {
		if (w == null) {
			return;
		}
		long startTime = System.currentTimeMillis();
		String dateHeader = buildDateHeader(lastModifiedDate);
		String query = "[out:json]" + dateHeader + ";way(" + w.getId() + "); out geom;";

		JSONArray elements = executeOverpassQuery(query);
		Map<EntityId, Entity> entities = parseEntities(elements, ctx);
		Way fetchedWay = (Way) entities.get(new EntityId(Entity.EntityType.WAY, w.getId()));
		if (fetchedWay != null) {
			for (Node n : fetchedWay.getNodes()) {
				w.addNode(n);
			}
		}
		log.info(String.format("Fetched geometry for way %d (%.2f sec)", w.getId(),
				(System.currentTimeMillis() - startTime) / 1e3));
	}

	public void fetchCompleteGeometry(Way w) {
		fetchCompleteGeometry(w, null, null);
	}

	public void fetchCompleteGeometryRelation(Relation relation, OsmDbAccessorContext ctx, Long lastModifiedDate) {
		if (relation == null) {
			return;
		}
		if (relation.getMembers() == null || relation.getMembers().isEmpty()) {
			fetchRelationMembers(relation, ctx, lastModifiedDate);
		}

		List<Long> wayIdsToFetch = getIncompleteWayIdsForRelation(relation);
		if (wayIdsToFetch.isEmpty()) {
			return;
		}

		long startTime = System.currentTimeMillis();

		// Member ids here are already in OsmAnd's internal encoding - OsmDbCreator.convertId()
		// shifts every positive OSM id left by ObfConstants.SHIFT_ID and packs a geohash into the
		// low bits. Asking Overpass for those (way 99214274955 rather than way 1550223046) simply
		// finds nothing, the relation stays incomplete and, before this, was dropped whole. Ask for
		// the decoded ids and remember which internal id each one belongs to.
		Map<Long, Long> internalIdByOsmId = new HashMap<>();
		for (Long internalId : wayIdsToFetch) {
			internalIdByOsmId.putIfAbsent(internalId, internalId);
			long decoded = internalId >> ObfConstants.SHIFT_ID;
			if (decoded > 0) {
				internalIdByOsmId.putIfAbsent(decoded, internalId);
			}
		}
		String wayIds = String.join(",",
				internalIdByOsmId.keySet().stream().map(String::valueOf).toArray(String[]::new));
		String dateHeader = buildDateHeader(lastModifiedDate);
		String query = "[out:json]" + dateHeader + ";way(id:" + wayIds + "); out geom;";

        JSONArray elements = executeOverpassQuery(query);
		Map<EntityId, Entity> fetchedEntities = parseEntities(elements, ctx, internalIdByOsmId);

		relation.initializeLinks(fetchedEntities);

		log.info(String.format("Fetched %d member ways for relation %d (%.2f sec)",
				wayIdsToFetch.size(), relation.getId(), (System.currentTimeMillis() - startTime) / 1e3));

		List<Long> stillIncomplete = getIncompleteWayIdsForRelation(relation);
		if (!stillIncomplete.isEmpty()) {
			log.warn(String.format("Relation %d: %d of %d fetched ways are still unresolved, e.g. %s"
							+ " (elements returned: %d)",
					relation.getId(), stillIncomplete.size(), wayIdsToFetch.size(),
					stillIncomplete.subList(0, Math.min(5, stillIncomplete.size())),
					elements == null ? -1 : elements.length()));
		}
	}

	public List<Long> getIncompleteWayIdsForRelation(Relation relation) {
		List<Long> wayIdsToFetch = new ArrayList<>();
		if (relation == null || relation.getMembers() == null) {
			return wayIdsToFetch;
		}
		for (RelationMember member : relation.getMembers()) {
			if (member.getEntity() instanceof Way || member.getEntityId().getType() == Entity.EntityType.WAY) {
				long wayId = member.getEntity() != null ? member.getEntity().getId() : member.getEntityId().getId();
				Way way = member.getEntity() instanceof Way ? (Way) member.getEntity() : null;

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
}