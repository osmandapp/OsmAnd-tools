package net.osmand.obf.preparation;

import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class OverpassFetcher {

    private static final Log log = LogFactory.getLog(OverpassFetcher.class);
    private static OverpassFetcher instance;
    private final String overpassUrl;

    private OverpassFetcher() {
        this.overpassUrl = System.getenv("OVERPASS_URL");
        if (this.overpassUrl == null || this.overpassUrl.isEmpty()) {
            log.warn("OVERPASS_URL environment variable is not set.");
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

    public void fetchCompleteGeometryRelation(Relation relation) {
        if (!isOverpassConfigured()) {
            log.error("Overpass URL is not configured.");
            return;
        }

        // Collect way IDs with missing nodes
        List<Long> wayIdsToFetch = relation.getMembers().stream()
                .filter(member -> member.getEntity() instanceof Way)
                .map(member -> ((Way) member.getEntity()).getId())
                .filter(wayId -> {
                    Way way = (Way) relation.getMembers().stream()
                            .filter(m -> m.getEntity() instanceof Way && ((Way) m.getEntity()).getId() == wayId)
                            .findFirst()
                            .map(Relation.RelationMember::getEntity)
                            .orElse(null);
                    return way != null && way.getNodes().stream().anyMatch(Objects::isNull);
                })
                .collect(Collectors.toList());

        if (wayIdsToFetch.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        String wayIds = wayIdsToFetch.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));

        String query = "[out:json];way(id:" + wayIds + "); out skel;";
        String urlString = overpassUrl + "/api/interpreter?data=" + query;

        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
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

                        // Create a new Way object
                        Way way = new Way(wayId);
                        List<Node> nodes = new ArrayList<>();

                        // Fetch nodes for the way
                        for (int j = 0; j < nodeIds.length(); j++) {
                            long nodeId = nodeIds.getLong(j);
                            Node node = (Node) fetchedEntities.get(new EntityId(Entity.EntityType.NODE, nodeId));
                            if (node != null) {
                                nodes.add(node);
                            }
                        }

                        // Set nodes for the way
                        way.getNodes().addAll(nodes);
                        fetchedEntities.put(new EntityId(Entity.EntityType.WAY, wayId), way);
                    }
                }

                // Update the relation with the fetched ways and nodes
                relation.initializeLinks(fetchedEntities);

                log.info("Fetched members for relation " + relation.getId() + ": " + wayIds + " - fetched in " + (System.currentTimeMillis() - startTime) + " ms");
            } else {
                log.error("Failed to fetch data from Overpass API. Response code: " + responseCode);
            }
        } catch (Exception e) {
            log.error("Error fetching data from Overpass API", e);
        }
    }
}