package net.osmand.obf.preparation;

import static net.osmand.router.TransportFerryHelper.CROSSINGS_TAG;
import static net.osmand.router.TransportFerryHelper.FERRY_STOPS_TAG;
import static net.osmand.router.TransportFerryHelper.JUNCTION_VALUE;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.data.LatLon;
import net.osmand.data.TransportRoute;
import net.osmand.data.TransportStop;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.EntityParser;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.edit.Way;
import net.osmand.router.FerryRoutingHelper;
import net.osmand.router.TransportFerryHelper;
import net.osmand.util.MapUtils;

// Public transport ferries: route=ferry ways without a route relation and ferry crossings of other routes
public class TransportFerryIndexHelper {

	private final Set<Long> relationWays = new HashSet<>();
	private final TLongLongHashMap syntheticStops = new TLongLongHashMap(); // stop id -> node id
	private final TLongHashSet junctionStops = new TLongHashSet();
	private final TLongObjectHashMap<TLongObjectHashMap<String>> crossings = new TLongObjectHashMap<>(); // route id -> stop id -> "interval:duration:length"

	private static boolean isFerry(Entity e) {
		return FerryRoutingHelper.FERRY.equals(e.getTag(OSMTagKey.ROUTE));
	}

	// relations pre-pass: ways of ferry relations don't become routes by themselves
	public void indexRelation(Relation rel, OsmDbAccessorContext ctx) throws SQLException {
		if (isFerry(rel)) {
			ctx.loadEntityRelation(rel);
			for (RelationMember member : rel.getMembers()) {
				if (member.getEntity() instanceof Way) {
					relationWays.add(member.getEntity().getId());
				}
			}
		}
	}

	public boolean isFerryWayRoute(Entity e) {
		return e instanceof Way && isFerry(e) && !relationWays.contains(e.getId());
	}

	// stops are ferry terminals along the way and way ends (synthetic stops if they aren't terminals)
	public boolean processFerryWay(Way way, TransportRoute directRoute, TransportRoute backwardRoute) {
		List<Node> nodes = way.getNodes();
		for (int i = 0; i < nodes.size(); i++) {
			Node n = nodes.get(i);
			boolean terminal = n != null && "ferry_terminal".equals(n.getTag(OSMTagKey.AMENITY));
			if (n != null && (terminal || i == 0 || i == nodes.size() - 1)) {
				TransportStop stop = EntityParser.parseTransportStop(n);
				if (!terminal) {
					syntheticStops.put(stop.getId(), n.getId());
					TransportFerryHelper.markSyntheticStop(stop); // the map hides it without the routes
				}
				directRoute.getForwardStops().add(stop);
				backwardRoute.getForwardStops().add(0, stop);
			}
		}
		directRoute.addWay(way);
		backwardRoute.addWay(way);
		return directRoute.getForwardStops().size() >= 2;
	}

	// non-ferry route going over a ferry way waits for the ferry before the stop after the crossing
	// (call before route ways are merged: merged ways lose their tags)
	public void registerFerryCrossings(TransportRoute route) {
		List<TransportStop> stops = route.getForwardStops();
		if (TransportFerryHelper.isFerry(route) || stops.size() < 2) {
			return;
		}
		for (Way w : route.getForwardWays()) {
			if (isFerry(w) && w.getFirstNode() != null && w.getLastNode() != null) {
				int start = getNearestStop(stops, w.getFirstNode().getLatLon());
				int end = getNearestStop(stops, w.getLastNode().getLatLon());
				if (start != end) {
					if (!crossings.containsKey(route.getId())) {
						crossings.put(route.getId(), new TLongObjectHashMap<>());
					}
					double length = getLength(w);
					int interval = TransportRoute.parseIntervalTagToSeconds(w.getTag(TransportRoute.INTERVAL_KEY));
					int duration = FerryRoutingHelper.parseDuration(w.getTag(FerryRoutingHelper.DURATION_TAG), length);
					crossings.get(route.getId()).put(stops.get(Math.max(start, end)).getId(),
							interval + ":" + duration + ":" + (int) length);
				}
			}
		}
	}

	private static double getLength(Way way) {
		double length = 0;
		List<Node> nodes = way.getNodes();
		for (int i = 1; i < nodes.size(); i++) {
			if (nodes.get(i - 1) != null && nodes.get(i) != null) {
				length += MapUtils.getDistance(nodes.get(i - 1).getLatLon(), nodes.get(i).getLatLon());
			}
		}
		return length;
	}

	private static int getNearestStop(List<TransportStop> stops, LatLon location) {
		int nearest = 0;
		for (int i = 1; i < stops.size(); i++) {
			if (MapUtils.getDistance(stops.get(i).getLocation(), location) < MapUtils.getDistance(stops.get(nearest).getLocation(), location)) {
				nearest = i;
			}
		}
		return nearest;
	}

	// Call after all ways are processed. Synthetic stop shared only by 2+ ferry ways is a junction in the water
	// (a stop on the shore has some other way like pier or footway).
	public void resolveJunctionStops(OsmDbAccessor accessor) throws SQLException {
		if (syntheticStops.isEmpty()) {
			return;
		}
		String nodeIds = Arrays.toString(syntheticStops.values()).replaceAll("[\\[\\]]", "");
		ResultSet rs = accessor.getDbConn().createStatement().executeQuery("select w.node, t.tags from ways w "
				+ "join ways t on t.id = w.id and t.ord = 0 where w.node in (" + nodeIds + ")");
		TLongIntHashMap ferryWays = new TLongIntHashMap();
		TLongHashSet otherWays = new TLongHashSet();
		while (rs.next()) {
			Way way = new Way(-1);
			accessor.readTags(way, rs.getBytes(2));
			if (isFerry(way)) {
				ferryWays.adjustOrPutValue(rs.getLong(1), 1, 1);
			} else {
				otherWays.add(rs.getLong(1));
			}
		}
		rs.getStatement().close();
		for (long stopId : syntheticStops.keys()) {
			long nodeId = syntheticStops.get(stopId);
			if (ferryWays.get(nodeId) >= 2 && !otherWays.contains(nodeId)) {
				junctionStops.add(stopId);
			}
		}
	}

	// ferry stop flags as route tags, see TransportFerryHelper
	public Map<String, String> getRouteTags(long routeId, List<TransportStop> stops) {
		Map<String, String> tags = new LinkedHashMap<>();
		TLongObjectHashMap<String> routeCrossings = crossings.get(routeId);
		for (int i = 0; i < stops.size(); i++) {
			long id = stops.get(i).getId();
			if (syntheticStops.containsKey(id)) {
				TransportFerryHelper.addStopTag(tags, FERRY_STOPS_TAG, i, junctionStops.contains(id) ? JUNCTION_VALUE : null);
			}
			if (routeCrossings != null && routeCrossings.containsKey(id)) {
				TransportFerryHelper.addStopTag(tags, CROSSINGS_TAG, i, routeCrossings.get(id));
			}
		}
		return tags;
	}
}
