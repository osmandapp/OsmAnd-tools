package net.osmand.obf.preparation;

import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.RouteDataObject;
import net.osmand.osm.MapRoutingTypes;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.router.RoutingContext;
import net.osmand.util.MapUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

// Speed camera of an enforcement relation is put on its "from" node and checked from "from" towards "to"
// ("device" acts as "to" if there is none, see Relation:enforcement). The roads from "from" to "to" are found
// in the first written routing section (as ImproveRoadConnectivity does), the direction is written on the second write
// and read by RouteDataObject.isDirectionApplicable.
public class SpeedCameraDirections {

	// search radius limit. ignore ways farther that distance("from", "to") x 2.
	private static final int SEARCH_DISTANCE_LIMIT_COEFFICIENT = 2;

	private final MapRoutingTypes routeTypes;
	private final List<SpeedCamera> cameras = new ArrayList<>();
	// way id -> point index -> "direction" rule id
	private final TLongObjectHashMap<TIntIntHashMap> directions = new TLongObjectHashMap<>();
	private int forwardRule, backwardRule, bothRule;

	private static class SpeedCamera {
		int x31, y31;
		TLongHashSet targets = new TLongHashSet();
		double maxDistance;
	}

	public SpeedCameraDirections(MapRoutingTypes routeTypes) {
		this.routeTypes = routeTypes;
	}

	public void addRelation(Relation relation, Node from) {
		SpeedCamera camera = new SpeedCamera();
		camera.x31 = getRoute31(MapUtils.get31TileNumberX(from.getLongitude()));
		camera.y31 = getRoute31(MapUtils.get31TileNumberY(from.getLatitude()));
		List<Entity> to = relation.getMemberEntities("to");
		to = to.isEmpty() ? relation.getMemberEntities("device") : to;
		for (Entity target : to) {
			if (target instanceof Node) {
				int x31 = getRoute31(MapUtils.get31TileNumberX(((Node) target).getLongitude()));
				int y31 = getRoute31(MapUtils.get31TileNumberY(((Node) target).getLatitude()));
				camera.targets.add(getPointId(x31, y31));
				camera.maxDistance = Math.max(camera.maxDistance,
						SEARCH_DISTANCE_LIMIT_COEFFICIENT * MapUtils.squareRootDist31(camera.x31, camera.y31, x31, y31));
			}
		}
		if (camera.targets.isEmpty()) {
			return;
		}
		registerDirectionRulesIfNeeded();
		cameras.add(camera);
	}
	
	private void registerDirectionRulesIfNeeded() {
		// rules are written before the routing section, so they are registered here. add only for maps with founded speedcams. 
		if (cameras.isEmpty()) {
			forwardRule = routeTypes.registerRule("direction", "forward").getInternalId();
			backwardRule = routeTypes.registerRule("direction", "backward").getInternalId();
			bothRule = routeTypes.registerRule("direction", "both").getInternalId();
		}
	}

	public void calculate(BinaryMapIndexReader reader) throws IOException {
		if (cameras.isEmpty()) {
			return;
		}
		RoutingConfiguration config = RoutingConfiguration.getDefault().build("car", new RoutingMemoryLimits(
				RoutingConfiguration.DEFAULT_MEMORY_LIMIT, RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT));
		RoutingContext ctx = new RoutePlannerFrontEnd().buildRoutingContext(config, null,
				new BinaryMapIndexReader[] { reader }, RouteCalculationMode.NORMAL);
		for (SpeedCamera camera : cameras) {
			RouteSegment first = findFirstSegment(ctx, camera);
			if (first == null) {
				continue;
			}
			for (RouteSegment s = ctx.loadRouteSegment(camera.x31, camera.y31, config.memoryLimitation); s != null; s = s.getNext()) {
				RouteDataObject road = s.getRoad();
				int ind = s.getSegmentStart();
				if (road.getId() == first.getRoad().getId()) {
					addDirection(road.getId(), ind, first.isPositive());
				} else if (ind == 0 || ind == road.getPointsLength() - 1) {
					// the road ends at "from" (road split at "from" or a side road): it goes towards "from"
					addDirection(road.getId(), ind, ind > 0);
				}
			}
		}
	}

	public TIntIntHashMap getDirections(long wayId) {
		return directions.get(wayId);
	}

	// the first segment from "from" of the shortest way along the roads to a "to"
	private RouteSegment findFirstSegment(RoutingContext ctx, SpeedCamera camera) {
		PriorityQueue<RouteSegment> queue = new PriorityQueue<>(Comparator.comparingDouble(RouteSegment::getDistanceFromStart));
		TLongHashSet visited = new TLongHashSet();
		visited.add(getPointId(camera.x31, camera.y31));
		addNextSegments(ctx, queue, null, camera.x31, camera.y31);
		while (!queue.isEmpty() && queue.peek().getDistanceFromStart() <= camera.maxDistance) {
			RouteSegment s = queue.poll();
			long end = getPointId(s.getEndPointX(), s.getEndPointY());
			if (camera.targets.contains(end)) {
				while (s.getParentRoute() != null) {
					s = s.getParentRoute();
				}
				return s;
			}
			if (visited.add(end)) {
				addNextSegments(ctx, queue, s, s.getEndPointX(), s.getEndPointY());
			}
		}
		return null;
	}

	private void addNextSegments(RoutingContext ctx, PriorityQueue<RouteSegment> queue, RouteSegment parent, int x31, int y31) {
		for (RouteSegment s = ctx.loadRouteSegment(x31, y31, ctx.config.memoryLimitation); s != null; s = s.getNext()) {
			for (int end : new int[] { s.getSegmentStart() - 1, s.getSegmentStart() + 1 }) {
				if (end >= 0 && end < s.getRoad().getPointsLength()) {
					RouteSegment next = new RouteSegment(s.getRoad(), s.getSegmentStart(), end);
					next.setParentRoute(parent);
					next.setDistanceFromStart((parent == null ? 0 : parent.getDistanceFromStart())
							+ (float) MapUtils.squareRootDist31(x31, y31, next.getEndPointX(), next.getEndPointY()));
					queue.add(next);
				}
			}
		}
	}

	// relations of both directions sharing the point keep it undirected
	private void addDirection(long wayId, int pointIndex, boolean forward) {
		TIntIntHashMap points = directions.get(wayId);
		if (points == null) {
			points = new TIntIntHashMap();
			directions.put(wayId, points);
		}
		int rule = forward ? forwardRule : backwardRule;
		points.put(pointIndex, points.containsKey(pointIndex) && points.get(pointIndex) != rule ? bothRule : rule);
	}

	// routing section keeps the coordinates with lower precision
	private static int getRoute31(int coordinate31) {
		return coordinate31 >> BinaryMapIndexWriter.ROUTE_SHIFT_COORDINATES << BinaryMapIndexWriter.ROUTE_SHIFT_COORDINATES;
	}

	private static long getPointId(int x31, int y31) {
		return ((long) x31 << 31) + y31;
	}
}
