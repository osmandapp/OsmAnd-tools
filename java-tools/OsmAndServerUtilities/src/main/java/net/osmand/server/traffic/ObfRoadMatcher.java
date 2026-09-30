package net.osmand.server.traffic;

import net.osmand.LocationsHolder;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.ObfConstants;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.data.QuadPointDouble;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.GpxRouteApproximation;
import net.osmand.router.RouteCalculationProgress;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.GpxPoint;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingContext;
import net.osmand.util.MapUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Matches traffic sensors to OBF roads with the OsmAnd router: a sensor line goes through the geometry-based
 * {@link GpxRouteApproximation}, a point sensor through {@link RoutePlannerFrontEnd#findRouteSegment}.
 */
public class ObfRoadMatcher implements Closeable {

	private static final double LINE_STEP_M = 10;     // the sensor line becomes a track with a point every 10 m
	private static final double MIN_LINE_SHARE = 0.3; // part of the line that has to lie on roads
	private static final double BACKTRACK_M = 2;
	private static final double END_M = 0.5;
	private static final double SIMPLIFY_M = 1.5;

	public static class RoadMatch {
		public RouteDataObject road;
		public int direction; // 1 along the road points, -1 against them, 0 unknown
		public double share = 1; // part of the sensor line that lies on this road
		public final Set<Long> osmIds = new LinkedHashSet<>(); // roads under the sensor line, in order
		public double[] pieceLat;
		public double[] pieceLon;

		public long getOsmId() {
			return ObfConstants.getOsmObjectId(road);
		}

		public double getMaxSpeedKmh() {
			float forward = speed(road.getMaximumSpeed(true));
			float backward = speed(road.getMaximumSpeed(false));
			float ms = direction > 0 ? forward : direction < 0 ? backward : Math.max(forward, backward);
			return ms > 0 ? ms * 3.6 : Double.NaN;
		}

		private static float speed(float ms) {
			return ms == RouteDataObject.NONE_MAX_SPEED ? 0 : ms;
		}
	}

	private final BinaryMapIndexReader[] readers;
	private final RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
	private final RoutingContext ctx;

	public ObfRoadMatcher(List<File> obfs) throws IOException {
		readers = new BinaryMapIndexReader[obfs.size()];
		for (int i = 0; i < readers.length; i++) {
			readers[i] = new BinaryMapIndexReader(new RandomAccessFile(obfs.get(i), "r"), obfs.get(i));
		}
		router.setUseGeometryBasedApproximation(true);
		router.setUseNativeApproximation(false);
		int memory = RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT * 8 * 2;
		RoutingConfiguration config = RoutingConfiguration.getDefault().build("car",
				new RoutingConfiguration.RoutingMemoryLimits(memory, memory), new HashMap<>());
		ctx = router.buildRoutingContext(config, null, readers, RoutePlannerFrontEnd.RouteCalculationMode.NORMAL);
	}

	/**
	 * Matches a sensor line drawn in the direction of travel. Returns the road that carries most of the line; the
	 * piece is the approximated route cut to the sensor line.
	 */
	public RoadMatch matchLine(double[] lat, double[] lon) throws IOException, InterruptedException {
		List<LatLon> track = densify(lat, lon);
		ctx.calculationProgress = new RouteCalculationProgress();
		GpxRouteApproximation gctx = new GpxRouteApproximation(ctx);
		List<GpxPoint> gpxPoints = router.generateGpxPoints(gctx, new LocationsHolder(track));
		List<RouteSegmentResult> route = router.searchGpxRoute(gctx, gpxPoints, null, false).collectFinalPointsAsRoute();

		RoadMatch match = new RoadMatch();
		Map<String, double[]> roadMeters = new HashMap<>(); // road id + direction -> meters
		Map<String, RouteSegmentResult> roadSegment = new HashMap<>();
		List<int[]> points31 = new ArrayList<>();
		double matched = 0;
		for (RouteSegmentResult seg : route) {
			RouteDataObject o = seg.getObject();
			if (o.getId() < 0) {
				continue; // unmatched parts come back as straight lines: not drawn, they make hooks at the ends
			}
			int step = seg.isForwardDirection() ? 1 : -1;
			double meters = 0;
			for (int i = seg.getStartPointIndex(); ; i += step) {
				points31.add(new int[] {o.getPoint31XTile(i), o.getPoint31YTile(i)});
				if (i == seg.getEndPointIndex()) {
					break;
				}
				meters += MapUtils.measuredDist31(o.getPoint31XTile(i), o.getPoint31YTile(i), o.getPoint31XTile(i + step), o.getPoint31YTile(i + step));
			}
			matched += meters;
			String key = o.getId() + (seg.isForwardDirection() ? "+" : "-");
			roadMeters.computeIfAbsent(key, k -> new double[1])[0] += meters;
			roadSegment.putIfAbsent(key, seg);
			match.osmIds.add(ObfConstants.getOsmObjectId(o));
		}
		double length = 0;
		for (int i = 1; i < track.size(); i++) {
			length += MapUtils.getDistance(track.get(i - 1), track.get(i));
		}
		List<Node> line = cutToSensorLine(points31, track, length);
		String bestKey = null;
		for (Map.Entry<String, double[]> e : roadMeters.entrySet()) {
			if (bestKey == null || e.getValue()[0] > roadMeters.get(bestKey)[0]) {
				bestKey = e.getKey();
			}
		}
		// the share on all roads decides: a long sensor line crosses many OSM ways
		if (bestKey == null || matched < MIN_LINE_SHARE * length || line.size() < 2) {
			return null;
		}
		RouteSegmentResult best = roadSegment.get(bestKey);
		match.road = best.getObject();
		match.direction = best.isForwardDirection() ? 1 : -1;
		match.share = Math.min(1, roadMeters.get(bestKey)[0] / length);
		// removes the metre-size zigzags where the approximation pieces join
		List<Node> simplified = new ArrayList<>();
		simplified.add(line.get(0));
		OsmMapUtils.simplifyDouglasPeucker(line, 0, line.size() - 1, simplified, SIMPLIFY_M);
		setPiece(match, simplified);
		return match;
	}

	/**
	 * Matches a point sensor to the road the router would snap it to. With a known heading only roads in that
	 * direction qualify (the router's candidates are tried in its order), and the piece follows the direction of travel.
	 */
	public RoadMatch matchPoint(double lat, double lon, double heading, double headingTolerance, String nameHint, double maxDistM,
	                            double pieceHalfM) throws IOException {
		RouteSegmentPoint nearest = router.findRouteSegment(lat, lon, ctx, null);
		if (nearest == null) {
			return null;
		}
		List<RouteSegmentPoint> candidates = new ArrayList<>();
		candidates.add(nearest);
		if (nearest.others != null) {
			candidates.addAll(nearest.others);
		}
		int x = MapUtils.get31TileNumberX(lon), y = MapUtils.get31TileNumberY(lat);
		String hint = normalize(nameHint);
		RouteSegmentPoint chosen = null;
		int chosenDirection = 0;
		boolean chosenByName = false;
		for (RouteSegmentPoint c : candidates) {
			if (MapUtils.measuredDist31(c.preciseX, c.preciseY, x, y) > maxDistM) {
				continue;
			}
			RouteDataObject road = c.getRoad();
			int direction = 0;
			if (!Double.isNaN(heading)) {
				int s = c.getSegmentStart(), e = c.getSegmentEnd();
				double diff = Math.abs(MapUtils.degreesDiff(heading31(road.getPoint31XTile(e) - road.getPoint31XTile(s),
						road.getPoint31YTile(e) - road.getPoint31YTile(s)), heading));
				int oneway = road.getOneway();
				if (diff <= headingTolerance && oneway >= 0) {
					direction = s < e ? 1 : -1;
				} else if (diff >= 180 - headingTolerance && oneway <= 0) {
					direction = s < e ? -1 : 1;
				} else {
					continue;
				}
			}
			boolean byName = hint != null && road.getName() != null && normalize(road.getName()).contains(hint);
			if (chosen == null || (byName && !chosenByName)) {
				chosen = c;
				chosenDirection = direction;
				chosenByName = byName;
			}
		}
		if (chosen == null) {
			return null;
		}
		RoadMatch match = new RoadMatch();
		match.road = chosen.getRoad();
		match.direction = chosenDirection;
		match.osmIds.add(match.getOsmId());
		setPiece(match, piece(chosen, pieceHalfM, chosenDirection));
		return match;
	}

	@Override
	public void close() throws IOException {
		for (BinaryMapIndexReader reader : readers) {
			reader.close();
		}
	}

	private static List<LatLon> densify(double[] lat, double[] lon) {
		List<LatLon> track = new ArrayList<>();
		track.add(new LatLon(lat[0], lon[0]));
		for (int i = 1; i < lat.length; i++) {
			int n = Math.max(1, (int) Math.ceil(MapUtils.getDistance(lat[i - 1], lon[i - 1], lat[i], lon[i]) / LINE_STEP_M));
			for (int j = 1; j <= n; j++) {
				double t = (double) j / n;
				track.add(new LatLon(lat[i - 1] + t * (lat[i] - lat[i - 1]), lon[i - 1] + t * (lon[i] - lon[i - 1])));
			}
		}
		return track;
	}

	/**
	 * Route points that move on along the sensor line, from its start to its end. The approximation pieces overlap a
	 * little (points going back make zigzags), and the route often goes on past the ends into a turn (a hook): so the
	 * cut is by the distance along the sensor line, not by the nearest point to its ends.
	 */
	private static List<Node> cutToSensorLine(List<int[]> points31, List<LatLon> track, double length) {
		int n = track.size();
		int[] tx = new int[n], ty = new int[n];
		double[] along = new double[n];
		for (int i = 0; i < n; i++) {
			tx[i] = MapUtils.get31TileNumberX(track.get(i).getLongitude());
			ty[i] = MapUtils.get31TileNumberY(track.get(i).getLatitude());
			along[i] = i == 0 ? 0 : along[i - 1] + MapUtils.measuredDist31(tx[i - 1], ty[i - 1], tx[i], ty[i]);
		}
		List<Node> line = new ArrayList<>();
		Node beforeStart = null;
		int[] last = null;
		double reached = -Double.MAX_VALUE;
		for (int[] p : points31) {
			double nearest = Double.MAX_VALUE, meters = 0;
			for (int i = 0; i < n - 1; i++) {
				QuadPointDouble pr = MapUtils.getProjectionPoint31(p[0], p[1], tx[i], ty[i], tx[i + 1], ty[i + 1]);
				double d = MapUtils.squareRootDist31(p[0], p[1], (int) pr.x, (int) pr.y);
				if (d < nearest) {
					nearest = d;
					meters = along[i] + MapUtils.measuredDist31(tx[i], ty[i], (int) pr.x, (int) pr.y);
				}
			}
			if (meters < reached - BACKTRACK_M || (last != null && last[0] == p[0] && last[1] == p[1])) {
				continue;
			}
			last = p;
			reached = Math.max(reached, meters);
			Node node = node31(p[0], p[1]);
			if (meters <= END_M) {
				beforeStart = node;
				continue;
			}
			if (beforeStart != null) {
				line.add(beforeStart);
				beforeStart = null;
			}
			line.add(node);
			if (meters >= length - END_M) {
				break;
			}
		}
		if (line.isEmpty() && beforeStart != null) {
			line.add(beforeStart);
		}
		return line;
	}

	// the road around a point sensor: halfM on each side of its projection, in the direction of travel
	private static List<Node> piece(RouteSegmentPoint point, double halfM, int direction) {
		RouteDataObject road = point.getRoad();
		int n = road.getPointsLength();
		double[] along = new double[n];
		for (int i = 1; i < n; i++) {
			along[i] = along[i - 1] + MapUtils.measuredDist31(road.getPoint31XTile(i - 1), road.getPoint31YTile(i - 1),
					road.getPoint31XTile(i), road.getPoint31YTile(i));
		}
		int s = Math.min(point.getSegmentStart(), point.getSegmentEnd());
		double at = along[s] + MapUtils.measuredDist31(road.getPoint31XTile(s), road.getPoint31YTile(s), point.preciseX, point.preciseY);
		double from = Math.max(0, at - halfM), to = Math.min(along[n - 1], at + halfM);
		List<Node> nodes = new ArrayList<>();
		nodes.add(pointAt(road, along, from));
		for (int i = 0; i < n; i++) {
			if (along[i] > from && along[i] < to) {
				nodes.add(node31(road.getPoint31XTile(i), road.getPoint31YTile(i)));
			}
		}
		nodes.add(pointAt(road, along, to));
		if (direction < 0) {
			Collections.reverse(nodes);
		}
		return nodes;
	}

	private static Node pointAt(RouteDataObject road, double[] along, double distance) {
		for (int i = 1; i < along.length; i++) {
			if (along[i] >= distance) {
				double len = along[i] - along[i - 1];
				double t = len == 0 ? 0 : (distance - along[i - 1]) / len;
				return node31(road.getPoint31XTile(i - 1) + t * (road.getPoint31XTile(i) - road.getPoint31XTile(i - 1)),
						road.getPoint31YTile(i - 1) + t * (road.getPoint31YTile(i) - road.getPoint31YTile(i - 1)));
			}
		}
		return node31(road.getPoint31XTile(along.length - 1), road.getPoint31YTile(along.length - 1));
	}

	private static Node node31(double x31, double y31) {
		return new Node(MapUtils.get31LatitudeY((int) y31), MapUtils.get31LongitudeX((int) x31), 0);
	}

	private static void setPiece(RoadMatch match, List<Node> nodes) {
		match.pieceLat = new double[nodes.size()];
		match.pieceLon = new double[nodes.size()];
		for (int i = 0; i < nodes.size(); i++) {
			match.pieceLat[i] = nodes.get(i).getLatitude();
			match.pieceLon[i] = nodes.get(i).getLongitude();
		}
	}

	// 0 = north, clockwise; 31-tile y grows to the south
	private static double heading31(double dx, double dy) {
		double h = Math.toDegrees(Math.atan2(dx, -dy));
		return h < 0 ? h + 360 : h;
	}

	private static String normalize(String s) {
		if (s == null) {
			return null;
		}
		String n = Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("\\p{M}", "").toLowerCase(Locale.ROOT);
		return n.replaceAll("[^a-z0-9]+", " ").trim();
	}
}
