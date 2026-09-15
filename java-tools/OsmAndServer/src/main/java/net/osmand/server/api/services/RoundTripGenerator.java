package net.osmand.server.api.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.router.RouteSegmentResult;
import net.osmand.util.MapUtils;

/**
 * Round trip prototype on top of HH routing (OsmAnd-Issues#2827).
 * <p>
 * A loop is a regular polygon of waypoints on a circle that passes through the start. The circle
 * centre lies in the loop direction, so the route leaves towards it and comes back from the other
 * side. Each candidate is routed start -> w1 -> ... -> wk -> start. The road detour is not known in
 * advance, so the radius is rescaled by the measured ratio of road length (or time) to the straight
 * polygon and the loop is routed again. Candidates in several directions are ranked by the length
 * error and by the share of roads driven twice, and returned variants must not share most roads.
 * <p>
 * Directions are searched in parallel: the HH search itself costs about 3 ms per leg, but the
 * last-mile search around every waypoint costs ~160 ms and is what the request waits for.
 */
public class RoundTripGenerator {

	public static final int MIN_SHAPE = 2;
	public static final int MAX_SHAPE = 5;
	public static final int DIRECTIONS = 8; // candidate loops, each one is routed up to MAX_ITERATIONS times
	public static final int MAX_ITERATIONS = 3;
	public static final double LENGTH_TOLERANCE = 0.08; // stop rescaling a loop within this share of the target
	public static final double INITIAL_DETOUR = 1.3; // road length / straight polygon length before anything is measured
	public static final double MAX_DETOUR = 1.0; // the polygon is never longer than the requested length
	public static final double OVERLAP_WEIGHT = 1.5; // score = length error + weight * overlap
	public static final double MAX_SIMILARITY = 0.5; // variants sharing more of their roads are duplicates
	public static final double MAX_LENGTH_ERROR = 0.35; // such candidates are only returned when nothing better exists
	// with a direction given the loops fan around it, both orientations of the direction itself come first
	private static final double[] DIRECTION_OFFSETS = { 0, 0, -30, 30, -60, 60, -90, 90 };

	public interface LoopRouter {
		/** @return route start -> via... -> start, null if it could not be built. Called from several threads. */
		List<RouteSegmentResult> route(LatLon start, List<LatLon> via) throws IOException, InterruptedException;
	}

	public static class Params {
		public double distance; // meters, used when time is 0
		public double time; // seconds
		public int variants = 3;
		public Double direction; // degrees, null means any
		public int shape = 3; // waypoints between start and finish
		public int seed;
		public double speed = 10; // m/s, first guess of the length of a time-limited loop
		public double maxSpeed = 30; // m/s, bounds how far a time-limited loop may reach (map selection)
		public int parallelism = 1; // how many loops may be routed at once (one routing context each)
	}

	public static class RoundTrip {
		public List<RouteSegmentResult> route;
		public List<LatLon> waypoints;
		public double heading;
		public boolean clockwise;
		public double radius;
		public double distance;
		public double time;
		public double overlap; // share of the length driven more than once
		public double lengthError; // |length - target| / target, length is time for time-limited loops
		public int iteration;
		Map<Piece, Double> pieces; // road piece -> its length

		public double score() {
			return lengthError + OVERLAP_WEIGHT * overlap;
		}

		public Map<String, Object> describe() {
			Map<String, Object> m = new HashMap<>();
			m.put("heading", Math.round(heading));
			m.put("clockwise", clockwise);
			m.put("radius", Math.round(radius));
			m.put("overlap", round3(overlap));
			m.put("lengthError", round3(lengthError));
			m.put("iteration", iteration);
			m.put("distance", Math.round(distance));
			m.put("time", Math.round(time));
			List<double[]> wpts = new ArrayList<>();
			for (LatLon l : waypoints) {
				wpts.add(new double[] { l.getLatitude(), l.getLongitude() });
			}
			m.put("waypoints", wpts);
			return m;
		}
	}

	private record Piece(long roadId, int index) {
	}

	private final LoopRouter router;
	public final List<RoundTrip> candidates = Collections.synchronizedList(new ArrayList<>());
	private final List<Double> ratios = Collections.synchronizedList(new ArrayList<>());
	public volatile int routings;
	public volatile long routingMs;

	public RoundTripGenerator(LoopRouter router) {
		this.router = router;
	}

	/** Farthest a waypoint can get from the start (the diameter of the largest circle), to select maps */
	public static double maxReach(Params p) {
		int k = shape(p);
		double length = p.time > 0 ? p.time * p.maxSpeed : p.distance / MAX_DETOUR;
		return 2 * length / polygonFactor(k);
	}

	public List<RoundTrip> generate(LatLon start, Params p) throws IOException, InterruptedException {
		int threads = Math.max(1, Math.min(p.parallelism, DIRECTIONS));
		if (threads == 1) {
			for (int d = 0; d < DIRECTIONS; d++) {
				routeDirection(start, p, d);
			}
		} else {
			ExecutorService pool = Executors.newFixedThreadPool(threads);
			try {
				List<Future<RoundTrip>> futures = new ArrayList<>();
				for (int d = 0; d < DIRECTIONS; d++) {
					final int direction = d;
					futures.add(pool.submit((Callable<RoundTrip>) () -> routeDirection(start, p, direction)));
				}
				for (Future<RoundTrip> f : futures) {
					try {
						f.get();
					} catch (ExecutionException e) {
						Throwable cause = e.getCause();
						if (cause instanceof InterruptedException) {
							throw (InterruptedException) cause;
						}
						if (cause instanceof IOException) {
							throw (IOException) cause;
						}
						throw new IOException(cause);
					}
				}
			} finally {
				pool.shutdownNow();
			}
		}
		return select(new ArrayList<>(candidates), p.variants);
	}

	/** Route one direction, rescaling the radius until the loop is long enough */
	private RoundTrip routeDirection(LatLon start, Params p, int d) throws IOException, InterruptedException {
		int k = shape(p);
		boolean byTime = p.time > 0;
		double target = byTime ? p.time : p.distance;
		double maxPerimeter = maxReach(p) / 2 * polygonFactor(k);
		double heading = heading(p, d);
		boolean clockwise = ((d + p.seed) & 1) == 0;
		double ratio = ratios.isEmpty() ? (byTime ? INITIAL_DETOUR / p.speed : INITIAL_DETOUR) : median(ratios);
		RoundTrip best = null;
		for (int it = 0; it < MAX_ITERATIONS; it++) {
			double perimeter = Math.min(target / ratio, maxPerimeter);
			RoundTrip rt = route(start, heading, clockwise, perimeter / polygonFactor(k), k, byTime, target);
			if (rt == null) {
				break;
			}
			rt.iteration = it;
			double measured = (byTime ? rt.time : rt.distance) / perimeter;
			ratios.add(measured);
			if (best == null || rt.lengthError < best.lengthError) {
				best = rt;
			}
			if (rt.lengthError <= LENGTH_TOLERANCE) {
				break;
			}
			ratio = measured;
		}
		if (best != null) {
			candidates.add(best);
		}
		return best;
	}

	private RoundTrip route(LatLon start, double heading, boolean clockwise, double radius, int k, boolean byTime,
			double target) throws IOException, InterruptedException {
		LatLon center = MapUtils.rhumbDestinationPoint(start, radius, heading);
		List<LatLon> via = new ArrayList<>();
		for (int i = 1; i <= k; i++) {
			// the start is at heading + 180 seen from the centre
			double angle = heading + 180 + (clockwise ? 1 : -1) * i * 360.0 / (k + 1);
			via.add(MapUtils.rhumbDestinationPoint(center, radius, angle));
		}
		long t = System.currentTimeMillis();
		List<RouteSegmentResult> res = router.route(start, via);
		routingMs += System.currentTimeMillis() - t;
		routings++;
		if (res == null || res.isEmpty()) {
			return null;
		}
		RoundTrip rt = new RoundTrip();
		rt.route = res;
		rt.waypoints = via;
		rt.heading = heading;
		rt.clockwise = clockwise;
		rt.radius = radius;
		rt.pieces = new HashMap<>();
		double repeated = 0;
		for (RouteSegmentResult s : res) {
			rt.time += s.getSegmentTime();
			RouteDataObject o = s.getObject();
			int st = s.getStartPointIndex();
			int en = s.getEndPointIndex();
			int dir = st <= en ? 1 : -1;
			for (int i = st; i != en; i += dir) {
				int j = i + dir;
				double len = MapUtils.measuredDist31(o.getPoint31XTile(i), o.getPoint31YTile(i),
						o.getPoint31XTile(j), o.getPoint31YTile(j));
				Piece piece = new Piece(o.getId(), Math.min(i, j));
				if (rt.pieces.put(piece, len) != null) {
					repeated += len;
				}
				rt.distance += len;
			}
		}
		rt.overlap = rt.distance > 0 ? repeated / rt.distance : 0;
		rt.lengthError = Math.abs((byTime ? rt.time : rt.distance) - target) / target;
		return rt;
	}

	static List<RoundTrip> select(List<RoundTrip> candidates, int count) {
		candidates.sort(Comparator.comparingDouble(RoundTrip::score));
		List<RoundTrip> res = new ArrayList<>();
		for (RoundTrip c : candidates) {
			if (res.size() >= count) {
				break;
			}
			if (!res.isEmpty() && c.lengthError > MAX_LENGTH_ERROR) {
				continue;
			}
			boolean duplicate = false;
			for (RoundTrip r : res) {
				if (similarity(c, r) > MAX_SIMILARITY) {
					duplicate = true;
					break;
				}
			}
			if (!duplicate) {
				res.add(c);
			}
		}
		return res;
	}

	/** Length of the roads both loops use, as a share of the shorter loop */
	static double similarity(RoundTrip a, RoundTrip b) {
		Map<Piece, Double> small = a.pieces.size() <= b.pieces.size() ? a.pieces : b.pieces;
		Map<Piece, Double> large = small == a.pieces ? b.pieces : a.pieces;
		double shared = 0;
		for (Map.Entry<Piece, Double> e : small.entrySet()) {
			if (large.containsKey(e.getKey())) {
				shared += e.getValue();
			}
		}
		double len = Math.min(a.distance, b.distance);
		return len > 0 ? shared / len : 0;
	}

	private static double heading(Params p, int d) {
		if (p.direction != null) {
			return normalize(p.direction + DIRECTION_OFFSETS[d % DIRECTION_OFFSETS.length]);
		}
		// golden angle: every seed gives another set of directions
		return normalize(p.seed * 137.508 + d * 360.0 / DIRECTIONS);
	}

	private static int shape(Params p) {
		return Math.max(MIN_SHAPE, Math.min(MAX_SHAPE, p.shape));
	}

	/** Perimeter of the polygon through the start with k waypoints, in radii */
	private static double polygonFactor(int k) {
		return 2 * (k + 1) * Math.sin(Math.PI / (k + 1));
	}

	private static double median(List<Double> values) {
		List<Double> s;
		synchronized (values) {
			s = new ArrayList<>(values);
		}
		Collections.sort(s);
		return s.get(s.size() / 2);
	}

	private static double normalize(double deg) {
		double r = deg % 360;
		return r < 0 ? r + 360 : r;
	}

	private static double round3(double v) {
		return Math.round(v * 1000) / 1000.0;
	}
}
