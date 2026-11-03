package net.osmand.obf.preparation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.binary.ObfConstants;
import net.osmand.binary.RouteDataObject.RestrictionInfo;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.util.MapUtils;

/**
 * Turn restrictions with two via ways: from F -(J0)- V1 -(J1)- V2 -(J2)- to T.
 * The router remembers one road back, so it cannot tell traffic that came onto V1 from F from traffic that
 * entered V1 from the side. A copy C of V1 is added to the routing data for traffic from F only:
 * C starts at a new point M inserted into F just before J0, its other points are moved off V1, so no other
 * road connects to it, and it ends at J1. The restriction is then written on C as a usual single via-way one
 * (C - via V2 - T), which the router already supports.
 * <ul>
 * <li>no_*: F - via V1 - V2 is closed, so F reaches V2 only over C; F still uses V1 itself.</li>
 * <li>only_*: F may not enter V1 at J0 or leave J0 by another road; C - J1 - only V2; C - via V2 - only T.</li>
 * </ul>
 * Copies carry osmand_via_copy=yes (routing.xml gives it destination_priority 0, so start and finish never
 * snap to a copy).
 */
public class MultiViaRestrictions {

	public static final String COPY_TAG = "osmand_via_copy";
	// route points are stored with ~30 cm precision: new points stay metres away from existing ones
	private static final double MIN_M_DIST = 1.0;
	private static final double MAX_M_PART = 0.4;
	private static final double SHIFT_DIST = 1.5;
	private static final double SHIFT_STEP = 2.0;
	private static final long COPY_ID_BASE = 1L << 40;
	private static final long COPY_NODE_ID_BASE = 1L << 47;

	private static class Copy {
		long id; // J0' .. J1, carries the restrictions
		long leadId; // M - J0': its own object, so a search meeting on the copy never has F as the parent
		long from;
		long v1;
		long p;
		long j0;
		Node m;
		List<Node> shifted = new ArrayList<>();
		boolean only;
		int depth;
	}

	private static class Chain {
		Way v1;
		Way v2;
		Way t;
		byte type;
	}

	private final Map<String, Copy> copies = new LinkedHashMap<>(); // "F V1" -> copy
	private final TLongObjectHashMap<List<Copy>> copiesByFrom = new TLongObjectHashMap<>();
	private final TLongObjectHashMap<List<Copy>> copiesByV1 = new TLongObjectHashMap<>();
	private final TLongObjectHashMap<List<Copy>> onlyCopiesByJ0 = new TLongObjectHashMap<>();
	private final TLongObjectHashMap<List<Chain>> chainsByFrom = new TLongObjectHashMap<>();
	// F - via V1 - V2 records that send F over the copy: never copied with via = copy
	private final java.util.Set<RestrictionInfo> closures = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
	private TLongObjectHashMap<List<RestrictionInfo>> restrictions;
	private long nodeIdCounter = 0;
	private boolean nestedDone;
	private boolean finished;

	/**
	 * @return true if the restriction is written by this class (the caller must not write its own record)
	 */
	public boolean addRestriction(Way f, Way v1, Way v2, Way t, byte type,
			TLongObjectHashMap<List<RestrictionInfo>> restrictions) {
		this.restrictions = restrictions;
		if (addChain(f, v1, v2, t, type, 0) == null) {
			return false;
		}
		Chain ch = new Chain();
		ch.v1 = v1;
		ch.v2 = v2;
		ch.t = t;
		ch.type = type;
		add(chainsByFrom, f.getId(), ch);
		return true;
	}

	/** @return the copy that carries this chain, or null if the chain is left as it is */
	private Copy addChain(Way f, Way v1, Way v2, Way t, byte type, int depth) {
		if (f.getId() == t.getId() || f.getId() == v1.getId() || f.getId() == v2.getId()
				|| v1.getId() == v2.getId() || t.getId() == v1.getId() || t.getId() == v2.getId()) {
			return null;
		}
		TLongArrayList fn = f.getNodeIds(), n1 = v1.getNodeIds(), n2 = v2.getNodeIds(), tn = t.getNodeIds();
		if (fn.size() < 2 || n1.size() < 2 || n2.size() < 2 || tn.size() < 2) {
			return null;
		}
		long j0 = endShared(n1, fn);
		if (j0 == 0) {
			return null;
		}
		long j1 = other(n1, j0);
		if (!isEnd(n2, j1)) {
			return null;
		}
		long j2 = other(n2, j1);
		if (!tn.contains(j2) || tn.contains(j1)) {
			return null;
		}
		boolean only = type == MapRenderingTypes.RESTRICTION_ONLY_LEFT_TURN
				|| type == MapRenderingTypes.RESTRICTION_ONLY_RIGHT_TURN
				|| type == MapRenderingTypes.RESTRICTION_ONLY_STRAIGHT_ON;
		if (!only && touches(fn, n2)) {
			// the record F - via V1 - V2 would also fire on a direct F - V2 junction
			return null;
		}
		int ow1 = oneway(v1);
		if ((ow1 > 0 && n1.get(0) != j0) || (ow1 < 0 && n1.get(0) == j0)) {
			return null;
		}
		String key = f.getId() + " " + v1.getId();
		Copy c = copies.get(key);
		if (c == null) {
			c = createCopy(f, v1, j0);
			if (c == null) {
				return null;
			}
			c.depth = depth;
			for (RestrictionInfo ri : get(restrictions, f.getId())) {
				if (ri.toWay == v1.getId()) {
					// F already has a restriction onto V1, leave the relation as it is
					return null;
				}
			}
			copies.put(key, c);
			add(copiesByFrom, f.getId(), c);
			add(copiesByV1, v1.getId(), c);
		}
		c.only |= only;
		if (only) {
			put(restrictions, f.getId(), v1.getId(), MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON, 0);
			put(restrictions, c.id, v2.getId(), MapRenderingTypes.RESTRICTION_ONLY_STRAIGHT_ON, 0);
			// via-way only_* is enforced by the router as ONLY_STRAIGHT_ON only
			put(restrictions, c.id, t.getId(), MapRenderingTypes.RESTRICTION_ONLY_STRAIGHT_ON, v2.getId());
			add(onlyCopiesByJ0, j0, c);
		} else {
			closures.add(put(restrictions, f.getId(), v2.getId(), MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON, v1.getId()));
			put(restrictions, c.id, t.getId(), type, v2.getId());
		}
		return c;
	}

	private Copy createCopy(Way f, Way v1, long j0) {
		List<Node> fNodes = f.getNodes();
		int ow = oneway(f);
		Node p = null, j = null;
		for (int i = 0; i < fNodes.size() && p == null; i++) {
			Node n = fNodes.get(i);
			if (n == null || n.getId() != j0) {
				continue;
			}
			if (i > 0 && ow >= 0 && fNodes.get(i - 1) != null) {
				p = fNodes.get(i - 1);
				j = n;
			} else if (i < fNodes.size() - 1 && ow <= 0 && fNodes.get(i + 1) != null) {
				p = fNodes.get(i + 1);
				j = n;
			}
		}
		if (p == null) {
			return null;
		}
		double seg = MapUtils.getDistance(p.getLatLon(), j.getLatLon());
		double a = Math.max(0.1, MIN_M_DIST / Math.max(seg, 0.01));
		if (a > MAX_M_PART) {
			return null;
		}
		if (isCopy(f.getId())) {
			// copies of one way meet at J1; far from J1 they are metres apart, so points inserted there stay apart
			a = MAX_M_PART;
		}
		List<Node> path = new ArrayList<>(v1.getNodes());
		if (path.isEmpty() || path.get(0) == null || path.get(path.size() - 1) == null || path.contains(null)) {
			return null;
		}
		if (path.get(0).getId() != j0) {
			java.util.Collections.reverse(path);
		}
		Copy c = new Copy();
		c.from = f.getId();
		c.v1 = v1.getId();
		c.p = p.getId();
		c.j0 = j0;
		long h = (f.getId() * 1_000_003L) ^ (v1.getId() * 0x9E3779B97F4A7C15L);
		c.id = (COPY_ID_BASE + ((h >>> 20) & ((1L << 39) - 1))) << ObfConstants.SHIFT_ID;
		c.leadId = c.id + (1L << (39 + ObfConstants.SHIFT_ID));
		c.m = new Node(j.getLatitude() + a * (p.getLatitude() - j.getLatitude()),
				j.getLongitude() + a * (p.getLongitude() - j.getLongitude()), nextNodeId());
		// points move sideways, a different distance for every copy of the same way, so no copy shares a point
		// with V1, with another copy or with a point inserted into V1 (only J1 is shared); a side where a new
		// point would fall on F, V1 or another copy (sharp U-turn geometry) is skipped
		List<Copy> others = copiesByV1.get(v1.getId());
		List<Node> avoid = new ArrayList<>(path.subList(0, path.size() - 1));
		for (Node n : fNodes) {
			if (n != null) {
				avoid.add(n);
			}
		}
		avoid.add(c.m);
		for (Copy o : others == null ? java.util.Collections.<Copy>emptyList() : others) {
			avoid.add(o.m);
			avoid.addAll(o.shifted.subList(0, o.shifted.size() - 1));
		}
		double base = SHIFT_DIST + SHIFT_STEP * (others == null ? 0 : others.size());
		List<Node> best = null;
		double bestScore = Double.MAX_VALUE;
		Node n1 = path.get(1);
		double segLen = MapUtils.getDistance(path.get(0).getLatLon(), n1.getLatLon());
		// J0' also moves a little along V1, so the lead M - J0' goes forward from F and is not a U-turn
		double ahead = Math.min(2.0, 0.3 * segLen) / Math.max(segLen, 0.01);
		for (int attempt = 0; attempt < 6; attempt++) {
			double off = (base + SHIFT_STEP * (attempt / 2)) * (attempt % 2 == 0 ? 1 : -1);
			List<Node> sh = new ArrayList<>();
			boolean ok = true;
			for (int k = 0; k < path.size() - 1 && ok; k++) {
				Node n = path.get(k);
				double[] nr = normal(k > 0 ? path.get(k - 1) : null, n, path.get(k + 1));
				double la0 = n.getLatitude(), lo0 = n.getLongitude();
				if (k == 0) {
					la0 += ahead * (n1.getLatitude() - n.getLatitude());
					lo0 += ahead * (n1.getLongitude() - n.getLongitude());
				}
				Node s = new Node(la0 + off * nr[1] / 111320.0,
						lo0 + off * nr[0] / (111320.0 * Math.cos(Math.toRadians(n.getLatitude()))), 0);
				for (Node av : avoid) {
					if (MapUtils.getDistance(av.getLatLon(), s.getLatLon()) < MIN_M_DIST) {
						ok = false;
						break;
					}
				}
				sh.add(s);
			}
			if (ok) {
				// prefer the side where the turn F -> lead is closest to the turn F -> V1
				double score = Math.abs(turn(p, j, sh.get(0)) - turn(p, j, n1)) + Math.abs(off) * 0.01;
				if (score < bestScore) {
					bestScore = score;
					best = sh;
				}
			}
		}
		if (best == null) {
			return null;
		}
		for (Node n : best) {
			c.shifted.add(new Node(n.getLatitude(), n.getLongitude(), nextNodeId()));
		}
		c.shifted.add(path.get(path.size() - 1));
		return c;
	}

	/**
	 * Chains inside chains: when V1 is itself `from` of another chain B, traffic on V1's copy is on V1 too, so
	 * B is added once more with the copy as `from` (it gets its own copy of B's via1). Up to 3 levels.
	 */
	private void prepareNested() {
		if (nestedDone) {
			return;
		}
		nestedDone = true;
		List<Copy> queue = new ArrayList<>(copies.values());
		for (int i = 0; i < queue.size(); i++) {
			Copy c = queue.get(i);
			List<Chain> chains = chainsByFrom.get(c.v1);
			if (chains == null || c.depth >= 3) {
				continue;
			}
			Way cw = copyWay(c, null);
			for (Chain b : chains) {
				int before = copies.size();
				Copy nc = addChain(cw, b.v1, b.v2, b.t, b.type, c.depth + 1);
				if (nc != null && copies.size() > before) {
					queue.add(nc);
				}
			}
		}
	}

	private Way copyWay(Copy c, Map<String, String> tags) {
		Way w = new Way(c.id);
		for (Node n : c.shifted) {
			w.addNode(n);
		}
		w.replaceTags(copyTags(tags));
		return w;
	}

	private Way leadWay(Copy c, Map<String, String> tags) {
		Way w = new Way(c.leadId);
		w.addNode(c.m);
		w.addNode(c.shifted.get(0));
		w.replaceTags(copyTags(tags));
		return w;
	}

	private static Map<String, String> copyTags(Map<String, String> tags) {
		Map<String, String> t = tags == null ? new LinkedHashMap<>() : new LinkedHashMap<>(tags);
		t.put("oneway", "yes");
		t.remove("oneway:conditional");
		t.put(COPY_TAG, "yes");
		return t;
	}

	/** Unit normal (east, north) to the right of travel at n, averaged over the segments around it. */
	private static double[] normal(Node prev, Node n, Node next) {
		double ex = 0, ny = 0;
		for (Node[] seg : new Node[][] { { prev, n }, { n, next } }) {
			if (seg[0] == null || seg[1] == null) {
				continue;
			}
			double dx = (seg[1].getLongitude() - seg[0].getLongitude()) * Math.cos(Math.toRadians(n.getLatitude()));
			double dy = seg[1].getLatitude() - seg[0].getLatitude();
			double len = Math.hypot(dx, dy);
			if (len > 0) {
				ex += dy / len;
				ny += -dx / len;
			}
		}
		double len = Math.hypot(ex, ny);
		return len == 0 ? new double[] { 0, 0 } : new double[] { ex / len, ny / len };
	}

	/** Signed turn angle in degrees at b for a -> b -> c. */
	private static double turn(Node a, Node b, Node c) {
		double c0 = Math.cos(Math.toRadians(b.getLatitude()));
		double h1 = Math.atan2(b.getLatitude() - a.getLatitude(), (b.getLongitude() - a.getLongitude()) * c0);
		double h2 = Math.atan2(c.getLatitude() - b.getLatitude(), (c.getLongitude() - b.getLongitude()) * c0);
		double d = Math.toDegrees(h2 - h1);
		while (d > 180) {
			d -= 360;
		}
		while (d < -180) {
			d += 360;
		}
		return d;
	}

	private static boolean isCopy(long id) {
		return id >= COPY_ID_BASE << ObfConstants.SHIFT_ID;
	}

	private long nextNodeId() {
		return COPY_NODE_ID_BASE + (++nodeIdCounter << ObfConstants.SHIFT_ID);
	}

	/** Routing nodes of F with M inserted before J0, or the way's own nodes. */
	public List<Node> routingNodes(Way w) {
		prepareNested();
		List<Copy> cs = copiesByFrom.get(w.getId());
		List<Node> nodes = w.getNodes();
		if (cs == null) {
			return nodes;
		}
		List<Node> res = new ArrayList<>(nodes);
		for (Copy c : cs) {
			for (int i = 0; i + 1 < res.size(); i++) {
				Node a = res.get(i), b = res.get(i + 1);
				if (a == null || b == null) {
					continue;
				}
				if ((a.getId() == c.p && b.getId() == c.j0) || (a.getId() == c.j0 && b.getId() == c.p)) {
					res.add(i + 1, c.m);
					break;
				}
			}
		}
		return res;
	}

	/** Copies of V1 to index right after V1, as ways with the copy tags. */
	public List<Way> copiesOf(Way v1, Map<String, String> v1RoutingTags) {
		prepareNested();
		List<Copy> cs = copiesByV1.get(v1.getId());
		if (cs == null) {
			return null;
		}
		List<Way> res = new ArrayList<>();
		for (Copy c : cs) {
			res.add(leadWay(c, v1RoutingTags));
			Way w = copyWay(c, v1RoutingTags);
			List<Node> nodes = routingNodes(w);
			if (nodes != w.getNodes()) {
				Way wn = new Way(c.id);
				for (Node n : nodes) {
					wn.addNode(n);
				}
				wn.replaceTags(w.getTags());
				w = wn;
			}
			res.add(w);
		}
		return res;
	}

	/** only_*: from F, J0 may be left by V1's copy only. Called for every routing way. */
	public void registerWayAtJunctions(Way w, TLongObjectHashMap<List<RestrictionInfo>> restrictions) {
		prepareNested();
		if (onlyCopiesByJ0.isEmpty()) {
			return;
		}
		TLongArrayList ids = w.getNodeIds();
		for (int i = 0; i < ids.size(); i++) {
			List<Copy> cs = onlyCopiesByJ0.get(ids.get(i));
			if (cs == null) {
				continue;
			}
			for (Copy c : cs) {
				if (w.getId() != c.from && w.getId() != c.v1) {
					put(restrictions, c.from, w.getId(), MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON, 0);
				}
			}
		}
	}

	/**
	 * Before writing: restrictions of V1 also bind its copies, F's own records with via V1 also get via C,
	 * and any no_* record A - via X - Y also closes the copy of Y made for X as from (chains inside chains).
	 */
	public void finish(TLongObjectHashMap<List<RestrictionInfo>> restrictions) {
		if (finished) {
			return;
		}
		finished = true;
		prepareNested();
		for (Copy c : copies.values()) {
			for (RestrictionInfo ri : new ArrayList<>(get(restrictions, c.v1))) {
				RestrictionInfo n = put(restrictions, c.id, ri.toWay, ri.type, ri.viaWay);
				if (closures.contains(ri)) {
					closures.add(n);
				}
			}
			for (RestrictionInfo ri : new ArrayList<>(get(restrictions, c.from))) {
				if (ri.viaWay == c.v1 && !closures.contains(ri)) {
					// F's traffic is on the lead when it enters the copy
					put(restrictions, c.leadId, ri.toWay, ri.type, c.id);
				}
			}
		}
		for (long from : restrictions.keys()) {
			for (RestrictionInfo ri : new ArrayList<>(get(restrictions, from))) {
				if (ri.viaWay == 0 || ri.type > MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON) {
					continue;
				}
				Copy nested = copies.get(ri.viaWay + " " + ri.toWay);
				if (nested != null) {
					put(restrictions, from, nested.leadId, ri.type, ri.viaWay);
				}
			}
		}
	}

	private static List<RestrictionInfo> get(TLongObjectHashMap<List<RestrictionInfo>> m, long id) {
		List<RestrictionInfo> l = m.get(id);
		return l == null ? java.util.Collections.emptyList() : l;
	}

	private static RestrictionInfo put(TLongObjectHashMap<List<RestrictionInfo>> m, long from, long to, int type, long via) {
		List<RestrictionInfo> l = m.get(from);
		if (l == null) {
			l = new ArrayList<>();
			m.put(from, l);
		}
		for (RestrictionInfo ri : l) {
			if (ri.toWay == to && ri.viaWay == via && ri.type == type) {
				return ri;
			}
		}
		RestrictionInfo ri = new RestrictionInfo();
		ri.toWay = to;
		ri.type = type;
		ri.viaWay = via;
		l.add(ri);
		return ri;
	}

	private static <T> void add(TLongObjectHashMap<List<T>> m, long k, T v) {
		List<T> l = m.get(k);
		if (l == null) {
			l = new ArrayList<>();
			m.put(k, l);
		}
		l.add(v);
	}

	private static boolean isEnd(TLongArrayList l, long n) {
		return l.get(0) == n || l.get(l.size() - 1) == n;
	}

	private static long other(TLongArrayList l, long end) {
		return l.get(0) == end ? l.get(l.size() - 1) : l.get(0);
	}

	private static long endShared(TLongArrayList v, TLongArrayList f) {
		if (f.contains(v.get(0))) {
			return v.get(0);
		}
		if (f.contains(v.get(v.size() - 1))) {
			return v.get(v.size() - 1);
		}
		return 0;
	}

	private static boolean touches(TLongArrayList a, TLongArrayList b) {
		for (int i = 0; i < a.size(); i++) {
			if (b.contains(a.get(i))) {
				return true;
			}
		}
		return false;
	}

	private static int oneway(Way w) {
		String o = w.getTag("oneway");
		if ("yes".equals(o) || "true".equals(o) || "1".equals(o)) {
			return 1;
		}
		if ("-1".equals(o)) {
			return -1;
		}
		if ("no".equals(o)) {
			return 0;
		}
		String j = w.getTag("junction");
		String h = w.getTag("highway");
		if ("roundabout".equals(j) || "circular".equals(j) || "motorway".equals(h) || "motorway_link".equals(h)) {
			return 1;
		}
		return 0;
	}
}
