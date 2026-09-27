package net.osmand.obf.preparation;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

/**
 * Coastline of the basemap levels without topology errors.
 * <ul>
 * <li>Coastline ways are glued into whole lines first, so the area filter of the small levels sees whole islands
 * (an island drawn as two open ways survived while the water around it was dropped).</li>
 * <li>Douglas-Peucker simplifies every tile piece on its own, so simplified pieces may cross or touch each other and
 * the rendered land and sea flip. Such segments get their farthest dropped point back until nothing crosses; where
 * even the original points cross at the stored precision (&lt; 1 m), a point is dropped instead.</li>
 * </ul>
 */
public class BasemapCoastlines {

	// BinaryMapIndexWriter stores x >> SHIFT_COORDINATES: pieces are checked at that precision
	private static final int SHIFT = BinaryMapIndexReader.SHIFT_COORDINATES;
	private static final int SIMPLIFY_EPSILON = 3;
	private static final int MAX_PASSES = 200;

	// the map object of a piece
	public interface PieceOutput {
		void setCoordinates(byte[] coordinates);

		void remove();
	}

	private final Log log;
	private List<Way> ways = new ArrayList<Way>();
	private final Map<Integer, List<Piece>> levels = new HashMap<Integer, List<Piece>>();

	public BasemapCoastlines(Log log) {
		this.log = log;
	}

	// keeps a coastline way until glue(); false after it
	public boolean collect(Way way) {
		if (ways == null) {
			return false;
		}
		Way copy = new Way(way.getId(), new ArrayList<Node>(way.getNodes()));
		copy.replaceTags(way.getTags());
		ways.add(copy);
		return true;
	}

	// glues the collected ways end to start into whole lines and hands every line to the processor
	public void glue(Consumer<Way> processor) {
		List<Way> ways = this.ways;
		this.ways = null;
		if (ways == null) {
			return;
		}
		TLongObjectHashMap<TIntArrayList> byStart = new TLongObjectHashMap<TIntArrayList>();
		for (int i = 0; i < ways.size(); i++) {
			if (ways.get(i).getNodes().size() >= 2) {
				long start = key(ways.get(i).getFirstNode());
				if (!byStart.containsKey(start)) {
					byStart.put(start, new TIntArrayList());
				}
				byStart.get(start).add(i);
			}
		}
		boolean[] used = new boolean[ways.size()];
		int lines = 0;
		for (int i = 0; i < ways.size(); i++) {
			if (used[i] || ways.get(i).getNodes().size() < 2) {
				continue;
			}
			used[i] = true;
			List<Node> line = new ArrayList<Node>(ways.get(i).getNodes());
			long start = key(line.get(0));
			int next;
			while (key(last(line)) != start && (next = unused(byStart.get(key(last(line))), used)) >= 0) {
				used[next] = true;
				List<Node> add = ways.get(next).getNodes();
				line.addAll(add.subList(1, add.size()));
			}
			Way glued = new Way(ways.get(i).getId(), line);
			glued.replaceTags(ways.get(i).getTags());
			processor.accept(glued);
			lines++;
		}
		log(String.format("Coastline: %d ways glued into %d lines", ways.size(), lines));
	}

	public Piece addPiece(int level, List<Node> nodes, int simplifyZoom) {
		Piece p = new Piece(nodes, simplifyZoom);
		levels.computeIfAbsent(level, l -> new ArrayList<Piece>()).add(p);
		return p;
	}

	// makes the pieces of a level cross or touch nothing, then drops the pieces shorter than the stored precision
	public void fixTopology(int level, Object levelName) {
		List<Piece> pieces = levels.remove(level);
		if (pieces == null) {
			return;
		}
		long time = System.currentTimeMillis();
		int added = 0, dropped = 0, pass = 0;
		Map<Piece, TIntArrayList> conflicts = findConflicts(pieces);
		for (; !conflicts.isEmpty() && pass < MAX_PASSES; pass++) {
			boolean changed = false;
			for (Entry<Piece, TIntArrayList> e : conflicts.entrySet()) {
				if (e.getKey().addBackPoints(e.getValue())) {
					changed = true;
					added++;
				}
			}
			if (!changed) {
				// every point is there and it still crosses at the stored precision
				for (Entry<Piece, TIntArrayList> e : conflicts.entrySet()) {
					if (e.getKey().dropPoints(e.getValue())) {
						changed = true;
						dropped++;
					}
				}
			}
			if (!changed) {
				break;
			}
			conflicts = findConflicts(pieces);
		}
		int removed = 0;
		for (Piece p : pieces) {
			if (p.isPoint()) {
				// its neighbours already meet in this point
				p.output.remove();
				removed++;
			}
		}
		log(String.format("Coastline level %s: %d pieces, %d got points back, %d dropped points, %d removed as shorter "
				+ "than the precision, %d still cross (%d passes, %d ms)", levelName, pieces.size(), added, dropped,
				removed, conflicts.size(), pass, System.currentTimeMillis() - time));
	}

	/** A piece of coastline inside one tile, simplified on its own. */
	public static class Piece {
		private final List<Node> nodes;
		private final int simplifyZoom;
		// points put back to resolve a conflict and points dropped for it
		private final TIntHashSet added = new TIntHashSet();
		private final TIntHashSet dropped = new TIntHashSet();
		private PieceOutput output;
		// the points written: indexes into nodes and coordinates at the stored precision
		private int[] kept;
		private int[] qx;
		private int[] qy;

		Piece(List<Node> nodes, int simplifyZoom) {
			this.nodes = nodes;
			this.simplifyZoom = simplifyZoom;
		}

		public void setOutput(PieceOutput output) {
			this.output = output;
		}

		public List<Node> simplify() {
			boolean[] keep = OsmMapUtils.simplifyDouglasPeucker(nodes, simplifyZoom, SIMPLIFY_EPSILON,
					new ArrayList<Node>(), true);
			// Douglas-Peucker ends a nearly closed piece with its first point, but the next piece starts at the real end
			keep[0] = true;
			keep[nodes.size() - 1] = true;
			for (int i : added.toArray()) {
				keep[i] = true;
			}
			for (int i : dropped.toArray()) {
				keep[i] = false;
			}
			TIntArrayList points = new TIntArrayList();
			for (int i = 0; i < keep.length; i++) {
				if (keep[i]) {
					points.add(i);
				}
			}
			removeSpikes(points);
			kept = points.toArray();
			qx = new int[kept.length];
			qy = new int[kept.length];
			List<Node> res = new ArrayList<Node>(kept.length);
			for (int i = 0; i < kept.length; i++) {
				qx[i] = x(nodes.get(kept[i])) >> SHIFT;
				qy[i] = y(nodes.get(kept[i])) >> SHIFT;
				res.add(nodes.get(kept[i]));
			}
			return res;
		}

		// at the stored precision a narrow spike folds back on itself: A B A' with A' on the line A-B
		private void removeSpikes(TIntArrayList points) {
			for (int i = 1; i + 1 < points.size() && points.size() > 2; ) {
				Node a = nodes.get(points.get(i - 1)), b = nodes.get(points.get(i)), c = nodes.get(points.get(i + 1));
				long ax = x(a) >> SHIFT, ay = y(a) >> SHIFT, bx = x(b) >> SHIFT, by = y(b) >> SHIFT;
				long cx = x(c) >> SHIFT, cy = y(c) >> SHIFT;
				boolean collinear = (bx - ax) * (cy - ay) - (by - ay) * (cx - ax) == 0;
				boolean back = (bx - ax) * (cx - bx) + (by - ay) * (cy - by) < 0;
				if (collinear && back) {
					points.removeAt(i);
					i = Math.max(i - 1, 1);
				} else {
					i++;
				}
			}
		}

		// puts back the dropped point farthest from each segment; false when the segments are original edges
		boolean addBackPoints(TIntArrayList segments) {
			boolean changed = false;
			for (int k = 0; k < segments.size(); k++) {
				int from = kept[segments.get(k)], to = kept[segments.get(k) + 1];
				long ax = x(nodes.get(from)), ay = y(nodes.get(from)), bx = x(nodes.get(to)), by = y(nodes.get(to));
				double farthest = -1;
				int point = -1;
				for (int i = from + 1; i < to; i++) {
					if (added.contains(i) || dropped.contains(i)) {
						continue;
					}
					long px = x(nodes.get(i)), py = y(nodes.get(i));
					double d = ax == bx && ay == by ? Math.hypot(px - ax, py - ay)
							: Math.abs((bx - ax) * (double) (py - ay) - (by - ay) * (double) (px - ax));
					if (d > farthest) {
						farthest = d;
						point = i;
					}
				}
				if (point >= 0) {
					added.add(point);
					changed = true;
				}
			}
			if (changed) {
				write();
			}
			return changed;
		}

		// drops an inner end point of each segment; false when there is none left
		boolean dropPoints(TIntArrayList segments) {
			boolean changed = false;
			for (int k = 0; k < segments.size(); k++) {
				int s = segments.get(k);
				int point = s + 1 < kept.length - 1 ? kept[s + 1] : s > 0 ? kept[s] : -1;
				if (point > 0 && dropped.add(point)) {
					changed = true;
				}
			}
			if (changed) {
				write();
			}
			return changed;
		}

		boolean isPoint() {
			for (int i = 1; i < qx.length; i++) {
				if (qx[i] != qx[0] || qy[i] != qy[0]) {
					return false;
				}
			}
			return true;
		}

		private void write() {
			ByteArrayOutputStream coordinates = new ByteArrayOutputStream();
			try {
				for (Node n : simplify()) {
					Algorithms.writeInt(coordinates, x(n));
					Algorithms.writeInt(coordinates, y(n));
				}
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
			output.setCoordinates(coordinates.toByteArray());
		}
	}

	// ---------------------------------------------------------------- conflicts

	// cell shifts of the grid levels (in stored units): a segment is registered at the finest level where it spans
	// at most 2x2 cells and tested against the segments of its own and every coarser level
	private static final int GRID_SHIFT0 = 6, GRID_LEVELS = 11;

	// the segments of each piece that cross or touch a segment of any piece
	private static Map<Piece, TIntArrayList> findConflicts(List<Piece> pieces) {
		int n = 0;
		for (Piece p : pieces) {
			n += p.qx.length - 1;
		}
		int[] segPiece = new int[n], segIndex = new int[n];
		byte[] segLevel = new byte[n];
		List<TLongObjectHashMap<TIntArrayList>> grid = new ArrayList<TLongObjectHashMap<TIntArrayList>>();
		for (int l = 0; l < GRID_LEVELS; l++) {
			grid.add(new TLongObjectHashMap<TIntArrayList>());
		}
		int s = 0;
		for (int i = 0; i < pieces.size(); i++) {
			Piece p = pieces.get(i);
			for (int j = 0; j + 1 < p.qx.length; j++, s++) {
				segPiece[s] = i;
				segIndex[s] = j;
				int minX = Math.min(p.qx[j], p.qx[j + 1]), maxX = Math.max(p.qx[j], p.qx[j + 1]);
				int minY = Math.min(p.qy[j], p.qy[j + 1]), maxY = Math.max(p.qy[j], p.qy[j + 1]);
				int l = 0;
				while (l < GRID_LEVELS - 1 && ((maxX >> shift(l)) - (minX >> shift(l)) > 1
						|| (maxY >> shift(l)) - (minY >> shift(l)) > 1)) {
					l++;
				}
				segLevel[s] = (byte) l;
				for (long cx = minX >> shift(l); cx <= maxX >> shift(l); cx++) {
					for (long cy = minY >> shift(l); cy <= maxY >> shift(l); cy++) {
						long cell = cell(cx, cy);
						if (!grid.get(l).containsKey(cell)) {
							grid.get(l).put(cell, new TIntArrayList(4));
						}
						grid.get(l).get(cell).add(s);
					}
				}
			}
		}
		Map<Piece, TIntArrayList> conflicts = new LinkedHashMap<Piece, TIntArrayList>();
		for (s = 0; s < n; s++) {
			Piece pa = pieces.get(segPiece[s]);
			int sa = segIndex[s];
			long ax = pa.qx[sa], ay = pa.qy[sa], bx = pa.qx[sa + 1], by = pa.qy[sa + 1];
			for (int l = segLevel[s]; l < GRID_LEVELS; l++) {
				int sh = shift(l);
				for (long cx = Math.min(ax, bx) >> sh; cx <= Math.max(ax, bx) >> sh; cx++) {
					for (long cy = Math.min(ay, by) >> sh; cy <= Math.max(ay, by) >> sh; cy++) {
						TIntArrayList inCell = grid.get(l).get(cell(cx, cy));
						for (int q = 0; inCell != null && q < inCell.size(); q++) {
							int t = inCell.get(q);
							Piece pb = pieces.get(segPiece[t]);
							int sb = segIndex[t];
							long px = pb.qx[sb], py = pb.qy[sb], qx = pb.qx[sb + 1], qy = pb.qy[sb + 1];
							boolean seenPair = l == segLevel[s] && t <= s;
							// each pair once: in the first cell both segments are in
							boolean firstCell = (Math.max(Math.min(ax, bx), Math.min(px, qx)) >> sh) == cx
									&& (Math.max(Math.min(ay, by), Math.min(py, qy)) >> sh) == cy;
							boolean neighbours = pa == pb && Math.abs(sa - sb) <= 1;
							if (!seenPair && firstCell && !neighbours && conflict(ax, ay, bx, by, px, py, qx, qy)) {
								addConflict(conflicts, pa, sa);
								addConflict(conflicts, pb, sb);
							}
						}
					}
				}
			}
		}
		return conflicts;
	}

	private static int shift(int level) {
		return GRID_SHIFT0 + 2 * level;
	}

	private static long cell(long cx, long cy) {
		return (cx << 32) | (cy & 0xffffffffL);
	}

	private static void addConflict(Map<Piece, TIntArrayList> conflicts, Piece p, int segment) {
		TIntArrayList l = conflicts.computeIfAbsent(p, k -> new TIntArrayList());
		if (!l.contains(segment)) {
			l.add(segment);
		}
	}

	// segments cross, an end point lies inside the other segment, or they coincide; a shared end point is fine
	private static boolean conflict(long ax, long ay, long bx, long by, long px, long py, long qx, long qy) {
		if (Math.max(ax, bx) < Math.min(px, qx) || Math.max(px, qx) < Math.min(ax, bx)
				|| Math.max(ay, by) < Math.min(py, qy) || Math.max(py, qy) < Math.min(ay, by)) {
			return false;
		}
		int o1 = orient(ax, ay, bx, by, px, py), o2 = orient(ax, ay, bx, by, qx, qy);
		int o3 = orient(px, py, qx, qy, ax, ay), o4 = orient(px, py, qx, qy, bx, by);
		boolean same = (ax == px && ay == py && bx == qx && by == qy) || (ax == qx && ay == qy && bx == px && by == py);
		return (o1 * o2 < 0 && o3 * o4 < 0) || same
				|| (o1 == 0 && inside(ax, ay, bx, by, px, py)) || (o2 == 0 && inside(ax, ay, bx, by, qx, qy))
				|| (o3 == 0 && inside(px, py, qx, qy, ax, ay)) || (o4 == 0 && inside(px, py, qx, qy, bx, by));
	}

	private static int orient(long ax, long ay, long bx, long by, long cx, long cy) {
		return Long.signum((bx - ax) * (cy - ay) - (by - ay) * (cx - ax));
	}

	// p on the segment a-b (known to be on its line), not at an end
	private static boolean inside(long ax, long ay, long bx, long by, long px, long py) {
		return !(px == ax && py == ay) && !(px == bx && py == by)
				&& Math.min(ax, bx) <= px && px <= Math.max(ax, bx) && Math.min(ay, by) <= py && py <= Math.max(ay, by);
	}

	// ---------------------------------------------------------------- helpers

	private static int x(Node n) {
		return MapUtils.get31TileNumberX(n.getLongitude());
	}

	private static int y(Node n) {
		return MapUtils.get31TileNumberY(n.getLatitude());
	}

	private static long key(Node n) {
		return ((long) x(n) << 32) | (y(n) & 0xffffffffL);
	}

	private static Node last(List<Node> line) {
		return line.get(line.size() - 1);
	}

	private static int unused(TIntArrayList candidates, boolean[] used) {
		for (int i = 0; candidates != null && i < candidates.size(); i++) {
			if (!used[candidates.get(i)]) {
				return candidates.get(i);
			}
		}
		return -1;
	}

	private void log(String msg) {
		if (log != null) {
			log.info(msg);
		} else {
			System.out.println(msg);
		}
	}
}
