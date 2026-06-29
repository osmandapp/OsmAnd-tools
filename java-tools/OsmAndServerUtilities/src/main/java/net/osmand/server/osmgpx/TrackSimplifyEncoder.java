package net.osmand.server.osmgpx;

import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.MapUtils;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Track geometry: Douglas-Peucker simplification + compact byte storage.
 * Simplify for display zoom 12-13 (few points, no visible loss at those zooms),
 * but store coordinates on the finer z18 cell grid (~0.6 m) so deltas fit in ~1 byte.
 */
public class TrackSimplifyEncoder {

	public static final int SIMPLIFY_ZOOM = 13;
	public static final int STORE_ZOOM = 18;
	private static final int STORE_SHIFT = 31 - (STORE_ZOOM + 8);

	// Douglas-Peucker over track points
	public static List<WptPt> simplifyPoints(List<WptPt> points, int zoom) {
		if (points.size() < 3) {
			return points;
		}
		List<Node> in = new ArrayList<>(points.size());
		for (WptPt p : points) {
			in.add(new Node(p.getLatitude(), p.getLongitude(), -1));
		}
		List<Node> out = new ArrayList<>();
		OsmMapUtils.simplifyDouglasPeucker(in, zoom + 8, 3, out, false);
		List<WptPt> res = new ArrayList<>(out.size());
		for (Node n : out) {
			res.add(new WptPt(n.getLatitude(), n.getLongitude()));
		}
		return res;
	}

	public static GpxFile simplifyGpx(GpxFile gpxFile, int zoom) {
		GpxFile res = new GpxFile(null);
		Track track = new Track();
		res.getTracks().add(track);
		for (Track t : gpxFile.getTracks()) {
			for (TrkSegment seg : t.getSegments()) {
				TrkSegment ns = new TrkSegment();
				ns.getPoints().addAll(simplifyPoints(seg.getPoints(), zoom));
				track.getSegments().add(ns);
			}
		}
		return res;
	}

	// Compact byte format: per segment -> z18-grid x/y, delta + zigzag varint (as in BinaryMapIndexWriter).
	public static byte[] encodeGeometry(GpxFile gpxFile) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		List<TrkSegment> segments = new ArrayList<>();
		for (Track t : gpxFile.getTracks()) {
			segments.addAll(t.getSegments());
		}
		writeVarint(out, segments.size());
		for (TrkSegment seg : segments) {
			List<WptPt> pts = seg.getPoints();
			writeVarint(out, pts.size());
			int px = 0, py = 0;
			for (WptPt p : pts) {
				int x = MapUtils.get31TileNumberX(p.getLongitude()) >> STORE_SHIFT;
				int y = MapUtils.get31TileNumberY(p.getLatitude()) >> STORE_SHIFT;
				writeVarint(out, zigzag(x - px));
				writeVarint(out, zigzag(y - py));
				px = x;
				py = y;
			}
		}
		return out.toByteArray();
	}

	public static GpxFile decodeGeometry(byte[] data) {
		int[] pos = {0};
		GpxFile gpx = new GpxFile(null);
		Track track = new Track();
		gpx.getTracks().add(track);
		int segCount = readVarint(data, pos);
		for (int s = 0; s < segCount; s++) {
			TrkSegment seg = new TrkSegment();
			int n = readVarint(data, pos);
			int px = 0, py = 0;
			for (int i = 0; i < n; i++) {
				px += unzigzag(readVarint(data, pos));
				py += unzigzag(readVarint(data, pos));
				seg.getPoints().add(new WptPt(
						MapUtils.get31LatitudeY(py << STORE_SHIFT), MapUtils.get31LongitudeX(px << STORE_SHIFT)));
			}
			track.getSegments().add(seg);
		}
		return gpx;
	}

	private static int zigzag(int v) {
		return (v << 1) ^ (v >> 31);
	}

	private static int unzigzag(int v) {
		return (v >>> 1) ^ -(v & 1);
	}

	private static void writeVarint(ByteArrayOutputStream out, int value) {
		int v = value;
		while ((v & ~0x7F) != 0) {
			out.write((v & 0x7F) | 0x80);
			v >>>= 7;
		}
		out.write(v);
	}

	private static int readVarint(byte[] data, int[] pos) {
		int result = 0, shift = 0, b;
		do {
			b = data[pos[0]++] & 0xFF;
			result |= (b & 0x7F) << shift;
			shift += 7;
		} while ((b & 0x80) != 0);
		return result;
	}
}
