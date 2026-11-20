package net.osmand.server.utils;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * The IANA time zone of a point, from the polygons of timezone-boundary-builder (with oceans, zones with the same
 * clock since now merged), simplified by {@link TimezoneMapGenerator} into the resource timezones.bin.gz.
 * The file is loaded on the first call. A zone this JVM does not know yet (newer tzdata) is skipped, so its area
 * gets the neighbouring zone.
 */
public class TimezoneMapper {

	static final int MAGIC = 0x4F545A31; // "OTZ1"
	static final double SCALE = 1e5; // coordinates are stored in 1e-5 degrees
	private static final String RESOURCE = "/timezones.bin.gz";
	private static final int CELL = (int) SCALE; // the index is a grid of 1 x 1 degree cells
	private static final int LAT_CELLS = 180;
	private static final int LON_CELLS = 360;

	private static volatile TimezoneMapper instance;

	private final String source;
	private final String[] zones;
	private final boolean[] knownZones;
	private final short[] polygonZones;
	private final int[][][] polygons; // polygon -> rings (outer first) -> lat, lon pairs
	private final int[][] bboxes; // polygon -> min lat, min lon, max lat, max lon
	private final int[][] cells; // lat cell * LON_CELLS + lon cell -> polygons touching it

	public static String latLngToTimezoneString(double lat, double lng) {
		return getInstance().zoneAt(lat, lng);
	}

	public static TimezoneMapper getInstance() {
		TimezoneMapper mapper = instance;
		if (mapper == null) {
			synchronized (TimezoneMapper.class) {
				mapper = instance;
				if (mapper == null) {
					try (InputStream in = TimezoneMapper.class.getResourceAsStream(RESOURCE)) {
						if (in == null) {
							throw new IllegalStateException(RESOURCE + " is missing");
						}
						mapper = new TimezoneMapper(in);
					} catch (IOException e) {
						throw new IllegalStateException("can't read " + RESOURCE, e);
					}
					instance = mapper;
				}
			}
		}
		return mapper;
	}

	TimezoneMapper(InputStream input) throws IOException {
		DataInputStream in = new DataInputStream(new BufferedInputStream(new GZIPInputStream(input)));
		if (in.readInt() != MAGIC) {
			throw new IOException("not a time zone file");
		}
		source = in.readUTF();
		zones = new String[in.readInt()];
		knownZones = new boolean[zones.length];
		Set<String> available = ZoneId.getAvailableZoneIds();
		for (int i = 0; i < zones.length; i++) {
			zones[i] = in.readUTF();
			knownZones[i] = available.contains(zones[i]);
		}
		int count = in.readInt();
		polygonZones = new short[count];
		polygons = new int[count][][];
		bboxes = new int[count][];
		List<List<Integer>> cellLists = new ArrayList<>(LAT_CELLS * LON_CELLS);
		for (int i = 0; i < LAT_CELLS * LON_CELLS; i++) {
			cellLists.add(null);
		}
		for (int p = 0; p < count; p++) {
			polygonZones[p] = in.readShort();
			int[][] rings = new int[readVarint(in)][];
			int[] bbox = { Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE };
			for (int r = 0; r < rings.length; r++) {
				int[] ring = new int[readVarint(in) * 2];
				int lat = 0, lon = 0;
				for (int i = 0; i < ring.length; i += 2) {
					lat += unzigzag(readVarint(in));
					lon += unzigzag(readVarint(in));
					ring[i] = lat;
					ring[i + 1] = lon;
					bbox[0] = Math.min(bbox[0], lat);
					bbox[1] = Math.min(bbox[1], lon);
					bbox[2] = Math.max(bbox[2], lat);
					bbox[3] = Math.max(bbox[3], lon);
				}
				rings[r] = ring;
			}
			polygons[p] = rings;
			bboxes[p] = bbox;
			for (int cy = latCell(bbox[0]); cy <= latCell(bbox[2]); cy++) {
				for (int cx = lonCell(bbox[1]); cx <= lonCell(bbox[3]); cx++) {
					int cell = cy * LON_CELLS + cx;
					if (cellLists.get(cell) == null) {
						cellLists.set(cell, new ArrayList<>());
					}
					cellLists.get(cell).add(p);
				}
			}
		}
		cells = new int[cellLists.size()][];
		for (int i = 0; i < cells.length; i++) {
			List<Integer> list = cellLists.get(i);
			cells[i] = list == null ? new int[0] : list.stream().mapToInt(Integer::intValue).toArray();
		}
	}

	public String getSource() {
		return source;
	}

	// the polygon that contains the point; simplified neighbours may overlap or leave a gap at the border,
	// then the zone whose border is farthest inside (overlap) or nearest (gap) wins
	public String zoneAt(double latitude, double longitude) {
		int lat = (int) Math.round(Math.max(-90, Math.min(90, latitude)) * SCALE);
		int lon = (int) Math.round(normalizeLongitude(longitude) * SCALE);
		double lonScale = Math.cos(Math.toRadians(latitude));
		int cy = latCell(lat);
		int cx = lonCell(lon);

		int best = -1;
		int matches = 0;
		double bestDepth = 0;
		for (int p : cells[cy * LON_CELLS + cx]) {
			if (knownZones[polygonZones[p]] && inBbox(p, lat, lon) && contains(p, lat, lon)) {
				matches++;
				if (matches == 1) {
					best = p;
					continue;
				}
				if (matches == 2) {
					bestDepth = borderDistance(best, lat, lon, lonScale);
				}
				double depth = borderDistance(p, lat, lon, lonScale);
				if (depth > bestDepth) {
					best = p;
					bestDepth = depth;
				}
			}
		}
		if (best >= 0) {
			return zones[polygonZones[best]];
		}
		double nearest = Double.MAX_VALUE;
		for (int dy = -1; dy <= 1; dy++) {
			for (int dx = -1; dx <= 1; dx++) {
				int y = cy + dy;
				int x = Math.floorMod(cx + dx, LON_CELLS);
				if (y < 0 || y >= LAT_CELLS) {
					continue;
				}
				for (int p : cells[y * LON_CELLS + x]) {
					if (!knownZones[polygonZones[p]]) {
						continue;
					}
					double distance = borderDistance(p, lat, lon, lonScale);
					if (distance < nearest) {
						nearest = distance;
						best = p;
					}
				}
			}
		}
		if (best >= 0) {
			return zones[polygonZones[best]];
		}
		int offset = (int) Math.round(normalizeLongitude(longitude) / 15);
		return offset == 0 ? "Etc/GMT" : "Etc/GMT" + (offset > 0 ? "-" : "+") + Math.abs(offset); // nautical time
	}

	private boolean inBbox(int p, int lat, int lon) {
		int[] bbox = bboxes[p];
		return lat >= bbox[0] && lat <= bbox[2] && lon >= bbox[1] && lon <= bbox[3];
	}

	// even-odd rule over all rings, so holes are excluded
	private boolean contains(int p, int lat, int lon) {
		boolean inside = false;
		for (int[] ring : polygons[p]) {
			int n = ring.length;
			for (int i = 0, j = n - 2; i < n; j = i, i += 2) {
				int yi = ring[i], xi = ring[i + 1], yj = ring[j], xj = ring[j + 1];
				if ((yi > lat) != (yj > lat) && lon < (double) (xj - xi) * (lat - yi) / (yj - yi) + xi) {
					inside = !inside;
				}
			}
		}
		return inside;
	}

	// the distance to the nearest edge of the polygon, longitude scaled to the latitude
	private double borderDistance(int p, int lat, int lon, double lonScale) {
		double min = Double.MAX_VALUE;
		for (int[] ring : polygons[p]) {
			int n = ring.length;
			for (int i = 0, j = n - 2; i < n; j = i, i += 2) {
				double ay = ring[j], ax = ring[j + 1] * lonScale, by = ring[i], bx = ring[i + 1] * lonScale;
				double px = lon * lonScale, py = lat;
				double dx = bx - ax, dy = by - ay;
				double len = dx * dx + dy * dy;
				double t = len == 0 ? 0 : Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / len));
				double ex = ax + t * dx - px, ey = ay + t * dy - py;
				min = Math.min(min, ex * ex + ey * ey);
			}
		}
		return Math.sqrt(min);
	}

	private static double normalizeLongitude(double lon) {
		double l = ((lon + 180) % 360 + 360) % 360 - 180;
		return l == -180 && lon > 0 ? 180 : l;
	}

	private static int latCell(int lat) {
		return Math.max(0, Math.min(LAT_CELLS - 1, Math.floorDiv(lat, CELL) + LAT_CELLS / 2));
	}

	private static int lonCell(int lon) {
		return Math.max(0, Math.min(LON_CELLS - 1, Math.floorDiv(lon, CELL) + LON_CELLS / 2));
	}

	private static int unzigzag(int v) {
		return (v >>> 1) ^ -(v & 1);
	}

	private static int readVarint(DataInputStream in) throws IOException {
		int result = 0;
		for (int shift = 0; ; shift += 7) {
			int b = in.readUnsignedByte();
			result |= (b & 0x7F) << shift;
			if ((b & 0x80) == 0) {
				return result;
			}
		}
	}
}
