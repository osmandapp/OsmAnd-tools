package net.osmand.server.utils;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;

/**
 * Builds the time zone file of {@link TimezoneMapper} from the GeoJSON of timezone-boundary-builder
 * (https://github.com/evansiroky/timezone-boundary-builder/releases, timezones-with-oceans-now.geojson.zip):
 * the polygons are simplified and written as varint deltas of 1e-5 degrees, gzipped.
 * <p>
 * Usage: TimezoneMapGenerator &lt;geojson or zip&gt; &lt;out timezones.bin.gz&gt; &lt;tolerance in degrees&gt; &lt;source label&gt;
 */
public class TimezoneMapGenerator {

	public static void main(String[] args) throws IOException {
		Path input = Path.of(args[0]);
		Path output = Path.of(args[1]);
		double tolerance = Double.parseDouble(args[2]);
		String source = args[3];

		Map<String, Integer> zones = new LinkedHashMap<>();
		List<int[]> polygonZones = new ArrayList<>();
		List<List<int[]>> polygons = new ArrayList<>();
		GeometryFactory factory = new GeometryFactory();
		ObjectMapper mapper = new ObjectMapper();
		try (InputStream in = open(input); JsonParser parser = mapper.getFactory().createParser(in)) {
			while (parser.nextToken() != null) {
				if (parser.currentToken() == JsonToken.FIELD_NAME && "features".equals(parser.getCurrentName())) {
					parser.nextToken();
					while (parser.nextToken() == JsonToken.START_OBJECT) {
						JsonNode feature = mapper.readTree(parser);
						String tzid = feature.get("properties").get("tzid").asText();
						int zone = zones.computeIfAbsent(tzid, k -> zones.size());
						for (Polygon polygon : readPolygons(feature.get("geometry"), factory)) {
							Geometry simple = tolerance > 0 ? TopologyPreservingSimplifier.simplify(polygon, tolerance) : polygon;
							for (int i = 0; i < simple.getNumGeometries(); i++) {
								List<int[]> rings = toRings((Polygon) simple.getGeometryN(i));
								if (!rings.isEmpty()) {
									polygonZones.add(new int[] { zone });
									polygons.add(rings);
								}
							}
						}
					}
				}
			}
		}

		long points = 0;
		try (DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(new GZIPOutputStream(Files.newOutputStream(output))))) {
			out.writeInt(TimezoneMapper.MAGIC);
			out.writeUTF(source);
			out.writeInt(zones.size());
			for (String zone : zones.keySet()) {
				out.writeUTF(zone);
			}
			out.writeInt(polygons.size());
			for (int p = 0; p < polygons.size(); p++) {
				out.writeShort(polygonZones.get(p)[0]);
				List<int[]> rings = polygons.get(p);
				writeVarint(out, rings.size());
				for (int[] ring : rings) {
					writeVarint(out, ring.length / 2);
					int lat = 0, lon = 0;
					for (int i = 0; i < ring.length; i += 2) {
						writeVarint(out, zigzag(ring[i] - lat));
						writeVarint(out, zigzag(ring[i + 1] - lon));
						lat = ring[i];
						lon = ring[i + 1];
					}
					points += ring.length / 2;
				}
			}
		}
		System.out.printf("%d zones, %d polygons, %d points, %d bytes%n", zones.size(), polygons.size(), points,
				Files.size(output));
	}

	private static InputStream open(Path input) throws IOException {
		InputStream in = Files.newInputStream(input);
		if (!input.toString().endsWith(".zip")) {
			return in;
		}
		ZipInputStream zip = new ZipInputStream(in);
		ZipEntry entry;
		while ((entry = zip.getNextEntry()) != null) {
			if (entry.getName().endsWith("json")) {
				return zip;
			}
		}
		throw new IOException("no json in " + input);
	}

	private static List<Polygon> readPolygons(JsonNode geometry, GeometryFactory factory) {
		List<Polygon> result = new ArrayList<>();
		String type = geometry.get("type").asText();
		JsonNode coordinates = geometry.get("coordinates");
		if ("Polygon".equals(type)) {
			result.add(readPolygon(coordinates, factory));
		} else if ("MultiPolygon".equals(type)) {
			for (JsonNode polygon : coordinates) {
				result.add(readPolygon(polygon, factory));
			}
		} else {
			throw new IllegalArgumentException("unexpected geometry " + type);
		}
		return result;
	}

	private static Polygon readPolygon(JsonNode rings, GeometryFactory factory) {
		LinearRing shell = readRing(rings.get(0), factory);
		LinearRing[] holes = new LinearRing[rings.size() - 1];
		for (int i = 1; i < rings.size(); i++) {
			holes[i - 1] = readRing(rings.get(i), factory);
		}
		return factory.createPolygon(shell, holes);
	}

	private static LinearRing readRing(JsonNode ring, GeometryFactory factory) {
		Coordinate[] coordinates = new Coordinate[ring.size()];
		for (int i = 0; i < ring.size(); i++) {
			coordinates[i] = new Coordinate(ring.get(i).get(0).asDouble(), ring.get(i).get(1).asDouble());
		}
		return factory.createLinearRing(coordinates);
	}

	// rings as lat, lon pairs in 1e-5 degrees, without the closing point; the outer ring first
	private static List<int[]> toRings(Polygon polygon) {
		List<int[]> rings = new ArrayList<>();
		if (polygon.isEmpty()) {
			return rings;
		}
		rings.add(toRing(polygon.getExteriorRing().getCoordinates()));
		for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
			rings.add(toRing(polygon.getInteriorRingN(i).getCoordinates()));
		}
		return rings;
	}

	private static int[] toRing(Coordinate[] coordinates) {
		int n = coordinates.length - 1;
		int[] ring = new int[n * 2];
		for (int i = 0; i < n; i++) {
			ring[i * 2] = (int) Math.round(coordinates[i].y * TimezoneMapper.SCALE);
			ring[i * 2 + 1] = (int) Math.round(coordinates[i].x * TimezoneMapper.SCALE);
		}
		return ring;
	}

	private static int zigzag(int v) {
		return (v << 1) ^ (v >> 31);
	}

	private static void writeVarint(DataOutputStream out, int v) throws IOException {
		while ((v & ~0x7F) != 0) {
			out.writeByte((v & 0x7F) | 0x80);
			v >>>= 7;
		}
		out.writeByte(v);
	}
}
