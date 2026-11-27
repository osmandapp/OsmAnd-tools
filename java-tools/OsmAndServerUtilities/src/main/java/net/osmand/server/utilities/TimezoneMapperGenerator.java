package net.osmand.server.utilities;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.zip.ZipFile;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.precision.EnhancedPrecisionOp;

import net.osmand.MainUtilities.CommandLineOpts;
import net.osmand.server.ServerUtilities;

/** Generates a standalone mapper using Tim Cooper's simplification and KD-tree approach. */
public class TimezoneMapperGenerator {

	private static final String RELEASES_URL =
			"https://api.github.com/repos/evansiroky/timezone-boundary-builder/releases/";
	private static final String SOURCE_URL =
			"https://github.com/evansiroky/timezone-boundary-builder/releases/download/";
	private static final String ASSET = "timezones.geojson.zip";
	static final String JAVA_FILE = TimezoneMapper.class.getSimpleName() + ".java";
	private static final HttpClient HTTP = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL)
			.connectTimeout(Duration.ofSeconds(30)).build();
	private static final double METRES_PER_DEGREE = Math.PI * 6371000 / 180;
	private static final double SIMPLIFY_METRES = 1500;
	private static final double MIN_DIAGONAL_METRES = 2000;
	private static final GeometryFactory GEOMETRY = new GeometryFactory();
	private final List<String> timezones = new ArrayList<>(List.of("unknown"));
	private final Map<String, Integer> timezoneIds = new LinkedHashMap<>();
	private final List<Shape> outputPolygons = new ArrayList<>();
	private long inputPoints;
	private long simplifiedPoints;

	public enum OutputFormat {
		JAVA
	}

	public record TimezoneBoundaryRelease(String version, URI polygons) {
	}

	/** Generates the mapper in --output (default: current working directory). */
	public static void main(String[] args) throws Exception {
		CommandLineOpts opts = new CommandLineOpts(args);
		if (opts.getBoolean("--help")) {
			System.out.println("""
					Usage: utilities.sh generate-timezone-mapper [options]

					Downloads timezone-boundary-builder polygons and generates a standalone
					coordinate-to-timezone mapper using simplification and a KD-tree.

					Options:
					  --release=TAG   timezone-boundary-builder release (default: latest).
					  --format=java   Output format (supported: java; default: java).
					  --output=DIR    Output directory (default: current working directory).
					  --help          Show this help.

					Output:
					  %s

					Review the result and replace the mapper source in OsmAndServerUtilities.
					Rebuild the utilities JAR, regenerate JSON with generate-timezone-test-data,
					and run TimezoneMapperTest to verify the updated mapper and resources.
					""".formatted(JAVA_FILE));
			return;
		}
		TimezoneBoundaryRelease release = fetchBoundaryRelease(opts.getOrDefault("--release", "latest"));
		OutputFormat format = OutputFormat.valueOf(opts.getOrDefault("--format", "java").toUpperCase(Locale.ROOT));
		Path outputDirectory = Path.of(opts.getOrDefault("--output", "."));
		generateMapper(release, outputDirectory.resolve(JAVA_FILE), format);
	}

	public static TimezoneBoundaryRelease fetchBoundaryRelease(String version)
			throws IOException, InterruptedException {
		String suffix = version.equals("latest") ? "latest" : "tags/" + version;
		String json = fetch(URI.create(RELEASES_URL + suffix),
				HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
		JsonObject release = new Gson().fromJson(json, JsonObject.class);
		for (JsonElement asset : release.getAsJsonArray("assets")) {
			JsonObject file = asset.getAsJsonObject();
			if (ASSET.equals(file.get("name").getAsString())) {
				return new TimezoneBoundaryRelease(
						release.get("tag_name").getAsString(),
						URI.create(file.get("browser_download_url").getAsString()));
			}
		}
		throw new IOException("No " + ASSET + " in timezone-boundary-builder release " + version);
	}

	public static void generateMapper(TimezoneBoundaryRelease release, Path output,
	                                  OutputFormat format) throws Exception {
		Path input = Files.createTempFile("timezone-mapper-", ".zip");
		try {
			generateMapper(release, download(release, input), output, format);
		} finally {
			Files.deleteIfExists(input);
		}
	}

	/** A local input also allows callers to regenerate repeatedly without downloading again. */
	public static void generateMapper(TimezoneBoundaryRelease release, Path input, Path output,
	                                  OutputFormat format) throws Exception {
		TimezoneMapperGenerator generator = new TimezoneMapperGenerator();
		List<Shape> shapes = generator.readPolygons(input);
		System.out.printf("Read %d zones, %d polygons; simplified %d to %d points%n",
				generator.timezones.size() - 1, shapes.size(),
				generator.inputPoints, generator.simplifiedPoints);
		Node root = generator.build(shapes, new Envelope(-180, 180, -90, 90), 0);
		String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(input)));
		Files.createDirectories(output.toAbsolutePath().getParent());
		switch (format) {
			case JAVA -> generator.writeJava(root, output, release.version(), hash);
		}
		System.out.printf("Generated %s (%s): %d KD-tree nodes, %d polygon tests%n",
				output.getFileName(), release.version(), root.size, generator.outputPolygons.size());
	}

	private static Path download(TimezoneBoundaryRelease release, Path input)
			throws IOException, InterruptedException {
		System.out.println("Downloading timezone-boundary-builder " + release.version());
		return fetch(release.polygons(), HttpResponse.BodyHandlers.ofFile(input));
	}

	static <T> T fetch(URI uri, HttpResponse.BodyHandler<T> handler) throws IOException, InterruptedException {
		HttpRequest request = HttpRequest.newBuilder(uri).header("User-Agent", ServerUtilities.USER_AGENT)
				.timeout(Duration.ofMinutes(10)).build();
		HttpResponse<T> response;
		try {
			response = HTTP.send(request, handler);
		} catch (IOException e) {
			throw new IOException("Failed to fetch " + uri + ": " + e, e);
		}
		if (response.statusCode() != 200) {
			throw new IOException(uri + ": HTTP " + response.statusCode());
		}
		return response.body();
	}

	private List<Shape> readPolygons(Path input) throws IOException {
		List<Shape> shapes = new ArrayList<>();
		Gson gson = new Gson();
		try (ZipFile zip = new ZipFile(input.toFile());
				JsonReader reader = new JsonReader(new InputStreamReader(
						zip.getInputStream(zip.getEntry("combined.json")), StandardCharsets.UTF_8))) {
			reader.beginObject();
			while (reader.hasNext()) {
				if (!reader.nextName().equals("features")) {
					reader.skipValue();
					continue;
				}
				reader.beginArray();
				while (reader.hasNext()) {
					JsonObject feature = gson.fromJson(reader, JsonObject.class);
					String timezone = feature.getAsJsonObject("properties").get("tzid").getAsString();
					int id = timezoneIds.computeIfAbsent(timezone, key -> {
						timezones.add(key);
						return timezones.size() - 1;
					});
					JsonObject geometry = feature.getAsJsonObject("geometry");
					JsonArray coordinates = geometry.getAsJsonArray("coordinates");
					switch (geometry.get("type").getAsString()) {
						case "Polygon" -> readPolygon(coordinates, id, shapes);
						case "MultiPolygon" -> {
							for (JsonElement polygon : coordinates) {
								readPolygon(polygon.getAsJsonArray(), id, shapes);
							}
						}
						default -> throw new IllegalArgumentException("Unsupported geometry: " + geometry.get("type"));
					}
				}
				reader.endArray();
			}
			reader.endObject();
		}
		if (shapes.isEmpty()) {
			throw new IllegalArgumentException("No timezone polygons in " + input);
		}
		return shapes;
	}

	private void readPolygon(JsonArray rings, int timezone, List<Shape> shapes) {
		LinearRing shell = ring(rings.get(0).getAsJsonArray());
		LinearRing[] holes = new LinearRing[rings.size() - 1];
		for (int i = 1; i < rings.size(); i++) {
			holes[i - 1] = ring(rings.get(i).getAsJsonArray());
		}
		Polygon polygon = GEOMETRY.createPolygon(shell, holes);
		inputPoints += polygon.getNumPoints();
		Envelope box = polygon.getEnvelopeInternal();
		double xScale = METRES_PER_DEGREE
				* Math.max(0.01, Math.cos(Math.toRadians((box.getMinY() + box.getMaxY()) / 2)));
		if (Math.hypot(box.getWidth() * xScale, box.getHeight() * METRES_PER_DEGREE) < MIN_DIAGONAL_METRES) {
			return;
		}
		LinearRing simplifiedShell = simplifyRing(shell.getCoordinates());
		if (simplifiedShell == null) {
			return;
		}
		List<LinearRing> simplifiedHoles = new ArrayList<>();
		for (LinearRing hole : holes) {
			LinearRing simplifiedHole = simplifyRing(hole.getCoordinates());
			if (simplifiedHole != null) {
				simplifiedHoles.add(simplifiedHole);
			}
		}
		Geometry simplified = GEOMETRY.createPolygon(simplifiedShell, simplifiedHoles.toArray(new LinearRing[0]));
		if (!simplified.isValid()) {
			simplified = simplified.buffer(0);
		}
		List<Shape> components = new ArrayList<>();
		addPolygons(simplified, timezone, components);
		for (Shape shape : components) {
			Envelope bounds = shape.box;
			if (Math.hypot(bounds.getWidth() * xScale, bounds.getHeight() * METRES_PER_DEGREE) >= MIN_DIAGONAL_METRES) {
				simplifiedPoints += shape.polygon.getNumPoints();
				shapes.add(shape);
			}
		}
	}

	private static LinearRing ring(JsonArray points) {
		Coordinate[] coordinates = new Coordinate[points.size()];
		for (int i = 0; i < coordinates.length; i++) {
			JsonArray point = points.get(i).getAsJsonArray();
			coordinates[i] = new Coordinate(point.get(0).getAsDouble(), point.get(1).getAsDouble());
		}
		return GEOMETRY.createLinearRing(coordinates);
	}

	private static LinearRing simplifyRing(Coordinate[] points) {
		// As in the original converter, keep all four extrema and simplify the arcs between them.
		int left = 0, right = 0, top = 0, bottom = 0;
		for (int i = 1; i < points.length - 1; i++) {
			if (points[i].x < points[left].x) left = i;
			if (points[i].x > points[right].x) right = i;
			if (points[i].y < points[bottom].y) bottom = i;
			if (points[i].y > points[top].y) top = i;
		}
		List<Integer> anchors = new ArrayList<>(new TreeSet<>(List.of(left, right, top, bottom)));
		List<Coordinate> result = new ArrayList<>();
		for (int i = 0; i < anchors.size(); i++) {
			int start = anchors.get(i);
			int end = anchors.get((i + 1) % anchors.size());
			if (end <= start) end += points.length - 1;
			simplifyArc(points, start, end, result);
		}
		if (result.size() < 3) {
			return null;
		}
		result.add(result.get(0));
		return GEOMETRY.createLinearRing(result.toArray(new Coordinate[0]));
	}

	private static void simplifyArc(Coordinate[] points, int start, int end, List<Coordinate> result) {
		int count = points.length - 1;
		Coordinate a = points[start % count];
		Coordinate b = points[end % count];
		double maximum = SIMPLIFY_METRES;
		int furthest = -1;
		for (int i = start + 1; i < end; i++) {
			Coordinate point = points[i % count];
			double ax = (a.x - point.x) * Math.cos(Math.toRadians((a.y + point.y) / 2));
			double ay = a.y - point.y;
			double bx = (b.x - point.x) * Math.cos(Math.toRadians((b.y + point.y) / 2));
			double by = b.y - point.y;
			double dx = bx - ax, dy = by - ay;
			double length = dx * dx + dy * dy;
			double fraction = length == 0 ? 0 : Math.max(0, Math.min(1, -(ax * dx + ay * dy) / length));
			double distance = Math.hypot(ax + fraction * dx, ay + fraction * dy) * METRES_PER_DEGREE;
			if (distance > maximum) {
				maximum = distance;
				furthest = i;
			}
		}
		if (furthest < 0) {
			result.add(a);
		} else {
			simplifyArc(points, start, furthest, result);
			simplifyArc(points, furthest, end, result);
		}
	}

	private static void addPolygons(Geometry geometry, int timezone, List<Shape> shapes) {
		if (geometry.isEmpty()) {
			return;
		}
		if (geometry instanceof Polygon polygon) {
			shapes.add(new Shape(timezone, polygon));
		} else if (geometry instanceof GeometryCollection) {
			for (int i = 0; i < geometry.getNumGeometries(); i++) {
				addPolygons(geometry.getGeometryN(i), timezone, shapes);
			}
		}
	}

	private Node build(List<Shape> shapes, Envelope cell, int depth) {
		Node node = new Node();
		if (shapes.isEmpty()) {
			return node;
		}
		Map<Integer, Integer> weights = new LinkedHashMap<>();
		Envelope bounds = new Envelope();
		for (Shape shape : shapes) {
			weights.merge(shape.timezone, shape.polygon.getNumPoints(), Integer::sum);
			bounds.expandToInclude(shape.box);
		}
		// Use the most complex timezone as the default to avoid testing its polygons.
		node.timezone = weights.entrySet().stream().max(Map.Entry.comparingByValue()).orElseThrow().getKey();
		if (weights.size() == 1) {
			return node;
		}
		int complexity = weights.values().stream().mapToInt(Integer::intValue).sum() - weights.get(node.timezone);
		if (complexity <= 50 || depth >= 32) {
			shapes.sort(Comparator.comparingInt(shape -> shape.polygon.getNumPoints()));
			node.shapes = shapes;
			return node;
		}
		double bestScore = (double) shapes.size() * shapes.size();
		boolean found = false;
		for (boolean latitude : new boolean[] { false, true }) {
			TreeSet<Float> candidates = new TreeSet<>();
			for (Shape shape : shapes) {
				candidates.add((float) (latitude ? shape.box.getMinY() : shape.box.getMinX()));
				candidates.add((float) (latitude ? shape.box.getMaxY() : shape.box.getMaxX()));
			}
			for (float pivot : candidates) {
				double low = latitude ? cell.getMinY() : cell.getMinX();
				double high = latitude ? cell.getMaxY() : cell.getMaxX();
				if (pivot <= low + 0.000001 || pivot >= high - 0.000001) {
					continue;
				}
				int left = 0, right = 0, leftZone = -1, rightZone = -1;
				for (Shape shape : shapes) {
					if ((latitude ? shape.box.getMinY() : shape.box.getMinX()) < pivot) {
						left++;
						leftZone = leftZone == -1 ? shape.timezone : leftZone == shape.timezone ? leftZone : 0;
					}
					if ((latitude ? shape.box.getMaxY() : shape.box.getMaxX()) > pivot) {
						right++;
						rightZone = rightZone == -1 ? shape.timezone : rightZone == shape.timezone ? rightZone : 0;
					}
				}
				left = leftZone > 0 ? 1 : left;
				right = rightZone > 0 ? 1 : right;
				double score = (double) left * left + (double) right * right;
				if (score < bestScore) {
					bestScore = score;
					node.latitude = latitude;
					node.pivot = pivot;
					found = true;
				}
			}
		}
		if (!found) {
			node.latitude = bounds.getHeight() > bounds.getWidth();
			node.pivot = (float) (node.latitude ? (bounds.getMinY() + bounds.getMaxY()) / 2
					: (bounds.getMinX() + bounds.getMaxX()) / 2);
		}
		Envelope leftCell = new Envelope(cell);
		Envelope rightCell = new Envelope(cell);
		if (node.latitude) {
			leftCell.init(cell.getMinX(), cell.getMaxX(), cell.getMinY(), node.pivot);
			rightCell.init(cell.getMinX(), cell.getMaxX(), node.pivot, cell.getMaxY());
		} else {
			leftCell.init(cell.getMinX(), node.pivot, cell.getMinY(), cell.getMaxY());
			rightCell.init(node.pivot, cell.getMaxX(), cell.getMinY(), cell.getMaxY());
		}
		node.left = build(clip(shapes, leftCell), leftCell, depth + 1);
		node.right = build(clip(shapes, rightCell), rightCell, depth + 1);
		node.size += node.left.size + node.right.size;
		return node;
	}

	private static List<Shape> clip(List<Shape> shapes, Envelope cell) {
		List<Shape> clipped = new ArrayList<>();
		Geometry rectangle = GEOMETRY.toGeometry(cell);
		for (Shape shape : shapes) {
			if (cell.contains(shape.box)) {
				clipped.add(shape);
			} else if (cell.intersects(shape.box)) {
				addPolygons(EnhancedPrecisionOp.intersection(shape.polygon, rectangle), shape.timezone, clipped);
			}
		}
		return clipped;
	}

	private void writeJava(Node root, Path output, String release, String hash) throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
			writer.write("/* Generated by TimezoneMapperGenerator. DO NOT EDIT.\n"
					+ " * Source: " + SOURCE_URL + release + "/" + ASSET + "\n"
					+ " * Source SHA-256: " + hash + "\n"
					+ " * Data: OpenStreetMap contributors, ODbL 1.0: https://www.openstreetmap.org/copyright\n"
					+ " * Approach: Tim Cooper, LatLongToTimezone (MIT): "
					+ "https://github.com/drtimcooper/LatLongToTimezone\n"
					+ " * Douglas-Peucker tolerance: " + SIMPLIFY_METRES
					+ " m; minimum polygon diagonal: " + MIN_DIAGONAL_METRES + " m.\n"
					+ " */\npackage " + TimezoneMapper.class.getPackageName() + ";\n\npublic class "
					+ TimezoneMapper.class.getSimpleName() + " {\n"
					+ "\tpublic static String latLngToTimezoneString(double lat, double lng) {\n"
					+ "\t\tif (!Double.isFinite(lat) || !Double.isFinite(lng) || lat < -90 || lat > 90 "
					+ "|| lng < -180 || lng > 180) return \"unknown\";\n"
					+ "\t\treturn TIMEZONES[lookup0((float) lat, (float) lng)];\n\t}\n\n"
					+ "\tprivate static final String[] TIMEZONES = {\n");
			for (String timezone : timezones) {
				writer.write("\t\t\"" + timezone + "\",\n");
			}
			writer.write("\t};\n\n");
			List<Node> methods = new ArrayList<>(List.of(root));
			for (int i = 0; i < methods.size(); i++) {
				writer.write("\tprivate static int lookup" + i + "(float lat, float lng) {\n");
				writeNode(methods.get(i), writer, "\t\t", methods);
				writer.write("\t}\n\n");
			}
			writer.write("\tprivate static final TzPolygon[] POLYGONS = initPolygons();\n\n");
			List<Integer> initializers = new ArrayList<>();
			for (int start = 0; start < outputPolygons.size();) {
				int id = initializers.size();
				initializers.add(id);
				writer.write("\tprivate static class Initializer" + id
						+ " {\n\t\tstatic void init(TzPolygon[] polygons) {\n");
				int points = 0;
				do {
					Polygon polygon = outputPolygons.get(start).polygon;
					writer.write("\t\t\tpolygons[" + start++ + "] = new TzPolygon(");
					writeRing(polygon.getExteriorRing().getCoordinates(), writer);
					for (int h = 0; h < polygon.getNumInteriorRing(); h++) {
						writer.write(", ");
						writeRing(polygon.getInteriorRingN(h).getCoordinates(), writer);
					}
					writer.write(");\n");
					points += polygon.getNumPoints();
				} while (start < outputPolygons.size() && points < 750);
				writer.write("\t\t}\n\t}\n\n");
			}
			writer.write("\tprivate static TzPolygon[] initPolygons() {\n\t\tTzPolygon[] polygons = new TzPolygon["
					+ outputPolygons.size() + "];\n");
			for (int id : initializers) {
				writer.write("\t\tInitializer" + id + ".init(polygons);\n");
			}
			writer.write("\t\treturn polygons;\n\t}\n\n");
			writer.write("""
						private static class TzPolygon {
							private final float[][] rings;
							TzPolygon(float[]... rings) { this.rings = rings; }
							boolean contains(float lat, float lng) {
								if (!inside(rings[0], lat, lng)) return false;
								for (int i = 1; i < rings.length; i++) {
									if (inside(rings[i], lat, lng)) return false;
								}
								return true;
							}
							private static boolean inside(float[] points, float lat, float lng) {
								boolean inside = false;
								for (int i = 0, j = points.length - 2; i < points.length; j = i, i += 2) {
									double yi = points[i], xi = points[i + 1], yj = points[j], xj = points[j + 1];
									if ((yi > lat) != (yj > lat) && lng < (xj - xi) * (lat - yi) / (yj - yi) + xi) {
										inside = !inside;
									}
								}
								return inside;
							}
						}
					}
					""");
		}
	}

	private void writeNode(Node node, BufferedWriter writer, String indent, List<Node> methods) throws IOException {
		if (node.left != null) {
			writer.write(indent + "if (" + (node.latitude ? "lat" : "lng") + " < " + node.pivot + "f) {\n");
			writeChild(node.left, writer, indent + "\t", methods);
			writer.write(indent + "} else {\n");
			writeChild(node.right, writer, indent + "\t", methods);
			writer.write(indent + "}\n");
		} else {
			if (node.shapes != null) {
				for (Shape shape : node.shapes) {
					if (shape.timezone != node.timezone) {
						writer.write(indent + "if (POLYGONS[" + outputPolygons.size()
								+ "].contains(lat, lng)) return " + shape.timezone + ";\n");
						outputPolygons.add(shape);
					}
				}
			}
			writer.write(indent + "return " + node.timezone + ";\n");
		}
	}

	private void writeChild(Node child, BufferedWriter writer, String indent, List<Node> methods) throws IOException {
		if (child.size > 100) {
			writer.write(indent + "return lookup" + methods.size() + "(lat, lng);\n");
			methods.add(child);
		} else {
			writeNode(child, writer, indent, methods);
		}
	}

	private static void writeRing(Coordinate[] points, BufferedWriter writer) throws IOException {
		writer.write("new float[] {");
		for (int i = 0; i < points.length - 1; i++) {
			if (i > 0) writer.write(", ");
			writer.write((float) (Math.round(points[i].y * 1e6) / 1e6) + "f, "
					+ (float) (Math.round(points[i].x * 1e6) / 1e6) + "f");
		}
		writer.write("}");
	}

	private static class Shape {
		final int timezone;
		final Polygon polygon;
		final Envelope box;
		Shape(int timezone, Polygon polygon) {
			this.timezone = timezone;
			this.polygon = polygon;
			box = polygon.getEnvelopeInternal();
		}
	}

	private static class Node {
		int timezone;
		int size = 1;
		boolean latitude;
		float pivot;
		Node left, right;
		List<Shape> shapes;
	}
}
