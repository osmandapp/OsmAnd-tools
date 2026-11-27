package net.osmand.server.utilities;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiFunction;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import net.osmand.MainUtilities.CommandLineOpts;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchPoiTypeFilter;
import net.osmand.data.Amenity;
import net.osmand.osm.PoiCategory;

/** Generates reference test data and provides shared timezone checks. */
public class TimezoneMapperTestDataGenerator {

	public static final String ALIASES_FILE = "TimezoneMapperAliases.json";
	public static final String IANA_FILE = "TimezoneMapperIANA.json";
	public static final String CITIES_FILE = "TimezoneMapperBasemapCities.json";
	private static final String IANA_SOURCE = "https://raw.githubusercontent.com/eggert/tz/";
	private static final String BASEMAP = "World_basemap_2.obf";
	private static final String BASEMAP_HINT =
			"Specify --basemap=/path/to/" + BASEMAP + " or set OBF_LOCATION=/path/to/maps.";
	private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

	/** Generates JSON files in --output (default: current working directory). */
	public static void main(String[] args) throws Exception {
		CommandLineOpts opts = new CommandLineOpts(args);
		if (opts.getBoolean("--help")) {
			System.out.println("""
					Usage: utilities.sh generate-timezone-test-data [options]

					Generates IANA timezone aliases/reference points and basemap city test data.
					Basemap expectations use the current TimezoneMapper compiled into this JAR.
					Rebuild this JAR first to use a changed mapper.

					Options:
					  --release=TAG   IANA version matching a timezone-boundary-builder release (default: latest).
					  --basemap=FILE  World basemap OBF (default: OBF_LOCATION/%s).
					  --output=DIR    Output directory (default: current working directory).
					  --help          Show this help.

					Outputs:
					  %s
					  %s
					  %s

					Review the generated JSON files and copy them to resources/test-resources/tz/.
					Rebuild and run TimezoneMapperTest to verify the updated resources.
					""".formatted(BASEMAP, ALIASES_FILE, IANA_FILE, CITIES_FILE));
			return;
		}
		String basemapOption = opts.getOpt("--basemap");
		Path basemap = basemapOption == null ? findWorldBasemap() : Path.of(basemapOption);
		Path outputDirectory = Path.of(opts.getOrDefault("--output", "."));
		String release = TimezoneMapperGenerator.fetchBoundaryRelease(
				opts.getOrDefault("--release", "latest")).version();
		generateTestData(release, basemap).writeJsonFiles(outputDirectory);
	}

	/** IANA inputs use exactly the same release tag as timezone-boundary-builder. */
	public static TimezoneTestData generateTestData(String release, Path basemap)
			throws IOException, InterruptedException {
		return generateTestData(release, basemap, TimezoneMapper::latLngToTimezoneString);
	}

	public static TimezoneTestData generateTestData(String release, Path basemap,
	                                                BiFunction<Double, Double, String> timezoneLookup)
			throws IOException, InterruptedException {
		String backward = TimezoneMapperGenerator.fetch(URI.create(IANA_SOURCE + release + "/backward"),
				HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
		String zones = TimezoneMapperGenerator.fetch(URI.create(IANA_SOURCE + release + "/zone1970.tab"),
				HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
		return new TimezoneTestData(generateTimezoneAliases(backward), generateIanaTestCases(zones),
				generateBasemapCityTestCases(basemap, timezoneLookup));
	}

	public static TimezoneTestData readTestDataResources() throws IOException {
		return new TimezoneTestData(readResource(ALIASES_FILE), readResource(IANA_FILE), readResource(CITIES_FILE));
	}

	private static JsonObject readResource(String filename) throws IOException {
		String resource = "/test-resources/tz/" + filename;
		try (InputStream input = TimezoneMapperTestDataGenerator.class.getResourceAsStream(resource);
				InputStreamReader reader = new InputStreamReader(
						Objects.requireNonNull(input, resource), StandardCharsets.UTF_8)) {
			return GSON.fromJson(reader, JsonObject.class);
		}
	}

	public static JsonObject generateTimezoneAliases(String backward) {
		Map<String, String> aliases = new TreeMap<>();
		for (String line : backward.lines().toList()) {
			String[] fields = line.trim().split("\\s+");
			if (fields.length >= 3 && fields[0].equals("Link")) {
				aliases.put(fields[2], fields[1]);
			}
		}
		if (aliases.isEmpty()) {
			throw new IllegalArgumentException("No IANA timezone aliases in backward");
		}
		return GSON.toJsonTree(aliases).getAsJsonObject();
	}

	public static JsonObject generateIanaTestCases(String zones) {
		JsonArray cases = new JsonArray();
		for (String line : zones.lines().toList()) {
			if (line.isBlank() || line.startsWith("#")) {
				continue;
			}
			String[] fields = line.split("\t");
			String coordinates = fields[1];
			int split = Math.max(coordinates.indexOf('+', 1), coordinates.indexOf('-', 1));
			String timezone = fields[2];
			JsonObject testCase = testCase(timezone.substring(timezone.lastIndexOf('/') + 1).replace('_', ' '),
					coordinate(coordinates.substring(0, split), 2),
					coordinate(coordinates.substring(split), 3), timezone);
			if (timezone.equals("Asia/Urumqi")) {
				// Both local Xinjiang time and Beijing time are used in Urumqi.
				JsonArray alternatives = new JsonArray();
				alternatives.add("Asia/Shanghai");
				testCase.add("alternatives", alternatives);
			}
			cases.add(testCase);
		}
		if (cases.size() == 0) {
			throw new IllegalArgumentException("No IANA reference locations in zone1970.tab");
		}
		JsonObject data = new JsonObject();
		data.add("cases", cases);
		return data;
	}

	public static JsonObject generateBasemapCityTestCases(Path basemap,
	                                                      BiFunction<Double, Double, String> timezoneLookup)
			throws IOException {
		List<Amenity> cities;
		try (RandomAccessFile file = new RandomAccessFile(basemap.toFile(), "r")) {
			BinaryMapIndexReader reader = new BinaryMapIndexReader(file, basemap.toFile());
			SearchPoiTypeFilter filter = new SearchPoiTypeFilter() {
				@Override
				public boolean accept(PoiCategory type, String subcategory) {
					return "administrative".equals(type.getKeyName()) && "city".equals(subcategory);
				}

				@Override
				public boolean isEmpty() {
					return false;
				}
			};
			cities = reader.searchPoi(BinaryMapIndexReader.buildSearchPoiRequest(
					0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, -1, filter, null));
		}
		cities.removeIf(city -> city.getName().isEmpty());
		if (cities.isEmpty()) {
			throw new IllegalArgumentException("No named administrative:city POIs in " + basemap.getFileName());
		}
		cities.sort(Comparator.comparing((Amenity city) -> city.getName())
				.thenComparingDouble(city -> city.getLocation().getLatitude())
				.thenComparingDouble(city -> city.getLocation().getLongitude()));
		JsonArray cases = new JsonArray();
		for (Amenity city : cities) {
			double lat = Math.round(city.getLocation().getLatitude() * 100_000) / 100_000.0;
			double lon = Math.round(city.getLocation().getLongitude() * 100_000) / 100_000.0;
			cases.add(testCase(city.getName(), lat, lon, timezoneLookup.apply(lat, lon)));
		}
		JsonObject data = new JsonObject();
		data.addProperty("source", basemap.getFileName().toString());
		data.add("cases", cases);
		return data;
	}

	public static Path findWorldBasemap() throws IOException {
		String location = System.getenv("OBF_LOCATION");
		if (location == null || location.isBlank()) {
			throw new IllegalArgumentException(BASEMAP_HINT);
		}
		Path root = Path.of(location);
		if (Files.isRegularFile(root) && BASEMAP.equals(root.getFileName().toString())) {
			return root;
		}
		Path basemap = root.resolve(BASEMAP);
		if (Files.isRegularFile(basemap)) {
			return basemap;
		}
		throw new IOException(BASEMAP + " not found in OBF_LOCATION. " + BASEMAP_HINT);
	}

	public static TimezoneCheckResult checkTimezoneMappings(String datasetName, JsonObject testData,
	                                                        JsonObject aliases,
	                                                        BiFunction<Double, Double, String> timezoneLookup) {
		TimezoneCase[] cases = GSON.fromJson(testData.get("cases"), TimezoneCase[].class);
		if (cases == null || cases.length == 0) {
			throw new AssertionError("No timezone cases: " + datasetName);
		}
		List<String> mismatches = new ArrayList<>();
		for (TimezoneCase testCase : cases) {
			String actual = timezoneLookup.apply(testCase.lat, testCase.lon);
			String normalized = normalize(actual, aliases);
			boolean matches = normalize(testCase.timezone, aliases).equals(normalized);
			if (!matches && testCase.alternatives != null) {
				matches = Arrays.stream(testCase.alternatives)
						.anyMatch(timezone -> normalize(timezone, aliases).equals(normalized));
			}
			if (!matches) {
				mismatches.add(testCase.city + " (" + testCase.lat + ", " + testCase.lon
						+ "): expected=" + testCase.timezone
						+ (testCase.alternatives == null ? ""
								: " alternatives=" + Arrays.toString(testCase.alternatives))
						+ " actual=" + actual);
			}
		}
		return new TimezoneCheckResult(datasetName, cases.length, mismatches);
	}

	public record TimezoneCheckResult(String datasetName, int total, List<String> mismatches) {
		public String formatSummary() {
			return datasetName + ": total=" + total + " mismatches=" + mismatches.size();
		}

		public void assertTimezonesMatch() {
			System.out.println(formatSummary());
			if (!mismatches.isEmpty()) {
				throw new AssertionError(formatSummary() + "\n" + String.join("\n", mismatches));
			}
		}
	}

	public record TimezoneTestData(JsonObject aliases, JsonObject iana, JsonObject cities) {
		public void writeJsonFiles(Path directory) throws IOException {
			Files.createDirectories(directory);
			writeJson(directory.resolve(ALIASES_FILE), aliases);
			writeJson(directory.resolve(IANA_FILE), iana);
			writeJson(directory.resolve(CITIES_FILE), cities);
			System.out.println("Generated " + ALIASES_FILE + ", " + IANA_FILE + ", " + CITIES_FILE);
		}
	}

	private static void writeJson(Path output, JsonObject data) throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
			GSON.toJson(data, writer);
			writer.write('\n');
		}
	}

	private static JsonObject testCase(String city, double lat, double lon, String timezone) {
		JsonObject testCase = new JsonObject();
		testCase.addProperty("city", city);
		testCase.addProperty("lat", lat);
		testCase.addProperty("lon", lon);
		testCase.addProperty("timezone", timezone);
		return testCase;
	}

	static String normalize(String timezone, JsonObject aliases) {
		while (aliases.has(timezone)) {
			timezone = aliases.get(timezone).getAsString();
		}
		return timezone;
	}

	private static double coordinate(String value, int degreeDigits) {
		if ((value.charAt(0) != '+' && value.charAt(0) != '-')
				|| (value.length() != degreeDigits + 3 && value.length() != degreeDigits + 5)) {
			throw new IllegalArgumentException("Invalid IANA coordinate: " + value);
		}
		int degrees = Integer.parseInt(value.substring(1, degreeDigits + 1));
		int minutes = Integer.parseInt(value.substring(degreeDigits + 1, degreeDigits + 3));
		int seconds = value.length() == degreeDigits + 5 ? Integer.parseInt(value.substring(degreeDigits + 3)) : 0;
		if (minutes >= 60 || seconds >= 60 || degrees > (degreeDigits == 2 ? 90 : 180)) {
			throw new IllegalArgumentException("Invalid IANA coordinate: " + value);
		}
		double coordinate = degrees + minutes / 60.0 + seconds / 3600.0;
		return Math.round((value.charAt(0) == '-' ? -coordinate : coordinate) * 1_000_000) / 1_000_000.0;
	}

	private static class TimezoneCase {
		String city;
		double lat;
		double lon;
		String timezone;
		String[] alternatives;
	}
}
