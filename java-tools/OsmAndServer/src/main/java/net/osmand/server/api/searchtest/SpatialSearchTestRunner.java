package net.osmand.server.api.searchtest;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.ObfConstants;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.obf.OBFDataCreator;
import net.osmand.obf.preparation.NameIndexCreator;
import net.osmand.search.core.spatial.SpatialSearchResult;
import net.osmand.search.core.spatial.SpatialTestSearchEngine;
import net.osmand.search.core.spatial.SpatialTextSearch;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

/** Executes spatial-search unit-test inputs and exposes their actual runtime results. */
public final class SpatialSearchTestRunner {
	public record CSVRow(String unitTest, String query, LatLon location, long osmId, String point, String result, String entityType) {}

	private record Phrase(String query, JSONObject settings) {}

	private static final Object RUN_LOCK = new Object();
	private final Path source;

    public SpatialSearchTestRunner(Path source) {
		NameIndexCreator.MIN_LIMIT_COMMON_NON_INDEXED = 0;
		this.source = source.toAbsolutePath().normalize();
	}

	public List<CSVRow> run() throws IOException {
		// ponytail: the search test setup uses shared caches; make executions concurrent only when those become isolated.
		synchronized (RUN_LOCK) {
			if (Files.isRegularFile(source)) {
				return runTest(source);
			}
			if (!Files.isDirectory(source)) {
				throw new IOException("UnitTest source is not a file or directory: " + source);
			}
			List<Path> files;
			try (var paths = Files.list(source)) {
				files = paths.filter(Files::isRegularFile)
						.filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".json"))
						.sorted()
						.toList();
			}
			List<CSVRow> hits = new ArrayList<>();
			for (Path file : files) {
				hits.addAll(runTest(file));
			}
			return hits;
		}
	}

	private List<CSVRow> runTest(Path testFile) throws IOException {
		JSONObject test = new JSONObject(Files.readString(testFile));
		String unitTest = baseName(testFile.getFileName().toString());
		JSONObject settings = test.optJSONObject("settings");
		if (test.optBoolean("ignore") || settings == null
				|| settings.optBoolean("disabled") || settings.optBoolean("ignore")) {
			return List.of();
		}

		List<BinaryMapIndexReader> readers = new ArrayList<>();
		try {
			if (settings.optBoolean("useData", true)) {
				loadReaders(testFile, test, readers);
				if (readers.isEmpty()) {
					throw new IOException("No OBF indexes loaded for " + testFile.getFileName());
				}
			}
			if (settings.optBoolean("world")) {
				readers.add(openReader(findRegionsFile(testFile.getParent())));
				readers.add(openReader(resolveObf(testFile.getParent(), "world_basemap.json.gz")));
			}

			List<CSVRow> rows = new ArrayList<>();
			List<Phrase> phrases = parsePhrases(test);
			for (int phraseIndex = 0; phraseIndex < phrases.size(); phraseIndex++) {
				Phrase phrase = phrases.get(phraseIndex);
				JSONObject phraseSettings = merge(settings, phrase.settings());
				if (phraseSettings.optBoolean("ignore")) {
					continue;
				}
				LatLon location = parseLocation(phraseSettings);
                SpatialTestSearchEngine searchEngine = new SpatialTestSearchEngine(parseSettings(phraseSettings), location, readers,
                        phraseSettings.optBoolean("translation"));
				List<SpatialSearchResult> results = searchEngine.searchResults(phrase.query(), false);
				int resultLimit = Math.min(expectedResultCount(test, phraseIndex), results.size());
				for (int resultIndex = 0; resultIndex < resultLimit; resultIndex++) {
					SpatialSearchResult result = results.get(resultIndex);
					MapObject object = result.getMainObject();
					LatLon point = result.getLatLon();
					rows.add(new CSVRow(unitTest, phrase.query(), location,
								object != null ? ObfConstants.getOsmObjectId(object) : -1,
								point != null ? String.format(Locale.US, "%.5f, %.5f", point.getLatitude(), point.getLongitude()) : null,
								searchEngine.formatResult(result),
								ResultActuator.getEntityType(object)
							));
				}
			}
			return rows;
		} finally {
			for (BinaryMapIndexReader reader : readers) {
				reader.close();
			}
		}
	}

	private void loadReaders(Path testFile, JSONObject test, List<BinaryMapIndexReader> readers) throws IOException {
		JSONArray files = test.optJSONArray("files");
		if (files == null) {
			readers.add(openReader(resolveObf(testFile.getParent(), testFile.getFileName().toString())));
			return;
		}
		for (int i = 0; i < files.length(); i++) {
			String file = files.optString(i, null);
			if (file != null && !file.isBlank()) {
				readers.add(openReader(resolveObf(testFile.getParent(), file)));
			}
		}
	}

	private BinaryMapIndexReader openReader(Path file) throws IOException {
		RandomAccessFile randomAccessFile = new RandomAccessFile(file.toFile(), "r");
		try {
			return new BinaryMapIndexReader(randomAccessFile, file.toFile());
		} catch (IOException | RuntimeException e) {
			randomAccessFile.close();
			throw e;
		}
	}

	private Path resolveObf(Path testDir, String fileName) throws IOException {
		String baseName = baseName(fileName);
		Path generated = testDir.resolve("gen").resolve(baseName + ".gen.obf");
		Path source = newest(testDir.resolve("src").resolve(baseName + ".json"),
				testDir.resolve("src").resolve(baseName + ".json.gz"));
		if (Files.isRegularFile(generated)
				&& (source == null || Files.getLastModifiedTime(generated).compareTo(Files.getLastModifiedTime(source)) >= 0)) {
			return generated;
		}

		Path original = testDir.resolve(baseName + ".obf");
		if (source == null && Files.isRegularFile(original)) {
			return original;
		}
		if (source == null) {
			throw new IOException("No generated OBF or source JSON found for " + fileName);
		}

		Files.createDirectories(generated.getParent());
		Path json = source;
		Path unpacked = null;
		Path temporaryObf = Files.createTempFile(generated.getParent(), baseName + "_", ".obf");
		try {
			if (source.getFileName().toString().endsWith(".gz")) {
				unpacked = Files.createTempFile(generated.getParent(), baseName + "_", ".json");
				try (GZIPInputStream input = new GZIPInputStream(Files.newInputStream(source))) {
					Files.copy(input, unpacked, StandardCopyOption.REPLACE_EXISTING);
				}
				json = unpacked;
			}
			new OBFDataCreator().create(temporaryObf.toString(), new String[] {json.toString()});
			Files.move(temporaryObf, generated, StandardCopyOption.REPLACE_EXISTING);
			return generated;
		} catch (SQLException e) {
			throw new IOException("Cannot generate OBF for " + fileName, e);
		} finally {
			Files.deleteIfExists(temporaryObf);
			Files.deleteIfExists(Path.of(temporaryObf + ".gz"));
			if (unpacked != null) {
				Files.deleteIfExists(unpacked);
			}
		}
	}

	private Path newest(Path... paths) throws IOException {
		Path newest = null;
		for (Path path : paths) {
			if (Files.isRegularFile(path) && (newest == null
					|| Files.getLastModifiedTime(path).compareTo(Files.getLastModifiedTime(newest)) > 0)) {
				newest = path;
			}
		}
		return newest;
	}

	private Path findRegionsFile(Path start) throws IOException {
		String androidPath = System.getenv("ANDROID_PATH");
		if (androidPath != null && !androidPath.isBlank()) {
			for (Path candidate : List.of(Path.of(androidPath, "OsmAnd-java", "regions.ocbf"),
					Path.of(androidPath, "regions.ocbf"))) {
				if (Files.isRegularFile(candidate)) {
					return candidate;
				}
			}
		}
		for (Path current = start; current != null; current = current.getParent()) {
			Path candidate = current.resolve("android").resolve("OsmAnd-java").resolve("regions.ocbf");
			if (Files.isRegularFile(candidate)) {
				return candidate;
			}
		}
		throw new IOException("regions.ocbf not found; set ANDROID_PATH");
	}

	private List<Phrase> parsePhrases(JSONObject test) {
		List<Phrase> phrases = new ArrayList<>();
		addPhrase(phrases, test.optString("phrase", null));
		JSONArray array = test.optJSONArray("phrases");
		if (array != null) {
			for (int i = 0; i < array.length(); i++) {
				addPhrase(phrases, array.optString(i, null));
			}
		}
		return phrases;
	}

	private void addPhrase(List<Phrase> phrases, String value) {
		if (value == null || value.isBlank()) {
			return;
		}
		int settingsStart = value.lastIndexOf('{');
		if (settingsStart < 0 || !value.trim().endsWith("}")) {
			phrases.add(new Phrase(value, null));
		} else {
			phrases.add(new Phrase(value.substring(0, settingsStart).trim(),
					new JSONObject(value.substring(settingsStart))));
		}
	}

	private JSONObject merge(JSONObject settings, JSONObject override) {
		JSONObject merged = new JSONObject(settings.toString());
		if (override != null) {
			for (String key : override.keySet()) {
				merged.put(key, override.get(key));
			}
		}
		return merged;
	}

	private LatLon parseLocation(JSONObject settings) {
		JSONObject location = settings.optJSONObject("location");
		if (location != null) {
			return new LatLon(location.getDouble("lat"), location.getDouble("lon"));
		}
		return settings.has("lat") && settings.has("lon")
				? new LatLon(settings.getDouble("lat"), settings.getDouble("lon"))
				: null;
	}

	private SpatialTextSearch.SpatialTextSearchSettings parseSettings(JSONObject json) {
		SpatialTextSearch.SpatialTextSearchSettings settings = SpatialTextSearch.SpatialTextSearchSettings.defaultSettings();
		settings.SEARCH_ADDR = json.optBoolean("SEARCH_ADDR", settings.SEARCH_ADDR);
		settings.SEARCH_POI = json.optBoolean("SEARCH_POI", settings.SEARCH_POI);
		settings.SEARCH_BUILDINGS = json.optBoolean("SEARCH_BUILDINGS", settings.SEARCH_BUILDINGS);
		settings.SEARCH_STREET_INTERSECTIONS = json.optBoolean("SEARCH_STREET_INTERSECTIONS", settings.SEARCH_STREET_INTERSECTIONS);
		settings.SEARCH_POI_INTERSECTIONS = json.optBoolean("SEARCH_POI_INTERSECTIONS", settings.SEARCH_POI_INTERSECTIONS);
		settings.SEARCH_POI_CATEGORIES = json.optBoolean("SEARCH_POI_CATEGORIES", settings.SEARCH_POI_CATEGORIES);
		settings.ALLOW_VIRTUAL_STREET_INTERSECTIONS = json.optBoolean("ALLOW_VIRTUAL_STREET_INTERSECTIONS",
				settings.ALLOW_VIRTUAL_STREET_INTERSECTIONS);
		settings.OPTIM_DELETE_EMBEDDED_BOUNDARIES = json.optBoolean("OPTIM_DELETE_EMBEDDED_BOUNDARIES",
				settings.OPTIM_DELETE_EMBEDDED_BOUNDARIES);
		settings.OPTIM_FLAG_POI_SAME_AS_CITY_STREET = json.optBoolean("OPTIM_FLAG_POI_SAME_AS_CITY_STREET",
				settings.OPTIM_FLAG_POI_SAME_AS_CITY_STREET);
		settings.DEDUPLICATE_RES = json.optBoolean("DEDUPLICATE_RES", settings.DEDUPLICATE_RES);
		settings.LIMIT_POI_CATEGORY_BY_FREQ = json.optInt("LIMIT_POI_CATEGORY_BY_FREQ", settings.LIMIT_POI_CATEGORY_BY_FREQ);
		settings.OPTIM_READ_COMMON_WORDS_LIMIT = json.optInt("OPTIM_READ_COMMON_WORDS_LIMIT", settings.OPTIM_READ_COMMON_WORDS_LIMIT);
		settings.LANG_DEDUPLICATE = json.optString("LANG_DEDUPLICATE", settings.LANG_DEDUPLICATE);
		settings.MIN_ELO_RATING = json.optInt("MIN_ELO_RATING", settings.MIN_ELO_RATING);
		settings.MIN_CHARACTERS_INCOMPLETE = json.optInt("MIN_CHARACTERS_INCOMPLETE", settings.MIN_CHARACTERS_INCOMPLETE);
		settings.LIMIT_ATOMIC_OBJECTS = json.optInt("LIMIT_ATOMIC_OBJECTS", settings.LIMIT_ATOMIC_OBJECTS);
		settings.LIMIT_STOP_GOALS_ANY_LEVEL_WHEN_REACHED_RES = json.optInt("LIMIT_ALL_GOALS_MAX_UNIQUE_OBJECTS",
				settings.LIMIT_STOP_GOALS_ANY_LEVEL_WHEN_REACHED_RES);
		settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES = json.optInt("LIMIT_STOP_OTHER_GOALS_WHEN_REACHED_UNIQUE_OBJECTS",
				settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES);
		settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES = json.optInt("LIMIT_GOAL_LEVEL_2",
				settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES);
		settings.DEV_USE_PIPELINE = json.optBoolean("DEV_USE_PIPELINE", settings.DEV_USE_PIPELINE);
		return settings;
	}

	private int expectedResultCount(JSONObject test, int phraseIndex) {
		JSONArray results = test.optJSONArray("results");
		if (results == null || results.isEmpty()) {
			return 0;
		}
		if (results.optJSONArray(0) == null) {
			return phraseIndex == 0 ? results.length() : 0;
		}
		JSONArray phraseResults = results.optJSONArray(phraseIndex);
		return phraseResults == null ? 0 : phraseResults.length();
	}

	private String baseName(String fileName) {
		String lower = fileName.toLowerCase(Locale.ROOT);
		for (String extension : List.of(".obf.gz", ".json.gz", ".osm.gz", ".obf", ".json")) {
			if (lower.endsWith(extension)) {
				return fileName.substring(0, fileName.length() - extension.length());
			}
		}
		return fileName;
	}
}
