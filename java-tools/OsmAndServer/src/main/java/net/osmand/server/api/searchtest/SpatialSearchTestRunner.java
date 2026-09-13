package net.osmand.server.api.searchtest;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.ObfConstants;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.obf.OBFDataCreator;
import net.osmand.obf.preparation.NameIndexCreator;
import net.osmand.osm.MapPoiTypes;
import net.osmand.server.api.services.search.PoiTypesService;
import net.osmand.search.core.spatial.SpatialSearchResult;
import net.osmand.search.core.spatial.SpatialSearchTestFile;
import net.osmand.search.core.spatial.SpatialSearchTestFile.Phrase;
import net.osmand.search.core.spatial.SpatialTestSearchEngine;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
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

	private static final Object RUN_LOCK = new Object();
	private final Path source;
	// POI types of the tests; the server's own POI types stay the process default
	private final MapPoiTypes poiTypes;
	private MapPoiTypes translationPoiTypes;

	public SpatialSearchTestRunner(Path source) {
		this.source = source.toAbsolutePath().normalize();
		poiTypes = new PoiTypesService().getMapPoiTypes(PoiTypesService.DEFAULT_SEARCH_LANG);
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
				loadReaders(testFile, test, settings.optString("sourceMap", null), readers);
				if (readers.isEmpty()) {
					throw new IOException("No OBF indexes loaded for " + testFile.getFileName());
				}
			}
			if (settings.optBoolean("world")) {
				readers.add(SpatialSearchTestFile.openReader(findRegionsFile(testFile.getParent()).toFile()));
				readers.add(SpatialSearchTestFile.openReader(resolveObf(testFile.getParent(), "world_basemap.json.gz", null).toFile()));
			}

			List<CSVRow> rows = new ArrayList<>();
			List<Phrase> phrases = SpatialSearchTestFile.parsePhrases(test);
			for (int phraseIndex = 0; phraseIndex < phrases.size(); phraseIndex++) {
				Phrase phrase = phrases.get(phraseIndex);
				JSONObject phraseSettings = SpatialSearchTestFile.merge(settings, phrase.settings());
				if (phraseSettings.optBoolean("ignore")) {
					continue;
				}
				LatLon location = SpatialSearchTestFile.parseLocation(phraseSettings);
				SpatialTestSearchEngine searchEngine = new SpatialTestSearchEngine(SpatialSearchTestFile.parseSettings(phraseSettings),
						location, readers, poiTypes(phraseSettings.optBoolean("translation")));
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

	private MapPoiTypes poiTypes(boolean translation) {
		if (!translation) {
			return poiTypes;
		}
		if (translationPoiTypes == null) {
			translationPoiTypes = new MapPoiTypes(null);
			translationPoiTypes.setPoiTranslator(new SpatialTestSearchEngine.TestPoiTranslator());
			translationPoiTypes.init();
		}
		return translationPoiTypes;
	}

	private void loadReaders(Path testFile, JSONObject test, String sourceMap, List<BinaryMapIndexReader> readers) throws IOException {
		JSONArray files = test.optJSONArray("files");
		if (files == null) {
			readers.add(SpatialSearchTestFile.openReader(resolveObf(testFile.getParent(), testFile.getFileName().toString(), sourceMap).toFile()));
			return;
		}
		for (int i = 0; i < files.length(); i++) {
			String file = files.optString(i, null);
			if (file != null && !file.isBlank()) {
				readers.add(SpatialSearchTestFile.openReader(resolveObf(testFile.getParent(), file, sourceMap).toFile()));
			}
		}
	}

	/** @param sourceMap download name of the map the test data comes from, its language group chooses the keys of names */
	private Path resolveObf(Path testDir, String fileName, String sourceMap) throws IOException {
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
			OBFDataCreator creator = new OBFDataCreator();
			creator.setSourceMap(sourceMap);
			// the tests generate their maps with every common word indexed
			int minCommonNonIndexed = NameIndexCreator.MIN_LIMIT_COMMON_NON_INDEXED;
			NameIndexCreator.MIN_LIMIT_COMMON_NON_INDEXED = 0;
			try {
				creator.create(temporaryObf.toString(), new String[] {json.toString()});
			} finally {
				NameIndexCreator.MIN_LIMIT_COMMON_NON_INDEXED = minCommonNonIndexed;
			}
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
