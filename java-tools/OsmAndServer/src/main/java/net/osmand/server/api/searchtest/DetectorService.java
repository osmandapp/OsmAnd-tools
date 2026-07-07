package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.amazonaws.util.StringInputStream;
import net.osmand.ResultMatcher;
import net.osmand.binary.*;
import net.osmand.data.*;
import net.osmand.obf.OBFDataCreator;
import net.osmand.osm.MapPoiTypes;
import net.osmand.router.RoutingContext;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.SearchExportSettings;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchPhrase;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.search.core.SearchWord;
import net.osmand.search.core.spatial.SpatialSearchContext;
import net.osmand.search.core.spatial.SpatialSearchResult;
import net.osmand.search.core.spatial.SpatialSearchResultsList;
import net.osmand.server.api.services.SearchService;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public interface DetectorService extends OBFService {
	SearchService getSearchService();

	default ResultMetric toMetric(SearchResult r) {
		return new ResultMetric(r.file == null ? "" : r.file.getFile().getName(), r.getDepth(), r.getFoundWordCount(),
				r.getUnknownPhraseMatchWeight(), r.getOtherWordsMatch(), r.location == null ? null :
				MapUtils.getDistance(r.requiredSearchPhrase.getSettings().getOriginalLocation(), r.location)/1000.0,
				r.getCompleteMatchRes().allWordsEqual, r.getCompleteMatchRes().allWordsInPhraseAreInResult);
	}

	record ResultsWithStats(List<AddressResult> results, Collection<BinaryMapIndexReaderStats.WordSearchStat> wordStats,
	                        Map<BinaryMapIndexReaderStats.BinaryMapIndexReaderApiName, BinaryMapIndexReaderStats.StatByAPI> statsByApi,
	                        String timeAll, SpatialSearchContext.SpatialSearchStats spatialStats, List<String> spatialCombinations) {}
	record ResultMetric(String obf, int depth, double foundWordCount, double unknownPhraseMatchWeight,
	                    Collection<String> otherWordsMatch, Double distance, boolean isEqual, boolean inResult) {}
	record AddressResult(String name, String type, String address, AddressResult parent, ResultMetric metric,
	                     LatLon location, String mainWord) {}

	default ResultsWithStats getResults(SearchService.SearchContext ctx, SearchService.SearchOption options, Boolean spatial) throws IOException {
		long startTime = System.currentTimeMillis();
		if (spatial != null && spatial) {
			SearchService.SpatialResults results = getSearchService().searchTestSpatial(ctx, options, null, true);
			String timeAll = String.format(Locale.US, "%.1f", (System.currentTimeMillis() - startTime) / 1e3);
			return toResults(ctx, results, timeAll);
		}
		SearchService.SearchResults result = getSearchService().getImmediateSearchResults(ctx, options, null);
		String mainWord = result.phrase() == null ? "" : result.phrase().getUnknownWordToSearch();

		List<AddressResult> results = new ArrayList<>();
		for (SearchResult r : result.results()) {
			AddressResult rec = toResult(r, mainWord, Collections.newSetFromMap(new IdentityHashMap<>()));
			results.add(rec);
		}
		String totalTime = String.format(Locale.US, "%.1f", (System.currentTimeMillis() - startTime) / 1e3);
		return new ResultsWithStats(results, result.settings().getStat().getWordStats().values(),
				result.settings().getStat().getByApis(), totalTime, null, null);
	}

	private ResultsWithStats toResults(SearchService.SearchContext ctx, SearchService.SpatialResults spatialResponse, String totalTime) {
		List<AddressResult> results = new ArrayList<>();
		if (spatialResponse == null || spatialResponse.results() == null || spatialResponse.results().mainResults == null) {
			return new ResultsWithStats(results, Collections.emptyList(), Collections.emptyMap(), totalTime, null, null);
		}
		for (SpatialSearchResult res : spatialResponse.results().mainResults) {
			AddressResult result = toResult(ctx, res);
			if (result != null) {
				results.add(result);
			}
		}
		return new ResultsWithStats(results, Collections.emptyList(), Collections.emptyMap(), totalTime,
				spatialResponse.stats(), spatialCombinations(spatialResponse));
	}

	private List<String> spatialCombinations(SearchService.SpatialResults spatialResponse) {
		if (spatialResponse == null || spatialResponse.results() == null || spatialResponse.results().combinations == null) {
			return Collections.emptyList();
		}
		List<String> combinations = new ArrayList<>();
		for (SpatialSearchResultsList combination : spatialResponse.results().combinations) {
			if (combination != null) {
				combinations.add(combination.toString(false));
			}
		}
		return combinations;
	}

	private AddressResult toResult(SearchService.SearchContext ctx, SpatialSearchResult res) {
		if (res == null) {
			return null;
		}
		LatLon location = res.getLatLon();
		List<MapObject> objects = res.getObjects();
		MapObject object = objects == null || objects.isEmpty() ? null : objects.get(0);
		Double distance = location == null ? null : MapUtils.getDistance(new LatLon(ctx.lat(), ctx.lon()), location) / 1000.0;
		ResultMetric metric = new ResultMetric("", res.visibleLevel(), res.matchedTokens(), res.sumOther(),
				Collections.emptyList(), distance, true, true);
		AddressResult parent = toParent(ctx, objects, 1, res.matchedTokens());
		return new AddressResult(spatialName(object, ctx.locale()), spatialType(object), spatialAddress(object, ctx.locale()),
				parent, metric, location, null);
	}

	private AddressResult toParent(SearchService.SearchContext ctx, List<MapObject> objects, int index, double foundWordCount) {
		if (objects == null || index >= objects.size()) {
			return null;
		}
		MapObject object = objects.get(index);
		LatLon location = object == null ? null : object.getLocation();
		Double distance = location == null ? null : MapUtils.getDistance(new LatLon(ctx.lat(), ctx.lon()), location) / 1000.0;
		ResultMetric metric = new ResultMetric("", index, foundWordCount, 0, Collections.emptyList(), distance, true, true);
		return new AddressResult(spatialName(object, ctx.locale()), spatialType(object), spatialAddress(object, ctx.locale()),
				toParent(ctx, objects, index + 1, foundWordCount), metric, location, null);
	}

	private String spatialName(MapObject object, String locale) {
		if (object == null) {
			return "";
		}
		String name = object.getName(locale);
		return Algorithms.isEmpty(name) ? object.getName() : name;
	}

	private String spatialType(MapObject object) {
		if (object instanceof Amenity) {
			return "poi";
		} else if (object instanceof Street) {
			return "street";
		} else if (object instanceof City city) {
			return city.getType() == null ? "city" : city.getType().name().toLowerCase(Locale.ROOT);
		} else if (object == null) {
			return "";
		}
		return object.getClass().getSimpleName().toLowerCase(Locale.ROOT);
	}

	private String spatialAddress(MapObject object, String locale) {
		if (object instanceof Amenity amenity) {
			return amenity.getCityFromTagGroups(locale);
		} else if (object instanceof Street street && street.getCity() != null) {
			return street.getCity().getName(locale);
		}
		return "";
	}

	private AddressResult toResult(SearchResult r, String mainWord, Set<SearchResult> seen) {
		if (r == null || r == r.parentSearchResult)
			return null;

		ResultMetric metric = toMetric(r);
		String type = r.objectType.name().toLowerCase();

		// If we've already visited this node, break the cycle by not traversing further
		if (!seen.add(r))
			return new AddressResult(r.toString(), type, r.addressName, null, metric, r.location, mainWord);

		AddressResult parent = toResult(r.parentSearchResult, mainWord, seen);
		return new AddressResult(r.toString(), type, r.addressName, parent, metric, r.location, mainWord);
	}

	record UnitTestPayload(
			@JsonProperty("name") String name,
			@JsonProperty("queries") String[] queries,
			@JsonProperty("resultsLimit") Integer resultsLimit,
			@JsonProperty("geocodingLimit") Integer geocodingLimit) {}

	record UnitTestResultsData(List<List<String>> results, JSONArray routing) {}
	record UnitTestSourceData(String jsonFilePath, List<String> jsonFilePaths, SearchService.SearchResults results) {
		UnitTestSourceData(String jsonFilePath, SearchService.SearchResults results) {
			this(jsonFilePath, Collections.singletonList(jsonFilePath), results);
		}
	}
	record UnitTestSearchData(SearchUICore.SearchResultCollection collection,
	                          SearchUICore.SearchResultMatcher matcher) {}

	default void createUnitTest(UnitTestPayload unitTest, SearchService.SearchContext ctx, OutputStream out) throws IOException, SQLException {
		Path rootTmp = Path.of(System.getProperty("java.io.tmpdir"));
		Path dirPath = Files.createTempDirectory(rootTmp, "unit-tests-");
		try {
			int limit = unitTest.resultsLimit();
			int geocodingLimit = unitTest.geocodingLimit();
			UnitTestResultsData unitTestData = buildUnitTestResults(unitTest.queries(), ctx, limit, geocodingLimit);
			UnitTestSourceData sourceData = createUnitTestSourceData(unitTest, ctx, dirPath, unitTestData.routing());
			SearchService.SearchResults result = sourceData.results();
			if (result == null) {
				return;
			}
			File jsonFile = dirPath.resolve(unitTest.name + ".json").toFile();

			OBFDataCreator creator = new OBFDataCreator();
			File outFile = creator.create(dirPath.resolve(unitTest.name + ".obf").toAbsolutePath().toString(),
					new String[] {sourceData.jsonFilePath()});

			// Build ZIP with JSON metadata and gzipped data, streaming directly to the servlet output
			SearchSettings settings = result.settings().setOriginalLocation(new LatLon(ctx.lat(), ctx.lon()));
			JSONObject settingsJson = settings.toJSON();
			JSONArray formattedResultsJson = new JSONArray();
			for (List<String> phraseResults : unitTestData.results()) {
				formattedResultsJson.put(new JSONArray(phraseResults));
			}
			Map<String, Object> rootJson = new LinkedHashMap<>();
			rootJson.put("description", String.format(Locale.US,
					"Created with Search Tool - Detector - Unit-Test (%d results, %d geocoding, routing %b)",
					limit, geocodingLimit, geocodingLimit > 0));
			if (geocodingLimit > 0) {
				rootJson.put("note", "@ prefix means reverse geocoding test for that result");
			}
			rootJson.put("settings", settingsJson);
			rootJson.put("phrases", unitTest.queries());
			rootJson.put("results", formattedResultsJson);
			String unitTestJson = new JSONObject(rootJson).toString(4) + "\n";
			Files.writeString(jsonFile.toPath(), unitTestJson, StandardCharsets.UTF_8);
			try (ZipOutputStream zipOut = new ZipOutputStream(out)) {
				// JSON metadata entry
				if (jsonFile.exists()) {
					ZipEntry jsonEntry = new ZipEntry(jsonFile.getName());
					zipOut.putNextEntry(jsonEntry);
					try (InputStream jsonIn = new StringInputStream(unitTestJson)) {
						Algorithms.streamCopy(jsonIn, zipOut);
					}
					zipOut.closeEntry();
				}

				// Gzipped data archive entry
				if (outFile.exists()) {
					ZipEntry gzEntry = new ZipEntry(outFile.getName());
					zipOut.putNextEntry(gzEntry);
					try (InputStream gzIn = new FileInputStream(outFile)) {
						Algorithms.streamCopy(gzIn, zipOut);
					}
					zipOut.closeEntry();
				}

				zipOut.finish();
				out.flush();
			}
		} finally {
			if (dirPath != null && Files.exists(dirPath)) {
				Files.walk(dirPath)
						.sorted(Comparator.reverseOrder())
						.forEach(p -> {
							File f = p.toFile();
							if (!f.delete()) {
								f.deleteOnExit();
							}
						});
			}
		}
	}

	private List<List<String>> emptyUnitTestResults(String[] phrases) {
		List<List<String>> results = new ArrayList<>();
		String[] phraseArray = phrases == null ? new String[0] : phrases;
		for (int i = 0; i < phraseArray.length; i++) {
			results.add(new ArrayList<>());
		}
		return results;
	}

	private UnitTestSourceData createUnitTestSourceData(UnitTestPayload unitTest, SearchService.SearchContext baseCtx,
			Path dirPath, JSONArray routing) throws IOException {
		SearchExportSettings exportSettings = new SearchExportSettings(true, true, -1);
		SearchService.SearchResults results = null;
		String[] queries = normalizedUnitTestQueries(unitTest.queries(), baseCtx.text());
		LinkedHashMap<String, Amenity> amenities = new LinkedHashMap<>();
		LinkedHashMap<Long, City> cities = new LinkedHashMap<>();
        for (String q : queries) {
            SearchService.SearchContext phraseCtx = new SearchService.SearchContext(
                    baseCtx.lat(), baseCtx.lon(), q, baseCtx.locale(),
                    baseCtx.baseSearch(), baseCtx.northWest(), baseCtx.southEast());
            SearchService.SearchResults queryResult = getSearchService().getImmediateSearchResults(
                    phraseCtx, new SearchService.SearchOption(true, exportSettings,
                            null, true, false, (net.osmand.search.core.ObjectType[]) null), null);
            if (results == null) {
                results = queryResult;
            }
            String unitTestJson = queryResult == null ? null : queryResult.unitTestJson();
            if (unitTestJson == null) {
                continue;
            }
            collectUnitTestSourceData(unitTestJson, amenities, cities);
        }
		
		return createUnitTestJson(dirPath, unitTest.name, results, routing, amenities, cities);
	}

	private UnitTestSourceData createUnitTestJson(Path dirPath, String name, SearchService.SearchResults results, JSONArray routing, Map<String, Amenity> amenities, Map<Long, City> cities) throws IOException {
		return createUnitTestJsonFile(dirPath.resolve(name + ".json").toFile(), results, routing, amenities, cities);
	}

	private UnitTestSourceData createUnitTestJsonFile(File sourceJsonFile, SearchService.SearchResults results, JSONArray routing,
			Map<String, Amenity> amenities, Map<Long, City> cities) throws IOException {
		JSONObject sourceJson = new JSONObject();
		if (!amenities.isEmpty()) {
			JSONArray amenitiesJson = new JSONArray();
			for (Amenity amenity : amenities.values()) {
				amenitiesJson.put(amenity.toJSON());
			}
			sourceJson.put("amenities", amenitiesJson);
		}
		if (!cities.isEmpty()) {
			JSONArray citiesJson = new JSONArray();
			for (City city : cities.values()) {
				citiesJson.put(city.toJSON(true));
			}
			sourceJson.put("cities", citiesJson);
		}
		if (!routing.isEmpty()) {
			sourceJson.put("routing", routing);
		}
		Files.writeString(sourceJsonFile.toPath(), sourceJson.toString(4), StandardCharsets.UTF_8);
		return new UnitTestSourceData(sourceJsonFile.getAbsolutePath(), results);
	}

	private String[] normalizedUnitTestQueries(String[] queries, String fallbackQuery) {
		List<String> normalized = new ArrayList<>();
		if (queries != null) {
			for (String query : queries) {
				if (!Algorithms.isEmpty(query)) {
					normalized.add(query);
				}
			}
		}
		if (normalized.isEmpty() && !Algorithms.isEmpty(fallbackQuery)) {
			normalized.add(fallbackQuery);
		}
		return normalized.toArray(new String[0]);
	}

	private void collectUnitTestSourceData(String unitTestJson, Map<String, Amenity> amenities, Map<Long, City> cities) {
		JSONObject sourceJson = new JSONObject(unitTestJson);
		JSONArray amenitiesJson = sourceJson.optJSONArray("amenities");
		if (amenitiesJson != null) {
			for (int i = 0; i < amenitiesJson.length(); i++) {
				Amenity amenity = Amenity.parseJSON(amenitiesJson.getJSONObject(i));
				String amenityKey = amenity.getId() + "|" + amenity.getType() + "|" + amenity.getSubType();
				amenities.putIfAbsent(amenityKey, amenity);
			}
		}
		JSONArray citiesJson = sourceJson.optJSONArray("cities");
		if (citiesJson != null) {
			for (int i = 0; i < citiesJson.length(); i++) {
				City city = City.parseJSON(citiesJson.getJSONObject(i));
				City existing = cities.get(city.getId());
				if (existing == null) {
					cities.put(city.getId(), city);
				} else {
					existing.mergeWith(city);
				}
			}
		}
	}

	private UnitTestResultsData buildUnitTestResults(String[] phrases, SearchService.SearchContext baseCtx, int limit, int geocodingLimit) throws IOException {
		List<List<String>> results = emptyUnitTestResults(phrases);
		JSONArray routing = new JSONArray();
		Map<String, RoutingContext> geocodingContexts = new HashMap<>();
		Map<String, Long> exportedRoutes = new LinkedHashMap<>();
		long nextRouteId = 1;
		GeocodingUtilities geoUtils = new GeocodingUtilities();
		String[] phraseArray = phrases == null ? new String[0] : phrases;
		for (int phraseIndex = 0; phraseIndex < phraseArray.length; phraseIndex++) {
			String query = phraseArray[phraseIndex];
			SearchService.SearchContext phraseCtx = new SearchService.SearchContext(
					baseCtx.lat(), baseCtx.lon(), query == null ? "" : query, baseCtx.locale(),
					baseCtx.baseSearch(), baseCtx.northWest(), baseCtx.southEast());
			SearchService.SearchResults searchResult = getSearchService().getImmediateSearchResults(
					phraseCtx,
					new SearchService.SearchOption(true, null, null, true, false, (net.osmand.search.core.ObjectType[]) null),
					null);
			SearchPhrase phrase = searchResult.phrase();
			List<SearchResult> searchResults = searchResult.results();
			if (phrase == null || searchResults == null) {
				continue;
			}

			List<String> phraseResults = results.get(phraseIndex);
			for (int i = 0; i < Math.min(limit, searchResults.size()); i++) {
				SearchResult searchResultItem = searchResults.get(i);
				boolean markGeocoding = i < geocodingLimit && isReverseGeocodingCandidate(searchResultItem);
				if (markGeocoding) {
					nextRouteId = exportReverseGeocodingRoutes(searchResultItem, geoUtils, geocodingContexts,
							exportedRoutes, routing, nextRouteId);
				}
				String formatted = SearchUICore.formatSearchResultForTest(false, searchResultItem, phrase);
				phraseResults.add(markGeocoding ? "@" + formatted : formatted);
			}
		}
		return new UnitTestResultsData(results, routing);
	}

	private boolean isReverseGeocodingCandidate(SearchResult searchResult) {
		return searchResult != null
				&& searchResult.file != null
				&& searchResult.location != null;
	}

	private long exportReverseGeocodingRoutes(SearchResult searchResult, GeocodingUtilities geoUtils,
			Map<String, RoutingContext> geocodingContexts, Map<String, Long> exportedRoutes,
			JSONArray routing, long nextRouteId) throws IOException {
		BinaryMapIndexReader reader = searchResult.file;
		File file = reader.getFile();
		if (file == null) {
			return nextRouteId;
		}
		String readerKey = file.getAbsolutePath();
		RoutingContext ctx = geocodingContexts.get(readerKey);
		if (ctx == null) {
			ctx = GeocodingUtilities.buildDefaultContextForPOI(reader);
			geocodingContexts.put(readerKey, ctx);
		}
		List<GeocodingUtilities.GeocodingResult> geoResults = geoUtils.reverseGeocodingSearch(
				ctx, searchResult.location.getLatitude(), searchResult.location.getLongitude(), false);
		for (GeocodingUtilities.GeocodingResult geoResult : geoResults) {
			if (geoResult.point == null || geoResult.point.getRoad() == null) {
				continue;
			}
			RouteDataObject road = geoResult.point.getRoad();
			String routeKey = road.region.getFilePointer() + ":" + road.region.getLength() + ":" + road.getId();
			if (exportedRoutes.containsKey(routeKey)) {
				continue;
			}
			long routeId = nextRouteId++;
			exportedRoutes.put(routeKey, routeId);
			routing.put(routeDataObjectToJson(road, routeId));
		}
		return nextRouteId;
	}

	private JSONObject routeDataObjectToJson(RouteDataObject road, long routeId) {
		JSONObject routeJson = new JSONObject();
		routeJson.put("id", routeId);
		routeJson.put("pointsX", toJsonArray(road.pointsX));
		routeJson.put("pointsY", toJsonArray(road.pointsY));
		JSONArray types = new JSONArray();
		if (road.types != null) {
			for (int type : road.types) {
				BinaryMapRouteReaderAdapter.RouteTypeRule rule = road.region.quickGetEncodingRule(type);
				if (rule != null) {
					JSONObject typeJson = new JSONObject();
					typeJson.put("tag", rule.getTag());
					typeJson.put("value", rule.getValue());
					types.put(typeJson);
				}
			}
		}
		routeJson.put("types", types);
		JSONArray names = new JSONArray();
		if (road.nameIds != null && road.names != null) {
			for (int nameId : road.nameIds) {
				BinaryMapRouteReaderAdapter.RouteTypeRule rule = road.region.quickGetEncodingRule(nameId);
				if (rule == null) {
					continue;
				}
				JSONObject nameJson = new JSONObject();
				nameJson.put("tag", rule.getTag());
				nameJson.put("value", road.names.get(nameId));
				names.put(nameJson);
			}
		}
		routeJson.put("names", names);
		return routeJson;
	}

	private JSONArray toJsonArray(int[] values) {
		JSONArray arr = new JSONArray();
		if (values == null) {
			return arr;
		}
		for (int value : values) {
			arr.put(value);
		}
		return arr;
	}

	private void unzipObf(File obfGzFile, File obfFile) throws IOException {
		GZIPInputStream gzin = new GZIPInputStream(new FileInputStream(obfGzFile));
		FileOutputStream fous = new FileOutputStream(obfFile);
		Algorithms.streamCopy(gzin, fous);
		fous.close();
		gzin.close();
	}
	
	default UnitTestSourceData executeUnitTest(File obfDir, String name) throws IOException {
		String sourceJsonText = Algorithms.getFileAsString(new File(obfDir, name + ".json"));
		JSONObject sourceJson = new JSONObject(sourceJsonText);
		JSONArray phrasesJson = sourceJson.optJSONArray("phrases");
		String singlePhrase = sourceJson.optString("phrase", null);
		JSONObject settingsJson = sourceJson.getJSONObject("settings");
		SearchSettings settings = SearchSettings.parseJSON(settingsJson);
		SearchExportSettings exportSettings = new SearchExportSettings(true, true, -1);
		settings.setExportSettings(exportSettings);
		settings.setRadiusLevel(1);
		
		List<String> queries = new ArrayList<>();
		if (singlePhrase != null) {
			queries.add(singlePhrase);
		}
		if (phrasesJson != null) {
			for (int i = 0; i < phrasesJson.length(); i++) {
				String phrase = phrasesJson.optString(i);
				if (phrase != null) {
					queries.add(phrase);
				}
			}
		}

		List<BinaryMapIndexReader> readers = new ArrayList<>();
		File file = new File(obfDir, name + ".obf");
		File gzFile = new File(obfDir, name + ".obf.gz");
		try {
			boolean useData = settingsJson.optBoolean("useData", true);
			JSONArray filesJson = sourceJson.optJSONArray("files");
			if (useData) {
				if (filesJson != null) {
					for (int i = 0; i < filesJson.length(); i++) {
						String fileName = filesJson.optString(i);
						if (fileName != null && fileName.endsWith(".obf.gz")) {
							gzFile = new File(obfDir, fileName);
							file = new File(obfDir, fileName.replace(".gz", ""));
							unzipObf(gzFile, file);
							readers.add(new BinaryMapIndexReader(new RandomAccessFile(file.getPath(), "r"), file));
						}
					}
				} else {
					unzipObf(gzFile, file);
					readers.add(new BinaryMapIndexReader(new RandomAccessFile(file.getPath(), "r"), file));
				}
			}

			final SearchUICore core = new SearchUICore(MapPoiTypes.getDefault(), "en", false);
			core.init();
			core.updateSettings(settings);

			ResultMatcher<SearchResult> rm = new ResultMatcher<>() {
				@Override
				public boolean publish(SearchResult object) {
					return true;
				}

				@Override
				public boolean isCancelled() {
					return false;
				}
			};
			settings.setOfflineIndexes(readers);

			SearchPhrase emptyPhrase = SearchPhrase.emptyPhrase(settings);
			LinkedHashMap<String, Amenity> amenities = new LinkedHashMap<>();
			LinkedHashMap<Long, City> cities = new LinkedHashMap<>();
			Map<String, LinkedHashMap<String, Amenity>> amenitiesByReader = new LinkedHashMap<>();
			Map<String, LinkedHashMap<Long, City>> citiesByReader = new LinkedHashMap<>();
			SearchService.SearchResults results = null;
			for (String q : queries) {
				UnitTestSearchData searchData = searchUnitTestQuery(core, emptyPhrase, settings, rm, q);
				SearchUICore.SearchResultCollection collection = searchData.collection();
				SearchUICore.SearchResultMatcher matcher = searchData.matcher();

				JSONObject json = SearchUICore.createTestJSON(collection,
						getExportedObjectsOrResultObjects(collection, matcher),
						getExportedCitiesOrResultCities(collection, matcher));
				String unitTestJson = json == null ? null : json.toString(4);

				collectUnitTestSourceData(unitTestJson, amenities, cities);
				if (readers.size() > 1) {
					collectUnitTestSourceDataByReader(collection, matcher, amenitiesByReader, citiesByReader);
				}
				results = new SearchService.SearchResults(collection.getCurrentSearchResults(), settings, unitTestJson, collection.getPhrase());
			}
			File sourceDir = new File(obfDir, "source");
			sourceDir.mkdir();
			
			if (readers.size() > 1) {
				List<String> jsonFilePaths = new ArrayList<>();
				for (BinaryMapIndexReader reader : readers) {
					String readerFileName = reader.getFile().getName();
					File sourceJsonFile = sourceDir.toPath().resolve(unitTestJsonFileName(readerFileName)).toFile();
					UnitTestSourceData data = createUnitTestJsonFile(sourceJsonFile, results, new JSONArray(),
							amenitiesByReader.getOrDefault(readerFileName, new LinkedHashMap<>()),
							citiesByReader.getOrDefault(readerFileName, new LinkedHashMap<>()));
					jsonFilePaths.add(data.jsonFilePath());
				}
				return new UnitTestSourceData(jsonFilePaths.get(0), jsonFilePaths, results);
			}
			
			return createUnitTestJson(sourceDir.toPath(), name, results, new JSONArray(), amenities, cities);
		} finally {
			for(BinaryMapIndexReader reader : readers) {
				reader.getFile().deleteOnExit();
			}
		}
	}

	private UnitTestSearchData searchUnitTestQuery(SearchUICore core, SearchPhrase emptyPhrase, SearchSettings settings,
			ResultMatcher<SearchResult> rm, String query) throws IOException {
		String[] arr = query == null ? new String[0] : query.split("[\\\\{}]");
		if (arr.length > 0 && arr[0].equals("POI_TYPE:")) {
			boolean displayDefaultPoiTypes = SearchCoreFactory.DISPLAY_DEFAULT_POI_TYPES;
			try {
				SearchCoreFactory.DISPLAY_DEFAULT_POI_TYPES = true;
				UnitTestSearchData categories = searchUnitTestPhrase(core, emptyPhrase.generateNewPhrase("", settings), rm);
				for (SearchResult searchResult : categories.collection().getCurrentSearchResults()) {
					if (arr.length > 1 && arr[1].equals(searchResult.localeName)) {
						String fullText = arr.length > 2 ? arr[2] : "";
						SearchPhrase phrase = emptyPhrase.generateNewPhrase(fullText, settings);
						phrase.getWords().add(new SearchWord(searchResult.localeName, searchResult));
						return searchUnitTestPhrase(core, phrase, rm);
					}
				}
				return categories;
			} finally {
				SearchCoreFactory.DISPLAY_DEFAULT_POI_TYPES = displayDefaultPoiTypes;
			}
		}
		return searchUnitTestPhrase(core, emptyPhrase.generateNewPhrase(query, settings), rm);
	}

	private UnitTestSearchData searchUnitTestPhrase(SearchUICore core, SearchPhrase phrase,
			ResultMatcher<SearchResult> rm) throws IOException {
		SearchUICore.SearchResultMatcher matcher = new SearchUICore.SearchResultMatcher(rm, phrase, 1, new AtomicInteger(1), -1);
		core.searchInternal(phrase, matcher);
		SearchUICore.SearchResultCollection collection = new SearchUICore.SearchResultCollection(phrase);
		collection.addSearchResults(matcher.getRequestResults(), true, true);
		return new UnitTestSearchData(collection, matcher);
	}

	private void collectUnitTestSourceDataByReader(SearchUICore.SearchResultCollection collection,
			SearchUICore.SearchResultMatcher matcher,
			Map<String, LinkedHashMap<String, Amenity>> amenitiesByReader,
			Map<String, LinkedHashMap<Long, City>> citiesByReader) {
		Map<String, List<SearchResult>> resultsByReader = new LinkedHashMap<>();
		for (SearchResult result : collection.getCurrentSearchResults()) {
			String readerFileName = unitTestReaderFileName(result);
			if (readerFileName != null) {
				resultsByReader.computeIfAbsent(readerFileName, k -> new ArrayList<>()).add(result);
			}
		}
		for (Map.Entry<String, List<SearchResult>> entry : resultsByReader.entrySet()) {
			SearchUICore.SearchResultCollection readerCollection = new SearchUICore.SearchResultCollection(collection.getPhrase());
			readerCollection.addSearchResults(entry.getValue(), false, false);
			JSONObject json = SearchUICore.createTestJSON(readerCollection,
					filterExportedObjects(entry.getValue(), getExportedObjectsOrResultObjects(readerCollection, matcher)),
					filterExportedCities(entry.getValue(), getExportedCitiesOrResultCities(readerCollection, matcher)));
			if (json != null) {
				collectUnitTestSourceData(json.toString(4),
						amenitiesByReader.computeIfAbsent(entry.getKey(), k -> new LinkedHashMap<>()),
						citiesByReader.computeIfAbsent(entry.getKey(), k -> new LinkedHashMap<>()));
			}
		}
	}

	private List<MapObject> getExportedObjectsOrResultObjects(SearchUICore.SearchResultCollection collection,
			SearchUICore.SearchResultMatcher matcher) {
		List<MapObject> exportedObjects = matcher.getExportedObjects();
		if (exportedObjects != null && !exportedObjects.isEmpty()) {
			return exportedObjects;
		}
		Set<MapObject> usedObjects = Collections.newSetFromMap(new IdentityHashMap<>());
		Set<City> usedCities = Collections.newSetFromMap(new IdentityHashMap<>());
		collectUnitTestResultObjects(collection.getCurrentSearchResults(), usedObjects, usedCities);
		return new ArrayList<>(usedObjects);
	}

	private List<City> getExportedCitiesOrResultCities(SearchUICore.SearchResultCollection collection,
			SearchUICore.SearchResultMatcher matcher) {
		List<City> exportedCities = matcher.getExportedCities();
		if (exportedCities != null && !exportedCities.isEmpty()) {
			return exportedCities;
		}
		Set<MapObject> usedObjects = Collections.newSetFromMap(new IdentityHashMap<>());
		Set<City> usedCities = Collections.newSetFromMap(new IdentityHashMap<>());
		collectUnitTestResultObjects(collection.getCurrentSearchResults(), usedObjects, usedCities);
		return new ArrayList<>(usedCities);
	}

	private List<MapObject> filterExportedObjects(List<SearchResult> results, List<MapObject> exportedObjects) {
		if (exportedObjects == null || exportedObjects.isEmpty()) {
			return exportedObjects;
		}
		Set<MapObject> usedObjects = Collections.newSetFromMap(new IdentityHashMap<>());
		Set<City> usedCities = Collections.newSetFromMap(new IdentityHashMap<>());
		collectUnitTestResultObjects(results, usedObjects, usedCities);
		List<MapObject> filtered = new ArrayList<>();
		for (MapObject object : exportedObjects) {
			if (usedObjects.contains(object) || object instanceof City city && usedCities.contains(city)) {
				filtered.add(object);
			}
		}
		return filtered;
	}

	private List<City> filterExportedCities(List<SearchResult> results, List<City> exportedCities) {
		if (exportedCities == null || exportedCities.isEmpty()) {
			return exportedCities;
		}
		Set<MapObject> usedObjects = Collections.newSetFromMap(new IdentityHashMap<>());
		Set<City> usedCities = Collections.newSetFromMap(new IdentityHashMap<>());
		collectUnitTestResultObjects(results, usedObjects, usedCities);
		List<City> filtered = new ArrayList<>();
		for (City city : exportedCities) {
			if (usedCities.contains(city)) {
				filtered.add(city);
			}
		}
		return filtered;
	}

	private void collectUnitTestResultObjects(List<SearchResult> results, Set<MapObject> usedObjects, Set<City> usedCities) {
		for (SearchResult result : results) {
			collectUnitTestResultObjects(result, usedObjects, usedCities);
		}
	}

	private void collectUnitTestResultObjects(SearchResult result, Set<MapObject> usedObjects, Set<City> usedCities) {
		if (result == null) {
			return;
		}
		collectUnitTestResultObject(result.object, usedObjects, usedCities);
		collectUnitTestResultObject(result.relatedObject, usedObjects, usedCities);
		if (result.parentSearchResult != result) {
			collectUnitTestResultObjects(result.parentSearchResult, usedObjects, usedCities);
		}
	}

	private void collectUnitTestResultObject(Object object, Set<MapObject> usedObjects, Set<City> usedCities) {
		if (object instanceof MapObject mapObject) {
			usedObjects.add(mapObject);
			if (mapObject instanceof City city) {
				usedCities.add(city);
			} else if (mapObject instanceof Street street && street.getCity() != null) {
				usedCities.add(street.getCity());
			}
		}
	}

	private String unitTestReaderFileName(SearchResult result) {
		if (result == null || result.file == null || result.file.getFile() == null) {
			return null;
		}
		return result.file.getFile().getName();
	}

	private String unitTestJsonFileName(String fileName) {
		int dot = fileName.lastIndexOf('.');
		return (dot > 0 ? fileName.substring(0, dot) : fileName) + ".json";
	}
}
