package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.amazonaws.util.StringInputStream;
import net.osmand.binary.*;
import net.osmand.data.*;
import net.osmand.obf.OBFDataCreator;
import net.osmand.router.RoutingContext;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.SearchExportSettings;
import net.osmand.search.core.SearchPhrase;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
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
			return toResults(ctx, results, startTime);
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

	private ResultsWithStats toResults(SearchService.SearchContext ctx, SearchService.SpatialResults spatialResponse, long startTime) {
		String totalTime = String.format(Locale.US, "%.1f", (System.currentTimeMillis() - startTime) / 1e3);
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
			@JsonProperty("geocodingLimit") Integer geocodingLimit,
			@JsonProperty("quote") Double quote,
			@JsonProperty("radius") Integer radius) {}

	record UnitTestSourceData(String jsonFilePath, List<String> jsonFilePaths, SearchSettings settings, List<List<String>> geoResults) {
		UnitTestSourceData(String jsonFilePath, SearchSettings settings, List<List<String>> geoResults) {
			this(jsonFilePath, Collections.singletonList(jsonFilePath), settings, geoResults);
		}
	}

	default void createUnitTest(UnitTestPayload unitTest, SearchService.SearchContext ctx, OutputStream out, boolean spatial) throws IOException, SQLException {
		Path rootTmp = Path.of(System.getProperty("java.io.tmpdir"));
		Path dirPath = Files.createTempDirectory(rootTmp, "unit-tests-");
		try {
			int limit = unitTest.resultsLimit();
			int geocodingLimit = unitTest.geocodingLimit();
			UnitTestSourceData sourceData = createUnitTestSourceData(unitTest, ctx, dirPath, spatial);
			if (sourceData.jsonFilePath == null) {
				return;
			}
			File configJsonFile = dirPath.resolve(unitTest.name + ".json").toFile();
			File sourceJsonFile = gzip(new File(sourceData.jsonFilePath));

			OBFDataCreator creator = new OBFDataCreator();
			File outFile = creator.create(dirPath.resolve(unitTest.name + ".obf").toAbsolutePath().toString(),
					new String[] {sourceData.jsonFilePath()});

			// Build ZIP with JSON metadata and gzipped data, streaming directly to the servlet output
			SearchSettings settings = sourceData.settings().setOriginalLocation(new LatLon(ctx.lat(), ctx.lon()));
			JSONObject settingsJson = settings.toJSON();
			JSONArray formattedResultsJson = new JSONArray();
			for (List<String> phraseResults : sourceData.geoResults()) {
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
			Files.writeString(configJsonFile.toPath(), unitTestJson, StandardCharsets.UTF_8);
			try (ZipOutputStream zipOut = new ZipOutputStream(out)) {
				// JSON metadata entry
				if (configJsonFile.exists()) {
					zipOut.putNextEntry(new ZipEntry(configJsonFile.getName()));
					try (InputStream is = new StringInputStream(unitTestJson)) {
						Algorithms.streamCopy(is, zipOut);
					}
					zipOut.closeEntry();
				}

				// JSON source entry
				if (sourceJsonFile.exists()) {
					zipOut.putNextEntry(new ZipEntry(sourceJsonFile.getName()));
					try (InputStream is = new FileInputStream(sourceJsonFile)) {
						Algorithms.streamCopy(is, zipOut);
					}
					zipOut.closeEntry();
				}
				
				// Gzipped data archive entry
				if (outFile.exists()) {
					zipOut.putNextEntry(new ZipEntry(outFile.getName()));
					try (InputStream is = new FileInputStream(outFile)) {
						Algorithms.streamCopy(is, zipOut);
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

	private String amenityKey(Amenity amenity) {
		return amenity.getId() + "|" + amenity.getType() + "|" + amenity.getSubType();
	}

	private City compactUnitTestCity(City city) {
		if (city.getStreets().size() <= 1) {
			return city;
		}
		JSONObject json = city.toJSON(false);
		json.remove("listOfStreets");
		return City.parseJSON(json);
	}

	private Street compactUnitTestStreet(Street street, City city) {
		if (street.getBuildings().size() <= 1) {
			return street;
		}
		JSONObject json = street.toJSON(false);
		json.remove("buildings");
		json.remove("intersectedStreets");
		return Street.parseJSON(city, json);
	}

	private City collectCompactUnitTestCity(City city, Map<Long, City> cities) {
		if (city == null || city.getId() == null) {
			return null;
		}
		City existing = cities.get(city.getId());
		if (existing == null) {
			existing = compactUnitTestCity(city);
			cities.put(existing.getId(), existing);
		}
		return existing;
	}

	private Street collectCompactUnitTestStreet(Street street, Map<Long, City> cities) {
		if (street == null || street.getCity() == null) {
			return null;
		}
		City city = collectCompactUnitTestCity(street.getCity(), cities);
		if (city == null) {
			return null;
		}
		for (Street existing : city.getStreets()) {
			if (existing.equals(street)) {
				return existing;
			}
		}
		Street compactStreet = compactUnitTestStreet(street, city);
		city.registerStreet(compactStreet);
		return compactStreet;
	}

	private void collectCompactUnitTestBuilding(Building building, Street parentStreet, Map<Long, City> cities) {
		Street street = collectCompactUnitTestStreet(parentStreet, cities);
		if (building != null && street != null) {
			street.addBuildingCheckById(Building.parseJSON(building.toJSON()));
		}
	}

	private boolean isWithinUnitTestSourceRadius(MapObject object, LatLon point, Integer radius) {
		if (radius == null || radius <= 0) {
			return true;
		}
		return object != null && object.getLocation() != null && point != null
				&& MapUtils.getDistance(point, object.getLocation()) < radius;
	}

	private void collectUnitTestSourceData(SearchService.SpatialResults spatialResponse, Map<Long, City> cities, Map<String, Amenity> amenities, UnitTestPayload unitTest) {
		if (spatialResponse == null || spatialResponse.results() == null || spatialResponse.results().mainResults == null) {
			return;
		}
		List<SpatialSearchResult> mainResults = spatialResponse.results().mainResults;
		for (int i = 0; i < Math.min(unitTest.resultsLimit(), mainResults.size()); i++) {
			SpatialSearchResult res = mainResults.get(i);
			if (res == null) {
				continue;
			}
			List<MapObject> objects = res.getObjects();
			if (objects == null) {
				return;
			}
			Building resultBuilding = null;
			Street resultBuildingStreet = null;
            for (MapObject object : objects) {
                if (object instanceof Building building) {
                    resultBuilding = building;
                    break;
                }
            }
            if (resultBuilding != null) {
                for (MapObject object : objects) {
                    if (object instanceof Street street) {
                        resultBuildingStreet = street;
                        break;
                    }
                }
                collectCompactUnitTestBuilding(resultBuilding, resultBuildingStreet, cities);
			}
			for (MapObject object : objects) {
				if (object instanceof Amenity amenity) {
					String amenityKey = amenityKey(amenity);
					amenities.putIfAbsent(amenityKey, amenity);
				} else if (object instanceof City city) {
					collectCompactUnitTestCity(city, cities);
				} else if (object instanceof Street street) {
					collectCompactUnitTestStreet(street, cities);
				}
			}
		}
	}

	private void filterSourceData(Map<String, Amenity> amenities, Map<Long, City> cities, UnitTestPayload unitTest) {
		int before = amenities.size();
		Double quote = unitTest.quote;
		if (quote != null && quote < 1.0) {
			amenities.entrySet().removeIf(entry -> Math.random() >= quote);
		}
		getLogger().info("Amenities: before = {}, after={}", before, amenities.size());

		before = cities.size();
		if (quote != null && quote < 1.0) {
			cities.entrySet().removeIf(e -> (e.getValue().getStreets() == null || e.getValue().getStreets().isEmpty()) && Math.random() >= quote);
		}
		getLogger().info("Cities: before = {}, after={}", before, cities.size());
		
		before = 0;
		int after = 0;
		for (City city : cities.values()) {
			List<Street> streets = city.getStreets();
			if (streets == null || streets.isEmpty()) {
				continue;
			}
			before += streets.size();
			if (quote != null && quote < 1.0) {
				streets.removeIf(street -> Math.random() >= quote);
			}
			after += streets.size();
		}
		getLogger().info("Streets: before = {}, after={}", before, after);
	}

	private void collectUnitTestSourceData(JSONObject sourceJson, Map<String, Amenity> amenities, Map<Long, City> cities,
	                                       LatLon point, UnitTestPayload unitTest) {
		JSONArray amenitiesJson = sourceJson.optJSONArray("amenities");
		if (amenitiesJson != null) {
			for (int i = 0; i < amenitiesJson.length(); i++) {
				Amenity amenity = Amenity.parseJSON(amenitiesJson.getJSONObject(i));
				if (isWithinUnitTestSourceRadius(amenity, point, unitTest.radius())) {
					amenities.putIfAbsent(amenityKey(amenity), amenity);
				}
			}
		}

		JSONArray citiesJson = sourceJson.optJSONArray("cities");
		if (citiesJson != null) {
			for (int i = 0; i < citiesJson.length(); i++) {
				City city = City.parseJSON(citiesJson.getJSONObject(i));
				city.getStreets().removeIf(street -> !isWithinUnitTestSourceRadius(street, point, unitTest.radius()));
				if (!isWithinUnitTestSourceRadius(city, point, unitTest.radius()) && city.getStreets().isEmpty()) {
					continue;
				}
				City existing = cities.get(city.getId());
				if (existing == null) {
					cities.put(city.getId(), city);
				} else {
					existing.mergeWith(city);
				}
			}
		}
	}
	
	private UnitTestSourceData createUnitTestSourceData(UnitTestPayload unitTest, SearchService.SearchContext baseCtx,
	                                                    Path dirPath, Boolean spatial) throws IOException {
		SearchExportSettings exportSettings = new SearchExportSettings(true, true, -1);
		SearchService.SearchOption options = new SearchService.SearchOption(true, exportSettings,
				null, true, false, (net.osmand.search.core.ObjectType[]) null);
		
		String[] queries = normalizedUnitTestQueries(unitTest.queries(), baseCtx.text());
		LinkedHashMap<String, Amenity> amenities = new LinkedHashMap<>();
		LinkedHashMap<Long, City> cities = new LinkedHashMap<>();
		SearchSettings settings = new SearchSettings(Collections.emptyList());
		LatLon point = new LatLon(baseCtx.lat(), baseCtx.lon());
		
		JSONArray routing = new JSONArray();
		long nextRouteId = 1;
		List<List<String>> geoResults = new ArrayList<>();
		if (unitTest.quote != null && unitTest.quote > 0.0 && unitTest.radius != null && unitTest.radius > 0) {
			GeocodingUtilities geoUtils = new GeocodingUtilities();
			Map<String, RoutingContext> geocodingContexts = new HashMap<>();
			Map<String, Long> exportedRoutes = new LinkedHashMap<>();
			for (String q : queries) {
				SearchService.SearchContext ctx = new SearchService.SearchContext(
						baseCtx.lat(), baseCtx.lon(), q, baseCtx.locale(),
						baseCtx.baseSearch(), baseCtx.northWest(), baseCtx.southEast());

				SearchService.SearchResults results = getSearchService().getImmediateSearchResults(ctx, options, null);
				SearchPhrase phrase = results.phrase();
				List<SearchResult> searchResults = results.results();
				if (phrase == null || searchResults == null) {
					continue;
				}
				List<String> phraseResults = new ArrayList<>();
				for (int i = 0; i < Math.min(unitTest.resultsLimit, results.results().size()); i++) {
					SearchResult searchResultItem = results.results().get(i);
					boolean markGeocoding = i < unitTest.geocodingLimit && isReverseGeocodingCandidate(searchResultItem);
					if (markGeocoding) {
						nextRouteId = exportReverseGeocodingRoutes(searchResultItem, geoUtils, geocodingContexts,
								exportedRoutes, routing, nextRouteId);
					}
					String formatted = SearchUICore.formatSearchResultForTest(false, searchResultItem, phrase);
					phraseResults.add(markGeocoding ? "@" + formatted : formatted);
				}
				geoResults.add(phraseResults);

				collectUnitTestSourceData(results.unitTestJson(), amenities, cities, point, unitTest);
				getLogger().info("Sampling search results for query '{}': {}, cities: {}, amenities: {}", q, searchResults.size(), cities.size(), amenities.size());
				settings = results.settings();
			}
			filterSourceData(amenities, cities, unitTest);
			getLogger().info("Filtered cities: {}, amenities: {}", cities, amenities);
		}

		SearchService.SpatialResults spatialResults;
		if (spatial != null && spatial) {
			for (String q : queries) {
				SearchService.SearchContext ctx = new SearchService.SearchContext(
						baseCtx.lat(), baseCtx.lon(), q, baseCtx.locale(),
						baseCtx.baseSearch(), baseCtx.northWest(), baseCtx.southEast());
				
				spatialResults = getSearchService().searchTestSpatial(ctx, options, null, false);
				collectUnitTestSourceData(spatialResults, cities, amenities, unitTest);
				
				int[] sizes = getStreetsBuildingSize(cities.values());
				getLogger().info("Spatial search results: {}, cities: {}, streets: {}, buildings: {}, amenities: {}", spatialResults.results().mainResults.size(), cities.size(), sizes[0], sizes[1], amenities.size());
			}
		}
		
		return createUnitTestJson(dirPath, unitTest.name, settings, routing, amenities, cities, geoResults);
	}
	
	private int[] getStreetsBuildingSize(Collection<City> cities) {
		int streetsSize = 0;
		int buildingsSize = 0;
		if (cities == null) {
			return new int[] {streetsSize, buildingsSize};
		}
		for (City city : cities) {
			if (city == null || city.getStreets() == null) {
				continue;
			}
			streetsSize += city.getStreets().size();
			for (Street street : city.getStreets()) {
				if (street != null && street.getBuildings() != null) {
					buildingsSize += street.getBuildings().size();
				}
			}
		}
		return new int[] {streetsSize, buildingsSize};
	}

	private UnitTestSourceData createUnitTestJson(Path dirPath, String name, SearchSettings settings, JSONArray routing, Map<String, Amenity> amenities, Map<Long, City> cities, List<List<String>> geoResults) throws IOException {
		return createUnitTestJsonFile(dirPath.resolve(name + ".json").toFile(), settings, routing, amenities, cities, geoResults);
	}

	private UnitTestSourceData createUnitTestJsonFile(File sourceJsonFile, SearchSettings settings, JSONArray routing,
			Map<String, Amenity> amenities, Map<Long, City> cities, List<List<String>> geoResults) throws IOException {
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
		if (routing != null && !routing.isEmpty()) {
			sourceJson.put("routing", routing);
		}
		Files.writeString(sourceJsonFile.toPath(), sourceJson.toString(4), StandardCharsets.UTF_8);
		return new UnitTestSourceData(sourceJsonFile.getAbsolutePath(), settings, geoResults);
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
}
