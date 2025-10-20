package net.osmand.server.api.searchtest;

import net.osmand.data.LatLon;
import net.osmand.search.core.SearchResult;
import net.osmand.server.api.searchtest.MapDataObjectFinder.Result;
import net.osmand.server.api.searchtest.MapDataObjectFinder.ResultType;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository.Dataset;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import net.osmand.server.api.services.SearchService;
import net.osmand.util.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVParser;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public interface DataService extends BaseService {

	SearchTestDatasetRepository getDatasetRepo();

	SearchTestCaseRepository getTestCaseRepo();

	PolyglotEngine getEngine();

	SearchService getSearchService();

	private Dataset checkDatasetInternal(Dataset dataset, boolean reload) {
		reload = dataset.total == null || reload;

		Path fullPath = null;
		dataset.setSourceStatus(Dataset.ConfigStatus.UNKNOWN);
		int rowCount = -1;
		try {
			if (dataset.type == Dataset.Source.Overpass) {
				fullPath = Files.createTempFile(Path.of(getCsvDownloadingDir()), "overpass_", ".csv");
				rowCount = queryOverpass(fullPath, dataset.source);
			} else {
				fullPath = Path.of(getCsvDownloadingDir(), dataset.source);
			}
			if (!Files.exists(fullPath)) {
				dataset.setError("File is not existed: " + fullPath);
				return dataset;
			}

			String header = getHeader(fullPath);
			if (header == null || header.trim().isEmpty()) {
				dataset.setError(rowCount == 0 ? "Source rows count is 0." : "File doesn't have header.");
				return dataset;
			}

			String delimiter = header.chars().filter(ch -> ch == ',').count() <
					header.chars().filter(ch -> ch == ';').count() ? ";" : ",";
			String[] columns =
					Stream.of(header.toLowerCase().split(delimiter)).map(DataService::sanitize).toArray(String[]::new);
			dataset.allCols = getObjectMapper().writeValueAsString(columns);
			List<String> colsList = Arrays.asList(columns);
			int latIndex = colsList.indexOf("lat");
			int lonIndex = colsList.indexOf("lon");
			int idIndex = colsList.indexOf("id");
			if (latIndex == -1 || lonIndex == -1 || idIndex == -1) {
				String error = String.format("Header doesn't include mandatory fields: 'lat', 'lon' or 'id' (%d, %d, %d)",
						latIndex, lonIndex, idIndex);
				getLogger().error("{} Header: {}", error, String.join(",", columns));
				dataset.setError(error);
				return dataset;
			}

			if (dataset.selCols == null)
				dataset.selCols = getObjectMapper().writeValueAsString(Arrays.stream(columns).filter(s ->
						s.startsWith("road") || s.startsWith("city") || s.startsWith("street")));
			if (reload) {
				List<String> sample = reservoirSample(fullPath, dataset.sizeLimit);

				dataset.total = sample.size() - 1;
				dataset.setSourceStatus(Dataset.ConfigStatus.UNKNOWN);
				dataset = getDatasetRepo().save(dataset);
				getJdbcTemplate().update("DELETE FROM dataset_result WHERE dataset_id = ?", dataset.id);

				char delim = delimiter.charAt(0);
				CSVFormat format = CSVFormat.DEFAULT.builder()
						.setDelimiter(delim)
						.setQuote('"')
						.setIgnoreSurroundingSpaces(false)
						.setTrim(false)
						.build();

				List<Object[]> batchArgs = new ArrayList<>();
				for (int i = 1; i < sample.size(); i++) {
					String line = sample.get(i);
					// Parse the CSV line using Apache Commons CSV to handle quoted fields with delimiters
					String[] record;
					try (CSVParser parser = CSVParser.parse(line, format)) {
						List<org.apache.commons.csv.CSVRecord> recs = parser.getRecords();
						if (recs.isEmpty()) {
							record = new String[0];
						} else {
							org.apache.commons.csv.CSVRecord r = recs.get(0);
							record = new String[r.size()];
							for (int c = 0; c < r.size(); c++) {
								record[c] = r.get(c);
							}
						}
					}
					String[] values = Collections.nCopies(columns.length, "").toArray(new String[0]);
					for (int j = 0; j < values.length && j < record.length; j++) {
						values[j] = crop(unquote(record[j]), 255);
					}
					if (values[latIndex] != null && values[lonIndex] != null && values[idIndex] != null)
						batchArgs.add(new Object[] {dataset.id, getObjectMapper().writeValueAsString(values)});
					else
						getLogger().warn("Dataset row: {} doesn't have lat={}, lon={} or id={}",
								getObjectMapper().writeValueAsString(values),
								values[latIndex] != null, values[lonIndex] != null, values[idIndex] != null);
				}

				String insertSql = "INSERT INTO dataset_result (dataset_id, value) VALUES (?, ?)";
				getJdbcTemplate().batchUpdate(insertSql, batchArgs);

				getLogger().info("Stored {} rows into dataset: {}", sample.size() - 1, dataset.name);
			}

			dataset.setSourceStatus(dataset.total != null ? Dataset.ConfigStatus.OK : Dataset.ConfigStatus.UNKNOWN);
			return getDatasetRepo().save(dataset);
		} catch (Exception e) {
			dataset.setError(e.getMessage() == null ? e.toString() : e.getMessage());

			getLogger().error("Failed to process and insert data from CSV file: {}", fullPath, e);
			return dataset;
		} finally {
			if (dataset.type == Dataset.Source.Overpass) {
				try {
					if (fullPath != null && !Files.deleteIfExists(fullPath)) {
						getLogger().warn("Could not delete temporary file: {}", fullPath);
					}
				} catch (IOException e) {
					getLogger().error("Error deleting temporary file: {}", fullPath, e);
				}
			}
		}
	}

	default TestCase generate(Dataset dataset, TestCase test) {
		if (dataset.getSourceStatus() != Dataset.ConfigStatus.OK) {
			test.status = TestCase.Status.FAILED;
			getLogger().info("Dataset {} is not in OK state ({}).", dataset.id, dataset.getSourceStatus());
			return test;
		}

		try {
			Map<Integer, String> rows = getJdbcTemplate().query(
					"SELECT id, value FROM dataset_result WHERE dataset_id = ? ORDER BY id", new Object[]{dataset.id},
					(ResultSet rs) -> {
						Map<Integer, String> result = new LinkedHashMap<>();
						while (rs.next()) {
							result.put(rs.getInt("id"), rs.getString("value"));
						}
						return result;
					}
			);
			assert rows != null;

			List<PolyglotEngine.GenRow> examples = getEngine().execute(getWebServerConfigDir(), test, rows);
			for (PolyglotEngine.GenRow example : examples) {
				if (example.point() != null)
					saveCaseResults(test, example);
				else
					getLogger().warn("Dataset row: {} has no point.", rows.get(example.dsResultId()));
			}
			test.status = TestCase.Status.GENERATED;
		} catch (Exception e) {
			getLogger().error("Generation of test-case failed for on dataset {}", dataset.id, e);
			test.setError(e.getMessage());
			test.status = TestCase.Status.FAILED;
		} finally {
			test.updated = LocalDateTime.now();
		}
		return getTestCaseRepo().save(test);
	}

	@Async
	default CompletableFuture<Dataset> createDataset(Dataset dataset) {
		return CompletableFuture.supplyAsync(() -> {
			Optional<Dataset> datasetOptional = getDatasetRepo().findByName(dataset.name);
			if (datasetOptional.isPresent()) {
				dataset.setError("Dataset is already created: " + dataset.name);
				return dataset;
			}

			dataset.created = LocalDateTime.now();
			dataset.updated = dataset.created;
			return checkDatasetInternal(dataset, true);
		});
	}

	default TestCase updateTestCase(Long id, Map<String, String> updates) {
		TestCase test = getTestCaseRepo().findById(id).orElseThrow(() ->
				new RuntimeException("TestCase not found with id: " + id));

		updates.forEach((key, value) -> {
			switch (key) {
				case "name" -> test.name = value;
				case "labels" -> test.labels = value;
			}
		});

		test.updated = LocalDateTime.now();
		return getTestCaseRepo().save(test);
	}

	@Async
	default CompletableFuture<Dataset> updateDataset(Long id, Boolean reload, Map<String, String> updates) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset dataset = getDatasetRepo().findById(id).orElseThrow(() ->
					new RuntimeException("Dataset not found with id: " + id));

			updates.forEach((key, value) -> {
				switch (key) {
					case "name" -> dataset.name = value;
					case "type" -> dataset.type = Dataset.Source.valueOf(value);
					case "source" -> dataset.source = value;
					case "sizeLimit" -> dataset.sizeLimit = Integer.valueOf(value);
					case "labels" -> dataset.labels = value;
				}
			});

			dataset.updated = LocalDateTime.now();
			dataset.setSourceStatus(Dataset.ConfigStatus.OK);
			return checkDatasetInternal(dataset, reload);
		});
	}

	default void saveCaseResults(TestCase test, PolyglotEngine.GenRow row) throws IOException {
		String sql =
				"INSERT INTO gen_result (ds_result_id, gen_count, case_id, dataset_id, row, query, error, duration, lat, lon, " +
						"timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		String rowJson = getObjectMapper().writeValueAsString(row.row());
		String[] outputArray = row.output() == null || row.count() <= 0 ? new String[]{null} :
				getObjectMapper().readValue(row.output(), String[].class);
		for (String query : outputArray) {
			getJdbcTemplate().update(sql, row.dsResultId(), row.count(), test.id, test.datasetId, rowJson, query, row.error(),
					row.duration(),
					row.point().getLatitude(), row.point().getLongitude(),
					new java.sql.Timestamp(System.currentTimeMillis()));
		}
	}

	int SEARCH_DUPLICATE_NAME_RADIUS = 5000;
	int FOUND_DEDUPLICATE_RADIUS = 100;

	default void saveRunResults(Map<String, Object> genRow, long genId, int count, Run run, String query, SearchService.SearchResultWrapper searchResult, LatLon targetPoint,
	                            LatLon searchPoint, long duration, String bbox, String error) throws IOException {
		final MapDataObjectFinder finder = new MapDataObjectFinder();
		long datasetId;
		try {
			datasetId = Long.parseLong((String) genRow.get("id"));
		} catch (NumberFormatException e) {
			datasetId = -1;
		}

		List<SearchResult> searchResults = searchResult == null ? Collections.emptyList() : searchResult.results();
		Map<String, Object> row = new LinkedHashMap<>();
		Result firstResult = finder.findFirstResult(searchResults, targetPoint, row);
		Result actualResult = finder.findActualResult(searchResults, targetPoint, datasetId, row);

		int resultsCount = searchResults.size();
		Integer distance = null, resPlace = null;
		String resultPoint = null;
		boolean found = false;
		if (firstResult != null) {
			int dupCount = 0;
			double closestDuplicate = MapUtils.getDistance(targetPoint, firstResult.searchResult().location);
			int dupInd = firstResult.place() - 1;
			String resName = firstResult.searchResult().toString(); // to do check to string is not too much
			for (int i = firstResult.place(); i < searchResults.size(); i++) {
				SearchResult sr = searchResults.get(i);
				double dist = MapUtils.getDistance(firstResult.searchResult().location, sr.location);
				if (resName.equals(sr.toString()) && dist < SEARCH_DUPLICATE_NAME_RADIUS) {
					dupCount++;
				} else {
					break;
				}
				if (MapUtils.getDistance(targetPoint, sr.location) < closestDuplicate) {
					closestDuplicate = MapUtils.getDistance(targetPoint, sr.location);
					dupInd = i;
				}
			}
			resPlace = firstResult.place();
			LatLon resPoint = firstResult.searchResult().location;
			resultPoint = String.format(Locale.US, "%f, %f", resPoint.getLatitude(), resPoint.getLongitude());
			distance = ((int) MapUtils.getDistance(targetPoint, resPoint) / 10) * 10;

			if (dupCount > 0) {
				row.put("dup_count", dupCount);
			}
			if (searchResult != null && searchResult.stat() != null) {
				row.put("stat_bytes", searchResult.stat().totalBytes);
				row.put("stat_time", searchResult.stat().totalTime);
			}
			row.put("time", duration);
			row.put("web_type", firstResult.searchResult().objectType);
			row.put("res_id", firstResult.toIdString());
			row.put("res_place", firstResult.toPlaceString());
			row.put("res_name", firstResult.placeName());
			if (actualResult == null && closestDuplicate < FOUND_DEDUPLICATE_RADIUS) {
				SearchResult sr = searchResults.get(dupInd);
				actualResult = new Result(ResultType.ByDist, null, dupInd + 1, sr);
			}
			if (actualResult != null) {
				row.put("actual_place", actualResult.toPlaceString());
				row.put("actual_id", actualResult.toIdString());
				row.put("actual_name", actualResult.placeName());
				LatLon pnt = actualResult.searchResult().location;
				row.put("actual_lat_lon", String.format(Locale.US, "%f, %f", pnt.getLatitude(), pnt.getLongitude()));
				found = actualResult.place() <= dupCount + firstResult.place();
			}
			found |= closestDuplicate < FOUND_DEDUPLICATE_RADIUS; // deduplication also count as found
		}

		String sql = "INSERT OR IGNORE INTO run_result (gen_id, gen_count, dataset_id, run_id, case_id, query, row, error, " +
				"duration, res_count, res_distance, res_lat_lon, res_place, lat, lon, bbox, timestamp, found, stat_bytes, stat_time) " +
				"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		String rowJson = getObjectMapper().writeValueAsString(row);
		getJdbcTemplate().update(sql, genId, count, run.datasetId, run.id, run.caseId, query, rowJson, error, duration,
				resultsCount,
				distance, resultPoint, resPlace,
				searchPoint == null ? null : searchPoint.getLatitude(),
				searchPoint == null ? null : searchPoint.getLongitude(),
				bbox,
				new java.sql.Timestamp(System.currentTimeMillis()), found,
				searchResult != null && searchResult.stat() != null ? searchResult.stat().totalBytes : null,
				searchResult != null && searchResult.stat() != null ? searchResult.stat().totalTime : null);
	}

	/**
	 * Find all datasets matching the given filters.
	 *
	 * @param name     Case-insensitive search for the dataset name.
	 * @param labels   Case-insensitive search for comma-separated labels associated with the dataset.
	 * @param pageable Pageable request defining the page number and size.
	 * @return Page of matching datasets.
	 */
	default Page<Dataset> getDatasets(String name, String labels, Pageable pageable) {
		return getDatasetRepo().findAllDatasets(name, labels, pageable);
	}

	default String getDatasetSample(Long datasetId) {
		Dataset dataset = getDatasetRepo().findById(datasetId).orElseThrow(() ->
				new RuntimeException("Dataset not found with id: " + datasetId));

		try {
			StringWriter stringWriter = new StringWriter();
			List<String> rows = getJdbcTemplate().queryForList(
					"SELECT value FROM dataset_result WHERE dataset_id = ?", String.class, dataset.id);
			if (rows.isEmpty()) {
				return "";
			}

			String[] headers = getObjectMapper().readValue(dataset.allCols, String[].class);
			CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader(headers).build();
			try (final CSVPrinter printer = new CSVPrinter(stringWriter, csvFormat)) {
				for (String jsonValue : rows) {
					printer.printRecord((Object[]) getObjectMapper().readValue(jsonValue, String[].class));
				}
			}

			return stringWriter.toString();
		} catch (Exception e) {
			getLogger().error("Failed to retrieve sample for dataset {}", datasetId, e);
			throw new RuntimeException("Failed to generate dataset sample: " + e.getMessage(), e);
		}
	}

	default Map<String, Object> getDatasetSample(Long datasetId, int position) {
		Dataset dataset = getDatasetRepo().findById(datasetId).orElseThrow(() ->
				new RuntimeException("Dataset not found with id: " + datasetId));

		String sql = "SELECT value FROM dataset_result WHERE dataset_id = ? ORDER BY id LIMIT 1 OFFSET ?";
		try {
			String[] headers = getObjectMapper().readValue(dataset.allCols, String[].class);
			Map<String, Object> sample = new LinkedHashMap<>();
			String jsonValues = getJdbcTemplate().queryForObject(sql, String.class, datasetId, position);
			String[] values = getObjectMapper().readValue(jsonValues, String[].class);
			for (int i = 0; i < headers.length; i++) {
				sample.put(headers[i], values[i]);
			}

			return sample;
		} catch (EmptyResultDataAccessException e) {
			return Collections.emptyMap();
		} catch (Exception e) {
			getLogger().error("Failed to retrieve sample row for dataset {} at position {}", datasetId, position, e);
			throw new RuntimeException("Failed to retrieve dataset sample row: " + e.getMessage(), e);
		}
	}

	default boolean deleteDataset(Long datasetId) {
		Optional<Dataset> dsOpt = getDatasetRepo().findById(datasetId);
		if (dsOpt.isEmpty()) {
			return false;
		}
		getJdbcTemplate().update("DELETE FROM dataset_result WHERE dataset_id = ?", datasetId);
		getJdbcTemplate().update("DELETE FROM run_result WHERE dataset_id = ?", datasetId);
		getJdbcTemplate().update("DELETE FROM gen_result WHERE dataset_id = ?", datasetId);
		getJdbcTemplate().update("DELETE FROM run WHERE dataset_id = ?", datasetId);
		getJdbcTemplate().update("DELETE FROM test_case WHERE dataset_id = ?", datasetId);

		Dataset ds = dsOpt.get();
		getDatasetRepo().delete(ds);
		return true;
	}

	/**
	 * Domains API (CRUD)
	 */
	default List<SearchTestDatasetRepository.Domain> getDomains(String query, int limit) {
		final String q = query == null ? "" : query;
		final int lim = Math.max(1, Math.min(limit <= 0 ? 20 : limit, 200));
		String sql = "SELECT id, name, data FROM domain " +
				"WHERE COALESCE(?, '') = '' OR lower(name) LIKE lower(?) || '%' OR lower(name) LIKE '%' || lower(?) || '%' " +
				"ORDER BY name LIMIT ?";
		try {
			return getJdbcTemplate().query(sql, (rs, i) -> {
				SearchTestDatasetRepository.Domain d = new SearchTestDatasetRepository.Domain();
				long id = rs.getLong("id");
				d.id = rs.wasNull() ? null : id;
				d.name = rs.getString("name");
				d.data = rs.getString("data");
				return d;
			}, q, q, q, lim);
		} catch (Exception e) {
			getLogger().error("Failed to list domains for query: {}", query, e);
			throw new RuntimeException("Failed to list domains: " + e.getMessage(), e);
		}
	}

	default SearchTestDatasetRepository.Domain createDomain(String name, String data) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Domain name must not be empty");
		}
		String n = name.trim();
		String d = data == null ? "" : data;
		Integer exists = getJdbcTemplate().queryForObject(
				"SELECT COUNT(1) FROM domain WHERE lower(name) = lower(?)", Integer.class, n);
		if (exists > 0) {
			throw new RuntimeException("Domain already exists: " + n);
		}
		getJdbcTemplate().update("INSERT INTO domain(name, data) VALUES(?, ?)", n, d);
		Long id = getJdbcTemplate().queryForObject("SELECT id FROM domain WHERE name = ?", Long.class, n);
		SearchTestDatasetRepository.Domain out = new SearchTestDatasetRepository.Domain();
		out.id = id;
		out.name = n;
		out.data = d;
		return out;
	}

	default SearchTestDatasetRepository.Domain updateDomain(Long id, Map<String, String> updates) {
		if (id == null) throw new IllegalArgumentException("Domain id is required");
		SearchTestDatasetRepository.Domain current = getJdbcTemplate().query(
				"SELECT id, name, data FROM domain WHERE id = ?",
				rs -> rs.next() ? new SearchTestDatasetRepository.Domain() {{
					long i = rs.getLong("id");
					this.id = rs.wasNull() ? null : i;
					this.name = rs.getString("name");
					this.data = rs.getString("data");
				}} : null,
				id);
		if (current == null) {
			throw new RuntimeException("Domain not found with id: " + id);
		}
		String newName = updates.getOrDefault("name", current.name);
		String newData = updates.getOrDefault("data", current.data);
		if (newName == null || newName.trim().isEmpty()) {
			throw new IllegalArgumentException("Domain name must not be empty");
		}
		getJdbcTemplate().update("UPDATE domain SET name = ?, data = ? WHERE id = ?", newName.trim(), newData, id);
		current.name = newName.trim();
		current.data = newData;
		return current;
	}

	default boolean deleteDomain(Long id) {
		if (id == null) return false;
		int n = getJdbcTemplate().update("DELETE FROM domain WHERE id = ?", id);
		return n > 0;
	}

	default List<String> getBranches() {
		try {
			String sql = "SELECT DISTINCT name FROM run WHERE name IS NOT NULL AND TRIM(name) <> '' ORDER BY name";
			return getJdbcTemplate().queryForList(sql, String.class);
		} catch (Exception e) {
			getLogger().error("Failed to retrieve branches", e);
			throw new RuntimeException("Failed to retrieve branches: " + e.getMessage(), e);
		}
	}

	default List<String> getAllLabels() {
		try {
			String sql = "SELECT labels FROM dataset";
			List<String> rows = getJdbcTemplate().queryForList(sql, String.class);
			sql = "SELECT labels FROM test_case";
			rows.addAll(getJdbcTemplate().queryForList(sql, String.class));

			Set<String> set = new LinkedHashSet<>();
			for (String label : rows) {
				if (label == null) {
					continue;
				}
				for (String l : label.split("[#,]+")) { // split by '#' or ',', treat consecutive as one
					String t = l.trim();
					if (!t.isEmpty()) {
						set.add(t);
					}
				}
			}

			List<String> results = new ArrayList<>(set);
			results.sort(null);
			return results;
		} catch (Exception e) {
			getLogger().error("Failed to retrieve labels", e);
			throw new RuntimeException("Failed to retrieve labels: " + e.getMessage(), e);
		}
	}

	static String sanitize(String input) {
		if (input == null) {
			return "";
		}
		return input.trim().toLowerCase().replaceAll("[^a-zA-Z0-9_]", "_");
	}
}
