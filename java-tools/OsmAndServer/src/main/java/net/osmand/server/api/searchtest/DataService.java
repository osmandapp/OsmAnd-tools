package net.osmand.server.api.searchtest;

import net.osmand.binary.*;
import net.osmand.data.*;
import net.osmand.search.core.SearchResult;
import net.osmand.server.api.services.search.ClassicSearchService;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository.Dataset;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import net.osmand.util.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static net.osmand.search.core.ObjectType.POI_TYPE;

public interface DataService extends BaseService {
	SearchTestDatasetRepository getDatasetRepo();

	SearchTestCaseRepository getTestCaseRepo();

	PolyglotEngine getEngine();

	static String sanitize(String input) {
		if (input == null) {
			return "";
		}
		return input.trim().toLowerCase().replaceAll("[^a-zA-Z0-9_]", "_");
	}

	private Dataset checkDatasetInternal(Dataset dataset, boolean reload) {
		long startedNs = System.nanoTime();
		reload = dataset.total == null || reload;

		Path fullPath = null;
		dataset.setSourceStatus(Dataset.ConfigStatus.UNKNOWN);
		int rowCount = -1;
		try {
			if (dataset.type == Dataset.Source.Overpass) {
				fullPath = Files.createTempFile(Path.of(getCsvDownloadingDir()), "overpass_", ".csv");
				rowCount = queryOverpass(fullPath, dataset.source);
			} else if (dataset.type == Dataset.Source.UnitTest) {
				fullPath = Files.createTempFile(Path.of(getCsvDownloadingDir()), "unit_test_", ".csv");
				rowCount = convertUnitTestsToCsv(fullPath, dataset.source);
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
						List<CSVRecord> recs = parser.getRecords();
						if (recs.isEmpty()) {
							record = new String[0];
						} else {
							CSVRecord r = recs.get(0);
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
			dataset = getDatasetRepo().save(dataset);
			if (dataset.type == Dataset.Source.UnitTest && reload) {
				recreateUnitTestCase(dataset);
			}
			return dataset;
		} catch (Exception e) {
			dataset.setError(e.getMessage() == null ? e.toString() : e.getMessage());

			getLogger().error("Failed to process and insert data from CSV file: {}", fullPath, e);
			return dataset;
		} finally {
			if (dataset.type == Dataset.Source.Overpass || dataset.type == Dataset.Source.UnitTest) {
				try {
					if (fullPath != null && !Files.deleteIfExists(fullPath)) {
						getLogger().warn("Could not delete temporary file: {}", fullPath);
					}
				} catch (IOException e) {
					getLogger().error("Error deleting temporary file: {}", fullPath, e);
				}
			}
			getLogger().info("PERF checkDatasetInternal datasetId={} type={} reload={} rows={} status={} elapsedMs={}",
					dataset.id, dataset.type, reload, dataset.total, dataset.getSourceStatus(),
					(System.nanoTime() - startedNs) / 1_000_000);
		}
	}

	private void recreateUnitTestCase(Dataset dataset) {
		getJdbcTemplate().update("DELETE FROM run_result WHERE case_id IN "
				+ "(SELECT id FROM test_case WHERE dataset_id = ? AND name = 'Main')", dataset.id);
		getJdbcTemplate().update("DELETE FROM run WHERE case_id IN "
				+ "(SELECT id FROM test_case WHERE dataset_id = ? AND name = 'Main')", dataset.id);
		getJdbcTemplate().update("DELETE FROM gen_result WHERE case_id IN "
				+ "(SELECT id FROM test_case WHERE dataset_id = ? AND name = 'Main')", dataset.id);
		getJdbcTemplate().update("DELETE FROM test_case WHERE dataset_id = ? AND name = 'Main'", dataset.id);

		TestCase test = new TestCase();
		test.datasetId = dataset.id;
		test.name = "Main";
		test.status = TestCase.Status.NEW;
		test.allCols = dataset.allCols;
		test.selCols = dataset.selCols;
		test.updated = LocalDateTime.now();
		generate(dataset, getTestCaseRepo().save(test));
	}

	private int convertUnitTestsToCsv(Path outputPath, String source) throws IOException {
		List<SpatialSearchTestRunner.CSVRow> rows = new SpatialSearchTestRunner(Path.of(source)).run();
		if (rows.isEmpty()) {
			throw new IOException("Unit-test execution produced no dataset rows: " + source);
		}
		CSVFormat format = CSVFormat.DEFAULT.builder().setHeader("lat", "lon", "unit-test", "query", "point", "id", "result", "entityType").build();
		try (BufferedWriter writer = Files.newBufferedWriter(outputPath);
			 CSVPrinter printer = new CSVPrinter(writer, format)) {
			for (SpatialSearchTestRunner.CSVRow row : rows) {
				printer.printRecord(row.location().getLatitude(), row.location().getLongitude(), row.unitTest(),
						sanitizeCsvValue(row.query()), row.point(), row.osmId(), sanitizeCsvValue(row.result()), row.entityType());
			}
		}
		return rows.size();
	}

	default TestCase generate(Dataset dataset, TestCase test) {
		long startedNs = System.nanoTime();
		int generatedRows = 0;
		if (dataset.getSourceStatus() != Dataset.ConfigStatus.OK) {
			test.status = TestCase.Status.FAILED;
			getLogger().info("Dataset {} is not in OK state ({}).", dataset.id, dataset.getSourceStatus());
			getLogger().info("PERF generate datasetId={} caseId={} type={} rows={} status={} elapsedMs={}",
					dataset.id, test.id, dataset.type, generatedRows, test.status,
					(System.nanoTime() - startedNs) / 1_000_000);
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

			List<PolyglotEngine.GenRow> examples = dataset.type == Dataset.Source.UnitTest
					? buildUnitTestExamples(dataset, rows)
					: getEngine().execute(getWebServerConfigDir(), test, rows);
			generatedRows = examples.size();
			double north = -Double.MAX_VALUE;
			double south = Double.MAX_VALUE;
			double west = Double.MAX_VALUE;
			double east = -Double.MAX_VALUE;
			boolean bboxInitialized = false;
			for (PolyglotEngine.GenRow example : examples) {
				if (example.point() != null) {
					LatLon point = example.point();
					north = Math.max(north, point.getLatitude());
					south = Math.min(south, point.getLatitude());
					west = Math.min(west, point.getLongitude());
					east = Math.max(east, point.getLongitude());
					bboxInitialized = true;
					saveCaseResults(test, example);
				} else {
					getLogger().warn("Dataset row: {} has no point.", rows.get(example.dsResultId()));
				}
			}
			if (bboxInitialized) {
				test.setNorthWest(String.format(Locale.US, "%.5f, %.5f", north, west));
				test.setSouthEast(String.format(Locale.US, "%.5f, %.5f", south, east));
			}
			test.status = TestCase.Status.GENERATED;
		} catch (Exception e) {
			getLogger().error("Generation of test-case failed for on dataset {}", dataset.id, e);
			test.setError(e.getMessage());
			test.status = TestCase.Status.FAILED;
		} finally {
			test.updated = LocalDateTime.now();
			getLogger().info("PERF generate datasetId={} caseId={} type={} rows={} status={} elapsedMs={}",
					dataset.id, test.id, dataset.type, generatedRows, test.status,
					(System.nanoTime() - startedNs) / 1_000_000);
		}
		return getTestCaseRepo().save(test);
	}

	private List<PolyglotEngine.GenRow> buildUnitTestExamples(Dataset dataset, Map<Integer, String> rows)
			throws IOException {
		String[] columns = getObjectMapper().readValue(dataset.allCols, String[].class);
		Map<String, Integer> indexes = new HashMap<>();
		for (int i = 0; i < columns.length; i++) {
			indexes.put(columns[i], i);
		}
		List<String> required = List.of("lat", "lon", "unit_test", "query", "point", "id", "result", "entitytype");
		if (!indexes.keySet().containsAll(required)) {
			throw new IOException("UnitTest dataset is missing columns: " + required.stream()
					.filter(column -> !indexes.containsKey(column)).toList());
		}

		Map<List<String>, List<Map<String, Object>>> grouped = new LinkedHashMap<>();
		Map<List<String>, Integer> sourceIds = new HashMap<>();
		for (Map.Entry<Integer, String> entry : rows.entrySet()) {
			String[] values = getObjectMapper().readValue(entry.getValue(), String[].class);
			String unitTest = values[indexes.get("unit_test")];
			List<String> key = List.of(values[indexes.get("lat")], values[indexes.get("lon")],
					unitTest, values[indexes.get("query")]);
			Map<String, Object> expected = new LinkedHashMap<>();
			expected.put("point", values[indexes.get("point")]);
			expected.put("id", Long.parseLong(values[indexes.get("id")]));
			expected.put("result", values[indexes.get("result")]);
			expected.put("entityType", values[indexes.get("entitytype")]);
			expected.put("unitTest", unitTest);
			grouped.computeIfAbsent(key, ignored -> new ArrayList<>()).add(expected);
			sourceIds.putIfAbsent(key, entry.getKey());
		}

		List<PolyglotEngine.GenRow> examples = new ArrayList<>();
		for (Map.Entry<List<String>, List<Map<String, Object>>> entry : grouped.entrySet()) {
			List<String> key = entry.getKey();
			examples.add(new PolyglotEngine.GenRow(sourceIds.get(key),
					new LatLon(Double.parseDouble(key.get(0)), Double.parseDouble(key.get(1))), entry.getValue(),
					getObjectMapper().writeValueAsString(new String[] {key.get(3)}), 1, null, 0, key.get(2)));
		}
		return examples;
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
				"INSERT INTO gen_result (ds_result_id, gen_count, case_id, dataset_id, row, query, unit_test, error, duration, lat, lon, " +
						"timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		String rowJson = getObjectMapper().writeValueAsString(row.row());
		String[] outputArray = row.output() == null || row.count() <= 0 ? new String[]{null} :
				getObjectMapper().readValue(row.output(), String[].class);
		for (String query : outputArray) {
			getJdbcTemplate().update(sql, row.dsResultId(), row.count(), test.id, test.datasetId, rowJson, query,
					row.unitTest(), row.error(),
					row.duration(),
					row.point().getLatitude(), row.point().getLongitude(),
					new Timestamp(System.currentTimeMillis()));
		}
	}

	default Object[] collectRunResults(ResultActuator actuator, long genId, int count, Run run, String query,
	                                   ClassicSearchService.SearchResults searchResult, LatLon targetPoint,
	                                   LatLon searchPoint, long duration, String bbox, String error) throws IOException {
		if (error != null || targetPoint == null) {
			return new Object[] {genId, count, run.datasetId, run.id, run.caseId, query, null, error, duration,
					0, null, null, null, searchPoint.getLatitude(), searchPoint.getLongitude(), bbox,
					new Timestamp(System.currentTimeMillis()), false, null, null};
		}

		List<SearchResult> searchResults = searchResult == null ? Collections.emptyList() : searchResult.results();
		int minDist = Integer.MAX_VALUE;
        for (SearchResult sr : searchResults) {
            if (sr.location == null && sr.objectType == POI_TYPE) {
                sr.location = targetPoint;
            }
			if (sr.location == null) {
				continue;
			}
			double dist = MapUtils.getDistance(targetPoint, sr.location);
			if (minDist > dist) {
				minDist = (int) dist;
			}
        }

		Map<String, Object> statMetrics = actuator.getMetrics();
		BinaryMapIndexReaderStats.SearchStat stat = searchResult != null && searchResult.settings() != null
				? searchResult.settings().getStat()
				: null;
		int resultsCount = searchResults.size();
		statMetrics.put("time", duration);
		
		if (stat != null) {
			statMetrics.put("min_dist", minDist);
			statMetrics.put("stat_bytes", stat.totalBytes);
			statMetrics.put("stat_time", stat.totalTime);
			int statResultsCount = 0;
			int statAmenityCount = 0;
			int statTransportCount = 0;
			int statAddressCount = 0;
			for (BinaryMapIndexReaderStats.WordSearchStat wordSearchStat : stat.getWordStats().values()) {
				if (wordSearchStat == null) {
					continue;
				}
				statResultsCount += wordSearchStat.results;
				if (wordSearchStat.resultCounts == null) {
					continue;
				}
				for (Map.Entry<String, Integer> entry : wordSearchStat.resultCounts.entrySet()) {
					String key = entry.getKey();
					Integer value = entry.getValue();
					int resCount = value == null ? 0 : value;
					if (key != null && key.startsWith("Amenity")) {
						statAmenityCount += resCount;
					} else if (key != null && key.startsWith("Transport")) {
						statTransportCount += resCount;
					} else {
						statAddressCount += resCount;
					}
				}
			}
			statMetrics.put("stat_results", statResultsCount);
			statMetrics.put("stat_amenity_count", statAmenityCount);
			statMetrics.put("stat_address_count", statAddressCount);
			statMetrics.put("stat_transport_count", statTransportCount);

			for (Map.Entry<BinaryMapIndexReaderStats.BinaryMapIndexReaderApiName, BinaryMapIndexReaderStats.StatByAPI> e : stat.getByApis().entrySet()) {
				statMetrics.put("stat_time_" + e.getKey().name(), e.getValue().time);
				statMetrics.put("stat_bytes_" + e.getKey().name(), e.getValue().bytes);
				statMetrics.put("stat_calls_" + e.getKey().name(), e.getValue().calls);
			}
			statMetrics.put("sub_stats", stat.getSubStatsSummary());
		} else {
			int statAmenityCount = 0;
			int statAddressCount = 0;
			for (SearchResult sr : searchResults) {
				if (sr.object instanceof Amenity || sr.objectType == POI_TYPE) {
					statAmenityCount++;
				} else {
					statAddressCount++;
				}
			}
			statMetrics.put("stat_results", resultsCount);
			statMetrics.put("stat_amenity_count", statAmenityCount);
			statMetrics.put("stat_address_count", statAddressCount);
			statMetrics.put("stat_transport_count", 0);
		}

		boolean found = actuator.isFound(searchResults);

		String statsJson = getObjectMapper().writeValueAsString(statMetrics);
		Object statTimeValue = statMetrics.get("stat_time");
		Object statBytesValue = statMetrics.get("stat_bytes");

		return new Object[] {genId, count, run.datasetId, run.id, run.caseId, query, statsJson, actuator.getError(), duration,
				resultsCount, actuator.getResultDistance(), actuator.getResultPoint(), actuator.getResultPlace(),
				searchPoint == null ? null : searchPoint.getLatitude(),
				searchPoint == null ? null : searchPoint.getLongitude(),
				bbox,
				new Timestamp(System.currentTimeMillis()), found,
				stat != null ? Long.valueOf(stat.totalBytes) : statBytesValue instanceof Number n ? n.longValue() : null,
				stat != null ? Long.valueOf(stat.totalTime) : statTimeValue instanceof Number n ? n.longValue() : null
		};
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
}
