package net.osmand.server.api.searchtest;

import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository.Dataset;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.util.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

public interface DataService extends BaseService {
	static String sanitize(String input) {
		if (input == null) {
			return "";
		}
		return input.toLowerCase().replaceAll("[^a-zA-Z0-9_]", "_");
	}

	SearchTestDatasetRepository getDatasetRepo();

	SearchTestCaseRepository getTestCaseRepo();

	PolyglotEngine getEngine();

	private Dataset checkDatasetInternal(Dataset dataset, boolean reload) {
		reload = dataset.total == null || reload;

		Path fullPath = null;
		dataset.setSourceStatus(Dataset.ConfigStatus.UNKNOWN);
		try {
			if (dataset.type == Dataset.Source.Overpass) {
				fullPath = queryOverpass(dataset.source);
			} else {
				fullPath = Path.of(getCsvDownloadingDir(), dataset.source);
			}
			if (!Files.exists(fullPath)) {
				dataset.setError("File is not existed: " + fullPath);
				return dataset;
			}

			String header = getHeader(fullPath);
			if (header == null || header.trim().isEmpty()) {
				dataset.setError("File doesn't have header.");
				return dataset;
			}

			String delimiter = header.chars().filter(ch -> ch == ',').count() <
					header.chars().filter(ch -> ch == ';').count() ? ";" : ",";
			String[] columns =
					Stream.of(header.toLowerCase().split(delimiter)).map(DataService::sanitize).toArray(String[]::new);
			dataset.allCols = getObjectMapper().writeValueAsString(columns);
			if (!Arrays.asList(columns).contains("lat") || !Arrays.asList(columns).contains("lon")) {
				String error = "Header doesn't include mandatory 'lat' or 'lon' fields.";
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

				List<Object[]> batchArgs = new ArrayList<>();
				for (int i = 1; i < sample.size(); i++) {
					String[] record = sample.get(i).split(delimiter);
					String[] values = Collections.nCopies(columns.length, "").toArray(new String[0]);
					for (int j = 0; j < values.length && j < record.length; j++) {
						values[j] = crop(unquote(record[j]), 255);
					}
					batchArgs.add(new Object[] {dataset.id, getObjectMapper().writeValueAsString(values)});
				}

				String insertSql = "INSERT INTO dataset_result (dataset_id, value) VALUES (?, ?)";
				getJdbcTemplate().batchUpdate(insertSql, batchArgs);

				getLogger().info("Stored {} rows into dataset: {}", sample.size(), dataset.name);
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
			List<String> rows = getJdbcTemplate().queryForList(
					"SELECT value FROM dataset_result WHERE dataset_id = ?", String.class, dataset.id);

			List<PolyglotEngine.GenRow> examples = getEngine().execute(getWebServerConfigDir(), test, rows);
			for (PolyglotEngine.GenRow example : examples) {
				saveCaseResults(test, example);
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
				new RuntimeException("Dataset not found with id: " + id));

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

	default void saveCaseResults(TestCase test, PolyglotEngine.GenRow data) throws IOException {
		String sql =
				"INSERT INTO gen_result (count, case_id, dataset_id, row, query, error, duration, lat, lon, " +
						"timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		String rowJson = getObjectMapper().writeValueAsString(data.row());
		String[] outputArray = data.output() == null || data.count() <= 0 ? new String[]{null} :
				getObjectMapper().readValue(data.output(), String[].class);
		for (String query : outputArray) {
			getJdbcTemplate().update(sql, data.count(), test.id, test.datasetId, rowJson, query, data.error(),
					data.duration(),
					data.point().getLatitude(), data.point().getLongitude(),
					new java.sql.Timestamp(System.currentTimeMillis()));
		}
	}

	default void saveRunResults(long genId, int count, Run run, String output, Map<String, Object> row,
	                            List<Feature> searchResults, LatLon targetPoint, long duration, String error) throws IOException {
		int resultsCount = searchResults.size();
		Feature minFeature = null;
		Integer minDistance = null, actualPlace = null;
		String closestResult = null;

		if (targetPoint != null && !searchResults.isEmpty()) {
			double minDistanceMeters = Double.MAX_VALUE;
			LatLon closestPoint = null;
			int place = 0;

			for (Feature feature : searchResults) {
				place++;
				if (feature == null) {
					continue;
				}
				LatLon foundPoint = getLatLon(feature);
				double distance = MapUtils.getDistance(targetPoint.getLatitude(), targetPoint.getLongitude(),
						foundPoint.getLatitude(), foundPoint.getLongitude());
				if (distance < minDistanceMeters) {
					minDistanceMeters = distance;
					closestPoint = foundPoint;
					actualPlace = place;
					minFeature = feature;
				}
			}

			if (closestPoint != null) {
				minDistance = (int) minDistanceMeters;
				closestResult = pointToString(closestPoint);
			}
		}

		if (minFeature != null) {
			for (Map.Entry<String, Object> e : minFeature.properties.entrySet())
				row.put(e.getKey(), e.getValue() == null ? "" : e.getValue().toString());
		}

		String sql = "INSERT OR IGNORE INTO run_result (gen_id, count, dataset_id, run_id, case_id, query, row, error, " +
				"duration, results_count, min_distance, closest_result, actual_place, lat, lon, timestamp) " +
				"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		String rowJson = getObjectMapper().writeValueAsString(row);
		getJdbcTemplate().update(sql, genId, count, run.datasetId, run.id, run.caseId, output, rowJson, error, duration,
				resultsCount,
				minDistance, closestResult, actualPlace, targetPoint == null ? null : targetPoint.getLatitude(),
				targetPoint == null ? null : targetPoint.getLongitude(),
				new java.sql.Timestamp(System.currentTimeMillis()));
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
}
