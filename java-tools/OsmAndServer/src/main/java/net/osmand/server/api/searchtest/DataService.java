package net.osmand.server.api.searchtest;

import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository.Dataset;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import net.osmand.server.api.searchtest.repo.SearchTestDatasetRepository;
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
import java.util.stream.Collectors;
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

			String tableName = "dataset_" + sanitize(dataset.name);
			String header = getHeader(fullPath);
			if (header == null || header.trim().isEmpty()) {
				dataset.setError("File doesn't have header.");
				return dataset;
			}

			String del = header.chars().filter(ch -> ch == ',').count() <
							header.chars().filter(ch -> ch == ';').count() ? ";" : ",";
			String[] headers =
					Stream.of(header.toLowerCase().split(del)).map(DataService::sanitize).toArray(String[]::new);
			dataset.allCols = getObjectMapper().writeValueAsString(headers);
			if (!Arrays.asList(headers).contains("lat") || !Arrays.asList(headers).contains("lon")) {
				String error = "Header doesn't include mandatory 'lat' or 'lon' fields.";
				LOGGER.error("{} Header: {}", error, String.join(",", headers));
				dataset.setError(error);
				return dataset;
			}

			dataset.selCols = getObjectMapper().writeValueAsString(Arrays.stream(headers).filter(s ->
					s.startsWith("road") || s.startsWith("city") ||
							s.startsWith("stree") || s.startsWith("addr")).toArray(String[]::new));
			if (reload) {
				List<String> sample = reservoirSample(fullPath, dataset.sizeLimit);
				insertSampleData(tableName, headers, sample.subList(1, sample.size()), del, true);
				dataset.total = sample.size() - 1;
				LOGGER.info("Stored {} rows into table: {}", sample.size(), tableName);
			}

			dataset.setSourceStatus(dataset.total != null ? Dataset.ConfigStatus.OK : Dataset.ConfigStatus.UNKNOWN);
			return getDatasetRepo().save(dataset);
		} catch (Exception e) {
			dataset.setError(e.getMessage() == null ? e.toString() : e.getMessage());

			LOGGER.error("Failed to process and insert data from CSV file: {}", fullPath, e);
			return dataset;
		} finally {
			if (dataset.type == Dataset.Source.Overpass) {
				try {
					if (fullPath != null && !Files.deleteIfExists(fullPath)) {
						LOGGER.warn("Could not delete temporary file: {}", fullPath);
					}
				} catch (IOException e) {
					LOGGER.error("Error deleting temporary file: {}", fullPath, e);
				}
			}
		}
	}


	default TestCase generate(Dataset dataset, TestCase test) {
		if (dataset.getSourceStatus() != Dataset.ConfigStatus.OK) {
			test.status = TestCase.Status.FAILED;
			LOGGER.info("Dataset {} is not in OK state ({}).", dataset.id, dataset.getSourceStatus());
			return test;
		}

		try {
			String tableName = "dataset_" + sanitize(dataset.name);

			String[] selCols = getObjectMapper().readValue(test.selCols, String[].class);
			List<String> columns = new ArrayList<>(), delCols = new ArrayList<>();
			Collections.addAll(columns, selCols);
			if (Arrays.stream(selCols).noneMatch("lon"::equals)) {
				columns.add("lon");
				delCols.add("lon");
			}
			if (Arrays.stream(selCols).noneMatch("lat"::equals)) {
				columns.add("lat");
				delCols.add("lat");
			}
			if (Arrays.stream(selCols).noneMatch("id"::equals)) {
				columns.add("id");
				delCols.add("id");
			}

			String sql = String.format("SELECT %s FROM %s", String.join(",", columns), tableName);
			List<Map<String, Object>> rows = getJdbcTemplate().queryForList(sql);
			List<PolyglotEngine.GenRow> examples = getEngine().execute(dataset.script, test, delCols, rows);
			for (PolyglotEngine.GenRow example : examples) {
				saveCaseResults(test, example);
			}
			test.status = TestCase.Status.GENERATED;
		} catch (Exception e) {
			LOGGER.error("Generation of test-case failed for on dataset {}", dataset.id, e);
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

		String sql = "INSERT INTO run_result (gen_id, count, dataset_id, run_id, case_id, query, row, error, " +
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

		String tableName = "dataset_" + sanitize(dataset.name);
		String sql = "SELECT * FROM " + tableName;
		try {
			StringWriter stringWriter = new StringWriter();
			List<Map<String, Object>> rows = getJdbcTemplate().queryForList(sql);
			if (rows.isEmpty()) {
				return "";
			}

			String[] headers = rows.get(0).keySet().toArray(new String[0]);
			CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader(headers).setDelimiter(';').build();
			try (final CSVPrinter printer = new CSVPrinter(stringWriter, csvFormat)) {
				for (Map<String, Object> row : rows) {
					printer.printRecord(row.values());
				}
			}

			return stringWriter.toString();
		} catch (Exception e) {
			LOGGER.error("Failed to retrieve sample for dataset {}", datasetId, e);
			throw new RuntimeException("Failed to generate dataset sample: " + e.getMessage(), e);
		}
	}

	default boolean deleteDataset(Long datasetId) {
		Optional<Dataset> dsOpt = getDatasetRepo().findById(datasetId);
		if (dsOpt.isEmpty()) {
			return false;
		}
		String tableName = "dataset_" + sanitize(dsOpt.get().name);
		getJdbcTemplate().execute("DROP TABLE IF EXISTS " + tableName);

		String deleteSql = "DELETE FROM run_result WHERE dataset_id = ?";
		getJdbcTemplate().update(deleteSql, datasetId);

		deleteSql = "DELETE FROM test_case WHERE dataset_id = ?";
		getJdbcTemplate().update(deleteSql, datasetId);

		Dataset ds = dsOpt.get();
		getDatasetRepo().delete(ds);
		return true;
	}

	default void insertSampleData(String tableName, String[] columns, List<String> sample, String delimiter,
								 boolean deleteBefore) {
		if (deleteBefore) {
			getJdbcTemplate().execute("DROP TABLE IF EXISTS " + tableName);
		}
		createDynamicTable(tableName, columns);

		String insertSql =
				"INSERT INTO " + tableName + " (" + String.join(", ", columns) + ") VALUES (" + String.join(", ",
						Collections.nCopies(columns.length, "?")) + ")";
		List<Object[]> batchArgs = new ArrayList<>();
		for (String s : sample) {
			String[] record = s.split(delimiter);
			String[] values = Collections.nCopies(columns.length, "").toArray(new String[0]);
			for (int j = 0; j < values.length && j < record.length; j++) {
				values[j] = crop(unquote(record[j]), 255);
			}
			batchArgs.add(values);
		}
		getJdbcTemplate().batchUpdate(insertSql, batchArgs);
		LOGGER.info("Batch inserted {} records into {}.", sample.size(), tableName);
	}

	private void createDynamicTable(String tableName, String[] columns) {
		String columnsDefinition =
				Stream.of(columns).map(header -> "\"" + header + "\" VARCHAR(255)").collect(Collectors.joining(", "));
		String createTableSql =
				String.format("CREATE TABLE IF NOT EXISTS %s (_id INTEGER PRIMARY KEY AUTOINCREMENT, " + "%s)",
						tableName, columnsDefinition);
		getJdbcTemplate().execute(createTableSql);
		LOGGER.info("Ensured table {} exists.", tableName);
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
			LOGGER.error("Failed to retrieve labels", e);
			throw new RuntimeException("Failed to retrieve labels: " + e.getMessage(), e);
		}
	}
}
