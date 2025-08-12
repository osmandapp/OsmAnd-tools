package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.entity.EvalJob;
import net.osmand.server.api.searchtest.repo.DatasetRepository;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.util.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class DataService extends UtilService {
	protected final DatasetRepository datasetRepository;
	protected final JdbcTemplate jdbcTemplate;
	protected final EntityManager em;

	public DataService(EntityManager em, DatasetRepository datasetRepository,
					   @Qualifier("testJdbcTemplate") JdbcTemplate jdbcTemplate, WebClient.Builder webClientBuilder,
					   ObjectMapper objectMapper) {
		super(webClientBuilder, objectMapper);

		this.em = em;
		this.datasetRepository = datasetRepository;
		this.jdbcTemplate = jdbcTemplate;
	}

	@Async
	public CompletableFuture<Dataset> refreshDataset(Long datasetId, boolean reload) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset dataset = datasetRepository.findById(datasetId).orElseThrow(() ->
					new RuntimeException("Dataset not found with id: " + datasetId));
			return checkDatasetInternal(dataset, reload);
		});
	}

	private Dataset checkDatasetInternal(Dataset dataset, boolean reload) {
		reload = dataset.total == null || reload;

		Path fullPath = null;
		dataset.setSourceStatus(Dataset.ConfigStatus.UNKNOWN);
		try {
			if (dataset.type == Dataset.Source.Overpass) {
				fullPath = queryOverpass(dataset.source);
			} else {
				fullPath = Path.of(csvDownloadingDir, dataset.source);
			}
			if (!Files.exists(fullPath)) {
				dataset.setError("File is not existed.");
				datasetRepository.save(dataset);
				return dataset;
			}

			String tableName = "dataset_" + sanitize(dataset.name);
			String header = getHeader(fullPath);
			if (header == null || header.trim().isEmpty()) {
				dataset.setError("File doesn't have header.");
				datasetRepository.save(dataset);
				return dataset;
			}

			String del =
					header.chars().filter(ch -> ch == ',').count() <
							header.chars().filter(ch -> ch == ';').count() ? ";" : ",";
			String[] headers =
					Stream.of(header.toLowerCase().split(del)).map(DataService::sanitize).toArray(String[]::new);
			updateColumns(dataset, headers);
			if (!Arrays.asList(headers).contains("lat") || !Arrays.asList(headers).contains("lon")) {
				dataset.setError("Header doesn't include mandatory 'lat' or 'lon' fields.");
				datasetRepository.save(dataset);
				return dataset;
			}

			if (reload) {
				List<String> sample = reservoirSample(fullPath, dataset.sizeLimit);
				insertSampleData(tableName, headers, sample.subList(1, sample.size()), del, true);
				dataset.total = sample.size() - 1;
				LOGGER.info("Stored {} rows into table: {}", sample.size(), tableName);
			}

			dataset.setSourceStatus(dataset.total != null ? Dataset.ConfigStatus.OK : Dataset.ConfigStatus.UNKNOWN);
			return datasetRepository.save(dataset);
		} catch (Exception e) {
			dataset.setError(e.getMessage() == null ? e.toString() : e.getMessage());

			LOGGER.error("Failed to process and insert data from CSV file: {}", fullPath, e);
			return datasetRepository.save(dataset);
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

	@Async
	public CompletableFuture<Dataset> createDataset(Dataset dataset) {
		return CompletableFuture.supplyAsync(() -> {
			Optional<Dataset> datasetOptional = datasetRepository.findByName(dataset.name);
			if (datasetOptional.isPresent()) {
				throw new RuntimeException("Dataset is already created: " + dataset.name);
			}

			if (dataset.script == null) {
				Path scriptPath = Path.of(webLocation, "js", "search-test", "modules", "lib", "default.js");
				try {
					dataset.script = Files.readString(scriptPath);
				} catch (IOException e) {
					LOGGER.error("Failed to read default script from {}", scriptPath, e);
					dataset.script = null;
				}
			}
			return datasetRepository.save(dataset);
		});
	}

	@Async
	public CompletableFuture<Dataset> updateDataset(Long datasetId, Map<String, String> updates) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset dataset = datasetRepository.findById(datasetId).orElseThrow(() ->
					new RuntimeException("Dataset not found with id: " + datasetId));

			updates.forEach((key, value) -> {
				switch (key) {
					case "name" -> dataset.name = value;
					case "type" -> dataset.type = Dataset.Source.valueOf(value);
					case "source" -> dataset.source = value;
					case "sizeLimit" -> dataset.sizeLimit = Integer.valueOf(value);
					case "fileName" -> dataset.fileName = value;
					case "script" -> dataset.script = value;
				}
			});

			dataset.updated = LocalDateTime.now();
			dataset.setSourceStatus(Dataset.ConfigStatus.OK);
			datasetRepository.save(dataset);
			return checkDatasetInternal(dataset, false);
		});
	}

	private void updateColumns(Dataset dataset, String[] headers) {
		try {
			dataset.allCols = objectMapper.writeValueAsString(headers);
			headers = Arrays.stream(headers).filter(s -> s.startsWith("road") || s.startsWith("city")
					|| s.startsWith("stree") || s.startsWith("addr")).toArray(String[]::new);
			dataset.selCols = objectMapper.writeValueAsString(headers);
		} catch (JsonProcessingException e) {
			LOGGER.error("Failed to parse script: {}", dataset.script, e);
			throw new RuntimeException("Failed to parse script: " + e.getMessage(), e);
		}
	}

	protected void saveResults(EvalJob job, Dataset dataset, String address, String originalJson,
							   List<Feature> searchResults, LatLon originalPoint, long duration, String error) {
		if (job == null || dataset == null) {
			return;
		}
		int resultsCount = searchResults.size();
		Integer minDistance = null, actualPlace = null;
		String closestResult = null;

		if (originalPoint != null && !searchResults.isEmpty()) {
			double minDistanceMeters = Double.MAX_VALUE;
			LatLon closestPoint = null;
			int place = 0;

			for (Feature feature : searchResults) {
				place++;
				if (feature == null) {
					continue;
				}
				LatLon foundPoint = getLatLon(feature);
				double distance = MapUtils.getDistance(originalPoint.getLatitude(), originalPoint.getLongitude(),
						foundPoint.getLatitude(), foundPoint.getLongitude());
				if (distance < minDistanceMeters) {
					minDistanceMeters = distance;
					closestPoint = foundPoint;
					actualPlace = place;
				}
			}

			if (closestPoint != null) {
				minDistance = (int) minDistanceMeters;
				closestResult = pointToString(closestPoint);
			}
		}

		String insertSql =
				"INSERT INTO eval_result (job_id, dataset_id, original, error, duration, results_count, " +
						"min_distance, closest_result, address, lat, lon, actual_place, timestamp) VALUES (?, ?, ?, ?,"
						+ " ?, ?,?, ?, ?, ?, ?, ?, ?)";
		jdbcTemplate.update(insertSql, job.id, dataset.id, originalJson, error, duration, resultsCount, minDistance,
				closestResult, address, originalPoint == null ? null : originalPoint.getLatitude(),
				originalPoint == null ? null : originalPoint.getLongitude(), actualPlace,
				new java.sql.Timestamp(System.currentTimeMillis()));
	}

	public Page<Dataset> getDatasets(String search, String status, Pageable pageable) {
		return datasetRepository.findAllDatasets(search, status, pageable);
	}

	public String getDatasetSample(Long datasetId) {
		Dataset dataset = datasetRepository.findById(datasetId).orElseThrow(() -> new RuntimeException("Dataset not " +
				"found with " + "id: " + datasetId));

		String tableName = "dataset_" + sanitize(dataset.name);
		String sql = "SELECT * FROM " + tableName;
		try {
			StringWriter stringWriter = new StringWriter();
			List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);

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

	public boolean deleteDataset(Long datasetId) {
		Optional<Dataset> dsOpt = datasetRepository.findById(datasetId);
		if (dsOpt.isEmpty()) {
			return false;
		}
		String tableName = "dataset_" + sanitize(dsOpt.get().name);
		jdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);

		String deleteSql = "DELETE FROM eval_result WHERE dataset_id = ?";
		jdbcTemplate.update(deleteSql, datasetId);

		deleteSql = "DELETE FROM eval_job WHERE dataset_id = ?";
		jdbcTemplate.update(deleteSql, datasetId);

		Dataset ds = dsOpt.get();
		datasetRepository.delete(ds);
		return true;
	}

	public void insertSampleData(String tableName, String[] columns, List<String> sample, String delimiter,
								 boolean deleteBefore) {
		if (deleteBefore) {
			jdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);
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
		jdbcTemplate.batchUpdate(insertSql, batchArgs);
		LOGGER.info("Batch inserted {} records into {}.", sample.size(), tableName);
	}

	protected void createDynamicTable(String tableName, String[] columns) {
		String columnsDefinition =
				Stream.of(columns).map(header -> "\"" + header + "\" VARCHAR(255)").collect(Collectors.joining(", "));
		String createTableSql =
				String.format("CREATE TABLE IF NOT EXISTS %s (_id INTEGER PRIMARY KEY AUTOINCREMENT," + " " + "%s)",
						tableName, columnsDefinition);
		jdbcTemplate.execute(createTableSql);
		LOGGER.info("Ensured table {} exists.", tableName);
	}
}
