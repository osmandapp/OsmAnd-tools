package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.io.Writer;
import java.util.*;
import java.util.stream.Collectors;

public interface ReportService {
	record TestCaseStatus(
			TestCase.Status status,
			long processed,
			long failed,
			long filtered,
			long empty,
			long duration) {
	}

	record RunStatus(
			SearchTestRunRepository.Run.Status status,
			long total,
			long processed,
			long failed,
			long duration,
			double averagePlace,
			long found,
			Map<String, Number> distanceHistogram,
			TestCaseStatus generatedChart) {
	}

	String REPORT_SQL = "WITH result AS (" +
			"    SELECT" +
			"        UPPER(COALESCE(json_extract(row, '$.web_type'), ''))               AS type," +
			"        count, lat, lon, query, closest_result, min_distance, actual_place, results_count, row," +
			"        actual_place <= ?                                                  AS is_place," +
			"        min_distance <= ?                                                  AS is_dist," +
			"        CAST(COALESCE(json_extract(row, '$.id'), 0) AS INTEGER)            AS id," +
			"        COALESCE(json_extract(row, '$.web_name'), '')                      AS web_name," +
			"        COALESCE(json_extract(row, '$.web_address1'), '')                  AS web_address1," +
			"        COALESCE(json_extract(row, '$.web_address2'), '')                  AS web_address2," +
			"        CAST(COALESCE(json_extract(row, '$.web_poi_id'), 0) AS INTEGER)    AS web_poi_id," +
			"        (query LIKE '%' || COALESCE(json_extract(row, '$.web_name'), '') || '%'" +
			"            AND query LIKE '%' || COALESCE(json_extract(row, '$.web_address1'), '') || '%'" +
			"            AND query LIKE '%' || COALESCE(json_extract(row, '$.web_address2'), '') || '%')" +
			"                                                                           AS is_addr_match," +
			"        ((CAST(COALESCE(json_extract(row, '$.web_poi_id'), 0) AS INTEGER)  / 2) =" +
			"         CAST(COALESCE(json_extract(row, '$.id'), 0) AS INTEGER))          AS is_poi_match" +
			"    FROM run_result AS r WHERE run_id = ? ORDER BY actual_place, min_distance" +
			") " +
			"SELECT CASE" +
			"        WHEN count <= 0 OR trim(query) = '' THEN 'Not Processed'" +
			"        WHEN is_place AND is_dist THEN 'Found'" +
			"        WHEN NOT is_place AND is_dist AND (type = 'POI' AND is_poi_match OR type <> 'POI' AND is_addr_match)" +
			"            THEN 'Near'" +
			"        WHEN is_place AND NOT is_dist AND (type = 'POI' AND is_poi_match OR type <> 'POI' AND is_addr_match)" +
			"            THEN 'Too Far' ELSE 'Not Found'" +
			"END AS \"group\", type, lat, lon, query, actual_place, closest_result, min_distance, results_count, row " +
			"FROM result UNION SELECT c1, c2, lat, lon, query, 0, '', 0, 0, row FROM " +
			"(SELECT 'Generated' as c1, CASE WHEN error IS NOT NULL THEN 'Error' WHEN query IS NULL THEN 'Filtered'" +
			"  WHEN count = 0 or trim(query) = '' THEN 'Empty' ELSE 'Processed' END as c2, lat, lon, query, row, id " +
			"FROM gen_result WHERE case_id = ? ORDER BY id)";

	JdbcTemplate getJdbcTemplate();

	ObjectMapper getObjectMapper();

	default void downloadRawResults(Writer writer, int placeLimit, int distLimit, Long caseId, Long runId,
								   String format) throws IOException {
		List<Map<String, Object>> results = getJdbcTemplate().queryForList(REPORT_SQL, placeLimit, distLimit, runId,
				caseId);
		if ("csv".equalsIgnoreCase(format)) {
			writeAsCsv(writer, results);
		} else if ("json".equalsIgnoreCase(format)) {
			writeAsJson(writer, results);
		} else {
			throw new IllegalArgumentException("Unsupported format: " + format);
		}
	}

	default Optional<TestCaseStatus> getTestCaseStatus(Long cased) {
		String sql = """
				SELECT (select status from test_case where id = case_id) AS status,
				    count(*) AS total,
				    count(*) FILTER (WHERE error IS NOT NULL) AS failed,
				    count(*) FILTER (WHERE (count = -1 or query IS NULL) and error IS NULL) AS filtered,
				    count(*) FILTER (WHERE count = 0 or trim(query) = '') AS empty,
				    sum(duration) AS duration
				FROM
				    gen_result
				WHERE
				    case_id = ?
				""";
		try {
			Map<String, Object> result = getJdbcTemplate().queryForMap(sql, cased);
			String status = (String) result.get("status");
			if (status == null)
				status = TestCase.Status.NEW.name();

			long total = ((Number) result.get("total")).longValue();

			Number number = ((Number) result.get("failed"));
			long failed = number == null ? 0 : number.longValue();

			number = ((Number) result.get("duration"));
			long duration = number == null ? 0 : number.longValue();

			number = ((Number) result.get("filtered"));
			long filtered = number == null ? 0 : number.longValue();

			number = ((Number) result.get("empty"));
			long empty = number == null ? 0 : number.longValue();

			TestCaseStatus report = new TestCaseStatus(TestCase.Status.valueOf(status), total, failed, filtered, empty,
					duration);
			return Optional.of(report);
		} catch (EmptyResultDataAccessException ee) {
			return Optional.empty();
		}
	}

	default Optional<RunStatus> getRunStatus(Long runId) {
		String sql = """
				SELECT (select status from run where id = run_id) AS status,
				    count(*) AS total,
				    count(*) FILTER (WHERE count > 0 and trim(query) <> '') AS processed,
				    count(*) FILTER (WHERE error IS NOT NULL) AS failed,
				    avg(actual_place) FILTER (WHERE actual_place IS NOT NULL) AS average_place,
				    sum(duration) AS duration,
				    count(*) FILTER (WHERE actual_place <= 10 and min_distance <= 50) AS found
				FROM
				    run_result
				WHERE
				    run_id = ?
				""";
		try {
			Map<String, Object> result = getJdbcTemplate().queryForMap(sql, runId);
			String status = (String) result.get("status");
			if (status == null)
				status = TestCase.Status.NEW.name();

			Number number = ((Number) result.get("average_place"));
			double averagePlace = number == null ? 0.0 : number.doubleValue();

			number = ((Number) result.get("total"));
			long total = number == null ? 0 : number.longValue();

			number = ((Number) result.get("processed"));
			long processed = number == null ? 0 : number.longValue();

			number = ((Number) result.get("failed"));
			long failed = number == null ? 0 : number.longValue();

			number = ((Number) result.get("duration"));
			long duration = number == null ? 0 : number.longValue();

			number = ((Number) result.get("found"));
			long found = number == null ? 0 : number.longValue();

			RunStatus report = new RunStatus(Run.Status.valueOf(status), total, processed, failed,
					duration, averagePlace, found, null, null);
			return Optional.of(report);
		} catch (EmptyResultDataAccessException ee) {
			return Optional.empty();
		}
	}

	default Optional<RunStatus> getRunReport(Long caseId, Long runId, int placeLimit, int distLimit) {
		Optional<TestCaseStatus> optCase = getTestCaseStatus(caseId);
		if (optCase.isEmpty()) {
			return Optional.empty();
		}

		final RunStatus status;
		if (runId != null) {
			Optional<RunStatus> opt = getRunStatus(runId);
			if (opt.isEmpty())
				return Optional.empty();
			status = opt.get();
		} else
			status = null;

		Map<String, Number> distanceHistogram = new LinkedHashMap<>();
		distanceHistogram.put("Generated", 0);
		distanceHistogram.put("Not Processed", 0);
		distanceHistogram.put("Found", 0);
		distanceHistogram.put("Near", 0);
		distanceHistogram.put("Too Far", 0);
		distanceHistogram.put("Not Found", 0);
		List<Map<String, Object>> results =
				getJdbcTemplate().queryForList("SELECT \"group\", count(*) as cnt FROM (" + REPORT_SQL + ") GROUP BY " +
						"\"group\"", placeLimit, distLimit, runId, caseId);
		for (Map<String, Object> values : results) {
			distanceHistogram.put(values.get("group").toString(), ((Number) values.get("cnt")).longValue());
		}

		RunStatus finalStatus;
		if (status == null) {
			TestCaseStatus caseStatus = optCase.get();
			finalStatus = new RunStatus(Run.Status.NEW, caseStatus.processed(), caseStatus.processed(),
					caseStatus.failed(), caseStatus.duration(), 0.0, 0, distanceHistogram, caseStatus);
		} else {
			finalStatus = new RunStatus(status.status(), status.total(), status.processed(), status.failed(),
					status.duration(), status.averagePlace(), status.found(), distanceHistogram, optCase.get());
		}
		return Optional.of(finalStatus);
	}

	default void writeAsJson(Writer writer, List<Map<String, Object>> results) throws IOException {
		List<Map<String, Object>> expanded = results.stream().map(row -> {
			Map<String, Object> out = new LinkedHashMap<>();
			// copy base fields except 'row'
			for (Map.Entry<String, Object> e : row.entrySet()) {
				if (!"row".equals(e.getKey())) {
					out.put(e.getKey(), e.getValue());
				}
			}
			Object rowObj = row.get("row");
			if (rowObj != null) {
				try {
					JsonNode rowNode = getObjectMapper().readTree(rowObj.toString());
					// For consistency with CSV, serialize values as text
					rowNode.fieldNames().forEachRemaining(fn -> {
						JsonNode v = rowNode.get(fn);
						out.put(fn, v == null || v.isNull() ? null : v.asText());
					});
				} catch (IOException e) {
					// ignore invalid JSON in 'row'
				}
			}
			return out;
		}).collect(Collectors.toList());
		getObjectMapper().writeValue(writer, expanded);
	}

	default void writeAsCsv(Writer writer, List<Map<String, Object>> results) throws IOException {
		if (results.isEmpty()) {
			writer.write("");
			return;
		}

		Set<String> headers = new LinkedHashSet<>();
		results.get(0).keySet().stream()
				.filter(k -> !k.equals("row"))
				.forEach(headers::add);

		Set<String> rowHeaders = new LinkedHashSet<>();
		for (Map<String, Object> row : results) {
			Object rowObj = row.get("row");
			if (rowObj != null) {
				try {
					JsonNode rowNode = getObjectMapper().readTree(rowObj.toString());
					rowNode.fieldNames().forEachRemaining(rowHeaders::add);
				} catch (IOException e) {
					// ignore invalid JSON in 'row'
				}
			}
		}
		headers.addAll(rowHeaders);

		CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
				.setHeader(headers.toArray(new String[0])).setDelimiter(";")
				.build();

		try (final CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {
			for (Map<String, Object> row : results) {
				List<String> record = new ArrayList<>();
				JsonNode rowNode = null;
				Object rowObj = row.get("row");
				if (rowObj != null) {
					try {
						rowNode = getObjectMapper().readTree(rowObj.toString());
					} catch (IOException e) {
						// keep rowNode null
					}
				}

				for (String header : headers) {
					if (row.containsKey(header)) {
						Object value = row.get(header);
						record.add(value != null ? value.toString().trim() : null);
					} else if (rowNode != null && rowNode.has(header)) {
						record.add(rowNode.get(header).asText());
					} else {
						record.add(null);
					}
				}
				printer.printRecord(record);
			}
		}
	}
}
