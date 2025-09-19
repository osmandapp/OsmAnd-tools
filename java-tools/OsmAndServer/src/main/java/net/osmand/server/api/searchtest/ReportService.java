package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

	Logger LOGGER = LoggerFactory.getLogger(ReportService.class);
	int DISTANCE_LIMIT = 50;
	String[] RESULT_PROPS = new String[] {"id", "web_type", "web_poi_id", "lat", "lon"};

	String BASE_SQL = """
			WITH result AS (
				SELECT gen_id,
					UPPER(COALESCE(json_extract(row, '$.web_type'), ''))                 AS type,
					count, lat || ', ' || lon as lat_lon, query, closest_result as result, 
					CAST((min_distance/10) AS INTEGER)*10 as distance, actual_place, results_count, row,
					CASE WHEN UPPER(COALESCE(json_extract(row, '$.web_type'), '')) = 'POI' 
					    THEN CAST(COALESCE(json_extract(row, '$.web_poi_id'), 0) AS INTEGER)
					    ELSE CAST(COALESCE(json_extract(row, '$.id'), 0) AS INTEGER) END AS id
				FROM run_result AS r WHERE run_id = ? ORDER BY gen_id
			)""";
	String REPORT_SQL = BASE_SQL + """
			 SELECT CASE
				WHEN count <= 0 OR trim(query) = '' THEN 'Not Processed'
				WHEN distance <= ? THEN 'Found'
				ELSE 'Not Found'
			END AS "group", type, lat_lon, query, result, distance, results_count, id, row FROM result""";
	String FULL_REPORT_SQL = REPORT_SQL + """ 
			 UNION SELECT c1, c2, lat_lon, query, 0, 0, 0, 0, '' as row FROM
			(SELECT 'Generated' as c1, CASE WHEN error IS NOT NULL THEN 'Error' WHEN query IS NULL THEN 'Filtered'
			  WHEN count = 0 or trim(query) = '' THEN 'Empty' ELSE 'Processed' END as c2, 
			     lat || ', ' || lon as lat_lon, query, 0, 0, 0, id, '' as row
			FROM gen_result WHERE case_id = ? ORDER BY id)""";
	String COMPARISON_REPORT_SQL = BASE_SQL + """
			 SELECT gen_id, type, lat_lon, query, result, distance, results_count, id
			FROM result WHERE COALESCE(is_place AND is_dist, false) = ? ORDER BY gen_id""";

	JdbcTemplate getJdbcTemplate();

	ObjectMapper getObjectMapper();

	SearchTestCaseRepository getTestCaseRepo();

	default List<Map<String, Object>[]> compare(boolean found, Long caseId, Long runId1, Long runId2) {
		// Load filtered results (Found/Not Found) for each run, ordered by gen_id
		List<Map<String, Object>> results1 = getJdbcTemplate().queryForList(COMPARISON_REPORT_SQL, DISTANCE_LIMIT, runId1, found);
		List<Map<String, Object>> results2 = getJdbcTemplate().queryForList(COMPARISON_REPORT_SQL, DISTANCE_LIMIT, runId2, found);

		List<Map<String, Object>[]> result = new ArrayList<>(Math.max(results1.size(), results2.size()));

		int i = 0, j = 0;
		while (i < results1.size() && j < results2.size()) {
			Map<String, Object> r1 = results1.get(i);
			Map<String, Object> r2 = results2.get(j);
			Integer id1 = (Integer) r1.get("gen_id");
			Integer id2 = (Integer) r2.get("gen_id");

			if (id1 == null && id2 == null) {
				result.add(new Map[]{r1, r2});
				i++;
				j++;
			} else if (id1 == null) {
				result.add(new Map[]{r1, null});
				i++;
			} else if (id2 == null) {
				result.add(new Map[]{null, r2});
				j++;
			} else if (id1.equals(id2)) {
				result.add(new Map[]{r1, r2});
				i++;
				j++;
			} else if (id1 < id2) {
				result.add(new Map[]{r1, null});
				i++;
			} else { // id2 < id1
				result.add(new Map[]{null, r2});
				j++;
			}
		}

		while (i < results1.size()) {
			result.add(new Map[]{results1.get(i++), null});
		}
		while (j < results2.size()) {
			result.add(new Map[]{null, results2.get(j++)});
		}

		return result;
	}

	default void downloadRawResults(Writer writer, Long caseId, Long runId, String format) throws IOException {
		TestCase test = getTestCaseRepo().findById(caseId).orElseThrow(() ->
				new RuntimeException("TestCase not found with id: " + caseId));

		if ("csv".equalsIgnoreCase(format)) {
			List<Map<String, Object>> results = extendTo(getJdbcTemplate().queryForList(REPORT_SQL, runId,
					DISTANCE_LIMIT), getObjectMapper().readValue(test.allCols, String[].class),
					getObjectMapper().readValue(test.selCols, String[].class));
			writeAsCsv(writer, results);
		} else if ("json".equalsIgnoreCase(format)) {
			List<Map<String, Object>> results = extendTo(getJdbcTemplate().queryForList(FULL_REPORT_SQL, runId,
					DISTANCE_LIMIT, caseId), getObjectMapper().readValue(test.allCols, String[].class),
					getObjectMapper().readValue(test.selCols, String[].class));
			getObjectMapper().writeValue(writer, results);
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
			LOGGER.error("Failed to process TestCaseStatus for {}.", cased, ee);
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
				    count(*) FILTER (WHERE min_distance <= ?) AS found
				FROM
				    run_result
				WHERE
				    run_id = ?
				""";
		try {
			Map<String, Object> result = getJdbcTemplate().queryForMap(sql, DISTANCE_LIMIT, runId);
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
			LOGGER.error("Failed to process RunStatus for {}.", runId, ee);
			return Optional.empty();
		}
	}

	default Optional<RunStatus> getRunReport(Long caseId, Long runId) {
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
		distanceHistogram.put("Not Found", 0);
		List<Map<String, Object>> results =
				getJdbcTemplate().queryForList("SELECT \"group\", count(*) as cnt FROM (" + FULL_REPORT_SQL + ") GROUP BY " +
						"\"group\"", runId, DISTANCE_LIMIT, caseId);
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

	default List<Map<String, Object>> extendTo(List<Map<String, Object>> results, String[] allCols, String[] selCols) {
	    // Exclude fields already exposed as top-level columns to avoid duplication
	    final java.util.Set<String> include = new java.util.HashSet<>(java.util.Arrays.asList(selCols));
		final java.util.Set<String> exclude = new java.util.HashSet<>(java.util.Arrays.asList(RESULT_PROPS));
		exclude.addAll(java.util.Arrays.asList(allCols));
	    return results.stream().peek(row -> {
	        Object rowObj = row.get("row");
			row.remove("row");
	        if (rowObj == null) return;
	        try {
		        Map<String, Object> in = new LinkedHashMap<>();
	            Map<String, Object> out = new LinkedHashMap<>();
	            JsonNode rowNode = getObjectMapper().readTree(rowObj.toString());
	            // For consistency with CSV, serialize values as text, skipping excluded keys
		        StringBuilder resultName = new StringBuilder();
	            rowNode.fieldNames().forEachRemaining(fn -> {
	                JsonNode v = rowNode.get(fn);
		            if (include.contains(fn)) {
						if (v != null && !v.isNull() && !v.asText().isEmpty())
			                in.put(fn, v.asText());
						return;
		            }
		            if (exclude.contains(fn))
			            return; // remove from the inner 'row' map

		            if (v != null && !v.isNull() && !v.asText().isEmpty())
						if (fn.startsWith("web_"))
							resultName.append(v.asText()).append(" ");
						else
		                    out.put(fn, v.asText());
	            });
		        row.put("result_name", resultName.toString().trim());
		        row.put("in_tags", getObjectMapper().writeValueAsString(in));
	            row.put("out_tags", getObjectMapper().writeValueAsString(out));
	        } catch (IOException e) {
	            // ignore invalid JSON in 'row'
	        }
	    }).collect(Collectors.toList());
	}

	default void writeAsCsv(Writer writer, List<Map<String, Object>> results) throws IOException {
		if (results.isEmpty()) {
			writer.write("");
			return;
		}

		Set<String> headers = new LinkedHashSet<>();
		results.get(0).keySet().stream()
				.filter(k -> !k.equals("out_tags") && !k.equals("in_tags"))
				.forEach(headers::add);

		Set<String> rowHeaders = new LinkedHashSet<>();
		for (String tagName : new String[] {"in_tags", "out_tags"}) {
			for (Map<String, Object> row : results) {
				Object rowObj = row.get(tagName);
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
		}

		CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader(headers.toArray(new String[0])).build();
		try (final CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {
			for (Map<String, Object> row : results) {
				List<String> record = new ArrayList<>();
				try {
					getObjectMapper().readValue(row.get("in_tags").toString(), Map.class).forEach((k, v) -> row.put((String) k, v));
					getObjectMapper().readValue(row.get("out_tags").toString(), Map.class).forEach((k, v) -> row.put((String) k, v));
				} catch (IOException e) {
					// keep rowNode null
				}

				for (String header : headers) {
					if (row.containsKey(header)) {
						Object value = row.get(header);
						record.add(value != null ? value.toString().trim() : null);
					} else {
						record.add(null);
					}
				}
				printer.printRecord(record);
			}
		}
	}
}
