package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.ServletOutputStream;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository;
import net.osmand.server.api.searchtest.repo.SearchTestRunRepository.Run;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
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

	String BASE_SQL = """
			WITH result AS (
				SELECT DENSE_RANK() OVER (ORDER BY g.ds_result_id) AS grp, ROW_NUMBER() OVER (PARTITION BY g.ds_result_id ORDER BY g.id) AS rn,
					UPPER(COALESCE(json_extract(r.row, '$.web_type'), 'absence')) AS type, g.id as gen_id,
					g.gen_count, g.lat || ', ' || g.lon as lat_lon, r.lat || ', ' || r.lon as search_lat_lon, 
					g.query, r.res_lat_lon, CAST((r.res_distance/10) AS INTEGER)*10 as res_distance, r.res_place, r.bbox as search_bbox,
					r.res_count, g.row AS in_row, r.row AS out_row, 
					CAST(COALESCE(json_extract(g.row, '$.id'), 0) AS INTEGER) AS id, r.found
				FROM gen_result AS g, run_result AS r WHERE g.id = r.gen_id AND run_id = ? ORDER BY g.id
			)""";
	String REPORT_SQL = BASE_SQL + """
			 SELECT CASE
				WHEN gen_count <= 0 OR query IS NULL OR trim(query) = '' THEN 'Not Processed'
				WHEN COALESCE(found, res_distance <= 50) THEN 'Found'
				ELSE 'Not Found'
			END AS "group", type, grp || '.' || rn AS row_id, gen_id, lat_lon, query, id, in_row, res_count, res_place, res_distance, 
			                search_lat_lon, search_bbox, res_lat_lon, out_row FROM result""";
	String FULL_REPORT_SQL = REPORT_SQL + """
			 UNION SELECT 'Generated', CASE 
			    WHEN error IS NOT NULL THEN 'Error' WHEN query IS NULL THEN 'Filtered'
				WHEN gen_count = 0 or trim(query) = '' THEN 'Empty' ELSE 'Processed' END, 
			DENSE_RANK() OVER (ORDER BY ds_result_id) || '.' || ROW_NUMBER() OVER (PARTITION BY ds_result_id ORDER BY id) AS row_id, id as gen_id, 
			lat || ', ' || lon as lat_lon, query, CAST(COALESCE(json_extract(row, '$.id'), 0) AS INTEGER) as id, 
			row as in_row, NULL, NULL, NULL, NULL, NULL, NULL, NULL as out_row FROM gen_result WHERE case_id = ? ORDER BY "group", gen_id""";
	String[] IN_PROPS = new String[]{"group", "type", "row_id", "id", "lat_lon", "search_lat_lon", "query"};
	String[] OUT_PROPS = new String[]{"res_name", "res_lat_lon", "res_place", "res_id", "actual_place", "actual_id", "res_count",
			"res_distance", "search_bbox"};

	JdbcTemplate getJdbcTemplate();

	ObjectMapper getObjectMapper();

	SearchTestCaseRepository getTestCaseRepo();

	SearchTestRunRepository getTestRunRepo();

	Logger getLogger();

	default List<Map<String, Object>> getRunResults(Long runId, boolean isFull) throws IOException {
		Run run = getTestRunRepo().findById(runId).orElseThrow(() ->
				new RuntimeException("Run not found with id: " + runId));
		TestCase test = getTestCaseRepo().findById(run.caseId).orElseThrow(() ->
				new RuntimeException("TestCase not found with id: " + run.caseId));

		List<Map<String, Object>> list  = isFull ?
			getJdbcTemplate().queryForList(FULL_REPORT_SQL, runId, run.caseId) :
			getJdbcTemplate().queryForList(REPORT_SQL + " ORDER BY gen_id", runId);

		return extendTo(list, getObjectMapper().readValue(test.allCols, String[].class));
	}

	default void downloadCsvResults(Writer writer, Long caseId, Long runId) throws IOException {
		TestCase test = getTestCaseRepo().findById(caseId).orElseThrow(() ->
				new RuntimeException("TestCase not found with id: " + caseId));

		List<Map<String, Object>> results = extendTo(getJdbcTemplate().queryForList(REPORT_SQL + " ORDER BY gen_id",
						runId), getObjectMapper().readValue(test.allCols, String[].class));
		writeAsCsv(writer, results);
	}

	default List<Map<String, Object>> extendTo(List<Map<String, Object>> results, String[] allCols) {
		// Exclude fields already exposed as top-level columns to avoid duplication
		final java.util.Set<String> exclude = new java.util.HashSet<>(java.util.Arrays.asList(IN_PROPS));
		exclude.add("web_type");
		exclude.addAll(java.util.Arrays.asList(allCols));

		return results.stream().map(srcRow -> {
			String inRowJson = (String) srcRow.get("in_row");
			String outRowJson = (String) srcRow.get("out_row");
			srcRow.remove("in_row");
			srcRow.remove("out_row");

			Map<String, Object> row = new LinkedHashMap<>();
			if (inRowJson == null) return row;
			for (String p : IN_PROPS)
				if (srcRow.containsKey(p))
					row.put(p, srcRow.get(p));

			row.remove("gen_id");
			try {
				Map<String, Object> out = new LinkedHashMap<>();
				StringBuilder resultName = new StringBuilder();
				if (outRowJson != null) {
					for (String p : OUT_PROPS)
						row.put(p, srcRow.get(p));

					JsonNode outRow = getObjectMapper().readTree(outRowJson);
					// For consistency with CSV, serialize values as text, skipping excluded keys
					outRow.fieldNames().forEachRemaining(fn -> {
						if (exclude.contains(fn))
							return; // remove from the inner 'row' map
						JsonNode v = outRow.get(fn);
						if (fn.startsWith("res_id") || fn.startsWith("res_place") || fn.startsWith("actual_place")
								|| fn.startsWith("actual_id") ||
								fn.startsWith("web_poi_id") || fn.startsWith("amenity_")) {
							row.put(fn, v.asText());
						} else if (fn.startsWith("web_name") || fn.startsWith("web_address") ||
								fn.startsWith("web_poi_name"))
							resultName.append(v.asText()).append(" ");
						else
							out.put(fn, v.asText());
					});

					row.put("res_name", resultName.toString().trim());
					row.put("res_tags", getObjectMapper().writeValueAsString(out));
				}
			} catch (IOException e) {
				// ignore invalid JSON in 'row'
			}
			return row;
		}).collect(Collectors.toList());
	}

	default Optional<TestCaseStatus> getTestCaseStatus(Long cased) {
		String sql = """
				SELECT (select status from test_case where id = case_id) AS status,
				    count(*) AS total,
				    count(*) FILTER (WHERE error IS NOT NULL) AS failed,
				    count(*) FILTER (WHERE (gen_count = -1 or query IS NULL) and error IS NULL) AS filtered,
				    count(*) FILTER (WHERE gen_count = 0 or trim(query) = '') AS empty,
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
				    count(*) FILTER (WHERE gen_count > 0 and trim(query) <> '') AS processed,
				    count(*) FILTER (WHERE error IS NOT NULL) AS failed,
				    sum(duration) AS duration,
				    count(*) FILTER (WHERE COALESCE(found, res_distance <= 50)) AS found_count
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

			Number number = ((Number) result.get("total"));
			long total = number == null ? 0 : number.longValue();

			number = ((Number) result.get("processed"));
			long processed = number == null ? 0 : number.longValue();

			number = ((Number) result.get("failed"));
			long failed = number == null ? 0 : number.longValue();

			number = ((Number) result.get("duration"));
			long duration = number == null ? 0 : number.longValue();

			number = ((Number) result.get("found_count"));
			long found = number == null ? 0 : number.longValue();

			RunStatus report = new RunStatus(Run.Status.valueOf(status), total, processed, failed,
					duration, 0, found, null, null);
			return Optional.of(report);
		} catch (EmptyResultDataAccessException ee) {
			LOGGER.error("Failed to process RunStatus for {}.", runId, ee);
			return Optional.empty();
		}
	}

	default Optional<RunStatus> getRunStatus(Long caseId, Long runId) {
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
						"\"group\"", runId, caseId);
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

	default void writeAsCsv(Writer writer, List<Map<String, Object>> results) throws IOException {
		if (results.isEmpty()) {
			writer.write("");
			return;
		}

		Set<String> headers = new LinkedHashSet<>();
		for (Map<String, Object> row : results) {
			headers.addAll(row.keySet());
		}

		CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader(headers.toArray(new String[0])).build();
		try (final CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {
			for (Map<String, Object> row : results) {
				List<String> record = new ArrayList<>();
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

	/**
	 * Converts a zero-based column index to an Excel column label (e.g., 0 -> A, 25 -> Z, 26 -> AA).
	 *
	 * @param idx zero-based column index
	 * @return Excel-style column name
	 */
	default String excelCol(int idx) {
		StringBuilder sb = new StringBuilder();
		int n = idx;
		do {
			int rem = n % 26;
			sb.append((char) ('A' + rem));
			n = n / 26 - 1;
		} while (n >= 0);
		return sb.reverse().toString();
	}

	default void compareReport(ServletOutputStream out, Long caseId, Long[] runIds) throws IOException {
		TestCase test = getTestCaseRepo().findById(caseId).orElseThrow(() ->
				new RuntimeException("TestCase not found with id: " + caseId));

		String[] allCols = getObjectMapper().readValue(test.allCols, String[].class);
		final String[] gen_cols = new String[]{"row_id", "id", "lat_lon", "search_lat_lon", "query"};
		final String[] run_cols = new String[]{"type", "res_name", "res_lat_lon", "res_place", "res_id",
				"actual_place", "actual_id", "res_count", "res_distance"};

		try (Workbook wb = new XSSFWorkbook()) {
			List<List<Map<String, Object>>> runs = new ArrayList<>(runIds.length);
			Map<Long, String> runNames = new HashMap<>();
			for (long runId : runIds) {
				runs.add(extendTo(getJdbcTemplate().queryForList(
						REPORT_SQL + " ORDER BY gen_id", runId), allCols));
				runNames.put(runId, getJdbcTemplate().queryForObject("SELECT name FROM run WHERE id = ?",
						String.class, runId));
			}

			Set<String> runHeaderSet = new LinkedHashSet<>();
			for (List<Map<String, Object>> runSet : runs)
				for (Map<String, Object> row : runSet)
					runHeaderSet.addAll(row.keySet());
			String[] runHeaders = runHeaderSet.toArray(new String[0]);

			// Styles
			CellStyle header = wb.createCellStyle();
			Font headerFont = wb.createFont();
			headerFont.setBold(true);
			header.setFont(headerFont);
			header.setWrapText(true);

			// Percentage style (e.g., 50%) for Stats % column
			CellStyle percentStyle = wb.createCellStyle();
			short pctFmt = wb.getCreationHelper().createDataFormat().getFormat("0%");
			percentStyle.setDataFormat(pctFmt);

			// Statistics
			Sheet statSheet = wb.createSheet("Stats");
			String[] groups = new String[]{"Found", "Not Found", "Not Processed"};
			int c = 0;
			// Precompute Comparison sheet group column letters for each run index
			String[] compGroupCols = new String[runIds.length];
			for (int iRun = 0; iRun < runIds.length; iRun++) {
				compGroupCols[iRun] = excelCol(iRun);
			}
			// Iterate all unordered pairs (i < j)
			for (int iRun = 0; iRun < runIds.length; iRun++) {
				for (int jRun = iRun + 1; jRun < runIds.length; jRun++) {
					Row sh = statSheet.getRow(0);
					if (sh == null) sh = statSheet.createRow(0);

					Cell cell = sh.createCell(c);
					cell.setCellValue(runNames.get(runIds[iRun]) + " (#" + runIds[iRun] + ")");
					cell.setCellStyle(header);

					cell = sh.createCell(c + 1);
					cell.setCellValue(runNames.get(runIds[jRun]) + " (#" + runIds[jRun] + ")");
					cell.setCellStyle(header);

					cell = sh.createCell(c + 2);
					cell.setCellValue("Sum");
					cell.setCellStyle(header);

					cell = sh.createCell(c + 3);
					cell.setCellValue("%");
					cell.setCellStyle(header);

					String compColI = compGroupCols[iRun];
					String compColJ = compGroupCols[jRun];
					String statColLeft = excelCol(c);
					String statColRight = excelCol(c + 1);
					String statColCount = excelCol(c + 2);
					// Row offset within this block
					int rowOffset = 1;
					for (int g = 0; g < groups.length; g++) {
						// r1: groups[0] vs groups[g]
						int r1Idx = rowOffset;
						Row r1 = statSheet.getRow(r1Idx);
						if (r1 == null) r1 = statSheet.createRow(r1Idx);
						cell = r1.createCell(c);
						cell.setCellValue(groups[0]);
						cell = r1.createCell(c + 1);
						cell.setCellValue(groups[g]);
						cell = r1.createCell(c + 2);
						cell.setCellFormula(String.format(
								"COUNTIFS(Comparison!%1$s:%1$s,%2$s%3$d,Comparison!%4$s:%4$s,%5$s%3$d)",
								compColI, statColLeft, r1Idx, compColJ, statColRight));
						Cell r1pct = r1.createCell(c + 3);
						int sumIdx = rowOffset + 3;
						r1pct.setCellFormula(String.format("%1$s%2$d/%1$s%3$d", statColCount, r1Idx + 1, sumIdx + 1));
						r1pct.setCellStyle(percentStyle);

						// r2: groups[1] vs groups[g]
						int r2Idx = rowOffset + 1;
						Row r2 = statSheet.getRow(r2Idx);
						if (r2 == null) r2 = statSheet.createRow(r2Idx);
						cell = r2.createCell(c);
						cell.setCellValue(groups[1]);
						cell = r2.createCell(c + 1);
						cell.setCellValue(groups[g]);
						cell = r2.createCell(c + 2);
						cell.setCellFormula(String.format(
								"COUNTIFS(Comparison!%1$s:%1$s,%2$s%3$d,Comparison!%4$s:%4$s,%5$s%3$d)",
								compColI, statColLeft, r2Idx, compColJ, statColRight));
						Cell r2pct = r2.createCell(c + 3);
						r2pct.setCellFormula(String.format("%1$s%2$d/%1$s%3$d", statColCount, r2Idx + 1, sumIdx + 1));
						r2pct.setCellStyle(percentStyle);

						// r3: groups[2] vs groups[g]
						int r3Idx = rowOffset + 2;
						Row r3 = statSheet.getRow(r3Idx);
						if (r3 == null) r3 = statSheet.createRow(r3Idx);
						cell = r3.createCell(c);
						cell.setCellValue(groups[2]);
						cell = r3.createCell(c + 1);
						cell.setCellValue(groups[g]);
						cell = r3.createCell(c + 2);
						cell.setCellFormula(String.format(
								"COUNTIFS(Comparison!%1$s:%1$s,%2$s%3$d,Comparison!%4$s:%4$s,%5$s%3$d)",
								compColI, statColLeft, r3Idx, compColJ, statColRight));
						Cell r3pct = r3.createCell(c + 3);
						r3pct.setCellFormula(String.format("%1$s%2$d/%1$s%3$d", statColCount, r3Idx + 1, sumIdx + 1));
						r3pct.setCellStyle(percentStyle);

						// r4: Sum row
						Row r4 = statSheet.getRow(sumIdx);
						if (r4 == null) r4 = statSheet.createRow(sumIdx);
						cell = r4.createCell(c + 2);
						cell.setCellFormula(String.format("SUM(%1$s%2$d:%1$s%3$d)", statColCount, r1Idx + 1, r3Idx + 1));
						cell.setCellStyle(header);

						rowOffset += 4;
					}

					// Autosize all 4 columns for this block
					for (int cc = c; cc < c + 4; cc++) {
						statSheet.autoSizeColumn(cc);
					}

					c += 4;
				}
			}

			// Sheet 1
			Sheet sheet = wb.createSheet("Comparison");
			Row h = sheet.createRow(0);
			sheet.createFreezePane(runIds.length + gen_cols.length, 1);

			// Group header: one column per run (to hold group name per run)
			for (c = 0; c < runIds.length; c++) {
				Cell cell = h.createCell(c);
				cell.setCellValue(runNames.get(runIds[c]));
				cell.setCellStyle(header);
			}
			// General columns header
			for (c = 0; c < gen_cols.length; c++) {
				Cell cell = h.createCell(runIds.length + c);
				cell.setCellValue(gen_cols[c]);
				cell.setCellStyle(header);
			}

			// Per-run block headers
			int r;
			// Helper to stringify values safely
			java.util.function.Function<Object, String> toStr = v -> v == null ? "" : String.valueOf(v);

			for (int ri = 0; ri < runIds.length; ri++) {
				int baseCol = runIds.length + gen_cols.length + ri * run_cols.length;
				for (c = 0; c < run_cols.length; c++) {
					Cell cell = h.createCell(baseCol + c);
					cell.setCellValue(run_cols[c] + "_" + (ri + 1));
					cell.setCellStyle(header);
				}

				Sheet runSheet = wb.createSheet(runNames.get(runIds[ri]) + " (#" + runIds[ri] + ")");
				r = 1;
				for (Map<String, Object> values : runs.get(ri)) {
					Row rh = runSheet.createRow(0);
					for (c = 0; c < runHeaders.length; c++) {
						Cell cell = rh.createCell(c);
						cell.setCellValue(runHeaders[c]);
						cell.setCellStyle(header);
					}

					Row row = runSheet.createRow(r++);
					for (int j = 0; j < runHeaders.length; j++) {
						Cell cell = row.createCell(j);
						cell.setCellValue(toStr.apply(values.get(runHeaders[j])));
					}
				}
				for (c = 0; c < runHeaders.length; c++) {
					runSheet.autoSizeColumn(c);
				}
			}

			// Body: create one row per generated item (gens is ordered by gen_id)
			r = 1;
			for (Map<String, Object> values : runs.get(0)) {
				Row row = sheet.createRow(r++);
				for (int j = 0; j < gen_cols.length; j++) {
					int colIdx = runIds.length + j;
					Cell cell = row.createCell(colIdx);
					cell.setCellValue(toStr.apply(values.get(gen_cols[j])));
				}
			}

			// For each run (index ri), fill group column and run block columns, aligned by row index
			for (int ri = 0; ri < runs.size(); ri++) {
				List<Map<String, Object>> runSet = runs.get(ri);
				int rowsCount = runSet.size();
				for (int idx = 0; idx < rowsCount; idx++) {
					Row row = sheet.getRow(idx + 1);
					if (row == null) row = sheet.createRow(idx + 1);
					Map<String, Object> values = idx < runSet.size() ? runSet.get(idx) : Collections.emptyMap();

					// Group column for this run
					{
						Cell cell = row.getCell(ri);
						if (cell == null) cell = row.createCell(ri);
						cell.setCellValue(toStr.apply(values.get("group")));
					}
					// Run block columns for this run
					int baseCol = runIds.length + gen_cols.length + ri * run_cols.length;
					for (c = 0; c < run_cols.length; c++) {
						Cell cell = row.getCell(baseCol + c);
						if (cell == null) cell = row.createCell(baseCol + c);
						cell.setCellValue(toStr.apply(values.get(run_cols[c])));
					}
				}
			}

			// Auto-size columns
			int totalCols = runIds.length + gen_cols.length + runIds.length * run_cols.length;
			for (c = 0; c < totalCols; c++) {
				sheet.autoSizeColumn(c);
			}

			wb.write(out);
		} catch (Exception e) {
			getLogger().error("Cannot create comparison report", e);
		}
	}
}
