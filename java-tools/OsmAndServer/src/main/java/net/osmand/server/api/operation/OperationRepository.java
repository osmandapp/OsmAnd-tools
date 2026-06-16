package net.osmand.server.api.operation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Repository
public class OperationRepository {

	public record OperationItem(String className, String name, String title, String paramsJson, String resultType,
							 boolean valid, LocalDateTime updatedTime) {}
	public record JobItem(Long id, String className, String operationName, String operationTitle, String name,
						 String description, String labels, String paramsJson, LocalDateTime createdTime,
						 LocalDateTime updatedTime) {}
	public record RunItem(Long id, Long jobId, String className, String operationName, String operationTitle, String jobName,
						 String status, String paramsJson, String resultJson, String errorText, Long elapsedMs,
						 Integer processed, Integer total, String progressText,
						 String summaryKey, Object summaryValue, LocalDateTime startedTime, LocalDateTime finishedTime,
						 LocalDateTime createdTime, LocalDateTime updatedTime) {

		public RunItem withProgress(int processed, int total, String progressText, long elapsedMs) {
			return new RunItem(id, jobId, className, operationName, operationTitle, jobName, status, paramsJson,
					resultJson, errorText, elapsedMs, processed, total, progressText, summaryKey, summaryValue,
					startedTime, finishedTime, createdTime, updatedTime);
		}
	}

	private static final String JOB_SELECT =
			"SELECT j.*, a.name operation_name, a.title operation_title FROM job j " +
			"LEFT JOIN operation a ON a.class_name = j.class_name";
	private static final String RUN_SELECT =
			"SELECT r.*, j.class_name, j.name job_name, a.name operation_name, a.title operation_title FROM run r " +
			"JOIN job j ON j.id = r.job_id LEFT JOIN operation a ON a.class_name = j.class_name";

	private final JdbcTemplate jdbc;
	private final ObjectMapper mapper;

	public OperationRepository(@Qualifier("operationJdbcTemplate") JdbcTemplate jdbc, ObjectMapper mapper) {
		this.jdbc = jdbc;
		this.mapper = mapper;
	}

	public void markAllOperationsInvalid() {
		jdbc.update("UPDATE operation SET valid = 0, updated_time = CURRENT_TIMESTAMP");
	}

	public void deleteOrphanOperations() {
		jdbc.update("DELETE FROM operation WHERE valid = 0 AND class_name NOT IN (SELECT class_name FROM job)");
	}

	public void upsertOperation(String className, String name, String title, String paramsJson, String resultType) {
		jdbc.update("INSERT INTO operation(class_name, name, title, params_json, result_type, valid, updated_time) " +
				"VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP) " +
				"ON CONFLICT(class_name) DO UPDATE SET name = excluded.name, title = excluded.title, " +
				"params_json = excluded.params_json, result_type = excluded.result_type, valid = 1, " +
				"updated_time = CURRENT_TIMESTAMP",
				className, name, title, paramsJson, resultType);
	}

	public List<OperationItem> getOperations() {
		return jdbc.query("SELECT * FROM operation ORDER BY valid DESC, name COLLATE NOCASE", OPERATION_MAPPER);
	}

	public Optional<OperationItem> getOperation(String className) {
		return jdbc.query("SELECT * FROM operation WHERE class_name = ?", OPERATION_MAPPER, className).stream().findFirst();
	}

	public List<JobItem> getJobs() {
		return jdbc.query(JOB_SELECT + " ORDER BY j.updated_time DESC", JOB_MAPPER);
	}

	public Optional<JobItem> getJob(long id) {
		return jdbc.query(JOB_SELECT + " WHERE j.id = ?", JOB_MAPPER, id).stream().findFirst();
	}

	public JobItem createJob(String className, String name, String description, String labels, String paramsJson) {
		jdbc.update("INSERT INTO job(class_name, name, description, labels, params_json, created_time, updated_time) " +
				"VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
				className, name, description, labels, paramsJson);
		return getJob(lastInsertId()).orElseThrow();
	}

	public JobItem updateJob(long id, String className, String name, String description, String labels, String paramsJson) {
		jdbc.update("UPDATE job SET class_name = ?, name = ?, description = ?, labels = ?, params_json = ?, " +
				"updated_time = CURRENT_TIMESTAMP WHERE id = ?",
				className, name, description, labels, paramsJson, id);
		return getJob(id).orElseThrow();
	}

	public void deleteJob(long id) {
		jdbc.update("DELETE FROM run WHERE job_id = ?", id);
		jdbc.update("DELETE FROM job WHERE id = ?", id);
	}

	public List<RunItem> getRuns(Long jobId) {
		if (jobId != null) {
			return jdbc.query(RUN_SELECT + " WHERE r.job_id = ? ORDER BY r.created_time DESC", runMapper, jobId);
		}
		return jdbc.query(RUN_SELECT + " ORDER BY r.created_time DESC", runMapper);
	}

	public Optional<RunItem> getRun(long id) {
		return jdbc.query(RUN_SELECT + " WHERE r.id = ?", runMapper, id).stream().findFirst();
	}

	public long insertRun(long jobId, String paramsJson) {
		jdbc.update("INSERT INTO run(job_id, status, params_json, elapsed_ms, created_time, updated_time) " +
				"VALUES (?, 'PENDING', ?, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", jobId, paramsJson);
		return lastInsertId();
	}

	public void markRunning(long runId) {
		jdbc.update("UPDATE run SET status = 'RUNNING', started_time = CURRENT_TIMESTAMP, " +
				"updated_time = CURRENT_TIMESTAMP WHERE id = ?", runId);
	}

	public void markSuccess(long runId, String resultJson, long elapsedMs) {
		jdbc.update("UPDATE run SET status = 'SUCCESS', result_json = ?, elapsed_ms = ?, " +
				"finished_time = CURRENT_TIMESTAMP, updated_time = CURRENT_TIMESTAMP WHERE id = ?",
				compress(resultJson), elapsedMs, runId);
	}

	public void markFailed(long runId, String resultJson, String errorText, long elapsedMs) {
		jdbc.update("UPDATE run SET status = 'FAILED', result_json = ?, error_text = ?, elapsed_ms = ?, " +
				"finished_time = CURRENT_TIMESTAMP, updated_time = CURRENT_TIMESTAMP WHERE id = ?",
				compress(resultJson), errorText, elapsedMs, runId);
	}

	public void markCancelled(long runId, long elapsedMs) {
		jdbc.update("UPDATE run SET status = 'CANCELLED', elapsed_ms = ?, finished_time = CURRENT_TIMESTAMP, " +
				"updated_time = CURRENT_TIMESTAMP WHERE id = ?", elapsedMs, runId);
	}

	public void requestCancel(long runId) {
		jdbc.update("UPDATE run SET status = 'CANCELLED', finished_time = COALESCE(finished_time, CURRENT_TIMESTAMP), " +
				"updated_time = CURRENT_TIMESTAMP WHERE id = ? AND status IN ('PENDING', 'RUNNING')", runId);
	}

	public void deleteRun(long runId) {
		jdbc.update("DELETE FROM run WHERE id = ?", runId);
	}

	private long lastInsertId() {
		return jdbc.queryForObject("SELECT last_insert_rowid()", Long.class);
	}

	private static final RowMapper<OperationItem> OPERATION_MAPPER = (rs, n) -> new OperationItem(
			rs.getString("class_name"), rs.getString("name"), rs.getString("title"), rs.getString("params_json"),
			rs.getString("result_type"), rs.getInt("valid") == 1, ts(rs, "updated_time"));

	private static final RowMapper<JobItem> JOB_MAPPER = (rs, n) -> new JobItem(
			rs.getLong("id"), rs.getString("class_name"), rs.getString("operation_name"), rs.getString("operation_title"),
			rs.getString("name"), rs.getString("description"), rs.getString("labels"), rs.getString("params_json"),
			ts(rs, "created_time"), ts(rs, "updated_time"));

	private final RowMapper<RunItem> runMapper = (rs, n) -> {
		String resultJson = decompress(rs.getString("result_json"));
		Map.Entry<String, Object> summary = firstPrimitiveEntry(resultJson);
		return new RunItem(rs.getLong("id"), rs.getLong("job_id"), rs.getString("class_name"),
				rs.getString("operation_name"), rs.getString("operation_title"), rs.getString("job_name"),
				rs.getString("status"), rs.getString("params_json"), resultJson, rs.getString("error_text"),
				rs.getLong("elapsed_ms"), null, null, null, summary == null ? null : summary.getKey(),
				summary == null ? null : summary.getValue(), ts(rs, "started_time"), ts(rs, "finished_time"),
				ts(rs, "created_time"), ts(rs, "updated_time"));
	};

	private static LocalDateTime ts(ResultSet rs, String column) throws SQLException {
		Timestamp ts = rs.getTimestamp(column);
		return ts == null ? null : ts.toLocalDateTime();
	}

	private Map.Entry<String, Object> firstPrimitiveEntry(String json) {
		if (json == null || json.isBlank()) {
			return null;
		}
		try {
			JsonNode node = mapper.readTree(json);
			if (!node.isObject()) {
				return null;
			}
			var fields = node.fields();
			while (fields.hasNext()) {
				Map.Entry<String, JsonNode> entry = fields.next();
				JsonNode value = entry.getValue();
				if (value == null || value.isNull() || value.isTextual() || value.isNumber() || value.isBoolean()) {
					Object plain = value == null || value.isNull() ? null : mapper.convertValue(value, Object.class);
					return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), plain);
				}
			}
		} catch (Exception e) {
			return null;
		}
		return null;
	}

	private static final String GZIP_PREFIX = "gzip:";
	private static final int GZIP_THRESHOLD = 1024;

	private static String compress(String json) {
		if (json == null || json.length() < GZIP_THRESHOLD) {
			return json;
		}
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
				gzip.write(json.getBytes(StandardCharsets.UTF_8));
			}
			return GZIP_PREFIX + Base64.getEncoder().encodeToString(out.toByteArray());
		} catch (IOException e) {
			throw new IllegalStateException("Failed to gzip result", e);
		}
	}

	private static String decompress(String value) {
		if (value == null || !value.startsWith(GZIP_PREFIX)) {
			return value;
		}
		try (GZIPInputStream gzip = new GZIPInputStream(
				new ByteArrayInputStream(Base64.getDecoder().decode(value.substring(GZIP_PREFIX.length()))))) {
			return new String(gzip.readAllBytes(), StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new IllegalStateException("Failed to gunzip result", e);
		}
	}
}
