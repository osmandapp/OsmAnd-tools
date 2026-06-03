package net.osmand.server.api.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.server.api.action.Action;
import net.osmand.server.api.action.ActionContext;
import net.osmand.server.api.action.UiAction;
import net.osmand.server.api.action.UiParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class ActionManagementService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActionManagementService.class);

    private final AtomicInteger threadCounter = new AtomicInteger();
    private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("action-management-run-" + threadCounter.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    });

    private final ConcurrentHashMap<Long, Future<?>> runningTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ActionContext> runningContexts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ActionDescriptor> descriptors = new ConcurrentHashMap<>();

    @Autowired
    @Qualifier("actionJdbcTemplate")
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ApplicationContext applicationContext;
    @Autowired
    private ObjectMapper objectMapper;

    public record ParamDescriptor(String name, String label, String type, boolean required, String defaultValue,
                                  String helpText, boolean context) {}
    public record ActionItem(String className, String name, String title, String paramsJson, String resultType,
                             boolean valid, LocalDateTime updatedTime) {}
    public record JobItem(Long id, String className, String actionName, String actionTitle, String name,
                          String description, String labels, String paramsJson, LocalDateTime createdTime,
                          LocalDateTime updatedTime) {}
    public record RunItem(Long id, Long jobId, String className, String actionName, String actionTitle, String jobName,
                          String status, String paramsJson, String resultJson, String errorText, Long elapsedMs,
                          String summaryKey, Object summaryValue, LocalDateTime startedTime, LocalDateTime finishedTime,
                          LocalDateTime createdTime, LocalDateTime updatedTime) {}
    public record JobRequest(String className, String name, String description, String labels, Map<String, Object> params) {}
    public record RunRequest(Map<String, Object> params) {}

    private record ActionDescriptor(Object bean, Class<?> targetClass, Method method, UiAction annotation,
                                    List<ParamDescriptor> params) {}

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        initSchema();
        discoverActions();
    }

    public void initSchema() {
        jdbcTemplate.execute("PRAGMA foreign_keys = ON");
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS action (" +
                "class_name TEXT PRIMARY KEY," +
                "name TEXT NOT NULL," +
                "title TEXT," +
                "params_json TEXT NOT NULL," +
                "result_type TEXT NOT NULL," +
                "valid INTEGER NOT NULL DEFAULT 1," +
                "updated_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP" +
                ")");
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS job (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "class_name TEXT NOT NULL," +
                "name TEXT," +
                "description TEXT," +
                "labels TEXT," +
                "params_json TEXT NOT NULL," +
                "created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "updated_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "FOREIGN KEY (class_name) REFERENCES action(class_name)" +
                ")");
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS run (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "job_id INTEGER NOT NULL," +
                "status TEXT NOT NULL," +
                "params_json TEXT NOT NULL," +
                "result_json TEXT," +
                "error_text TEXT," +
                "elapsed_ms INTEGER NOT NULL DEFAULT 0," +
                "started_time TIMESTAMP," +
                "finished_time TIMESTAMP," +
                "created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "updated_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "FOREIGN KEY (job_id) REFERENCES job(id) ON DELETE CASCADE" +
                ")");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_job_class_name ON job(class_name)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_job_updated_time ON job(updated_time DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_run_job_id ON run(job_id)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_run_status ON run(status)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_run_created_time ON run(created_time DESC)");
    }

    public synchronized void discoverActions() {
        jdbcTemplate.update("UPDATE action SET valid = 0, updated_time = CURRENT_TIMESTAMP");
        descriptors.clear();
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(UiAction.class);
        for (Object bean : beans.values()) {
            Class<?> targetClass = org.springframework.aop.support.AopUtils.getTargetClass(bean);
            if (!Action.class.isAssignableFrom(targetClass)) {
                continue;
            }
            UiAction uiAction = targetClass.getAnnotation(UiAction.class);
            Method runMethod = findRunMethod(targetClass);
            List<ParamDescriptor> params = describeParams(runMethod);
            String paramsJson = toJson(params);
            jdbcTemplate.update("INSERT INTO action(class_name, name, title, params_json, result_type, valid, updated_time) " +
                            "VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP) " +
                            "ON CONFLICT(class_name) DO UPDATE SET name = excluded.name, title = excluded.title, " +
                            "params_json = excluded.params_json, result_type = excluded.result_type, valid = 1, updated_time = CURRENT_TIMESTAMP",
                    targetClass.getName(), uiAction.name(), uiAction.title(), paramsJson, runMethod.getReturnType().getName());
            descriptors.put(targetClass.getName(), new ActionDescriptor(bean, targetClass, runMethod, uiAction, params));
        }
    }

    private Method findRunMethod(Class<?> targetClass) {
        Method found = null;
        for (Method method : targetClass.getMethods()) {
            if (!method.getName().equals("run")) {
                continue;
            }
            if (found != null) {
                throw new IllegalStateException("Action " + targetClass.getName() + " has overloaded run methods");
            }
            found = method;
        }
        if (found == null) {
            throw new IllegalStateException("Action " + targetClass.getName() + " has no run method");
        }
        return found;
    }

    private List<ParamDescriptor> describeParams(Method method) {
        List<ParamDescriptor> params = new ArrayList<>();
        for (Parameter parameter : method.getParameters()) {
            Class<?> type = parameter.getType();
            if (ActionContext.class.isAssignableFrom(type)) {
                params.add(new ParamDescriptor(parameter.getName(), "Context", "context", false, "", "", true));
                continue;
            }
            if (!isSupportedParamType(type)) {
                throw new IllegalStateException("Unsupported action parameter type: " + type.getName());
            }
            UiParam uiParam = parameter.getAnnotation(UiParam.class);
            String name = uiParam != null && !uiParam.name().isBlank() ? uiParam.name() : parameter.getName();
            String label = uiParam != null && !uiParam.name().isBlank() ? uiParam.name() : name;
            String help = uiParam == null ? "" : uiParam.title();
            String defaultValue = uiParam == null ? "" : uiParam.defaultValue();
            boolean required = uiParam != null && uiParam.required();
            params.add(new ParamDescriptor(name, label, inputType(type), required, defaultValue, help, false));
        }
        return params;
    }

    private boolean isSupportedParamType(Class<?> type) {
        if (type == byte.class || type == Byte.class) {
            return false;
        }
        return type == String.class || type == boolean.class || type == Boolean.class || type == char.class || type == Character.class ||
                type == short.class || type == Short.class || type == int.class || type == Integer.class || type == long.class || type == Long.class ||
                type == float.class || type == Float.class || type == double.class || type == Double.class;
    }

    private String inputType(Class<?> type) {
        if (type == boolean.class || type == Boolean.class) {
            return "checkbox";
        }
        if (type == char.class || type == Character.class || type == String.class) {
            return "text";
        }
        return "number";
    }

    public List<ActionItem> getActions() {
        return jdbcTemplate.query("SELECT * FROM action ORDER BY valid DESC, name COLLATE NOCASE", actionMapper());
    }

    public Optional<ActionItem> getAction(String className) {
        List<ActionItem> res = jdbcTemplate.query("SELECT * FROM action WHERE class_name = ?", actionMapper(), className);
        return res.stream().findFirst();
    }

    public List<JobItem> getJobs() {
        return jdbcTemplate.query("SELECT j.*, a.name action_name, a.title action_title FROM job j LEFT JOIN action a ON a.class_name = j.class_name ORDER BY j.updated_time DESC", jobMapper());
    }

    public Optional<JobItem> getJob(long id) {
        List<JobItem> res = jdbcTemplate.query("SELECT j.*, a.name action_name, a.title action_title FROM job j LEFT JOIN action a ON a.class_name = j.class_name WHERE j.id = ?", jobMapper(), id);
        return res.stream().findFirst();
    }

    public JobItem createJob(JobRequest request) {
        Objects.requireNonNull(request, "request");
        String paramsJson = toJson(request.params() == null ? Collections.emptyMap() : request.params());
        jdbcTemplate.update("INSERT INTO job(class_name, name, description, labels, params_json, created_time, updated_time) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                request.className(), request.name(), request.description(), request.labels(), paramsJson);
        Long id = jdbcTemplate.queryForObject("SELECT last_insert_rowid()", Long.class);
        return getJob(id).orElseThrow();
    }

    public JobItem updateJob(long id, JobRequest request) {
        Objects.requireNonNull(request, "request");
        String paramsJson = toJson(request.params() == null ? Collections.emptyMap() : request.params());
        jdbcTemplate.update("UPDATE job SET class_name = ?, name = ?, description = ?, labels = ?, params_json = ?, updated_time = CURRENT_TIMESTAMP WHERE id = ?",
                request.className(), request.name(), request.description(), request.labels(), paramsJson, id);
        return getJob(id).orElseThrow();
    }

    public void deleteJob(long id) {
        jdbcTemplate.update("PRAGMA foreign_keys = ON");
        jdbcTemplate.update("DELETE FROM run WHERE job_id = ?", id);
        jdbcTemplate.update("DELETE FROM job WHERE id = ?", id);
    }

    public List<RunItem> getRuns(Long jobId) {
        String sql = "SELECT r.*, j.class_name, j.name job_name, a.name action_name, a.title action_title " +
                "FROM run r JOIN job j ON j.id = r.job_id LEFT JOIN action a ON a.class_name = j.class_name";
        if (jobId != null) {
            return jdbcTemplate.query(sql + " WHERE r.job_id = ? ORDER BY r.created_time DESC", runMapper(), jobId);
        }
        return jdbcTemplate.query(sql + " ORDER BY r.created_time DESC", runMapper());
    }

    public Optional<RunItem> getRun(long id) {
        List<RunItem> res = jdbcTemplate.query("SELECT r.*, j.class_name, j.name job_name, a.name action_name, a.title action_title " +
                "FROM run r JOIN job j ON j.id = r.job_id LEFT JOIN action a ON a.class_name = j.class_name WHERE r.id = ?", runMapper(), id);
        return res.stream().findFirst();
    }

    public RunItem startRun(long jobId, RunRequest request) {
        JobItem job = getJob(jobId).orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));
        Map<String, Object> params = readMap(job.paramsJson());
        if (request != null && request.params() != null) {
            params.putAll(request.params());
        }
        String paramsJson = toJson(params);
        jdbcTemplate.update("INSERT INTO run(job_id, status, params_json, elapsed_ms, created_time, updated_time) VALUES (?, 'PENDING', ?, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", jobId, paramsJson);
        Long runId = jdbcTemplate.queryForObject("SELECT last_insert_rowid()", Long.class);
        Future<?> future = executor.submit(() -> executeRun(runId, job.className(), params));
        runningTasks.put(runId, future);
        return getRun(runId).orElseThrow();
    }

    private void executeRun(long runId, String className, Map<String, Object> params) {
        ActionContext context = new ActionContext();
        runningContexts.put(runId, context);
        long started = System.currentTimeMillis();
        jdbcTemplate.update("UPDATE run SET status = 'RUNNING', started_time = CURRENT_TIMESTAMP, updated_time = CURRENT_TIMESTAMP WHERE id = ?", runId);
        try {
            ActionDescriptor descriptor = descriptors.get(className);
            if (descriptor == null) {
                throw new IllegalStateException("Action is not discovered or not valid: " + className);
            }
            Object[] args = buildArguments(descriptor, params, context);
            Object result = descriptor.method().invoke(descriptor.bean(), args);
            String resultJson = toJson(result == null ? Collections.emptyMap() : result);
            long elapsed = System.currentTimeMillis() - started;
            jdbcTemplate.update("UPDATE run SET status = 'SUCCESS', result_json = ?, elapsed_ms = ?, finished_time = CURRENT_TIMESTAMP, updated_time = CURRENT_TIMESTAMP WHERE id = ?",
                    resultJson, elapsed, runId);
        } catch (InvocationTargetException e) {
            Throwable target = e.getTargetException();
            if (target instanceof InterruptedException || target instanceof CancellationException) {
                markCancelled(runId, started);
            } else {
                markFailed(runId, started, target);
            }
        } catch (InterruptedException | CancellationException e) {
            markCancelled(runId, started);
            Thread.currentThread().interrupt();
        } catch (Throwable e) {
            if (Thread.currentThread().isInterrupted()) {
                markCancelled(runId, started);
            } else {
                markFailed(runId, started, e);
            }
        } finally {
            runningTasks.remove(runId);
            runningContexts.remove(runId);
        }
    }

    private Object[] buildArguments(ActionDescriptor descriptor, Map<String, Object> params, ActionContext context) throws InterruptedException {
        Parameter[] parameters = descriptor.method().getParameters();
        Object[] args = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];
            if (ActionContext.class.isAssignableFrom(parameter.getType())) {
                args[i] = context;
            } else {
                UiParam uiParam = parameter.getAnnotation(UiParam.class);
                String name = uiParam != null && !uiParam.name().isBlank() ? uiParam.name() : parameter.getName();
                args[i] = convertValue(params.get(name), parameter.getType());
            }
        }
        return args;
    }

    private Object convertValue(Object value, Class<?> type) {
        if (value == null || (value instanceof String s && s.isBlank())) {
            if (type.isPrimitive()) {
                if (type == boolean.class) return false;
                if (type == char.class) return '\0';
                if (type == short.class) return (short) 0;
                if (type == int.class) return 0;
                if (type == long.class) return 0L;
                if (type == float.class) return 0f;
                if (type == double.class) return 0d;
            }
            return null;
        }
        if (type == String.class) return String.valueOf(value);
        if (type == boolean.class || type == Boolean.class) return value instanceof Boolean b ? b : Boolean.parseBoolean(String.valueOf(value));
        if (type == char.class || type == Character.class) return String.valueOf(value).isEmpty() ? '\0' : String.valueOf(value).charAt(0);
        if (type == short.class || type == Short.class) return value instanceof Number n ? n.shortValue() : Short.parseShort(String.valueOf(value));
        if (type == int.class || type == Integer.class) return value instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(value));
        if (type == long.class || type == Long.class) return value instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(value));
        if (type == float.class || type == Float.class) return value instanceof Number n ? n.floatValue() : Float.parseFloat(String.valueOf(value));
        if (type == double.class || type == Double.class) return value instanceof Number n ? n.doubleValue() : Double.parseDouble(String.valueOf(value));
        throw new IllegalArgumentException("Unsupported parameter type: " + type.getName());
    }

    public void deleteRun(long runId) {
        Future<?> future = runningTasks.remove(runId);
        if (future != null) {
            future.cancel(true);
        }
        ActionContext context = runningContexts.remove(runId);
        if (context != null) {
            context.cancel();
        }
        jdbcTemplate.update("DELETE FROM run WHERE id = ?", runId);
    }

    public RunItem cancelRun(long runId) {
        ActionContext context = runningContexts.get(runId);
        if (context != null) {
            context.cancel();
        }
        Future<?> future = runningTasks.get(runId);
        if (future != null) {
            future.cancel(true);
        }
        jdbcTemplate.update("UPDATE run SET status = 'CANCELLED', finished_time = COALESCE(finished_time, CURRENT_TIMESTAMP), updated_time = CURRENT_TIMESTAMP WHERE id = ? AND status IN ('PENDING', 'RUNNING')", runId);
        return getRun(runId).orElseThrow();
    }

    private void markCancelled(long runId, long started) {
        jdbcTemplate.update("UPDATE run SET status = 'CANCELLED', elapsed_ms = ?, finished_time = CURRENT_TIMESTAMP, updated_time = CURRENT_TIMESTAMP WHERE id = ?",
                System.currentTimeMillis() - started, runId);
    }

    private void markFailed(long runId, long started, Throwable e) {
        LOGGER.warn("Action run {} failed", runId, e);
        jdbcTemplate.update("UPDATE run SET status = 'FAILED', result_json = ?, error_text = ?, elapsed_ms = ?, finished_time = CURRENT_TIMESTAMP, updated_time = CURRENT_TIMESTAMP WHERE id = ?",
                toJson(Collections.singletonMap("exception", exceptionMessage(e))), stackTrace(e), System.currentTimeMillis() - started, runId);
    }

    private String exceptionMessage(Throwable e) {
        if (e.getMessage() != null && !e.getMessage().isBlank()) {
            return e.getMessage();
        }
        return e.getClass().getName();
    }

    private RowMapper<ActionItem> actionMapper() {
        return (rs, rowNum) -> new ActionItem(rs.getString("class_name"), rs.getString("name"), rs.getString("title"),
                rs.getString("params_json"), rs.getString("result_type"), rs.getInt("valid") == 1, ts(rs, "updated_time"));
    }

    private RowMapper<JobItem> jobMapper() {
        return (rs, rowNum) -> new JobItem(rs.getLong("id"), rs.getString("class_name"), rs.getString("action_name"),
                rs.getString("action_title"), rs.getString("name"), rs.getString("description"), rs.getString("labels"),
                rs.getString("params_json"), ts(rs, "created_time"), ts(rs, "updated_time"));
    }

    private RowMapper<RunItem> runMapper() {
        return new RowMapper<>() {
            @Override
            public RunItem mapRow(ResultSet rs, int rowNum) throws SQLException {
                String resultJson = rs.getString("result_json");
                Map.Entry<String, Object> summary = firstPrimitiveEntry(resultJson);
                return new RunItem(rs.getLong("id"), rs.getLong("job_id"), rs.getString("class_name"), rs.getString("action_name"),
                        rs.getString("action_title"), rs.getString("job_name"), rs.getString("status"), rs.getString("params_json"),
                        resultJson, rs.getString("error_text"), rs.getLong("elapsed_ms"),
                        summary == null ? null : summary.getKey(), summary == null ? null : summary.getValue(),
                        ts(rs, "started_time"), ts(rs, "finished_time"), ts(rs, "created_time"), ts(rs, "updated_time"));
            }
        };
    }

    private LocalDateTime ts(ResultSet rs, String column) throws SQLException {
        Timestamp ts = rs.getTimestamp(column);
        return ts == null ? null : ts.toLocalDateTime();
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize JSON", e);
        }
    }

    private Map<String, Object> readMap(String json) {
        if (json == null || json.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            return objectMapper.readValue(json, new TypeReference<LinkedHashMap<String, Object>>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse JSON", e);
        }
    }

    private Map.Entry<String, Object> firstPrimitiveEntry(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(json);
            if (!node.isObject()) {
                return null;
            }
            var fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                JsonNode value = entry.getValue();
                if (value == null || value.isNull() || value.isTextual() || value.isNumber() || value.isBoolean()) {
                    return new java.util.AbstractMap.SimpleImmutableEntry<>(entry.getKey(), value == null || value.isNull() ? null : objectMapper.convertValue(value, Object.class));
                }
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    private String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}

