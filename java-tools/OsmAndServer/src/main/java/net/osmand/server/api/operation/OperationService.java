package net.osmand.server.api.operation;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;

import net.osmand.server.api.operation.OperationRegistry.OperationDescriptor;
import net.osmand.server.api.operation.OperationRepository.OperationItem;
import net.osmand.server.api.operation.OperationRepository.JobItem;
import net.osmand.server.api.operation.OperationRepository.RunItem;

@Service
public class OperationService {
	private static final Logger LOGGER = LoggerFactory.getLogger(OperationService.class);

	public record JobRequest(String className, String name, String description, String labels, Map<String, Object> params) {}
	public record RunRequest(Map<String, Object> params) {}

	private record RunningOperation(Future<?> future, OperationContext context, long startedMs) {}

	private final OperationRepository repository;
	private final OperationRegistry registry;
	private final ObjectMapper mapper;
	private final ObjectMapper paramMapper;

	private final ExecutorService executor = Executors.newFixedThreadPool(
			Math.max(2, Runtime.getRuntime().availableProcessors()), runnable -> {
				Thread thread = new Thread(runnable, "operation-run");
				thread.setDaemon(true);
				return thread;
			});
	private final ConcurrentHashMap<Long, RunningOperation> running = new ConcurrentHashMap<>();
	private static final long FLUSH_INTERVAL_MS = 5_000;
	private final ScheduledExecutorService flusher = Executors.newSingleThreadScheduledExecutor(runnable -> {
		Thread thread = new Thread(runnable, "operation-flush");
		thread.setDaemon(true);
		return thread;
	});

	public OperationService(OperationRepository repository, OperationRegistry registry, ObjectMapper mapper) {
		this.repository = repository;
		this.registry = registry;
		this.mapper = mapper;
		this.paramMapper = mapper.copy();
		this.paramMapper.coercionConfigDefaults().setCoercion(CoercionInputShape.EmptyString, CoercionAction.AsNull);
		flusher.scheduleWithFixedDelay(this::flushRunning, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
	}

	private void flushRunning() {
		running.forEach((runId, op) -> {
			try {
				Object partial = op.context().snapshotResult();
				if (partial != null) {
					repository.saveProgressResult(runId, toJson(partial), System.currentTimeMillis() - op.startedMs());
				}
			} catch (Exception e) {
				LOGGER.debug("Failed to flush partial result for run {}", runId, e);
			}
		});
	}

	public List<OperationItem> getOperations() {
		return repository.getOperations();
	}

	public Optional<OperationItem> getOperation(String className) {
		return repository.getOperation(className);
	}

	public List<JobItem> getJobs() {
		return repository.getJobs();
	}

	public Optional<JobItem> getJob(long id) {
		return repository.getJob(id);
	}

	public JobItem createJob(JobRequest request) {
		return repository.createJob(request.className(), request.name(), request.description(), request.labels(),
				toJson(params(request.params())));
	}

	public JobItem updateJob(long id, JobRequest request) {
		return repository.updateJob(id, request.className(), request.name(), request.description(), request.labels(),
				toJson(params(request.params())));
	}

	public void deleteJob(long id) {
		repository.deleteJob(id);
	}

	public List<RunItem> getRuns(Long jobId) {
		return repository.getRuns(jobId).stream().map(this::overlayProgress).toList();
	}

	public Optional<RunItem> getRun(long id) {
		return repository.getRun(id).map(this::overlayProgress);
	}

	private RunItem overlayProgress(RunItem run) {
		RunningOperation active = running.get(run.id());
		if (active == null) {
			return run;
		}
		OperationContext context = active.context();
		long elapsed = System.currentTimeMillis() - active.startedMs();
		return run.withProgress(context.getProcessed(), context.getTotal(), context.getProgressText(), elapsed);
	}

	public RunItem startRun(long jobId, RunRequest request) {
		JobItem job = repository.getJob(jobId).orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));
		Map<String, Object> params = readMap(job.paramsJson());
		if (request != null && request.params() != null) {
			params.putAll(request.params());
		}
		long runId = repository.insertRun(jobId, toJson(params));
		OperationContext context = new OperationContext();
		Future<?> future = executor.submit(() -> executeRun(runId, job.className(), params, context));
		running.put(runId, new RunningOperation(future, context, System.currentTimeMillis()));
		return repository.getRun(runId).orElseThrow();
	}

	public RunItem cancelRun(long runId) {
		interrupt(running.get(runId));
		repository.requestCancel(runId);
		return repository.getRun(runId).orElseThrow();
	}

	public void deleteRun(long runId) {
		interrupt(running.remove(runId));
		repository.deleteRun(runId);
	}

	private void executeRun(long runId, String className, Map<String, Object> params, OperationContext context) {
		long started = System.currentTimeMillis();
		repository.markRunning(runId);
		try {
			OperationDescriptor descriptor = registry.resolve(className)
					.orElseThrow(() -> new IllegalStateException("Operation is not discovered or not valid: " + className));
			Object args = descriptor.paramsType() == null ? params : paramMapper.convertValue(params, descriptor.paramsType());
			Object result = invoke(descriptor.bean(), args, context);
			String resultJson = toJson(result == null ? Collections.emptyMap() : result);
			if (context.isCancelled()) {
				repository.markCancelled(runId, resultJson, elapsed(started)); // keep what was found before cancel
			} else {
				repository.markSuccess(runId, resultJson, elapsed(started));
			}
		} catch (Throwable e) {
			Object partial = context.snapshotResult();
			String partialJson = partial == null ? null : toJson(partial);
			if (isCancellation(e) || context.isCancelled() || Thread.currentThread().isInterrupted()) {
				if (partialJson != null) {
					repository.markCancelled(runId, partialJson, elapsed(started));
				} else {
					repository.markCancelled(runId, elapsed(started));
				}
			} else if (partialJson != null) {
				repository.markFailed(runId, partialJson, stackTrace(e), elapsed(started));
				LOGGER.warn("Operation run {} failed", runId, e);
			} else {
				markFailed(runId, started, e);
			}
		} finally {
			running.remove(runId);
		}
	}

	@SuppressWarnings("unchecked")
	private Object invoke(Operation<?> operation, Object args, OperationContext context) {
		return ((Operation<Object>) operation).run(args, context);
	}

	private void markFailed(long runId, long started, Throwable e) {
		LOGGER.warn("Operation run {} failed", runId, e);
		repository.markFailed(runId, toJson(Collections.singletonMap("exception", exceptionMessage(e))),
				stackTrace(e), elapsed(started));
	}

	private boolean isCancellation(Throwable e) {
		return e instanceof InterruptedException || e instanceof CancellationException;
	}

	private void interrupt(RunningOperation running) {
		if (running != null) {
			// cooperative cancel only: the operation loop checks isCancelled() each user/file and stops,
			// returning its partial result. No hard interrupt, so in-flight DB calls finish and nothing is lost.
			running.context().cancel();
		}
	}

	private Map<String, Object> params(Map<String, Object> params) {
		return params == null ? Collections.emptyMap() : params;
	}

	private long elapsed(long started) {
		return System.currentTimeMillis() - started;
	}

	private String exceptionMessage(Throwable e) {
		return e.getMessage() != null && !e.getMessage().isBlank() ? e.getMessage() : e.getClass().getName();
	}

	private String stackTrace(Throwable e) {
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}

	private String toJson(Object value) {
		try {
			return mapper.writeValueAsString(value);
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to serialize JSON", e);
		}
	}

	private Map<String, Object> readMap(String json) {
		if (json == null || json.isBlank()) {
			return new LinkedHashMap<>();
		}
		try {
			return mapper.readValue(json, new TypeReference<LinkedHashMap<String, Object>>() {});
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to parse JSON", e);
		}
	}
}
