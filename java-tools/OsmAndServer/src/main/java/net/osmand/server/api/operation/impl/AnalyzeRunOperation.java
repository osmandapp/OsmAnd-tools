package net.osmand.server.api.operation.impl;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.operation.OperationContext;
import net.osmand.server.api.operation.OperationRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository;

@Component
@AdminOperation(name = "analyze-run")
public class AnalyzeRunOperation extends AbstractParallelOperation<AnalyzeRunOperation.Params> {

	private static final int BATCH = 1000;

	private final OperationRepository runs;
	private final CloudUserFilesRepository files;
	private final ObjectMapper mapper;

	public AnalyzeRunOperation(OperationRepository runs, CloudUserFilesRepository files, ObjectMapper mapper) {
		this.runs = runs;
		this.files = files;
		this.mapper = mapper;
	}

	public record Params(String runIds, Boolean calcSize, Integer threads) {}

	@Override
	public Object run(Params params, OperationContext ctx) {
		List<Long> runIds = parseRunIds(params.runIds());
		if (runIds.isEmpty()) {
			throw new IllegalArgumentException("runIds is required (comma-separated, e.g. 142,143)");
		}
		Set<Long> unique = new LinkedHashSet<>();
		for (Long runId : runIds) {
			var run = runs.getRun(runId)
					.orElseThrow(() -> new IllegalArgumentException("Run not found: " + runId));
			unique.addAll(ids(run));
		}
		List<Long> ids = new ArrayList<>(unique);
		if (params.calcSize() != null && !params.calcSize()) {
			return Map.of("files", ids.size());
		}
		AtomicLong filesize = new AtomicLong();
		AtomicLong zipfilesize = new AtomicLong();
		forEach(clampThreads(params.threads()), batches(ids), ctx, batch -> {
			for (var f : files.findAllById(batch)) {
				filesize.addAndGet(f.filesize == null ? 0 : f.filesize);
				zipfilesize.addAndGet(f.zipfilesize == null ? 0 : f.zipfilesize);
			}
		});
		return Map.of("files", ids.size(), "filesize", size(filesize.get()), "zipfilesize", size(zipfilesize.get()));
	}

	private static List<Long> parseRunIds(String raw) {
		List<Long> out = new ArrayList<>();
		if (raw != null) {
			for (String part : raw.split(",")) {
				String s = part.trim();
				if (!s.isEmpty()) {
					out.add(Long.parseLong(s));
				}
			}
		}
		return out;
	}

	private static List<List<Long>> batches(List<Long> ids) {
		List<List<Long>> out = new ArrayList<>();
		for (int i = 0; i < ids.size(); i += BATCH) {
			out.add(ids.subList(i, Math.min(ids.size(), i + BATCH)));
		}
		return out;
	}

	private static String size(long bytes) {
		return bytes < 1024 * 1024
				? String.format(Locale.US, "%.1f KB", bytes / 1024.0)
				: String.format(Locale.US, "%.1f MB", bytes / (1024.0 * 1024));
	}

	private List<Long> ids(OperationRepository.RunItem run) {
		List<Long> ids = new ArrayList<>();
		try {
			JsonNode fileIds = mapper.readTree(run.paramsJson()).get("fileIds");
			if (fileIds != null && fileIds.isArray()) {
				fileIds.forEach(node -> ids.add(node.asLong()));
			} else {
				JsonNode found = mapper.readTree(run.resultJson()).get("foundFiles");
				if (found != null) {
					found.forEach(node -> ids.add(node.get("id").asLong()));
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException("Bad run data: " + e.getMessage(), e);
		}
		return ids;
	}
}
