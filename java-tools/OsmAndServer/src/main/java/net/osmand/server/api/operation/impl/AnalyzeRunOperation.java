package net.osmand.server.api.operation.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

	private final OperationRepository runs;
	private final CloudUserFilesRepository files;
	private final ObjectMapper mapper;

	public AnalyzeRunOperation(OperationRepository runs, CloudUserFilesRepository files, ObjectMapper mapper) {
		this.runs = runs;
		this.files = files;
		this.mapper = mapper;
	}

	public record Params(Long runId, Boolean calcSize, Integer threads) {}

	@Override
	public Object run(Params params, OperationContext ctx) {
		var run = runs.getRun(params.runId())
				.orElseThrow(() -> new IllegalArgumentException("Run not found: " + params.runId()));
		List<Long> ids = ids(run);
		if (params.calcSize() != null && !params.calcSize()) {
			return Map.of("files", ids.size());
		}
		AtomicLong filesize = new AtomicLong();
		AtomicLong zipfilesize = new AtomicLong();
		forEach(clampThreads(params.threads()), files.findAllById(ids), ctx, f -> {
			filesize.addAndGet(f.filesize == null ? 0 : f.filesize);
			zipfilesize.addAndGet(f.zipfilesize == null ? 0 : f.zipfilesize);
		});
		return Map.of("files", ids.size(), "filesize", size(filesize.get()), "zipfilesize", size(zipfilesize.get()));
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
