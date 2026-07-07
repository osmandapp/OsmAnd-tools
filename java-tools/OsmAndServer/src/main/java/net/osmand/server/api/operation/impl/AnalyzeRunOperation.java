package net.osmand.server.api.operation.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
		Map<String, Set<Long>> groups = new LinkedHashMap<>();
		for (Long runId : runIds) {
			collect(runs.getRun(runId)
					.orElseThrow(() -> new IllegalArgumentException("Run not found: " + runId)), groups);
		}
		boolean calcSize = params.calcSize() == null || params.calcSize();
		int threads = clampThreads(params.threads());

		Map<String, Object> result = new LinkedHashMap<>();
		groups.forEach((group, ids) -> result.put(group, measure(new ArrayList<>(ids), calcSize, threads, ctx)));
		return result;
	}

	// file count + size of the found id versions and, separately, of all versions of those files (what deleteCompletely removes)
	private Map<String, Object> measure(List<Long> ids, boolean calcSize, int threads, OperationContext ctx) {
		Map<String, Object> r = new LinkedHashMap<>();
		r.put("files", ids.size());
		if (calcSize) {
			AtomicLong filesize = new AtomicLong(), zipfilesize = new AtomicLong();
			AtomicLong allFilesize = new AtomicLong(), allZipfilesize = new AtomicLong();
			Set<String> countedFiles = ConcurrentHashMap.newKeySet();
			forEach(threads, batches(ids), ctx, batch -> {
				for (var f : files.findAllById(batch)) {
					filesize.addAndGet(orZero(f.filesize));
					zipfilesize.addAndGet(orZero(f.zipfilesize));
					if (countedFiles.add(f.userid + "\n" + f.type + "\n" + f.name)) {
						for (var v : files.findAllByUseridAndNameAndTypeOrderByUpdatetimeDesc(f.userid, f.name, f.type)) {
							if (orZero(v.filesize) >= 0) { // skip -1 tombstones
								allFilesize.addAndGet(orZero(v.filesize));
								allZipfilesize.addAndGet(orZero(v.zipfilesize));
							}
						}
					}
				}
			});
			r.put("filesize", size(filesize.get()));
			r.put("zipfilesize", size(zipfilesize.get()));
			r.put("allVersionsFilesize", size(allFilesize.get()));
			r.put("allVersionsZipfilesize", size(allZipfilesize.get()));
		}
		return r;
	}

	private static long orZero(Long value) {
		return value == null ? 0 : value;
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

	private void collect(OperationRepository.RunItem run, Map<String, Set<Long>> groups) {
		try {
			JsonNode fileIds = mapper.readTree(run.paramsJson()).get("fileIds");
			if (fileIds != null && fileIds.isArray()) {
				Set<Long> g = groups.computeIfAbsent("files", k -> new LinkedHashSet<>());
				fileIds.forEach(node -> g.add(node.asLong()));
				return;
			}
			JsonNode found = mapper.readTree(run.resultJson()).get("foundFiles");
			if (found != null && found.isArray()) {
				found.forEach(node -> {
					String group = node.hasNonNull("tag") ? node.get("tag").asText() : "files";
					groups.computeIfAbsent(group, k -> new LinkedHashSet<>()).add(node.get("id").asLong());
				});
			}
		} catch (Exception e) {
			throw new IllegalStateException("Bad run data: " + e.getMessage(), e);
		}
	}
}
