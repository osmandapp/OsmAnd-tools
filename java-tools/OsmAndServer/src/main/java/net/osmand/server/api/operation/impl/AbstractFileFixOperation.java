package net.osmand.server.api.operation.impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import net.osmand.server.api.operation.OperationContext;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.StorageService.InternalZipFile;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;

public abstract class AbstractFileFixOperation extends AbstractParallelOperation<AbstractFileFixOperation.Params> {

	private static final String TMP_PREFIX = "op-fix-";
	private static final String TMP_SUFFIX = ".tmp";

	private final UserBatchReader users;
	protected final CloudUserFilesRepository filesRepository;
	protected final UserdataService userdataService;
	protected final StorageService storageService;

	protected AbstractFileFixOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
									   UserdataService userdataService, StorageService storageService) {
		this.users = new UserBatchReader(usersRepository);
		this.filesRepository = filesRepository;
		this.userdataService = userdataService;
		this.storageService = storageService;
	}

	public record Params(
			Integer userId,         // null/empty = all users; a number = single user (for testing)
			Boolean testRun,         // null/true = only count, no write (safe default); false = actually write
			LocalDate updatedAfter,  // null = no lower bound; else only files updated on/after this date
			LocalDate updatedBefore, // null = no upper bound; else only files updated on/before this date
			Set<String> fileTypes,   // null/empty = all types; otherwise only these (e.g. GPX, FAVOURITES)
			Integer usersFrom,       // null/0 = from the first user; else skip this many users (ordered by id) and start there
			Integer usersPercent,    // null/100 = all users; otherwise that % of all users
			Integer filesPercent,    // null/100 = all files; otherwise that % of each user's files
			List<Long> fileIds,      // null/empty = normal scan; otherwise process only these file ids
			Integer threads          // null/1 = sequential; 2..10 = parallel processing
	) {}

	protected abstract boolean fix(UserFile file, boolean testRun) throws IOException;

	protected boolean accepts(String name) {
		return true;
	}

	private static boolean isTest(Params params) {
		return params.testRun() == null || params.testRun();
	}

	@Override
	public Object run(Params params, OperationContext ctx) {
		Stats stats = new Stats(isTest(params));
		ctx.setResultSupplier(stats::toResult);
		if (params.fileIds() != null && !params.fileIds().isEmpty()) {
			runForFiles(params, ctx, stats);
		} else {
			runForUsers(params, ctx, stats);
		}
		return stats.toResult();
	}

	private void runForUsers(Params params, OperationContext ctx, Stats stats) {
		int threads = clampThreads(params.threads());
		if (params.userId() != null) {
			forEach(threads, List.of(params.userId()), ctx, userId -> {
				processUser(userId, params, stats, Integer.MAX_VALUE);
				ctx.setProgress(1, 1, String.format("1/1 users · %d found", stats.found.get()));
			});
			return;
		}
		int total = users.total();
		int from = params.usersFrom() == null ? 0 : Math.max(0, params.usersFrom());
		List<Integer> userIds = users.sample(total, UserBatchReader.limit(total, params.usersPercent()), from, ctx);
		int fileCap = fileCap(params, userIds, ctx);
		int totalUsers = userIds.size();
		AtomicInteger done = new AtomicInteger();
		forEach(threads, userIds, ctx, userId -> {
			if (stats.scanned.get() < fileCap) {
				processUser(userId, params, stats, fileCap);
			}
			int d = done.incrementAndGet();
			ctx.setProgress(d, totalUsers, String.format("%d/%d users · %d found", d, totalUsers, stats.found.get()));
		});
	}

	private void runForFiles(Params params, OperationContext ctx, Stats stats) {
		List<Long> ids = params.fileIds();
		int total = ids.size();
		AtomicInteger done = new AtomicInteger();
		forEach(clampThreads(params.threads()), ids, ctx, id -> {
			stats.scanned.incrementAndGet();
			processFileById(id, params, stats);
			int d = done.incrementAndGet();
			ctx.setProgress(d, total, String.format("%d/%d files · %d found", d, total, stats.found.get()));
		});
	}

	private void processUser(int userId, Params params, Stats stats, int fileCap) {
		stats.users.incrementAndGet();
		boolean userFound = false;
		Long afterMs = params.updatedAfter() == null ? null
				: params.updatedAfter().atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
		Long beforeMs = params.updatedBefore() == null ? null
				: params.updatedBefore().plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
		UserFilesResults res = userdataService.generateFiles(userId, null, false, false, typesOf(params));
		for (UserFileNoData fileNoData : res.uniqueFiles) {
			if (afterMs != null && fileNoData.updatetimems < afterMs) {
				continue;
			}
			if (beforeMs != null && fileNoData.updatetimems >= beforeMs) {
				continue;
			}
			if (!accepts(fileNoData.name)) {
				continue;
			}
			if (stats.scanned.get() >= fileCap) {
				break;
			}
			stats.scanned.incrementAndGet();
			if (processFileById(fileNoData.id, params, stats)) {
				userFound = true;
			}
		}
		if (userFound) {
			stats.usersFound.incrementAndGet();
		}
	}

	private boolean processFileById(long id, Params params, Stats stats) {
		UserFile file = filesRepository.findById(id).orElse(null);
		if (file == null || !accepts(file.name)) {
			stats.skipped.incrementAndGet();
			return false;
		}
		try {
			boolean test = isTest(params);
			if (!fix(file, test)) {
				stats.skipped.incrementAndGet();
				return false;
			}
			stats.found.incrementAndGet();
			String tag = tag(file);
			if (tag != null) {
				stats.byTag.computeIfAbsent(tag, k -> new AtomicInteger()).incrementAndGet();
			}
			if (test) {
				stats.foundFiles.add(tag == null
						? Map.of("userid", file.userid, "id", file.id, "file", file.name)
						: Map.of("userid", file.userid, "id", file.id, "file", file.name, "tag", tag));
			}
			return true;
		} catch (Exception e) {
			stats.failed.incrementAndGet();
			stats.failedFiles.add(Map.of("userid", file.userid, "id", file.id, "file", file.name, "error", String.valueOf(e.getMessage())));
			return false;
		}
	}

	protected byte[] read(UserFile file) throws IOException {
		InputStream raw = userdataService.getInputStream(file);
		if (raw == null) {
			throw new IOException("File content is not available");
		}
		try (InputStream in = new GZIPInputStream(new BufferedInputStream(raw))) {
			return in.readAllBytes();
		}
	}

	protected void save(UserFile file, byte[] content) throws IOException {
		File tmp = Files.createTempFile(TMP_PREFIX, TMP_SUFFIX).toFile();
		try {
			Files.write(tmp.toPath(), content);
			InternalZipFile zip = InternalZipFile.buildFromFileAndDelete(tmp);
			storageService.save(userdataService.userFolder(file), userdataService.storageFileName(file), zip);
			if (storageService.storeLocally()) {
				file.data = zip.getBytes();
				file.filesize = zip.getContentSize();
				file.zipfilesize = zip.getSize();
				filesRepository.save(file);
			}
		} finally {
			Files.deleteIfExists(tmp.toPath());
		}
	}

	private int fileCap(Params params, List<Integer> userIds, OperationContext ctx) {
		if (params.filesPercent() == null || params.filesPercent() >= 100) {
			return Integer.MAX_VALUE;
		}
		AtomicInteger sum = new AtomicInteger();
		forEach(clampThreads(params.threads()), userIds, ctx, userId ->
				sum.addAndGet(userdataService.generateFiles(userId, null, false, false, typesOf(params)).uniqueFiles.size()));
		return UserBatchReader.limit(sum.get(), params.filesPercent());
	}

	public Set<String> supportedTypes() {
		return null;
	}

	// optional tag added to each found file entry and counted in byTag; null = no tag (default)
	protected String tag(UserFile file) {
		return null;
	}

	private Set<String> typesOf(Params params) {
		Set<String> supported = supportedTypes();
		Set<String> chosen = params.fileTypes() == null ? Set.of() : params.fileTypes();
		if (supported == null) {
			return chosen; // empty = all
		}
		if (chosen.isEmpty()) {
			return supported;
		}
		Set<String> r = new HashSet<>(chosen);
		r.retainAll(supported);
		return r.isEmpty() ? supported : r;
	}

	protected static final class Stats {
		final boolean testRun;
		final AtomicInteger found = new AtomicInteger();
		final AtomicInteger scanned = new AtomicInteger();
		final AtomicInteger skipped = new AtomicInteger();
		final AtomicInteger failed = new AtomicInteger();
		final AtomicInteger users = new AtomicInteger();
		final AtomicInteger usersFound = new AtomicInteger();
		final Map<String, AtomicInteger> byTag = new ConcurrentHashMap<>();
		final List<Map<String, Object>> foundFiles = Collections.synchronizedList(new ArrayList<>());
		final List<Map<String, Object>> failedFiles = Collections.synchronizedList(new ArrayList<>());

		Stats(boolean testRun) {
			this.testRun = testRun;
		}

		Map<String, Object> toResult() {
			Map<String, Object> r = new LinkedHashMap<>();
			r.put("found", found.get());
			r.put("scanned", scanned.get());
			r.put("skipped", skipped.get());
			r.put("failed", failed.get());
			r.put("users", users.get());
			r.put("usersFound", usersFound.get());
			if (!byTag.isEmpty()) {
				Map<String, Object> tags = new LinkedHashMap<>();
				byTag.forEach((k, v) -> tags.put(k, v.get()));
				r.put("byTag", tags);
			}
			r.put("testRun", testRun);
			r.put("foundFiles", new ArrayList<>(foundFiles));
			r.put("failedFiles", new ArrayList<>(failedFiles));
			return r;
		}
	}
}
