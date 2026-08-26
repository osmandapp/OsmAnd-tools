package net.osmand.server.api.operation.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.operation.OperationContext;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;

/**
 * Deletes trashed files (filesize &lt; 0) older than a month. Free-inactive user cleanup lives in notify-inactive-users.
 * Class name is kept (prod jobs reference it); only the operation name changed to delete-old-trash.
 */
@Component
@AdminOperation(name = "delete-old-trash")
public class DeleteOldFilesOperation extends AbstractFileFixOperation {

	private static final long MONTH_MS = 31L * 86400000L;

	private final UserBatchReader users;
	private final CloudUsersRepository usersRepository;

	public DeleteOldFilesOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
	                               UserdataService userdataService, StorageService storageService) {
		super(usersRepository, filesRepository, userdataService, storageService);
		this.users = new UserBatchReader(usersRepository);
		this.usersRepository = usersRepository;
	}

	@Override
	public Object run(Params params, OperationContext ctx) {
		Stats stats = new Stats(isTest(params));
		ctx.setResultSupplier(stats::toResult);
		int threads = clampThreads(params.threads());
		if (params.fileIds() != null && !params.fileIds().isEmpty()) {
			forFiles(params.fileIds(), params, stats, threads, ctx);
			return stats.toResult();
		}
		List<Integer> userIds = sampleUsers(params, ctx);
		AtomicInteger done = new AtomicInteger();
		forEach(threads, userIds, ctx, userId -> {
			processUser(userId, params, stats);
			int d = done.incrementAndGet();
			ctx.setProgress(d, userIds.size(), String.format("%d/%d users · %d found", d, userIds.size(), stats.found.get()));
		});
		return stats.toResult();
	}

	private List<Integer> sampleUsers(Params params, OperationContext ctx) {
		if (params.userId() != null) {
			return List.of(params.userId());
		}
		int total = users.total();
		int from = params.usersFrom() == null ? 0 : Math.max(0, params.usersFrom());
		return users.sample(total, UserBatchReader.limit(total, params.usersPercent()), from, ctx);
	}

	private void forFiles(List<Long> ids, Params params, Stats stats, int threads, OperationContext ctx) {
		AtomicInteger done = new AtomicInteger();
		forEach(threads, ids, ctx, id -> {
			filesRepository.findById(id).ifPresent(uf -> handle(new UserFileNoData(uf), null, params, stats));
			int d = done.incrementAndGet();
			ctx.setProgress(d, ids.size(), String.format("%d/%d files · %d found", d, ids.size(), stats.found.get()));
		});
	}

	private void processUser(int userId, Params params, Stats stats) {
		stats.users.incrementAndGet();
		if (usersRepository.findById(userId) == null) {
			return;
		}
		UserFilesResults res = userdataService.generateFiles(userId, null, true, false, null);
		boolean found = false;
		for (UserFileNoData f : newestPerFile(res.allFiles)) {
			if (f.filesize < 0 && System.currentTimeMillis() - f.updatetimems > MONTH_MS) {
				found |= handle(f, "trash-1m", params, stats);
			}
		}
		if (found) {
			stats.usersFound.incrementAndGet();
		}
	}

	private static List<UserFileNoData> newestPerFile(List<UserFileNoData> allFiles) {
		Set<String> seen = new HashSet<>();
		return allFiles.stream().filter(f -> seen.add(f.type + "____" + f.name)).toList();
	}

	private boolean handle(UserFileNoData f, String tag, Params params, Stats stats) {
		stats.scanned.incrementAndGet();
		stats.found.incrementAndGet();
		if (tag != null) {
			stats.byTag.computeIfAbsent(tag, k -> new AtomicInteger()).incrementAndGet();
		}
		try {
			if (isTest(params)) {
				stats.foundFiles.add(tag == null
						? Map.of("userid", f.userid, "id", f.id, "file", f.name)
						: Map.of("userid", f.userid, "id", f.id, "file", f.name, "tag", tag));
			} else {
				filesRepository.findById(f.id).ifPresent(this::deleteCompletely);
			}
			return true;
		} catch (Exception e) {
			stats.failed.incrementAndGet();
			stats.failedFiles.add(Map.of("userid", f.userid, "id", f.id, "file", f.name, "error", String.valueOf(e.getMessage())));
			return false;
		}
	}

	@Override
	protected boolean fix(UserFile file, Params params) {
		throw new UnsupportedOperationException("delete-old-trash scans in run()");
	}
}
