package net.osmand.server.api.operation.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.operation.ExtraParamsOperation;
import net.osmand.server.api.operation.OperationContext;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.api.services.UserSubscriptionService;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;
import net.osmand.util.Algorithms;

/**
 * Finds or deletes two kinds of stale files:
 * - free-inactive: all files of a free (non-pro) account that has been free for over 2 years, with no device sync
 *   for over 2 years (user_account_devices.udpatetime) and no file activity for over 1 year (user_files.updatetime);
 * - trash-1m:      deleted files older than a month.
 */
@Component
@AdminOperation(name = "delete-old-files")
public class DeleteOldFilesOperation extends AbstractFileFixOperation implements ExtraParamsOperation {

	private static final long YEAR_MS = 365L * 86400000L;
	private static final long TWO_YEARS_MS = 2 * YEAR_MS;
	private static final long MONTH_MS = 31L * 86400000L;

	public record Extra(Boolean freeInactive, Boolean oldTrash) {}

	private final UserBatchReader users;
	private final CloudUsersRepository usersRepository;
	private final CloudUserDevicesRepository devicesRepository;
	private final UserSubscriptionService userSubService;

	public DeleteOldFilesOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
	                               UserdataService userdataService, StorageService storageService,
	                               CloudUserDevicesRepository devicesRepository, UserSubscriptionService userSubService) {
		super(usersRepository, filesRepository, userdataService, storageService);
		this.users = new UserBatchReader(usersRepository);
		this.usersRepository = usersRepository;
		this.devicesRepository = devicesRepository;
		this.userSubService = userSubService;
	}

	@Override
	public Class<?> extraParamsType() {
		return Extra.class;
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
		Extra extra = ctx.getExtraParams() instanceof Extra e ? e : new Extra(null, null);
		boolean freeInactive = extra.freeInactive() == null || extra.freeInactive();
		boolean oldTrash = extra.oldTrash() == null || extra.oldTrash();
		List<Integer> userIds = sampleUsers(params, ctx);
		AtomicInteger done = new AtomicInteger();
		forEach(threads, userIds, ctx, userId -> {
			processUser(userId, freeInactive, oldTrash, params, stats);
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

	private void processUser(int userId, boolean freeInactive, boolean oldTrash, Params params, Stats stats) {
		stats.users.incrementAndGet();
		CloudUser user = usersRepository.findById(userId);
		if (user == null) {
			return;
		}
		UserFilesResults res = userdataService.generateFiles(userId, null, true, false, null);
		boolean found = false;
		if (freeInactive && isFreeAndInactive(userId, user, res)) {
			for (UserFileNoData f : res.uniqueFiles) {
				found |= handle(f, "free-inactive", params, stats);
			}
		}
		if (oldTrash) {
			for (UserFileNoData f : newestPerFile(res.allFiles)) {
				if (f.filesize < 0 && System.currentTimeMillis() - f.updatetimems > MONTH_MS) {
					found |= handle(f, "trash-1m", params, stats);
				}
			}
		}
		if (found) {
			stats.usersFound.incrementAndGet();
		}
	}

	// free for 2+ years (pro expired long ago, or registered 2+ years ago if never pro), no device sync
	// for 2+ years and no file activity for 1+ year
	private boolean isFreeAndInactive(int userId, CloudUser user, UserFilesResults res) {
		long now = System.currentTimeMillis();
		return Algorithms.isEmpty(user.orderid)
				&& freeOver2Years(userId, user, now)
				&& now - lastDeviceSync(userId) > TWO_YEARS_MS
				&& now - lastFileActivity(res) > YEAR_MS;
	}

	// free for over 2 years: pro expired 2+ years ago, or (never pro) registered 2+ years ago
	private boolean freeOver2Years(int userId, CloudUser user, long now) {
		long proExpiry = userSubService.lastProExpiry(userId);
		if (proExpiry > 0) {
			return now - proExpiry > TWO_YEARS_MS;
		}
		return user.regTime != null && now - user.regTime.getTime() > TWO_YEARS_MS;
	}

	private static long lastFileActivity(UserFilesResults res) {
		long last = 0;
		for (UserFileNoData f : res.allFiles) {
			last = Math.max(last, f.updatetimems);
		}
		return last;
	}

	// newest device (re)connect across the user's devices
	private long lastDeviceSync(int userId) {
		long last = 0;
		for (CloudUserDevice d : devicesRepository.findByUserid(userId)) {
			if (d.udpatetime != null) {
				last = Math.max(last, d.udpatetime.getTime());
			}
		}
		return last;
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
		throw new UnsupportedOperationException("delete-old-files scans in run()");
	}
}
