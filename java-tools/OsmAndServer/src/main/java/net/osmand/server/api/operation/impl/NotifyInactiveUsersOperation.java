package net.osmand.server.api.operation.impl;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.operation.ExtraParamsOperation;
import net.osmand.server.api.operation.InactiveUserNoticeRepository;
import net.osmand.server.api.operation.InactiveUserNoticeRepository.Notice;
import net.osmand.server.api.operation.OperationContext;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserSubscriptionService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;

/**
 * Mailing lists of free-inactive users (had-pro / never-pro) as comma-separated user ids. State is kept across runs:
 * new users are registered only with registerNew, a user active again is dropped, a user still inactive after the
 * grace period (default 30 days) is listed for deletion and deleted only with deleteFiles in a prod run.
 * Free-inactive = free 2+ years, no device sync 2+ years, no file activity 1+ year.
 */
@Component
@AdminOperation(name = "notify-inactive-users")
public class NotifyInactiveUsersOperation extends AbstractFileFixOperation implements ExtraParamsOperation {

	private static final long YEAR_MS = 365L * 86400000L;
	private static final long TWO_YEARS_MS = 2 * YEAR_MS;
	private static final long DAY_MS = 86400000L;
	private static final int DEFAULT_GRACE_DAYS = 30;

	// graceDays: days after notification before a still-inactive user is eligible for deletion (null = 30)
	// registerNew: store new users as notified (prod only); deleteFiles: delete files of eligible users (prod only)
	public record Extra(Integer graceDays, Boolean hadPro, Boolean neverPro, Boolean registerNew, Boolean deleteFiles) {
	}

	private final UserBatchReader users;
	private final CloudUsersRepository usersRepository;
	private final CloudUserDevicesRepository devicesRepository;
	private final UserSubscriptionService userSubService;
	private final InactiveUserNoticeRepository noticeRepository;
	private final Object dbLock = new Object();

	public NotifyInactiveUsersOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
	                                    UserdataService userdataService, StorageService storageService,
	                                    CloudUserDevicesRepository devicesRepository, UserSubscriptionService userSubService,
	                                    InactiveUserNoticeRepository noticeRepository) {
		super(usersRepository, filesRepository, userdataService, storageService);
		this.users = new UserBatchReader(usersRepository);
		this.usersRepository = usersRepository;
		this.devicesRepository = devicesRepository;
		this.userSubService = userSubService;
		this.noticeRepository = noticeRepository;
	}

	@Override
	public Class<?> extraParamsType() {
		return Extra.class;
	}

	@Override
	public Object run(Params params, OperationContext ctx) {
		Result result = new Result(isTest(params));
		ctx.setResultSupplier(result::toResult);

		Extra extra = ctx.getExtraParams() instanceof Extra e ? e : new Extra(null, null, null, null, null);
		boolean prod = !isTest(params);
		Options options = new Options(prod,
				extra.hadPro() == null || extra.hadPro(),
				extra.neverPro() == null || extra.neverPro(),
				(extra.graceDays() == null ? DEFAULT_GRACE_DAYS : Math.max(0, extra.graceDays())) * DAY_MS,
				prod && Boolean.TRUE.equals(extra.registerNew()),
				prod && Boolean.TRUE.equals(extra.deleteFiles()));

		List<Integer> userIds = sampleUsers(params, ctx);
		int total = userIds.size();
		AtomicInteger done = new AtomicInteger();
		forEach(clampThreads(params.threads()), userIds, ctx, userId -> {
			processUser(userId, options, result);
			int d = done.incrementAndGet();
			ctx.setProgress(d, total, String.format("%d/%d users · %d new · %d still inactive · %d reactivated · %d deleted",
					d, total, result.newUsers.size(), result.stillInactive.size(), result.reactivated.size(),
					result.deleted.size()));
		});

		return result.toResult();
	}

	private List<Integer> sampleUsers(Params params, OperationContext ctx) {
		if (params.userId() != null) {
			return List.of(params.userId());
		}
		int total = users.total();
		int from = params.usersFrom() == null ? 0 : Math.max(0, params.usersFrom());
		return users.sample(total, UserBatchReader.limit(total, params.usersPercent()), from, ctx);
	}

	private record Options(boolean prod, boolean includeHadPro, boolean includeNeverPro, long graceMs,
	                       boolean registerNew, boolean deleteFiles) {
	}

	private void processUser(int userId, Options options, Result result) {
		result.users.incrementAndGet();
		CloudUser user = usersRepository.findById(userId);
		if (user == null) {
			return;
		}
		UserFilesResults res = userdataService.generateFiles(userId, null, true, false, null);
		long proExpiry = userSubService.latestProExpiry(userId);
		boolean inactive = isFreeAndInactive(userId, user, res, proExpiry);

		Optional<Notice> existing;
		synchronized (dbLock) {
			existing = noticeRepository.find(userId);
		}

		if (existing.isEmpty()) {
			if (!inactive) {
				return;
			}
			boolean hadPro = proExpiry > 0;
			if (hadPro && !options.includeHadPro()) {
				return;
			}
			if (!hadPro && !options.includeNeverPro()) {
				return;
			}
			String category = hadPro ? InactiveUserNoticeRepository.CATEGORY_HAD_PRO
					: InactiveUserNoticeRepository.CATEGORY_NEVER_PRO;
			result.newUsers.add(category, userId);
			if (options.registerNew()) {
				synchronized (dbLock) {
					noticeRepository.insertNotified(userId, user.email, category);
				}
			}
			return;
		}

		Notice notice = existing.get();
		if (!inactive) {
			result.reactivated.add(notice.category(), userId);
			if (options.prod()) {
				synchronized (dbLock) {
					noticeRepository.delete(userId);
				}
			}
			return;
		}
		if (InactiveUserNoticeRepository.STATUS_DELETED.equals(notice.status())) {
			return; // files already deleted, nothing more to do
		}
		long notifiedMs = notice.notifiedTime() == null ? 0
				: notice.notifiedTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
		if (System.currentTimeMillis() - notifiedMs < options.graceMs()) {
			result.stillInactive.add(notice.category(), userId);
			return;
		}
		result.eligibleForDelete.add(notice.category(), userId);
		if (!options.deleteFiles()) {
			return;
		}
		try {
			for (UserFileNoData f : res.uniqueFiles) {
				filesRepository.findById(f.id).ifPresent(this::deleteCompletely);
			}
			synchronized (dbLock) {
				noticeRepository.markDeleted(userId);
			}
			result.deleted.add(notice.category(), userId);
		} catch (Exception e) {
			Map<String, Object> fail = new LinkedHashMap<>();
			fail.put("userid", userId);
			fail.put("error", String.valueOf(e.getMessage()));
			result.failedUsers.add(fail);
		}
	}

	private boolean isFreeAndInactive(int userId, CloudUser user, UserFilesResults res, long proExpiry) {
		long now = System.currentTimeMillis();
		return freeOver2Years(user, now, proExpiry)
				&& now - lastDeviceSync(userId) > TWO_YEARS_MS
				&& now - lastFileActivity(res) > YEAR_MS;
	}

	private boolean freeOver2Years(CloudUser user, long now, long proExpiry) {
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

	private long lastDeviceSync(int userId) {
		long last = 0;
		for (CloudUserDevice d : devicesRepository.findByUserid(userId)) {
			if (d.udpatetime != null) {
				last = Math.max(last, d.udpatetime.getTime());
			}
		}
		return last;
	}

	@Override
	protected boolean fix(UserFile file, Params params) {
		throw new UnsupportedOperationException("notify-inactive-users scans in run()");
	}

	// user ids split by category, output as comma-separated lists ready for send-email
	private static final class UserLists {
		final List<String> hadPro = Collections.synchronizedList(new ArrayList<>());
		final List<String> neverPro = Collections.synchronizedList(new ArrayList<>());

		void add(String category, int userId) {
			(InactiveUserNoticeRepository.CATEGORY_HAD_PRO.equals(category) ? hadPro : neverPro).add(String.valueOf(userId));
		}

		int size() {
			return hadPro.size() + neverPro.size();
		}

		void putTo(Map<String, Object> r, String name) {
			r.put(name + "Count", size());
			r.put(name + "HadPro", String.join(",", new ArrayList<>(hadPro)));
			r.put(name + "NeverPro", String.join(",", new ArrayList<>(neverPro)));
		}
	}

	private static final class Result {
		final boolean testRun;
		final UserLists newUsers = new UserLists();
		final UserLists stillInactive = new UserLists();
		final UserLists reactivated = new UserLists();
		final UserLists eligibleForDelete = new UserLists();
		final UserLists deleted = new UserLists();
		final List<Map<String, Object>> failedUsers = Collections.synchronizedList(new ArrayList<>());
		final AtomicInteger users = new AtomicInteger();

		Result(boolean testRun) {
			this.testRun = testRun;
		}

		Map<String, Object> toResult() {
			Map<String, Object> r = new LinkedHashMap<>();
			r.put("users", users.get());
			r.put("testRun", testRun);
			r.put("failed", failedUsers.size());
			newUsers.putTo(r, "new");
			stillInactive.putTo(r, "stillInactive");
			reactivated.putTo(r, "reactivated");
			eligibleForDelete.putTo(r, "eligibleForDelete");
			deleted.putTo(r, "deleted");
			r.put("failedUsers", new ArrayList<>(failedUsers));
			return r;
		}
	}
}
