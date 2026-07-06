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
import net.osmand.util.Algorithms;

/**
 * Two mailing lists of free-inactive users (had-pro / never-pro) for a support warning mail. State is kept across
 * runs: a user still inactive after the grace period (default 30 days) gets their files deleted instead of re-mailed;
 * a user active again before that is dropped. Free-inactive = free 2+ years, no device sync 2+ years, no file activity 1+ year.
 */
@Component
@AdminOperation(name = "notify-inactive-users")
public class NotifyInactiveUsersOperation extends AbstractFileFixOperation implements ExtraParamsOperation {

	private static final long YEAR_MS = 365L * 86400000L;
	private static final long TWO_YEARS_MS = 2 * YEAR_MS;
	private static final long DAY_MS = 86400000L;
	private static final int DEFAULT_GRACE_DAYS = 30;

	// graceDays: wait before deleting a still-inactive user (null = 30)
	public record Extra(Integer graceDays, Boolean hadPro, Boolean neverPro) {
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

		Extra extra = ctx.getExtraParams() instanceof Extra e ? e : new Extra(null, null, null);
		boolean includeHadPro = extra.hadPro() == null || extra.hadPro();
		boolean includeNeverPro = extra.neverPro() == null || extra.neverPro();
		long graceMs = (extra.graceDays() == null ? DEFAULT_GRACE_DAYS : Math.max(0, extra.graceDays())) * DAY_MS;

		List<Integer> userIds = sampleUsers(params, ctx);
		int total = userIds.size();
		AtomicInteger done = new AtomicInteger();
		forEach(clampThreads(params.threads()), userIds, ctx, userId -> {
			processUser(userId, includeHadPro, includeNeverPro, graceMs, params, result);
			int d = done.incrementAndGet();
			ctx.setProgress(d, total, String.format("%d/%d users · %d to mail · %d deleted",
					d, total, result.notifiedHadPro.size() + result.notifiedNeverPro.size(), result.deleted.size()));
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

	private void processUser(int userId, boolean includeHadPro, boolean includeNeverPro, long graceMs,
	                         Params params, Result result) {
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
			if (hadPro && !includeHadPro) {
				return;
			}
			if (!hadPro && !includeNeverPro) {
				return;
			}
			String category = hadPro ? InactiveUserNoticeRepository.CATEGORY_HAD_PRO
					: InactiveUserNoticeRepository.CATEGORY_NEVER_PRO;
			String username = username(user);
			(hadPro ? result.notifiedHadPro : result.notifiedNeverPro).add(username);
			if (!isTest(params)) {
				synchronized (dbLock) {
					noticeRepository.insertNotified(userId, user.email, category);
				}
			}
			return;
		}

		Notice notice = existing.get();
		if (!inactive) {
			result.reactivated.incrementAndGet();
			if (!isTest(params)) {
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
		if (System.currentTimeMillis() - notifiedMs < graceMs) {
			result.pending.incrementAndGet();
			return;
		}
		// still inactive after grace period: delete files, do not mail again
		Map<String, Object> entry = new LinkedHashMap<>();
		entry.put("userid", userId);
		entry.put("email", user.email);
		entry.put("category", notice.category());
		result.deleted.add(entry);
		if (!isTest(params)) {
			try {
				for (UserFileNoData f : res.uniqueFiles) {
					filesRepository.findById(f.id).ifPresent(this::deleteCompletely);
				}
				synchronized (dbLock) {
					noticeRepository.markDeleted(userId);
				}
			} catch (Exception e) {
				result.failed.incrementAndGet();
				Map<String, Object> fail = new LinkedHashMap<>();
				fail.put("userid", userId);
				fail.put("error", String.valueOf(e.getMessage()));
				result.failedUsers.add(fail);
			}
		}
	}

	private boolean isFreeAndInactive(int userId, CloudUser user, UserFilesResults res, long proExpiry) {
		long now = System.currentTimeMillis();
		return Algorithms.isEmpty(user.orderid)
				&& freeOver2Years(user, now, proExpiry)
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

	private static String username(CloudUser user) {
		return Algorithms.isEmpty(user.email) ? "id:" + user.id : user.email;
	}

	@Override
	protected boolean fix(UserFile file, Params params) {
		throw new UnsupportedOperationException("notify-inactive-users scans in run()");
	}

	private static final class Result {
		final boolean testRun;
		final List<String> notifiedHadPro = Collections.synchronizedList(new ArrayList<>());
		final List<String> notifiedNeverPro = Collections.synchronizedList(new ArrayList<>());
		final List<Map<String, Object>> deleted = Collections.synchronizedList(new ArrayList<>());
		final List<Map<String, Object>> failedUsers = Collections.synchronizedList(new ArrayList<>());
		final AtomicInteger users = new AtomicInteger();
		final AtomicInteger reactivated = new AtomicInteger();
		final AtomicInteger pending = new AtomicInteger();
		final AtomicInteger failed = new AtomicInteger();

		Result(boolean testRun) {
			this.testRun = testRun;
		}

		Map<String, Object> toResult() {
			Map<String, Object> r = new LinkedHashMap<>();
			r.put("toMail", notifiedHadPro.size() + notifiedNeverPro.size());
			r.put("deletedCount", deleted.size());
			r.put("hadProCount", notifiedHadPro.size());
			r.put("neverProCount", notifiedNeverPro.size());
			r.put("reactivated", reactivated.get());
			r.put("pending", pending.get());
			r.put("failed", failed.get());
			r.put("users", users.get());
			r.put("testRun", testRun);
			// arrays -> UI renders each as a downloadable list (mailHadPro.txt / mailNeverPro.txt), one username per line
			r.put("mailHadPro", new ArrayList<>(notifiedHadPro));
			r.put("mailNeverPro", new ArrayList<>(notifiedNeverPro));
			r.put("deleted", new ArrayList<>(deleted));
			r.put("failedUsers", new ArrayList<>(failedUsers));
			return r;
		}
	}
}
