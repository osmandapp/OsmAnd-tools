package net.osmand.server.api.operation.impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.node.ObjectNode;

import net.osmand.server.api.operation.Operation;
import net.osmand.server.api.operation.OperationContext;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.StorageService.InternalZipFile;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;

public abstract class AbstractFileFixOperation implements Operation<AbstractFileFixOperation.Params> {

	private static final String TMP_PREFIX = "op-fix-";
	private static final String TMP_SUFFIX = ".tmp";

	protected final CloudUsersRepository usersRepository;
	protected final CloudUserFilesRepository filesRepository;
	protected final UserdataService userdataService;
	protected final StorageService storageService;

	protected AbstractFileFixOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
									   UserdataService userdataService, StorageService storageService) {
		this.usersRepository = usersRepository;
		this.filesRepository = filesRepository;
		this.userdataService = userdataService;
		this.storageService = storageService;
	}

	public record Params(
			Integer userId,         // null/empty = all users; a number = single user (for testing)
			Boolean testRun,         // null/true = only count, no write (safe default); false = actually write
			LocalDate updatedAfter,  // null = no date filter; else only files updated on/after this date
			Set<String> fileTypes,   // null/empty = all types; otherwise only these (e.g. GPX, FAVOURITES)
			Integer usersPercent,    // null/100 = all users; otherwise that % of all users
			Integer filesPercent     // null/100 = all files; otherwise that % of each user's files
	) {}

	protected abstract byte[] processFile(UserFile file, boolean testRun) throws IOException;

	protected abstract ObjectNode fix(UserFile file, boolean testRun) throws IOException;

	private static boolean isTest(Params params) {
		return params.testRun() == null || params.testRun();
	}

	@Override
	public Object run(Params params, OperationContext ctx) {
		List<Integer> userIds = resolveUsers(params);
		int fileCap = fileCap(userIds, params);
		int total = userIds.size();
		Stats stats = new Stats(isTest(params));
		int doneUsers = 0;
		for (Integer userId : userIds) {
			if (ctx.isCancelled() || stats.scanned >= fileCap) {
				break;
			}
			processUser(userId, params, stats, fileCap);
			doneUsers++;
			ctx.setProgress(doneUsers, total, String.format("%d/%d users · %d fixed", doneUsers, total, stats.fixed));
		}
		return stats.toResult();
	}

	private void processUser(int userId, Params params, Stats stats, int fileCap) {
		stats.users++;
		boolean userFixed = false;
		Long afterMs = params.updatedAfter() == null ? null
				: params.updatedAfter().atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
		UserFilesResults res = userdataService.generateFiles(userId, null, false, false, typesOf(params));
		for (UserFileNoData fileNoData : res.uniqueFiles) {
			if (afterMs != null && fileNoData.updatetimems < afterMs) {
				continue;
			}
			if (stats.scanned >= fileCap) {
				break;
			}
			stats.scanned++;
			try {
				UserFile file = filesRepository.findById(fileNoData.id).orElse(null);
				if (file == null) {
					stats.skipped++;
					continue;
				}
				byte[] fixed = processFile(file, isTest(params));
				if (fixed == null) {
					stats.skipped++;
				} else {
					if (!isTest(params)) {
						save(file, fixed);
					}
					stats.fixed++;
					userFixed = true;
					stats.fixedFiles.add(Map.of("userid", userId, "file", file.name));
				}
			} catch (Exception e) {
				stats.failed++;
				stats.failedFiles.add(Map.of("userid", userId, "file", fileNoData.name, "error", String.valueOf(e.getMessage())));
			}
		}
		if (userFixed) {
			stats.usersFixed++;
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

	private List<Integer> resolveUsers(Params params) {
		if (params.userId() != null) {
			return List.of(params.userId());
		}
		List<Integer> ids = new ArrayList<>();
		for (CloudUser user : usersRepository.findAll()) {
			ids.add(user.id);
		}
		int max = limit(ids.size(), params.usersPercent());
		return ids.subList(0, Math.min(max, ids.size()));
	}

	private static int limit(int total, Integer percent) {
		if (percent == null || percent >= 100) {
			return total;
		}
		return (int) Math.ceil(total * Math.max(0, percent) / 100.0);
	}

	private int fileCap(List<Integer> userIds, Params params) {
		if (params.filesPercent() == null || params.filesPercent() >= 100) {
			return Integer.MAX_VALUE;
		}
		int total = 0;
		for (int userId : userIds) {
			total += userdataService.generateFiles(userId, null, false, false, typesOf(params)).uniqueFiles.size();
		}
		return limit(total, params.filesPercent());
	}

	public Set<String> supportedTypes() {
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
		int fixed;
		int scanned;
		int skipped;
		int failed;
		int users;
		int usersFixed;
		final List<Map<String, Object>> fixedFiles = new ArrayList<>();
		final List<Map<String, Object>> failedFiles = new ArrayList<>();

		Stats(boolean testRun) {
			this.testRun = testRun;
		}

		Map<String, Object> toResult() {
			Map<String, Object> r = new LinkedHashMap<>();
			r.put("fixed", fixed);
			r.put("scanned", scanned);
			r.put("skipped", skipped);
			r.put("failed", failed);
			r.put("users", users);
			r.put("usersFixed", usersFixed);
			r.put("testRun", testRun);
			r.put("fixedFiles", fixedFiles);
			r.put("failedFiles", failedFiles);
			return r;
		}
	}
}
