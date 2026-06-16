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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
			boolean testRun,         // true = only count what would be fixed, no write
			LocalDate updatedAfter,  // null = no date filter; else only files updated on/after this date
			int maxUsers,            // empty/0 = no limit
			int maxFiles             // empty/0 = no limit
	) {}

	protected abstract byte[] processFile(UserFile file) throws IOException;

	protected abstract ObjectNode fix(UserFile file) throws IOException;

	@Override
	public Object run(Params params, OperationContext ctx) {
		List<Integer> userIds = resolveUsers(params);
		int total = userIds.size();
		Stats stats = new Stats(params.testRun());
		int doneUsers = 0;
		for (Integer userId : userIds) {
			if (ctx.isCancelled()) {
				break;
			}
			processUser(userId, params, stats);
			doneUsers++;
			ctx.setProgress(doneUsers, total, String.format("%d/%d users · %d fixed", doneUsers, total, stats.fixed));
		}
		return stats.toResult();
	}

	private void processUser(int userId, Params params, Stats stats) {
		stats.users++;
		boolean userFixed = false;
		Long afterMs = params.updatedAfter() == null ? null
				: params.updatedAfter().atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
		UserFilesResults res = userdataService.generateFiles(userId, null, false, false, Collections.emptySet());
		for (UserFileNoData fileNoData : res.uniqueFiles) {
			if (afterMs != null && fileNoData.updatetimems < afterMs) {
				continue;
			}
			if (params.maxFiles() > 0 && stats.scanned >= params.maxFiles()) {
				break;
			}
			stats.scanned++;
			try {
				UserFile file = filesRepository.findById(fileNoData.id).orElse(null);
				if (file == null) {
					stats.skipped++;
					continue;
				}
				byte[] fixed = processFile(file);
				if (fixed == null) {
					stats.skipped++;
				} else {
					if (!params.testRun()) {
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
			if (params.maxUsers() > 0 && ids.size() >= params.maxUsers()) {
				break;
			}
		}
		return ids;
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
