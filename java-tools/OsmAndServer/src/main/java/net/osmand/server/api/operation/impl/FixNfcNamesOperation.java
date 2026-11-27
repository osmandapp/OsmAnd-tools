package net.osmand.server.api.operation.impl;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipException;

import jakarta.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.ShareFileService;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.shared.util.KUnicode;

/** Renames Cloud file names to NFC. */
@Component
@AdminOperation(name = "fix-nfc-names")
public class FixNfcNamesOperation extends AbstractFileFixOperation {

	private static final Logger LOG = LoggerFactory.getLogger(FixNfcNamesOperation.class);

	private final ShareFileService shareFileService;
	private final TransactionTemplate transaction;
	private final Set<Long> filesWithBothForms = ConcurrentHashMap.newKeySet();

	public FixNfcNamesOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
								UserdataService userdataService, StorageService storageService,
								ShareFileService shareFileService, PlatformTransactionManager transactionManager) {
		super(usersRepository, filesRepository, userdataService, storageService);
		this.shareFileService = shareFileService;
		this.transaction = new TransactionTemplate(transactionManager);
	}

	@Override
	protected boolean accepts(String name) {
		return name != null && !name.equals(KUnicode.INSTANCE.normalizeNFC(name));
	}

	@Override
	protected List<UserFileNoData> listFiles(int userId, Params params) {
		List<UserFileNoData> result = new ArrayList<>();
		Set<String> names = new HashSet<>();
		for (UserFileNoData file : userdataService.generateFiles(userId, null, true, false, typesOf(params)).allFiles) {
			if (names.add(UserdataService.getFileId(file))) {
				result.add(file);
			}
		}
		return result;
	}

	@Override
	protected boolean fix(UserFile file, Params params) {
		String newName = KUnicode.INSTANCE.normalizeNFC(file.name);
		boolean bothForms;
		if (isTest(params)) {
			bothForms = filesRepository.existsByUseridAndNameAndType(file.userid, newName, file.type);
		} else {
			Map<UserFile, byte[]> oldContent = new HashMap<>();
			try {
				bothForms = Boolean.TRUE.equals(transaction.execute(status -> {
					try {
						return renameToNfc(file, newName, oldContent);
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				}));
			} catch (RuntimeException e) {
				restoreContent(oldContent);
				throw e;
			}
		}
		if (bothForms) {
			filesWithBothForms.add(file.id);
		}
		return true;
	}

	private void saveNfcInfoContent(List<UserFile> versions, Map<UserFile, byte[]> oldContent) throws IOException {
		for (int i = versions.size() - 1; i >= 0; i--) {
			UserFile v = versions.get(i);
			if (!UserdataService.hasContent(v)) {
				continue;
			}
			byte[] content = readInfoContent(v);
			byte[] nfcContent = null;
			try {
				nfcContent = content == null ? null : UserdataService.getNfcInfoContent(content);
			} catch (IOException e) {
				logKeptInfoContent(v, e.getMessage());
			}
			if (nfcContent != null) {
				oldContent.put(v, content);
				save(v, nfcContent);
			}
		}
	}

	@Nullable
	private byte[] readInfoContent(UserFile v) throws IOException {
		if (v.filesize > UserdataService.MAX_INFO_CONTENT_SIZE) {
			logKeptInfoContent(v, "over " + UserdataService.MAX_INFO_CONTENT_SIZE + " bytes");
			return null;
		}
		byte[] content;
		try {
			content = readIfExists(v);
		} catch (ZipException | EOFException e) {
			logKeptInfoContent(v, e.getMessage());
			return null;
		}
		if (content == null) {
			logKeptInfoContent(v, "no content");
		}
		return content;
	}

	private void logKeptInfoContent(UserFile v, String reason) {
		LOG.warn("Keeping .info content as is: id={}, userid={}, name={}: {}", v.id, v.userid, v.name, reason);
	}

	private void restoreContent(Map<UserFile, byte[]> oldContent) {
		for (Map.Entry<UserFile, byte[]> entry : oldContent.entrySet()) {
			UserFile v = entry.getKey();
			try {
				save(v, entry.getValue());
			} catch (IOException | RuntimeException e) {
				LOG.warn("Failed to restore .info content: id={}, userid={}, name={}: {}", v.id, v.userid, v.name, e.getMessage());
			}
		}
	}

	private boolean renameToNfc(UserFile file, String newName, Map<UserFile, byte[]> oldContent) throws IOException {
		filesRepository.lockFileNames(file.userid);
		List<UserFile> versions = getVersions(file, file.name);
		boolean infoFile = file.name.endsWith(UserdataService.INFO_EXT);
		saveNfcInfoContent(infoFile ? versions : getVersions(file, newName + UserdataService.INFO_EXT), oldContent);
		List<UserFile> nfcVersions = getVersions(file, newName);
		List<UserFile> staleDeletions = getStaleDeletions(versions, nfcVersions);
		filesRepository.deleteAll(staleDeletions);
		for (UserFile v : versions) {
			if (!staleDeletions.contains(v)) {
				userdataService.renameFileVersion(v, newName);
			}
		}
		shareFileService.renameShareFile(file.name, newName, file.userid);
		return !nfcVersions.isEmpty();
	}

	private List<UserFile> getVersions(UserFile file, String name) {
		return filesRepository.findAllByUseridAndNameAndTypeOrderByUpdatetimeDesc(file.userid, name, file.type);
	}

	private List<UserFile> getStaleDeletions(List<UserFile> versions, List<UserFile> nfcVersions) {
		if (versions.isEmpty() || nfcVersions.isEmpty()) {
			return List.of();
		}
		boolean deletedInBothForms = !UserdataService.hasContent(versions.get(0))
				&& !UserdataService.hasContent(nfcVersions.get(0));
		if (deletedInBothForms) {
			return List.of();
		}
		List<UserFile> allVersions = new ArrayList<>(versions);
		allVersions.addAll(nfcVersions);
		allVersions.sort(Comparator.comparing((UserFile v) -> v.updatetime).reversed());
		List<UserFile> staleDeletions = new ArrayList<>();
		for (UserFile v : allVersions) {
			if (UserdataService.hasContent(v)) {
				break;
			}
			staleDeletions.add(v);
		}
		return staleDeletions;
	}

	@Override
	protected String tag(UserFile file) {
		return filesWithBothForms.remove(file.id) ? file.type + " both forms" : file.type;
	}
}
