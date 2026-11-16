package net.osmand.server.api.operation.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

/** Renames files whose name is not in NFC, all versions at once; when both forms exist, drops deletions newer than the last live version (test run only lists them). */
@Component
@AdminOperation(name = "fix-nfc-names")
public class FixNfcNamesOperation extends AbstractFileFixOperation {

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
			bothForms = Boolean.TRUE.equals(transaction.execute(status -> renameToNfc(file, newName)));
		}
		if (bothForms) {
			filesWithBothForms.add(file.id);
		}
		return true;
	}

	private boolean renameToNfc(UserFile file, String newName) {
		List<UserFile> versions = getVersions(file, file.name);
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
