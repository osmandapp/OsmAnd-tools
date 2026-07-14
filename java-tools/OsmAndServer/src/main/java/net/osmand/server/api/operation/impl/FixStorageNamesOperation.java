package net.osmand.server.api.operation.impl;

import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.util.Algorithms;

/** Migrates files with too-long names to a bounded storagename (test run only lists them). */
@Component
@AdminOperation(name = "fix-storage-names")
public class FixStorageNamesOperation extends AbstractFileFixOperation {

	public FixStorageNamesOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
									UserdataService userdataService, StorageService storageService) {
		super(usersRepository, filesRepository, userdataService, storageService);
	}

	@Override
	protected boolean fix(UserFile file, Params params) {
		boolean found = false;
		for (UserFile v : filesRepository.findAllByUseridAndNameAndTypeOrderByUpdatetimeDesc(file.userid, file.name, file.type)) {
			if (needsMigration(v)) {
				if (!isTest(params)) {
					userdataService.migrateStorageName(v);
				}
				found = true;
			}
		}
		return found;
	}

	private boolean needsMigration(UserFile v) {
		return Algorithms.isEmpty(v.storagename) && v.filesize != null && v.filesize >= 0
				&& userdataService.computeStorageName(v.type, v.name, v.updatetime) != null;
	}
}
