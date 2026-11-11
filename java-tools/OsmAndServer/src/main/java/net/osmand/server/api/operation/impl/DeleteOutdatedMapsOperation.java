package net.osmand.server.api.operation.impl;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.CloudMapFilesService;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;

/** Deletes uploaded copies of maps removed from the catalog. */
@Component
@AdminOperation(name = "delete-outdated-maps")
public class DeleteOutdatedMapsOperation extends AbstractFileFixOperation {

	private static final Logger LOG = LoggerFactory.getLogger(DeleteOutdatedMapsOperation.class);
	private static final long MAX_LINK_ZIP_SIZE = 1000;

	private final CloudMapFilesService cloudMapFilesService;

	public DeleteOutdatedMapsOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
	                                   UserdataService userdataService, StorageService storageService,
	                                   CloudMapFilesService cloudMapFilesService) {
		super(usersRepository, filesRepository, userdataService, storageService);
		this.cloudMapFilesService = cloudMapFilesService;
	}

	@Override
	public Set<String> supportedTypes() {
		return Set.of(UserdataService.FILE_TYPE);
	}

	@Override
	protected boolean accepts(String name) {
		return name != null && cloudMapFilesService.isDeletedMapFile(name, UserdataService.FILE_TYPE);
	}

	@Override
	protected List<UserFileNoData> listFiles(int userId, Params params) {
		return filesRepository.listFilesByUserid(userId, null, UserdataService.FILE_TYPE);
	}

	@Override
	protected boolean fix(UserFile file, Params params) {
		if (!hasUploadedContent(file)) {
			return false;
		}
		if (!isTest(params)) {
			LOG.info("Deleting outdated map version: userid={}, id={}, name={}", file.userid, file.id, file.name);
			userdataService.deleteFileVersion(null, file.userid, file.name, file.type, file);
		}
		return true;
	}

	private static boolean hasUploadedContent(UserFile file) {
		return file.filesize != null && file.filesize > 0
				&& file.zipfilesize != null && file.zipfilesize > MAX_LINK_ZIP_SIZE;
	}
}
