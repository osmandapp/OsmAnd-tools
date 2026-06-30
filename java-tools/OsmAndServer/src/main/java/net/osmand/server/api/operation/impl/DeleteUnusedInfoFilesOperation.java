package net.osmand.server.api.operation.impl;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;

/**
 * Deletes *.info files whose paired *.gpx (same name and type) no longer exists (latest version missing or deleted).
 */
@Component
@AdminOperation(name = "delete-unused-info-files")
public class DeleteUnusedInfoFilesOperation extends AbstractFileFixOperation {

	private static final Logger LOG = LoggerFactory.getLogger(DeleteUnusedInfoFilesOperation.class);
	private static final String INFO_EXT = ".info";
	private static final String GPX_INFO_EXT = ".gpx.info";

	public DeleteUnusedInfoFilesOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
	                                      UserdataService userdataService, StorageService storageService) {
		super(usersRepository, filesRepository, userdataService, storageService);
	}

	@Override
	public Set<String> supportedTypes() {
		return Set.of(UserdataService.FILE_TYPE_GPX);
	}

	@Override
	protected boolean accepts(String name) {
		return name != null && name.endsWith(GPX_INFO_EXT);
	}

	@Override
	protected boolean fix(UserFile file, Params params) {
		String gpxName = file.name.substring(0, file.name.length() - INFO_EXT.length());
		UserFile gpx = userdataService.getLastFileVersion(file.userid, gpxName, file.type);
		if (gpx != null && gpx.filesize >= 0) {
			return false; // paired gpx still exists
		}
		if (!isTest(params)) {
			LOG.info("Deleting unused info file: userid={}, name={}, type={}", file.userid, file.name, file.type);
			try {
				userdataService.deleteFileVersion(null, file.userid, file.name, file.type, file);
			} catch (Exception e) {
				throw new IllegalStateException(
						"Failed to delete unused info file userid=" + file.userid + " name=" + file.name, e);
			}
		}
		return true;
	}
}
