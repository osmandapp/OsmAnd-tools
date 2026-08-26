package net.osmand.server.api.operation.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.AmazonS3Exception;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;

/**
 * Deletes DB files whose object is missing from S3
 */
@Component
@AdminOperation(name = "delete-missing-in-cloud")
public class DeleteFilesMissingInCloudOperation extends AbstractFileFixOperation {

	private static final Logger LOG = LoggerFactory.getLogger(DeleteFilesMissingInCloudOperation.class);
	private static final String LOCAL_STORAGE = "local";
	private static final int HTTP_NOT_FOUND = 404;

	public DeleteFilesMissingInCloudOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
	                                           UserdataService userdataService, StorageService storageService) {
		super(usersRepository, filesRepository, userdataService, storageService);
	}

	@Override
	protected boolean fix(UserFile file, Params params) throws IOException {
		if (existsInCloud(file)) {
			return false; // object present (or not an S3 file) -> keep
		}
		if (!isTest(params)) {
			LOG.info("Deleting (missing in cloud): userid={}, name={}, type={}", file.userid, file.name, file.type);
			deleteCompletely(file);
		}
		return true; // missing in cloud -> counted/recorded (deleted only when !testRun)
	}

	private boolean existsInCloud(UserFile file) {
		if (file.data != null) {
			return true; // stored in the DB blob, not in S3
		}
		if (file.storage == null || file.storage.isBlank() || LOCAL_STORAGE.equals(file.storage)) {
			return true; // not an S3-backed file -> never delete on this rule
		}
		try {
			return userdataService.fileExists(file);
		} catch (AmazonS3Exception e) {
			if (e.getStatusCode() == HTTP_NOT_FOUND) {
				return false;
			}
			// any other S3 error (timeout, 5xx, access denied) -> do not treat as missing
			throw new IllegalStateException("S3 existence check failed for id=" + file.id + " name=" + file.name, e);
		}
	}
}
