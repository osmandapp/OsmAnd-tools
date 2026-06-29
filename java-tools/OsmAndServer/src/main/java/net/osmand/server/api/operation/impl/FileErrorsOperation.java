package net.osmand.server.api.operation.impl;

import com.google.gson.JsonElement;

import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.api.services.WebUserdataService;
import net.osmand.server.api.services.WebUserdataService.FileError;

/**
 * Finds files whose read failed (error stored in user_files.gendetails 'error')
 */
@Component
@AdminOperation(name = "file-errors")
public class FileErrorsOperation extends AbstractFileFixOperation {

	public FileErrorsOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
							   UserdataService userdataService, StorageService storageService) {
		super(usersRepository, filesRepository, userdataService, storageService);
	}

	@Override
	protected boolean fix(UserFile file, boolean testRun) {
		// for now only detect files with a stored read error; the actual fix will be added later
		return errorType(file) != null;
	}

	@Override
	protected String tag(UserFile file) {
		return errorType(file);
	}

	private static String errorType(UserFile file) {
		if (file.details == null) {
			return null;
		}
		JsonElement error = file.details.get(WebUserdataService.ERROR_DETAILS);
		if (error == null || error.isJsonNull()) {
			return null;
		}
		String text = error.getAsString();
		for (FileError type : FileError.values()) {
			if (text.contains(type.id)) {
				return type.id;
			}
		}
		return null;
	}
}
