package net.osmand.server.api.operation.impl;

import java.util.List;

import com.google.gson.JsonElement;

import org.springframework.stereotype.Component;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.api.services.WebUserdataService;
import net.osmand.server.api.services.WebUserdataService.FileError;
import net.osmand.server.api.services.WebUserdataService.UserFileUpdate;

/**
 * Finds files whose read failed (error stored in user_files.gendetails 'error')
 */
@Component
@AdminOperation(name = "file-errors")
public class FileErrorsOperation extends AbstractFileFixOperation {

	private final WebUserdataService webUserdataService;
	private final CloudUserDevicesRepository devicesRepository;

	public FileErrorsOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
							   UserdataService userdataService, StorageService storageService,
							   WebUserdataService webUserdataService, CloudUserDevicesRepository devicesRepository) {
		super(usersRepository, filesRepository, userdataService, storageService);
		this.webUserdataService = webUserdataService;
		this.devicesRepository = devicesRepository;
	}

	@Override
	protected boolean fix(UserFile file, Params params) {
		if (errorType(file) == null) {
			return false;
		}
		if (isTest(params)) {
			return true;
		}
		if (Boolean.TRUE.equals(params.reanalyze())) {
			reanalyze(file);
			return errorType(file) != null; // keep only files that still fail after re-analysis
		}
		deleteCompletely(file);
		return true;
	}

	private void reanalyze(UserFile file) {
		CloudUserDevice dev = devicesRepository.findById(file.deviceid);
		if (dev == null) {
			throw new IllegalStateException("no device for file id=" + file.id);
		}
		webUserdataService.refreshListFiles(List.of(new UserFileUpdate(file.name, file.type, false, null)), dev);
		file.details = filesRepository.findById(file.id).map(f -> f.details).orElse(null);
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
