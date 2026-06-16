package net.osmand.server.api.operation.impl;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.operation.OperationContext;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;

/**
 * Deletes *.info files whose paired *.gpx (same name and type) no longer exists (latest version missing or deleted).
 */
@Component
@AdminOperation(name = "delete-info-files")
public class DeleteUnusedInfoFilesOperation extends AbstractFileFixOperation {

	private static final String INFO_EXT = ".info";
	private static final String GPX_INFO_EXT = ".gpx.info";

	private final CloudUserDevicesRepository devicesRepository;
	private volatile boolean testRun;

	public DeleteUnusedInfoFilesOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
	                                      UserdataService userdataService, StorageService storageService,
	                                      CloudUserDevicesRepository devicesRepository) {
		super(usersRepository, filesRepository, userdataService, storageService);
		this.devicesRepository = devicesRepository;
	}

	@Override
	public Object run(Params params, OperationContext ctx) {
		testRun = params.testRun();
		return super.run(params, ctx);
	}

	@Override
	protected byte[] processFile(UserFile file) {
		fix(file);
		return null; // deletion happens in fix(); there is no content to write back
	}

	@Override
	protected ObjectNode fix(UserFile file) {
		if (file.name == null || !file.name.endsWith(GPX_INFO_EXT)) {
			return null;
		}
		String gpxName = file.name.substring(0, file.name.length() - INFO_EXT.length());
		UserFile gpx = userdataService.getLastFileVersion(file.userid, gpxName, file.type);
		if (gpx != null && gpx.filesize >= 0) {
			return null; // paired gpx still exists
		}
		if (!testRun) {
			CloudUserDevice dev = devicesRepository.findById(file.deviceid);
			if (dev == null) {
				throw new IllegalStateException("no device for file");
			}
			userdataService.deleteFile(file.name, file.type, null, null, dev);
		}
		return null;
	}
}
