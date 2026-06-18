package net.osmand.server.api.operation.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import net.osmand.server.api.operation.AdminOperation;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.api.services.UserdataService;

/**
 * Deletes *.info files whose content cannot be read as JSON (e.g. a gpx was written into the info file by mistake).
 */
@Component
@AdminOperation(name = "delete-broken-info-files")
public class DeleteBrokenInfoFilesOperation extends AbstractFileFixOperation {

	private static final Logger LOG = LoggerFactory.getLogger(DeleteBrokenInfoFilesOperation.class);
	private static final Gson GSON_WITH_NANS = new GsonBuilder().serializeSpecialFloatingPointValues().create();
	private static final String INFO_EXT = ".info";

	private final CloudUserDevicesRepository devicesRepository;

	public DeleteBrokenInfoFilesOperation(CloudUsersRepository usersRepository, CloudUserFilesRepository filesRepository,
										  UserdataService userdataService, StorageService storageService,
										  CloudUserDevicesRepository devicesRepository) {
		super(usersRepository, filesRepository, userdataService, storageService);
		this.devicesRepository = devicesRepository;
	}

	@Override
	public Set<String> supportedTypes() {
		return Set.of(UserdataService.FILE_TYPE_GPX);
	}

	@Override
	protected boolean accepts(String name) {
		return name != null && name.endsWith(INFO_EXT);
	}

	@Override
	protected byte[] processFile(UserFile file, boolean testRun) throws IOException {
		fix(file, testRun);
		return null;
	}

	@Override
	protected ObjectNode fix(UserFile file, boolean testRun) throws IOException {
		if (isReadableJson(read(file))) {
			return null; // info file is valid json -> keep
		}
		if (!testRun) {
			CloudUserDevice dev = devicesRepository.findById(file.deviceid);
			if (dev == null) {
				throw new IllegalStateException("no device for file");
			}
			LOG.info("Deleting broken info file: userid={}, name={}", file.userid, file.name);
			try {
				userdataService.deleteFile(file.name, file.type, null, null, dev);
			} catch (Exception e) {
				throw new IllegalStateException(
						"Failed to delete broken info file userid=" + file.userid + " name=" + file.name, e);
			}
		}
		return null;
	}

	private static boolean isReadableJson(byte[] content) {
		try {
			GSON_WITH_NANS.fromJson(new String(content, StandardCharsets.UTF_8), JsonElement.class);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
}
