package net.osmand.server.api.services;

import jakarta.transaction.Transactional;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;

import static net.osmand.server.api.repo.CloudUserFilesRepository.*;
import static net.osmand.server.api.services.UserdataService.*;

@Service
public class GpxInfoFileService {

	@Autowired
	UserdataService userdataService;

	// Updating the entire gpx.info file is efficient for most cases.
	// For large info files with many waypoint groups, using a diff may be the better approach.

	@Transactional
	public ResponseEntity<String> updateGpxInfoFile(MultipartFile file, String name, CloudUserDevicesRepository.CloudUserDevice dev,
	                                                Long updatetime) throws IOException {
		UserFile lastFileVersion = userdataService.getUserFile(name, UserdataService.FILE_TYPE_GPX, updatetime, dev);
		StorageService.InternalZipFile zipfile = StorageService.InternalZipFile.buildFromMultipartFile(file);
		ResponseEntity<String> res = userdataService.uploadFile(zipfile, dev, name, FILE_TYPE_GPX, System.currentTimeMillis());
		if (lastFileVersion != null && lastFileVersion.filesize != -1) {
			userdataService.deleteFileVersion(null, dev.userid, name, FILE_TYPE_GPX, lastFileVersion);
		}
		return res;
	}
}
