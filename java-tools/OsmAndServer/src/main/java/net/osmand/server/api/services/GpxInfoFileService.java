package net.osmand.server.api.services;

import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;

import static net.osmand.server.api.repo.CloudUserDevicesRepository.*;
import static net.osmand.server.api.services.StorageService.*;
import static net.osmand.server.api.services.UserdataService.*;

@Service
public class GpxInfoFileService {

	@Autowired
	UserdataService userdataService;

	/**
	 * Updates or creates a GPX info file
	 * Updating the entire gpx.info file is efficient for most cases.
	 * For large info files with many waypoint groups, using a diff may be the better approach.
	 */
	@Transactional
	public ResponseEntity<String> updateGpxInfoFile(MultipartFile file, String name, CloudUserDevice dev,
	                                                Long updatetime) throws IOException {
		InternalZipFile zipfile = InternalZipFile.buildFromMultipartFile(file);
		ResponseEntity<String> res = userdataService.uploadFile(zipfile, dev, name, FILE_TYPE_GPX, System.currentTimeMillis());
		//if updatetime is null, this is a new info file; upload it only
		if (updatetime != null) { 
			userdataService.deleteFileVersion(updatetime, dev.userid, name, FILE_TYPE_GPX, null);
		}
		return res;
	}
}
