package net.osmand.server.controllers.user;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.ShareFileRepository;
import net.osmand.server.api.services.GpxService;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.api.services.ShareFileService;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import jakarta.transaction.Transactional;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static net.osmand.server.api.services.ShareFileService.PRIVATE_SHARE_TYPE;
import static net.osmand.server.api.services.UserdataService.FILE_NOT_FOUND;
import static net.osmand.server.api.services.UserdataService.FILE_WAS_DELETED;


@Controller
@RequestMapping({"/share"})
public class ShareFileController {

	@Autowired
	UserdataService userdataService;

	@Autowired
	ShareFileService shareFileService;

	@Autowired
	protected OsmAndMapsService osmAndMapsService;

	@Autowired
	protected GpxService gpxService;

	Gson gson = new Gson();
	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();

	protected static final Log LOGGER = LogFactory.getLog(ShareFileController.class);

	public record FileDownloadResult(InputStream inputStream, String fileName, String contentType) {
	}

	@PostMapping(path = {"/generate-link"}, produces = "application/json")
	public ResponseEntity<String> generateLink(@RequestParam String fileName,
	                                           @RequestParam String fileType,
	                                           @RequestParam Boolean publicAccess) {
		CloudUserDevicesRepository.CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		CloudUserFilesRepository.UserFile userFile = userdataService.getUserFile(fileName, fileType, null, dev);
		if (userFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		UUID uuid = shareFileService.generateSharedCode(userFile, publicAccess);
		if (uuid == null) {
			return ResponseEntity.badRequest().body("Error generating link");
		}
		return ResponseEntity.ok(gson.toJson(Map.of("uuid", uuid.toString())));
	}

	@GetMapping(path = {"/get/{uuid}"}, produces = "application/json")
	@Transactional
	public ResponseEntity<?> getFile(@PathVariable String uuid) throws IOException {
		ShareFileRepository.ShareFile shareFile = shareFileService.getShareFileByUuid(uuid);
		if (shareFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		ResponseEntity<String> errorAccess = shareFileService.checkAccessAndReturnError(shareFile);
		if (errorAccess != null) {
			return errorAccess;
		}
		CloudUserFilesRepository.UserFile userFile = shareFileService.getUserFile(shareFile);
		FileDownloadResult fileResult = shareFileService.downloadFile(userFile);
		if (fileResult == null) {
			return ResponseEntity.badRequest().body("Error downloading file");
		}
		return ResponseEntity.ok()
				.header("Content-Disposition", "attachment; filename=" + fileResult.fileName)
				.contentType(org.springframework.http.MediaType.valueOf(fileResult.contentType))
				.body(new InputStreamResource(fileResult.inputStream));
	}

	@GetMapping(path = {"/join/{uuid}"}, produces = "application/json")
	@Transactional
	public ResponseEntity<?> getGpx(@PathVariable String uuid) throws IOException {
		ShareFileRepository.ShareFile shareFile = shareFileService.getShareFileByUuid(uuid);
		if (shareFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		CloudUserFilesRepository.UserFile userFile = shareFileService.getUserFile(shareFile);
		if (userFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		} else if (userFile.filesize == -1) {
			return ResponseEntity.ok().body(FILE_WAS_DELETED);
		}
		ResponseEntity<String> errorAccess = shareFileService.checkAccessAndReturnError(shareFile);
		if (errorAccess != null) {
			return errorAccess;
		}
		if (shareFile.publicAccess) {
			CloudUserDevicesRepository.CloudUserDevice dev = osmAndMapsService.checkUser();
			if (dev != null && dev.userid != shareFile.ownerid) {
				boolean hasAccess = shareFileService.hasUserAccessToSharedFile(shareFile, dev.userid);
				if (!hasAccess) {
					boolean created = shareFileService.createPublicReadAccess(shareFile, dev);
					if (!created) {
						return ResponseEntity.badRequest().body("Error creating public access");
					}
				}
			}
		}
		GpxFile gpxFile = shareFileService.getFile(userFile);
		if (gpxFile != null && gpxFile.getError() == null) {
			GpxTrackAnalysis analysis = gpxFile.getAnalysis(0);
			WebGpxParser.TrackData gpxData = gpxService.buildTrackDataFromGpxFile(gpxFile, analysis);
			if (gpxData != null) {
				return ResponseEntity.ok(gsonWithNans.toJson(Map.of(
						"gpx_data", gpxData,
						"name", userFile.name,
						"type", userFile.type
				)));
			}
		}
		if (gpxFile == null) {
			LOGGER.error("Error getting gpx data: gpxFile is null");
		} else {
			LOGGER.error("Error getting gpx data: " + gpxFile.getError());
		}
		return ResponseEntity.badRequest().body("Error getting gpx data");
	}

	@GetMapping(path = {"/request-access"}, produces = "application/json")
	@Transactional
	public ResponseEntity<?> requestAccess(@RequestParam String uuid, @RequestParam String nickname) {
		CloudUserDevicesRepository.CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		ShareFileRepository.ShareFile shareFile = shareFileService.getShareFileByUuid(uuid);
		if (shareFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		if (dev.userid == shareFile.ownerid) {
			return ResponseEntity.badRequest().body("You are the owner of this file");
		}
		shareFileService.requestAccess(shareFile, dev, nickname);
		return ResponseEntity.ok("Access requested");
	}

	@GetMapping(path = {"/get-share-file-info"}, produces = "application/json")
	public ResponseEntity<String> getFileInfo(@RequestParam String fileName,
	                                          @RequestParam String fileType,
	                                          @RequestParam boolean createIfNotExists) {
		CloudUserDevicesRepository.CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		ShareFileRepository.ShareFile shareFile = shareFileService.getFileByOwnerAndFilepath(dev.userid, fileName);
		if (shareFile == null) {
			if (createIfNotExists) {
				CloudUserFilesRepository.UserFile userFile = userdataService.getUserFile(fileName, fileType, null, dev);
				if (userFile == null) {
					return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
				}
				shareFile = shareFileService.createShareFile(userFile, false, null);
			} else {
				return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
			}
		}
		CloudUsersRepository.CloudUser user = userdataService.getUserById(shareFile.ownerid);
		if (user == null) {
			return ResponseEntity.badRequest().body("Error getting user info");
		}
		String ownerName = user.nickname != null ? user.nickname : user.email;
		ShareFileRepository.ShareFileDTO shareFileDto = new ShareFileRepository.ShareFileDTO(shareFile, true);
		return ResponseEntity.ok(gson.toJson(Map.of("owner", ownerName, "file", shareFileDto)));
	}

	@GetMapping(path = {"/edit-access-list"}, produces = "application/json")
	public ResponseEntity<String> editWhitelist(@RequestBody Map<Integer, String> accessMap,
	                                            @RequestParam String fileName) {
		CloudUserDevicesRepository.CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		ShareFileRepository.ShareFile shareFile = shareFileService.getFileByOwnerAndFilepath(dev.userid, fileName);
		if (shareFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		boolean success = shareFileService.editAccessList(shareFile, accessMap);
		if (!success) {
			return ResponseEntity.badRequest().body("Error editing access list");
		}
		return ResponseEntity.ok("Access list edited");
	}

	@PostMapping(path = {"/update-requests"}, produces = "application/json")
	public ResponseEntity<String> updateRequests(@RequestBody Map<Integer, String> requests,
	                                             @RequestParam long fileId) {
		CloudUserDevicesRepository.CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		ShareFileRepository.ShareFile shareFile = shareFileService.getFileById(fileId);
		if (shareFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		if (shareFile.ownerid != dev.userid) {
			return ResponseEntity.badRequest().body("You are not the owner of this file");
		}
		boolean success = shareFileService.updateRequests(requests);
		if (!success) {
			return ResponseEntity.badRequest().body("Error updating requests");
		}
		shareFile = shareFileService.getFileById(fileId);
		ShareFileRepository.ShareFileDTO shareFileDto = new ShareFileRepository.ShareFileDTO(shareFile, true);

		return ResponseEntity.ok(gson.toJson(shareFileDto));
	}

	@GetMapping(path = {"/change-share-type"}, produces = "application/json")
	public ResponseEntity<String> changeShareType(@RequestParam String filePath,
	                                              @RequestParam String fileType,
	                                              @RequestParam String shareType,
	                                              @RequestParam boolean createIfNotExists) {
		CloudUserDevicesRepository.CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		ShareFileRepository.ShareFile shareFile = shareFileService.getFileByOwnerAndFilepath(dev.userid, filePath);
		if (shareFile == null) {
			if (createIfNotExists) {
				CloudUserFilesRepository.UserFile userFile = userdataService.getUserFile(filePath, fileType, null, dev);
				if (userFile == null) {
					return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
				}
				shareFile = shareFileService.createShareFile(userFile, false, null);
			} else {
				return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
			}
		}
		boolean success = shareFileService.changeFileShareType(shareFile, shareType);
		if (!success) {
			return ResponseEntity.badRequest().body("Error changing share type");
		}
		if (shareType.equals(PRIVATE_SHARE_TYPE)) {
			return ResponseEntity.ok(FILE_WAS_DELETED);
		}
		shareFile = shareFileService.getFileByOwnerAndFilepath(dev.userid, filePath);
		ShareFileRepository.ShareFileDTO shareFileDto = new ShareFileRepository.ShareFileDTO(shareFile, !shareFile.publicAccess);

		return ResponseEntity.ok(gson.toJson(shareFileDto));
	}

	@GetMapping(path = {"/get-shared-with-me"}, produces = "application/json")
	public ResponseEntity<String> getSharedWithMe(@RequestParam String type) {
		CloudUserDevicesRepository.CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		UserdataController.UserFilesResults files = shareFileService.getSharedWithMe(dev.userid, type);
		return ResponseEntity.ok(gson.toJson(files));
	}

	@PostMapping(path = {"/remove-shared-with-me-file"}, produces = "application/json")
	public ResponseEntity<String> removeSharedWithMeFile(@RequestBody String name,
	                                                     @RequestParam String type) {
		CloudUserDevicesRepository.CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		boolean removed = shareFileService.removeSharedWithMeFile(name, type, dev);
		if (!removed) {
			return ResponseEntity.badRequest().body("Error removing file");
		}
		return ResponseEntity.ok("File removed");
	}

	@PostMapping(path = {"/save-shared-file"}, produces = "application/json")
	public ResponseEntity<String> saveSharedFile(@RequestBody List<String> names,
	                                             @RequestParam String type) throws IOException {
		CloudUserDevicesRepository.CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		if (names.size() != 2) {
			return ResponseEntity.badRequest().body("Error saving file");
		}
		return shareFileService.saveSharedFile(names.get(0), type, names.get(1), dev);
	}

}
