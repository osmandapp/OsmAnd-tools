package net.osmand.server.controllers.user;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.repo.ShareFileRepository;
import net.osmand.server.api.services.GpxService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.api.services.ShareFileService;
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

import javax.transaction.Transactional;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static net.osmand.server.api.services.UserdataService.FILE_NOT_FOUND;


@Controller
@RequestMapping({"/share"})
public class ShareFileController {

	@Autowired
	UserdataService userdataService;

	@Autowired
	ShareFileService shareFileService;

	@Autowired
	protected GpxService gpxService;

	Gson gson = new Gson();
	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();

	protected static final Log LOGGER = LogFactory.getLog(ShareFileController.class);

	private static final String ERROR_GETTING_SHARED_INFO = "Error getting shared info";

	public record FileDownloadResult(InputStream inputStream, String fileName, String contentType) {
	}

	@PostMapping(path = {"/generate-link"}, produces = "application/json")
	public ResponseEntity<String> generateLink(@RequestParam String fileName,
	                                           @RequestParam String fileType,
	                                           @RequestParam String type,
	                                           @RequestParam(required = false) String groupActions) {
		PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		PremiumUserFilesRepository.UserFile userFile = userdataService.getUserFile(fileName, fileType, null, dev);
		if (userFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		String code = shareFileService.generateSharedCode(userFile, type, groupActions);
		if (code == null) {
			return ResponseEntity.badRequest().body("Error generating link");
		}
		String name = shareFileService.getNamePartForCode(fileName);
		String url = "/share/get?code=" + code + "-" + name;

		return ResponseEntity.ok(gson.toJson(Map.of("url", url)));
	}

	@PostMapping(path = {"/get"}, produces = "application/json")
	@Transactional
	public ResponseEntity<?> getFile(@RequestParam String code) throws IOException {
		ShareFileRepository.ShareFile shareFile = shareFileService.getShareFileByCode(code);
		if (shareFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		PremiumUserFilesRepository.UserFile userFile = shareFileService.getUserFile(shareFile);
		if (userFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		ShareFileService.FileSharedInfo info = shareFileService.getSharedInfo(shareFile.getInfo());
		if (info == null) {
			return ResponseEntity.badRequest().body(ERROR_GETTING_SHARED_INFO);
		}
		boolean hasAccess = shareFileService.checkAccess(info);
		if (!hasAccess) {
			return ResponseEntity.badRequest().body("You don't have access to this file");
		}
		PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
		if (dev != null) {
			shareFileService.storeUserAccess(dev, shareFile, info);
		}
		FileDownloadResult fileResult = shareFileService.downloadFile(userFile);
		if (fileResult == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		return ResponseEntity.ok()
				.header("Content-Disposition", "attachment; filename=" + fileResult.fileName)
				.contentType(org.springframework.http.MediaType.valueOf(fileResult.contentType))
				.body(new InputStreamResource(fileResult.inputStream));
	}

	@PostMapping(path = {"/get-gpx"}, produces = "application/json")
	@Transactional
	public ResponseEntity<?> getGpx(@RequestParam String code) throws IOException {
		ShareFileRepository.ShareFile shareFile = shareFileService.getShareFileByCode(code);
		if (shareFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		PremiumUserFilesRepository.UserFile userFile = shareFileService.getUserFile(shareFile);
		if (userFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		ShareFileService.FileSharedInfo info = shareFileService.getSharedInfo(shareFile.getInfo());
		if (info == null) {
			return ResponseEntity.badRequest().body(ERROR_GETTING_SHARED_INFO);
		}
		boolean hasAccess = shareFileService.checkAccess(info);
		if (!hasAccess) {
			return ResponseEntity.badRequest().body("File is private");
		}
		PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
		if (dev != null) {
			shareFileService.storeUserAccess(dev, shareFile, info);
		}
		GpxFile gpxFile = shareFileService.getFile(userFile);
		if (gpxFile.getError() == null) {
			GpxTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
			WebGpxParser.TrackData gpxData = gpxService.getTrackDataByGpxFile(gpxFile, null, analysis);
			if (gpxData != null) {
				return ResponseEntity.ok(gsonWithNans.toJson(Map.of("gpx_data", gpxData)));
			}
		}
		LOGGER.error("Error getting gpx data: " + gpxFile.getError());
		return ResponseEntity.badRequest().body("Error getting gpx data");
	}

	@PostMapping(path = {"/edit-blacklist"}, produces = "application/json")
	public ResponseEntity<String> editBlacklist(@RequestBody List<String> list,
	                                            @RequestParam String name,
	                                            @RequestParam String type) {
		PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		if (!shareFileService.editBlacklist(dev, name, type, list)) {
			return ResponseEntity.badRequest().body("Error editing blacklist");
		}
		return ResponseEntity.ok("Blacklist edited");
	}

	@GetMapping(path = {"/edit-whitelist"}, produces = "application/json")
	public ResponseEntity<String> editWhitelist(@RequestBody List<String> list,
	                                            @RequestParam String name,
	                                            @RequestParam String type) {
		PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		PremiumUserFilesRepository.UserFile userFile = userdataService.getUserFile(name, type, null, dev);
		if (userFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		if (!shareFileService.editWhitelist(dev, name, type, list)) {
			return ResponseEntity.badRequest().body("Error editing whitelist");
		}
		return ResponseEntity.ok("Whitelist edited");
	}

	@GetMapping(path = {"/get-shared-info"}, produces = "application/json")
	public ResponseEntity<String> getSharedInfo(@RequestParam String name,
	                                            @RequestParam String type) {
		PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		ShareFileService.FileSharedInfo info = shareFileService.getSharedInfo(dev, name, type);
		if (info == null) {
			return ResponseEntity.badRequest().body(ERROR_GETTING_SHARED_INFO);
		}
		return ResponseEntity.ok(gson.toJson(info));
	}

	@GetMapping(path = {"/check-access"}, produces = "application/json")
	public ResponseEntity<String> checkAccess(@RequestParam String code) {
		PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		ShareFileService.FileSharedInfo info = shareFileService.getSharedInfo(code);
		if (info == null) {
			return ResponseEntity.badRequest().body(ERROR_GETTING_SHARED_INFO);
		}
		ShareFileService.FileSharedInfo.SharingType type = info.getSharingType();
		Set<ShareFileService.FileSharedInfo.GroupAction> groupActions = info.getGroupActions();

		return ResponseEntity.ok(gson.toJson(Map.of("hasAccess", shareFileService.checkAccess(info), "type", type, "groupActions", groupActions)));
	}

}
