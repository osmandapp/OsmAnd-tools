package net.osmand.server.controllers.user;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.services.GpxService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.api.services.ShareGpxService;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
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
	ShareGpxService shareGpxService;

	@Autowired
	protected GpxService gpxService;

	Gson gson = new Gson();
	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();

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
		String code = shareGpxService.generateSharedCode(userFile, type, groupActions);

		return ResponseEntity.ok(gson.toJson(Map.of("url", "/share/get?code=" + code)));
	}

	@PostMapping(path = {"/get"}, produces = "application/json")
	@Transactional
	public ResponseEntity<?> getFile(@RequestParam String code) throws IOException {
		PremiumUserFilesRepository.UserFile userFile = shareGpxService.getUserFileBySharedUrl(code);
		if (userFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		ShareGpxService.FileSharedInfo info = shareGpxService.getSharedInfo(userFile.id);
		if (info == null) {
			return ResponseEntity.badRequest().body(ERROR_GETTING_SHARED_INFO);
		}
		boolean hasAccess = shareGpxService.checkAccess(info);
		if (!hasAccess) {
			return ResponseEntity.badRequest().body("You don't have access to this file");
		}
		PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
		if (dev != null) {
			shareGpxService.storeUserAccess(dev, userFile, info);
		}
		FileDownloadResult fileResult = shareGpxService.downloadFile(userFile);
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
		PremiumUserFilesRepository.UserFile userFile = shareGpxService.getUserFileBySharedUrl(code);
		if (userFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		ShareGpxService.FileSharedInfo info = shareGpxService.getSharedInfo(userFile.id);
		if (info == null) {
			return ResponseEntity.badRequest().body(ERROR_GETTING_SHARED_INFO);
		}
		boolean hasAccess = shareGpxService.checkAccess(info);
		if (!hasAccess) {
			return ResponseEntity.badRequest().body("File is private");
		}
		PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
		if (dev != null) {
			shareGpxService.storeUserAccess(dev, userFile, info);
		}
		GpxFile gpxFile = shareGpxService.getFile(userFile);
		if (gpxFile.getError() == null) {
			GpxTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
			WebGpxParser.TrackData gpxData = gpxService.getTrackDataByGpxFile(gpxFile, null, analysis);
			if (gpxData != null) {
				return ResponseEntity.ok(gsonWithNans.toJson(Map.of("gpx_data", gpxData)));
			}
		}
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
		PremiumUserFilesRepository.UserFile userFile = userdataService.getUserFile(name, type, null, dev);
		if (userFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		boolean created = shareGpxService.editBlacklist(userFile, list);
		if (!created) {
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
		boolean created = shareGpxService.editWhitelist(userFile, list);
		if (!created) {
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
		PremiumUserFilesRepository.UserFile userFile = userdataService.getUserFile(name, type, null, dev);
		if (userFile == null) {
			return ResponseEntity.badRequest().body(FILE_NOT_FOUND);
		}
		ShareGpxService.FileSharedInfo info = shareGpxService.getSharedInfo(userFile.id);
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
		ShareGpxService.FileSharedInfo info = shareGpxService.getSharedInfo(code);
		if (info == null) {
			return ResponseEntity.badRequest().body(ERROR_GETTING_SHARED_INFO);
		}
		ShareGpxService.FileSharedInfo.SharingType type = info.getSharingType();
		Set<ShareGpxService.FileSharedInfo.GroupAction> groupActions = info.getGroupActions();

		return ResponseEntity.ok(gson.toJson(Map.of("hasAccess", shareGpxService.checkAccess(info), "type", type, "groupActions", groupActions)));
	}

}
