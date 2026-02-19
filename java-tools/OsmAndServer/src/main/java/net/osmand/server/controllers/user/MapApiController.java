package net.osmand.server.controllers.user;

import java.io.*;

import net.osmand.server.api.repo.*;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import okio.Buffer;

import static net.osmand.server.api.services.WebUserdataService.*;
import static net.osmand.server.api.services.UserdataService.*;
import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.GZIPInputStream;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import net.osmand.map.OsmandRegions;
import net.osmand.server.api.services.*;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.util.Algorithms;
import okio.Source;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import net.osmand.server.WebSecurityConfiguration.OsmAndProUser;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;
import org.xmlpull.v1.XmlPullParserException;

@RestController
@RequestMapping("/mapapi")
public class MapApiController {

	protected static final Log LOG = LogFactory.getLog(MapApiController.class);

	private static final String INFO_KEY = "info";

	@Autowired
	AuthenticationManager authManager;

	@Autowired
	WebUserdataService webUserdataService;

	@Autowired
	CloudUsersRepository usersRepository;

	@Autowired
	UserdataService userdataService;

	@Autowired
	TrackAnalyzerService trackAnalyzerService;

	@Autowired
	ShareFileService shareFileService;

	@Autowired
	protected GpxService gpxService;

	@Autowired
	OsmAndMapsService osmAndMapsService;

	@Autowired
	private EmailSenderService emailSender;

	@Autowired
	private UserSubscriptionService userSubService;

	@Autowired
	protected DeviceSubscriptionsRepository subscriptionsRepo;

	@Autowired
	DeviceInAppPurchasesRepository deviceInAppPurchasesRepository;

	OsmandRegions osmandRegions;

	Gson gson = new Gson();

	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();

	public static class UserPasswordPost {
		// security alert: don’t add fields to this class
		public String username;
		public String password;
		public String token;
		public String lang;
	}

	@GetMapping(path = { "/auth/loginForm" }, produces = "text/html;charset=UTF-8")
	public AbstractResource loginForm() {
		return new ClassPathResource("/test-map-pro-login.html");
	}

	@GetMapping(path = { "/auth/info" }, produces = "application/json")
	public String userInfo(java.security.Principal user) {
		if (user == null) {
			return gson.toJson(user);
		}
		if (user instanceof Authentication) {
			Object obj = ((Authentication) user).getPrincipal();
			if (obj instanceof OsmAndProUser) {
				OsmAndProUser pu = (OsmAndProUser) ((Authentication) user).getPrincipal();
				List<String> roles = pu.getAuthorities().stream()
						.map(GrantedAuthority::getAuthority)
						.toList();
				Map<String, Object> result = new HashMap<>();
				result.put("username", pu.getUsername());
				result.put("roles", roles);
				return gson.toJson(result);
			}
			return gson.toJson(obj);
		}
		return gson.toJson(null);
	}

	private ResponseEntity<String> okStatus() {
		return ResponseEntity.ok(gson.toJson(Collections.singletonMap("status", "ok")));
	}
	
	@PostMapping(path = {"/auth/login"}, consumes = "application/json", produces = "application/json")
	public ResponseEntity<String> loginUser(@RequestBody UserPasswordPost credentials,
	                                        HttpServletRequest request,
	                                        java.security.Principal user) throws ServletException {
		final String EMAIL_ERROR = "error_email";
		final String PASSWORD_ERROR = "error_password";
		
		if (user != null) {
			request.logout();
		}
		String username = credentials.username;
		String password = credentials.password;
		if (username == null || password == null) {
			return ResponseEntity.badRequest().body("Username and password are required");
		}
		
		ResponseEntity<String> response = userdataService.checkUserEmail(username);
		if (response.getStatusCodeValue() != 200) {
			return ResponseEntity.badRequest().body(EMAIL_ERROR);
		}
		
		UsernamePasswordAuthenticationToken pwt = new UsernamePasswordAuthenticationToken(username, password);
		try {
			authManager.authenticate(pwt);
		} catch (AuthenticationException e) {
			return ResponseEntity.badRequest().body(PASSWORD_ERROR);
		}
		request.login(username, password);

		CloudUserDevice dev = osmAndMapsService.checkUser();
		userdataService.updateDeviceLangInfo(dev, credentials.lang, BRAND_DEVICE_WEB, MODEL_DEVICE_WEB);

		return okStatus();
	}

	@PostMapping(path = {"/auth/delete-account"})
	public ResponseEntity<String> deleteAccount(@RequestParam String token, HttpServletRequest request)
			throws ServletException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		return userdataService.deleteAccount(token, dev, request);
	}

	@PostMapping(path = { "/auth/activate" }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<String> activateMapUser(@RequestBody UserPasswordPost credentials) {
		String username = credentials.username;
		String password = credentials.password;
		String token = credentials.token;
		if (username == null || password == null) {
			return ResponseEntity.badRequest().body("Username and password are required");
		}
		
		ResponseEntity<String> validRes = userdataService.validateToken(username, token);
		if (validRes.getStatusCodeValue() != 200) {
			return validRes;
		}
		
		ResponseEntity<String> res = userdataService.webUserActivate(username, token, password, credentials.lang);
		if (res.getStatusCodeValue() < 300) {
			return okStatus();
		}
		return res;
	}

	@PostMapping(path = { "/auth/logout" }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<String> logoutMapUser(HttpServletRequest request) throws ServletException {
		request.logout();
		return okStatus();
	}
	
	@PostMapping(path = {"/auth/register"}, consumes = "application/json", produces = "application/json")
	public ResponseEntity<String> registerMapUser(@RequestBody UserPasswordPost credentials,
	                                              @RequestParam String lang,
	                                              @RequestParam boolean isNew,
	                                              HttpServletRequest request) {
		String username = credentials.username;
		if (username == null) {
			return ResponseEntity.badRequest().body("Username is required");
		}
		return userdataService.webUserRegister(username, lang, isNew, request);
	}
	
	@PostMapping(path = {"/auth/validate-token"}, consumes = "application/json", produces = "application/json")
	public ResponseEntity<String> registerMapUser(@RequestBody UserPasswordPost credentials) {
		String username = credentials.username;
		String token = credentials.token;
		if (username == null) {
			return ResponseEntity.badRequest().body("Username and token are required");
		}
		if (token == null) {
			return ResponseEntity.badRequest().body("Token are required");
		}
		return userdataService.validateToken(username, token);
	}

	@PostMapping(value = "/upload-file", consumes = MULTIPART_FORM_DATA_VALUE)
	public ResponseEntity<String> uploadFile(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
	                                     @RequestParam String name, @RequestParam String type) throws IOException {
		// This could be slow series of checks (token, user, subscription, amount of space):
		// probably it's better to support multiple file upload without these checks
		CloudUserDevice dev = osmAndMapsService.checkUser();

		if (dev == null || name.contains("/../")) {
			return userdataService.tokenNotValidResponse();
		}
		userdataService.uploadMultipartFile(file, dev, name, type, System.currentTimeMillis());

		return okStatus();
	}

	@PostMapping(value = "/delete-file")
	public ResponseEntity<String> deleteFile(@RequestParam String name, @RequestParam String type) {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		userdataService.deleteFile(name, type, null, null, dev);
		return userdataService.ok();
	}

	@PostMapping(value = "/delete-file-version")
	public ResponseEntity<String> deleteFile(@RequestParam String name,
	                                         @RequestParam String type,
	                                         @RequestParam Long updatetime) {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		} else {
			return userdataService.deleteFileVersion(updatetime, dev.userid, name, type, null);
		}
	}
	
	@GetMapping(value = "/delete-file-all-versions")
	public ResponseEntity<String> deleteFileAllVersions(@RequestParam String name,
	                                         @RequestParam String type, @RequestParam Long updatetime, @RequestParam boolean isTrash) {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		} else {
			return userdataService.deleteFileAllVersions(dev.userid, name, type, updatetime, isTrash);
		}
	}

	@GetMapping(value = "/rename-file")
	public ResponseEntity<String> renameFile(@RequestParam String oldName,
	                                         @RequestParam String newName,
	                                         @RequestParam String type,
	                                         @RequestParam boolean saveCopy) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		if (!oldName.equals(newName)) {
			return webUserdataService.renameFile(oldName, newName, type, dev, saveCopy);
		}
		return ResponseEntity.badRequest().body("Old track name and new track name are the same!");
	}

	@GetMapping(value = "/rename-folder")
	public ResponseEntity<String> renameFolder(@RequestParam String folderName,
	                                           @RequestParam String type,
	                                           @RequestParam String newFolderName) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		return userdataService.renameFolder(folderName, newFolderName, type, dev);
	}

	@GetMapping(value = "/delete-folder")
	public ResponseEntity<String> deleteFolder(@RequestParam String folderName,
	                                           @RequestParam String type) {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		return userdataService.deleteFolder(folderName, type, dev);
	}

	@GetMapping(value = "/list-files")
	public ResponseEntity<String> listFiles(@RequestParam(required = false) String name,
	                                        @RequestParam(required = false) String type,
	                                        @RequestParam(required = false, defaultValue = "false") boolean addDevices,
	                                        @RequestParam(required = false, defaultValue = "false") boolean allVersions) {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		Set<String> types = userdataService.parseFileTypes(type);
		UserFilesResults res = userdataService.generateFiles(dev.userid, name, allVersions, true, types);
		Map<String, Set<String>> sharedFilesMap = shareFileService.getFilesByOwner(dev.userid);

		res.uniqueFiles.forEach(nd -> {
			String ext = nd.name.substring(nd.name.lastIndexOf('.') + 1);
			boolean isGpx = "gpx".equalsIgnoreCase(ext);

			boolean isGPZTrack = nd.type.equalsIgnoreCase("gpx") && isGpx;
			boolean isFavorite = nd.type.equals(FILE_TYPE_FAVOURITES) && isGpx;

			if (isGPZTrack) {
				JsonObject details = nd.details != null ? nd.details : new JsonObject();
				if (!webUserdataService.detailsPresent(details)) {
					details.add(UPDATE_DETAILS, gson.toJsonTree(nd.updatetimems));
				}
				nd.details = details;
			}

			if (isGPZTrack || isFavorite) {
				boolean isSharedFile = webUserdataService.isShared(nd, sharedFilesMap);
				nd.details.add(SHARE, gson.toJsonTree(isSharedFile));
			}
		});

		if (addDevices && res.allFiles != null) {
			Map<Integer, String> devices = new HashMap<>();
			for (UserFileNoData nd : res.allFiles) {
				webUserdataService.addDeviceInformation(nd, devices);
			}
		}
		return ResponseEntity.ok(gson.toJson(res));
	}

	@GetMapping(value = "/update-smartfolders")
	public ResponseEntity<String> updateSmartFolders() {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		List<UserdataService.SmartFolderWeb> res = userdataService.updateWebSmartFolders(dev.userid);
		return ResponseEntity.ok(gson.toJson(res));
	}

	@PostMapping(value = "/refresh-list-files")
	public ResponseEntity<String> refreshListFiles(@RequestBody List<UserFileUpdate> files) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		return webUserdataService.refreshListFiles(files, dev);
	}

	@GetMapping(value = "/download-file")
	public void getFile(HttpServletResponse response, HttpServletRequest request,
	                    @RequestParam String name,
	                    @RequestParam String type,
	                    @RequestParam(required = false) Boolean simplified,
	                    @RequestParam(required = false) Long updatetime,
	                    @RequestParam(required = false) Boolean shared) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			ResponseEntity<String> error = userdataService.tokenNotValidResponse();
			response.setStatus(error.getStatusCodeValue());
			response.getWriter().write(Objects.requireNonNull(error.getBody()));
			return;
		}
		CloudUserFilesRepository.UserFile userFile;
		if (shared != null && shared) {
			userFile = shareFileService.getSharedWithMeFile(name, type, dev);
		} else {
			userFile = userdataService.getUserFile(name, type, updatetime, dev);
		}
		if (userFile != null) {
			boolean isSimplified = simplified != null && simplified;
			userdataService.getFile(userFile, response, request, name, type, dev, isSimplified);
		}
	}

	@GetMapping(value = "/download-file-from-prev-version")
	public void getFilePrevVersion(HttpServletResponse response, HttpServletRequest request,
	                               @RequestParam String name,
	                               @RequestParam String type,
	                               @RequestParam Long updatetime) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			ResponseEntity<String> error = userdataService.tokenNotValidResponse();
			response.setStatus(error.getStatusCodeValue());
			response.getWriter().write(Objects.requireNonNull(error.getBody()));
			return;
		}
		CloudUserFilesRepository.UserFile userFile = userdataService.getFilePrevVersion(name, type, updatetime, dev);
		if (userFile != null) {
			userdataService.getFile(userFile, response, request, name, type, dev, false);
		}
	}
	
	@GetMapping(value = "/restore-file")
	public ResponseEntity<String> restoreFile(@RequestParam String name, @RequestParam String type, @RequestParam Long updatetime) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		return userdataService.restoreFile(name, type, updatetime, dev);
	}
	
	public static class FileData {
		public String name;
		public String type;
		public Long updatetime;
	}
	
	@PostMapping(value = "/empty-trash")
	public ResponseEntity<String> emptyTrash(@RequestBody List<FileData> files) {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		return userdataService.emptyTrash(files, dev);
	}

	@GetMapping(value = "/get-gpx-info")
	public ResponseEntity<String> getGpxInfo(@RequestParam(name = "name") String name,
	                                         @RequestParam(name = "type") String type,
	                                         @RequestParam(name = "updatetime", required = false) Long updatetime) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		InputStream in = null;
		try {
			UserFile userFile = userdataService.getUserFile(name, type, updatetime, dev);
			if (webUserdataService.analysisPresent(ANALYSIS, userFile)) {
				return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, userFile.details.get(ANALYSIS))));
			}
			in = userdataService.getInputStream(dev, userFile);
			GpxFile gpxFile;
			in = new GZIPInputStream(in);
			try (Source source = new Buffer().readFrom(in)) {
				gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
			} catch (IOException e) {
				return ResponseEntity.badRequest().body(String.format("Error reading file %s", userFile.name));
			}
			if (gpxFile.getError() != null) {
				return ResponseEntity.badRequest().body(String.format("File %s not found", userFile.name));
			}
			GpxTrackAnalysis analysis = webUserdataService.getAnalysis(userFile, gpxFile);
			if (!webUserdataService.analysisPresent(ANALYSIS, userFile)) {
				webUserdataService.saveAnalysis(ANALYSIS, userFile, analysis);
			}
			return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, analysis)));
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	@GetMapping(path = {"/get-srtm-gpx-info"}, produces = "application/json")
	public ResponseEntity<String> getSrtmGpx(@RequestParam(name = "name") String name,
	                                         @RequestParam(name = "type") String type,
	                                         @RequestParam(name = "updatetime", required = false) Long updatetime) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		InputStream in = null;
		try {
			UserFile userFile = userdataService.getUserFile(name, type, updatetime, dev);
			if (webUserdataService.analysisPresent(SRTM_ANALYSIS, userFile)) {
				return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, userFile.details.get(SRTM_ANALYSIS))));
			}
			in = userdataService.getInputStream(dev, userFile);
			GpxFile gpxFile;
			in = new GZIPInputStream(in);
			try (Source source = new Buffer().readFrom(in)) {
				gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
			} catch (IOException e) {
				return ResponseEntity.badRequest().body(String.format("Error reading file %s", userFile.name));
			}
			if (gpxFile.getError() != null) {
				return ResponseEntity.badRequest().body(String.format("File %s not found", userFile.name));
			}
			GpxFile srtmGpx = gpxService.calculateSrtmAltitude(gpxFile, null);
			GpxTrackAnalysis analysis = srtmGpx == null ? null : webUserdataService.getAnalysis(userFile, srtmGpx);
			if (!webUserdataService.analysisPresent(SRTM_ANALYSIS, userFile)) {
				webUserdataService.saveAnalysis(SRTM_ANALYSIS, userFile, analysis);
			}
			return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, analysis)));
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	@PostMapping(value = "/download-backup")
	public void createBackup(HttpServletResponse response,
	                         @RequestParam(name = "updatetime", required = false) boolean includeDeleted,
	                         @RequestParam String format,
	                         @RequestBody List<String> data) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			ResponseEntity<String> error = userdataService.tokenNotValidResponse();
			response.setStatus(error.getStatusCodeValue());
			if (error.getBody() != null) {
				response.getWriter().write(error.getBody());
			}
			return;
		}
		userdataService.getBackup(response, dev, Set.copyOf(data), includeDeleted, format);
	}

	@PostMapping(value = "/download-backup-folder")
	public void createBackupFolder(@RequestParam String format,
	                               @RequestParam String type,
	                               @RequestParam(required = false) String folderName,
								   @RequestParam(required = false) Boolean shared,
	                               HttpServletResponse response) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			ResponseEntity<String> error = userdataService.tokenNotValidResponse();
			response.setStatus(error.getStatusCodeValue());
			if (error.getBody() != null) {
				response.getWriter().write(error.getBody());
			}
			return;
		}
		if (folderName != null) {
			userdataService.getBackupFolder(response, dev, folderName, format, type, null);
		} else if (shared != null && shared) {
			List<CloudUserFilesRepository.UserFile> files = shareFileService.getOriginalSharedWithMeFiles(dev, type);
			userdataService.getBackupFolder(response, dev, null, format, type, files);
		}
	}

	@GetMapping(path = { "/check_download" }, produces = "text/html;charset=UTF-8")
	public ResponseEntity<String> checkDownload(@RequestParam(value = "file_name", required = false) String fn,
			@RequestParam(value = "file_size", required = false) String sz) throws IOException {
		return okStatus();
	}

	@RequestMapping(path = {"/download-obf"})
	public ResponseEntity<Resource> downloadObf(HttpServletResponse response,
	                                            @RequestBody List<String> names,
	                                            @RequestParam(required = false) Boolean shared,
	                                            @RequestParam(required = false) String sharedType)
			throws IOException, SQLException, XmlPullParserException, InterruptedException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		FileInputStream fis = null;
		File targetObf = null;
		try (OutputStream os = response.getOutputStream()) {
			List<CloudUserFilesRepository.UserFile> selectedFiles = null;
			if (shared != null && shared && sharedType != null) {
				selectedFiles = shareFileService.getOriginalSharedWithMeFiles(dev, sharedType);
			}
			Map<String, GpxFile> files = userdataService.getGpxFilesMap(dev, names, selectedFiles);
			targetObf = osmAndMapsService.getObf(files);
			fis = new FileInputStream(targetObf);
			Algorithms.streamCopy(fis, os);

			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + targetObf.getName());
			headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-binary");
			headers.add(HttpHeaders.CONTENT_LENGTH, targetObf.length() + "");

			return ResponseEntity.ok().headers(headers).body(new FileSystemResource(targetObf));
		} finally {
			if (fis != null) {
				fis.close();
			}
			if (targetObf != null) {
				targetObf.delete();
			}
		}
	}

	@GetMapping(path = {"/get-account-info"})
	public ResponseEntity<String> getAccountInfo() {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		CloudUsersRepository.CloudUser pu = usersRepository.findById(dev.userid);
		Map<String, String> info;
		String errorMsg = userSubService.verifyAndRefreshProOrderId(pu);
		info = userSubService.getUserAccountInfo(pu, errorMsg);
		info.put("nickname", pu.nickname);
		info.put("regtime", pu.regTime != null ? String.valueOf(pu.regTime.getTime()) : "");

		return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, info)));
	}

	@PostMapping(path = {"/auth/send-code"})
	public ResponseEntity<String> sendCode(@RequestParam String action, @RequestParam String lang) {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		CloudUsersRepository.CloudUser pu = usersRepository.findById(dev.userid);
		if (pu == null) {
			return ResponseEntity.badRequest().body("User not found");
		}
		return userdataService.sendCode(action, lang, pu);
	}

	@PostMapping(path = {"/auth/send-code-to-new-email"})
	public ResponseEntity<String> sendCodeToNewEmail(@RequestParam String action, @RequestParam String lang, @RequestParam String email, @RequestParam String code) {
		if (emailSender.isEmail(email)) {
			CloudUserDevice dev = osmAndMapsService.checkUser();
			if (dev == null) {
				return userdataService.tokenNotValidResponse();
			}
			// check token from old email
			CloudUsersRepository.CloudUser currentAcc = usersRepository.findById(dev.userid);
			if (currentAcc == null) {
				return ResponseEntity.badRequest().body("User is not registered");
			}
			ResponseEntity<String> response = userdataService.confirmCode(currentAcc.email, code);
			if (!response.getStatusCode().is2xxSuccessful()) {
				return response;
			}
			// check if new email is not registered
			CloudUsersRepository.CloudUser pu = usersRepository.findByEmailIgnoreCase(email);
			if (pu != null) {
				return ResponseEntity.badRequest().body("User was already registered with such email");
			}
			// create temp user with new email
			pu = new CloudUsersRepository.CloudUser();
			pu.email = email;
			pu.regTime = new Date();
			pu.orderid = null;
			usersRepository.saveAndFlush(pu);

			// send code to new email
			return userdataService.sendCode(action, lang, pu);
		}
		return ResponseEntity.badRequest().body("Please enter valid email");
	}

	@PostMapping(path = {"/auth/change-email"})
	public ResponseEntity<String> changeEmail(@RequestBody UserPasswordPost credentials, HttpServletRequest request) throws ServletException {
		String username = credentials.username;
		String token = credentials.token;
		if (username == null || token == null) {
			return ResponseEntity.badRequest().body("Username and token are required");
		}
		username = username.toLowerCase().trim();
		if (emailSender.isEmail(username)) {
			CloudUserDevice dev = osmAndMapsService.checkUser();
			if (dev == null) {
				return userdataService.tokenNotValidResponse();
			}
			return userdataService.changeEmail(username, token, dev, request);
		}
		return ResponseEntity.badRequest().body("Please enter valid email");
	}

	@GetMapping(path = {"/regions-by-latlon"})
	public String getRegionsByLatlon(@RequestParam("lat") double lat, @RequestParam("lon") double lon) throws IOException {
		List<String> regions = new ArrayList<>();
		if(osmandRegions == null) {
			osmandRegions = new OsmandRegions();
			osmandRegions.prepareFile();
		}
		regions = osmandRegions.getRegionsToDownload(lat, lon, regions);
		return gson.toJson(Map.of("regions", regions));
	}

	@PostMapping(path = {"/get-tracks-by-seg"}, produces = "application/json")
	public ResponseEntity<String> getTracksBySegment(@RequestBody TrackAnalyzerService.TrackAnalyzerRequest request) throws IOException {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		return ResponseEntity.ok(gsonWithNans.toJson(trackAnalyzerService.getTracksBySegment(request, dev)));
	}

}
