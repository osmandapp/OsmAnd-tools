package net.osmand.server.controllers.user;

import java.io.*;

import net.osmand.server.api.repo.*;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.primitives.Metadata;
import okio.Buffer;
import okio.GzipSource;
import okio.Okio;

import static net.osmand.server.api.services.FavoriteService.FILE_TYPE_FAVOURITES;
import static net.osmand.server.api.services.UserdataService.*;
import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import net.osmand.map.OsmandRegions;
import net.osmand.server.api.services.*;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.util.Algorithms;
import okio.Source;
import org.apache.commons.lang3.time.DateUtils;
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
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import net.osmand.server.WebSecurityConfiguration.OsmAndProUser;
import net.osmand.server.api.repo.PremiumUserDevicesRepository.PremiumUserDevice;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFile;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFileNoData;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;
import org.xmlpull.v1.XmlPullParserException;

@RestController
@RequestMapping("/mapapi")
public class MapApiController {

	protected static final Log LOG = LogFactory.getLog(MapApiController.class);
	private static final String ANALYSIS = "analysis";
	private static final String METADATA = "metadata";
	private static final String SHARE = "share";
	private static final String SRTM_ANALYSIS = "srtm-analysis";
	private static final String DONE_SUFFIX = "-done";
	private static final String FAV_POINT_GROUPS = "pointGroups";

	private static final long ANALYSIS_RERUN = 1692026215870L; // 14-08-2023

	private static final String INFO_KEY = "info";

	private static final int LOG_SLOW_WEB_LIST_FILES_MS = 1000;

	@Autowired
	PremiumUserFilesRepository userFilesRepository;

	@Autowired
	AuthenticationManager authManager;

	@Autowired
	PremiumUsersRepository usersRepository;
	
	@Autowired
	PremiumUserDevicesRepository userDevicesRepository;

	@Autowired
	UserdataService userdataService;

	@Autowired
	ShareFileService shareFileService;

	@Autowired
	protected GpxService gpxService;

	@Autowired
	WebGpxParser webGpxParser;


	@Autowired
	OsmAndMapsService osmAndMapsService;

	@Autowired
	private EmailSenderService emailSender;

	@Autowired
	protected DeviceSubscriptionsRepository subscriptionsRepo;

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
			// hide device accesscceToekn
			if (obj instanceof OsmAndProUser) {
				OsmAndProUser pu = (OsmAndProUser) ((Authentication) user).getPrincipal();
				obj = Collections.singletonMap("username", pu.getUsername());

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
		
		if (user != null && !user.getName().equals(credentials.username)) {
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

		PremiumUserDevice dev = userdataService.checkUser();
		userdataService.updateDeviceLangInfo(dev, credentials.lang, BRAND_DEVICE_WEB, MODEL_DEVICE_WEB);

		return okStatus();
	}

	@PostMapping(path = {"/auth/delete-account"})
	public ResponseEntity<String> deleteAccount(@RequestParam String token, HttpServletRequest request)
			throws ServletException {
		PremiumUserDevice dev = userdataService.checkUser();
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
		PremiumUserDevice dev = userdataService.checkUser();

		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		userdataService.uploadMultipartFile(file, dev, name, type, System.currentTimeMillis());

		return okStatus();
	}

	@PostMapping(value = "/delete-file")
	public ResponseEntity<String> deleteFile(@RequestParam String name, @RequestParam String type) {
		PremiumUserDevice dev = userdataService.checkUser();
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
		PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		} else {
			return userdataService.deleteFileVersion(updatetime, dev.userid, name, type, null);
		}
	}
	
	@GetMapping(value = "/delete-file-all-versions")
	public ResponseEntity<String> deleteFileAllVersions(@RequestParam String name,
	                                         @RequestParam String type, @RequestParam Long updatetime, @RequestParam boolean isTrash) {
		PremiumUserDevice dev = userdataService.checkUser();
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
		PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		if (!oldName.equals(newName)) {
			return userdataService.renameFile(oldName, newName, type, dev, saveCopy);
		}
		return ResponseEntity.badRequest().body("Old track name and new track name are the same!");
	}

	@GetMapping(value = "/rename-folder")
	public ResponseEntity<String> renameFolder(@RequestParam String folderName,
	                                           @RequestParam String type,
	                                           @RequestParam String newFolderName) throws IOException {
		PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		return userdataService.renameFolder(folderName, newFolderName, type, dev);
	}

	@GetMapping(value = "/delete-folder")
	public ResponseEntity<String> deleteFolder(@RequestParam String folderName,
	                                           @RequestParam String type) {
		PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		return userdataService.deleteFolder(folderName, type, dev);
	}

	@GetMapping(value = "/list-files")
	public ResponseEntity<String> listFiles(@RequestParam(required = false) String name,
	                                        @RequestParam(required = false) String type,
	                                        @RequestParam(required = false, defaultValue = "false") boolean addDevices,
	                                        @RequestParam(required = false, defaultValue = "false") boolean allVersions) throws IOException {
		long start = System.currentTimeMillis();
		PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		UserFilesResults res = userdataService.generateFiles(dev.userid, name, allVersions, true, type);
		List<ShareFileRepository.ShareFile> shareList = shareFileService.getFilesByOwner(dev.userid);
		List <UserFileNoData> filesToIgnore = new ArrayList<>();
		int cloudReads = 0, cacheWrites = 0;
		for (UserFileNoData nd : res.uniqueFiles) {
			String ext = nd.name.substring(nd.name.lastIndexOf('.') + 1);
			boolean isGPZTrack = nd.type.equalsIgnoreCase("gpx") && ext.equalsIgnoreCase("gpx") && !analysisPresent(ANALYSIS, nd.details);
			boolean isFavorite = nd.type.equals(FILE_TYPE_FAVOURITES) && ext.equalsIgnoreCase("gpx") && !analysisPresentFavorites(ANALYSIS, nd.details);
			if (isGPZTrack || isFavorite) {
				Optional<UserFile> of = userFilesRepository.findById(nd.id);
				if (of.isPresent()) {
					GpxTrackAnalysis analysis = null;
					UserFile uf = of.get();
					InputStream in = uf.data != null ? new ByteArrayInputStream(uf.data)
							: userdataService.getInputStream(uf);
					cloudReads++;
					if (in != null) {
						in = new GZIPInputStream(in);
						GpxFile gpxFile;
						try (Source source = new Buffer().readFrom(in)) {
							gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
						} catch (IOException e) {
							LOG.error(String.format(
									"web-list-files-error: load-gpx-error %s id=%d userid=%d error (%s)",
									uf.name, uf.id, uf.userid, e.getMessage()));
							filesToIgnore.add(nd);
							continue;
						}
						if (gpxFile.getError() != null) {
							LOG.error(String.format(
									"web-list-files-error: corrupted-gpx-file %s id=%d userid=%d error (%s)",
									uf.name, uf.id, uf.userid, gpxFile.getError().getMessage()));
							filesToIgnore.add(nd);
							continue;
						}
						if (isGPZTrack) {
							analysis = getAnalysis(uf, gpxFile);
							Metadata metadata = gpxFile.getMetadata();
							if (!metadata.isEmpty()) {
								uf.details.add(METADATA, gson.toJsonTree(metadata));
							}
						} else {
							Map<String, WebGpxParser.WebPointsGroup> groups = webGpxParser.getPointsGroups(gpxFile);
							Map<String, Map<String,String>> pointGroupsAnalysis = new HashMap<>();
							groups.keySet().forEach(k -> {
								Map<String, String> groupInfo = new HashMap<>();
								WebGpxParser.WebPointsGroup group = groups.get(k);
								groupInfo.put("color", group.color);
								groupInfo.put("groupSize", String.valueOf(group.points.size()));
								groupInfo.put("hidden", String.valueOf(isHidden(group)));
								pointGroupsAnalysis.put(k, groupInfo);
							});
							uf.details.add(FAV_POINT_GROUPS, gson.toJsonTree(gsonWithNans.toJson(pointGroupsAnalysis)));
						}
					} else {
						LOG.error(String.format(
								"web-list-files-error: no-input-stream %s id=%d userid=%d", uf.name, uf.id, uf.userid));
						filesToIgnore.add(nd);
					}
					saveAnalysis(ANALYSIS, uf, analysis);
					nd.details = uf.details.deepCopy();
					cacheWrites++;
				}
			}
			if (analysisPresent(ANALYSIS, nd.details)) {
				nd.details.get(ANALYSIS).getAsJsonObject().remove("speedData");
				nd.details.get(ANALYSIS).getAsJsonObject().remove("elevationData");
				nd.details.get(ANALYSIS).getAsJsonObject().remove("pointsAttributesData");
			}
			if (analysisPresent(SRTM_ANALYSIS, nd.details)) {
				nd.details.get(SRTM_ANALYSIS).getAsJsonObject().remove("speedData");
				nd.details.get(SRTM_ANALYSIS).getAsJsonObject().remove("elevationData");
				nd.details.get(SRTM_ANALYSIS).getAsJsonObject().remove("pointsAttributesData");
			}
			nd.details.add(SHARE, gson.toJsonTree(isShared(nd, shareList)));
		}
		if (addDevices && res.allFiles != null) {
			Map<Integer, String> devices = new HashMap<>();
			for (UserFileNoData nd : res.allFiles) {
				addDeviceInformation(nd, devices);
			}
		}

		long elapsed = System.currentTimeMillis() - start;

		if (LOG_SLOW_WEB_LIST_FILES_MS > 0 && elapsed > LOG_SLOW_WEB_LIST_FILES_MS) {
			LOG.info(String.format(
					"web-list-files-slow: userid=%d totalFiles=%d ignoredFiles=%d cloudReads=%d cacheWrites=%d elapsed=%d ms",
					dev.userid, res.uniqueFiles.size(), filesToIgnore.size(), cloudReads, cacheWrites, elapsed));
		}

		res.uniqueFiles.removeAll(filesToIgnore);
		return ResponseEntity.ok(gson.toJson(res));
	}

	private boolean isShared(UserFileNoData file, List<ShareFileRepository.ShareFile> shareList) {
		for (ShareFileRepository.ShareFile sf : shareList) {
			if (sf.getFilepath().equals(file.name) && sf.getType().equals(file.type)) {
				return true;
			}
		}
		return false;
	}

	private void addDeviceInformation(UserFileNoData file, Map<Integer, String> devices) {
		String deviceInfo = devices.get(file.deviceid);
		if (deviceInfo == null) {
			PremiumUserDevice device = userDevicesRepository.findById(file.deviceid);
			if (device != null && device.brand != null && device.model != null) {
				deviceInfo = device.brand + "__model__" + device.model;
				devices.put(file.deviceid, deviceInfo);
			}
		}
		file.setDeviceInfo(deviceInfo);
	}

	private boolean isHidden(WebGpxParser.WebPointsGroup group) {
		for (WebGpxParser.Wpt wpt:  group.points) {
			if (wpt.hidden != null && wpt.hidden.equals("true")) {
				return true;
			}
		}
		return false;
	}

	private boolean analysisPresent(String tag, UserFile userFile) {
		if(userFile == null) {
			return false;
		}
		JsonObject details = userFile.details;
		return analysisPresent(tag, details);
	}

	private boolean analysisPresent(String tag, JsonObject details) {
		return details != null && details.has(tag + DONE_SUFFIX)
				&& details.get(tag + DONE_SUFFIX).getAsLong() >= ANALYSIS_RERUN
				&& details.has(tag) && !details.get(tag).isJsonNull();
	}

	private boolean analysisPresentFavorites(String tag, JsonObject details) {
		return details != null
				&& details.has(tag + DONE_SUFFIX)
				&& details.has(FAV_POINT_GROUPS)
				&& !details.get(FAV_POINT_GROUPS).getAsString().equals("{}")
				&& details.get(tag + DONE_SUFFIX).getAsLong() >= ANALYSIS_RERUN;
	}

	@GetMapping(value = "/download-file")
	public void getFile(HttpServletResponse response, HttpServletRequest request,
			@RequestParam String name,
			@RequestParam String type,
			@RequestParam(required = false) Long updatetime) throws IOException {
		PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			ResponseEntity<String> error = userdataService.tokenNotValidResponse();
            response.setStatus(error.getStatusCodeValue());
            response.getWriter().write(Objects.requireNonNull(error.getBody()));
            return;
        }
		PremiumUserFilesRepository.UserFile userFile = userdataService.getUserFile(name, type, updatetime, dev);
		if (userFile != null) {
			userdataService.getFile(userFile, response, request, name, type, dev);
		}
	}
	
	@GetMapping(value = "/download-file-from-prev-version")
	public void getFilePrevVersion(HttpServletResponse response, HttpServletRequest request,
	                               @RequestParam String name,
	                               @RequestParam String type,
	                               @RequestParam Long updatetime) throws IOException {
		PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			ResponseEntity<String> error = userdataService.tokenNotValidResponse();
			response.setStatus(error.getStatusCodeValue());
			response.getWriter().write(Objects.requireNonNull(error.getBody()));
			return;
		}
		PremiumUserFilesRepository.UserFile userFile = userdataService.getFilePrevVersion(name, type, updatetime, dev);
		if (userFile != null) {
			userdataService.getFile(userFile, response, request, name, type, dev);
		}
	}
	
	@GetMapping(value = "/restore-file")
	public ResponseEntity<String> restoreFile(@RequestParam String name, @RequestParam String type, @RequestParam Long updatetime) throws IOException {
		PremiumUserDevice dev = userdataService.checkUser();
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
		PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		return userdataService.emptyTrash(files, dev);
	}

	@GetMapping(value = "/get-gpx-info")
	public ResponseEntity<String> getGpxInfo(@RequestParam(name = "name") String name,
	                                         @RequestParam(name = "type") String type,
	                                         @RequestParam(name = "updatetime", required = false) Long updatetime) throws IOException {
		PremiumUserDevice dev = userdataService.checkUser();
		InputStream in = null;
		try {
			UserFile userFile = userdataService.getUserFile(name, type, updatetime, dev);
			if (analysisPresent(ANALYSIS, userFile)) {
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
			GpxTrackAnalysis analysis = getAnalysis(userFile, gpxFile);
			if (!analysisPresent(ANALYSIS, userFile)) {
				saveAnalysis(ANALYSIS, userFile, analysis);
			}
			return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, analysis)));
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	private GpxTrackAnalysis getAnalysis(UserFile file, GpxFile gpxFile) {
		gpxFile.setPath(file.name);
		GpxTrackAnalysis analysis = gpxFile.getAnalysis(0); // keep 0
		gpxService.cleanupFromNan(analysis);

		return analysis;
	}

	private void saveAnalysis(String tag, UserFile file, GpxTrackAnalysis analysis) {
		if (analysis != null) {
			if (file.details == null) {
				file.details = new JsonObject();
			}
			analysis.getPointAttributes().clear();

			Map<String, Object> res = new HashMap<>();
			res.put("totalDistance", analysis.getTotalDistance());
			res.put("timeMoving", analysis.getTimeMoving());
			res.put("points", analysis.getPoints());
			res.put("wptPoints", analysis.getWptPoints());

			file.details.add(tag, gsonWithNans.toJsonTree(res));
		}
		file.details.addProperty(tag + DONE_SUFFIX, System.currentTimeMillis());
		userFilesRepository.save(file);
	}


	@GetMapping(path = {"/get-srtm-gpx-info"}, produces = "application/json")
	public ResponseEntity<String> getSrtmGpx(@RequestParam(name = "name") String name,
	                                         @RequestParam(name = "type") String type,
	                                         @RequestParam(name = "updatetime", required = false) Long updatetime) throws IOException {
		PremiumUserDevice dev = userdataService.checkUser();
		InputStream in = null;
		try {
			UserFile userFile = userdataService.getUserFile(name, type, updatetime, dev);
			if (analysisPresent(SRTM_ANALYSIS, userFile)) {
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
			GpxTrackAnalysis analysis = srtmGpx == null ? null : getAnalysis(userFile, srtmGpx);
			if (!analysisPresent(SRTM_ANALYSIS, userFile)) {
				saveAnalysis(SRTM_ANALYSIS, userFile, analysis);
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
		PremiumUserDevice dev = userdataService.checkUser();
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
	                               @RequestParam String folderName,
	                               @RequestParam String type,
	                               HttpServletResponse response) throws IOException {
		PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			ResponseEntity<String> error = userdataService.tokenNotValidResponse();
			response.setStatus(error.getStatusCodeValue());
			if (error.getBody() != null) {
				response.getWriter().write(error.getBody());
			}
			return;
		}
		userdataService.getBackupFolder(response, dev, folderName, format, type);
	}

	@GetMapping(path = { "/check_download" }, produces = "text/html;charset=UTF-8")
	public ResponseEntity<String> checkDownload(@RequestParam(value = "file_name", required = false) String fn,
			@RequestParam(value = "file_size", required = false) String sz) throws IOException {
		return okStatus();
	}

	@RequestMapping(path = {"/download-obf"})
	public ResponseEntity<Resource> downloadObf(HttpServletResponse response, @RequestBody List<String> names)
			throws IOException, SQLException, XmlPullParserException, InterruptedException {
		PremiumUserDevice dev = userdataService.checkUser();
		InputStream is = null;
		FileInputStream fis = null;
		File targetObf = null;
		try (OutputStream os = response.getOutputStream()) {
			Map<String, GpxFile> files = new HashMap<>();
			for (String name : names) {
				if (!name.endsWith(EMPTY_FILE_NAME)) {
					UserFile userFile = userdataService.getUserFile(name, "GPX", null, dev);
					if (userFile != null) {
						is = userdataService.getInputStream(dev, userFile);
						GpxFile file = GpxUtilities.INSTANCE.loadGpxFile(null, new GzipSource(Okio.source(is)), null, false);
						if (file.getError() == null) {
							file.setModifiedTime(userFile.updatetime.getTime());
							files.put(name, file);
						}
					}
				}
			}
			targetObf = osmAndMapsService.getObf(files);
			fis = new FileInputStream(targetObf);
			Algorithms.streamCopy(fis, os);

			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + targetObf.getName());
			headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-binary");
			headers.add(HttpHeaders.CONTENT_LENGTH, targetObf.length() + "");

			return ResponseEntity.ok().headers(headers).body(new FileSystemResource(targetObf));
		} finally {
			if (is != null) {
				is.close();
			}
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
		final String ACCOUNT_KEY = "account";
		final String FREE_ACCOUNT = "Free";
		final String PRO_ACCOUNT = "Osmand Pro";
		final String TYPE_SUB = "type";
		final String START_TIME_KEY = "startTime";
		final String EXPIRE_TIME_KEY = "expireTime";
		final String MAX_ACCOUNT_SIZE = "maxAccSize";
		final String NICKNAME = "nickname";

		PremiumUserDevice dev = userdataService.checkUser();
		PremiumUsersRepository.PremiumUser pu = usersRepository.findById(dev.userid);
		Map<String, String> info = new HashMap<>();

		info.put(NICKNAME, pu.nickname);

		String orderId = pu.orderid;
		if (orderId == null) {
			info.put(ACCOUNT_KEY, FREE_ACCOUNT);
			info.put(MAX_ACCOUNT_SIZE, String.valueOf((MAXIMUM_FREE_ACCOUNT_SIZE)));
		} else {
			List<DeviceSubscriptionsRepository.SupporterDeviceSubscription> subscriptions = subscriptionsRepo.findByOrderId(orderId);
			DeviceSubscriptionsRepository.SupporterDeviceSubscription subscription = subscriptions.stream()
					.filter(s -> s.valid)
					.findFirst()
					.orElse(null);
			if (subscription != null) {
				info.put(ACCOUNT_KEY, PRO_ACCOUNT);
				info.put(TYPE_SUB, subscription.sku);
				Date prepareStartTime = DateUtils.truncate(subscription.starttime, Calendar.SECOND);
				Date prepareExpireTime = DateUtils.truncate(subscription.expiretime, Calendar.SECOND);
				info.put(START_TIME_KEY, prepareStartTime.toString());
				info.put(EXPIRE_TIME_KEY, prepareExpireTime.toString());
				info.put(MAX_ACCOUNT_SIZE, String.valueOf((MAXIMUM_ACCOUNT_SIZE)));
			}
		}
		return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, info)));
	}

	@PostMapping(path = {"/auth/send-code"})
	public ResponseEntity<String> sendCode(@RequestParam String action, @RequestParam String lang) {
		PremiumUserDevice dev = userdataService.checkUser();
		if (dev == null) {
			return userdataService.tokenNotValidResponse();
		}
		PremiumUsersRepository.PremiumUser pu = usersRepository.findById(dev.userid);
		if (pu == null) {
			return ResponseEntity.badRequest().body("User not found");
		}
		return userdataService.sendCode(action, lang, pu);
	}

	@PostMapping(path = {"/auth/send-code-to-new-email"})
	public ResponseEntity<String> sendCodeToNewEmail(@RequestParam String action, @RequestParam String lang, @RequestParam String email, @RequestParam String code) {
		if (emailSender.isEmail(email)) {
			PremiumUserDevice dev = userdataService.checkUser();
			if (dev == null) {
				return userdataService.tokenNotValidResponse();
			}
			// check token from old email
			PremiumUsersRepository.PremiumUser currentAcc = usersRepository.findById(dev.userid);
			if (currentAcc == null) {
				return ResponseEntity.badRequest().body("User is not registered");
			}
			ResponseEntity<String> response = userdataService.confirmCode(currentAcc.email, code);
			if (!response.getStatusCode().is2xxSuccessful()) {
				return response;
			}
			// check if new email is not registered
			PremiumUsersRepository.PremiumUser pu = usersRepository.findByEmailIgnoreCase(email);
			if (pu != null) {
				return ResponseEntity.badRequest().body("User was already registered with such email");
			}
			// create temp user with new email
			pu = new PremiumUsersRepository.PremiumUser();
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
			PremiumUserDevice dev = userdataService.checkUser();
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

}
