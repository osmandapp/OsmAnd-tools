package net.osmand.server.controllers.user;

import java.io.*;

import static net.osmand.server.api.services.UserdataService.MAXIMUM_ACCOUNT_SIZE;
import static net.osmand.server.api.services.UserdataService.MAXIMUM_FREE_ACCOUNT_SIZE;
import static net.osmand.server.controllers.user.FavoriteController.FILE_TYPE_FAVOURITES;
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

import com.google.gson.JsonParser;
import net.osmand.map.OsmandRegions;
import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.server.api.services.*;
import net.osmand.server.controllers.pub.UserSessionResources;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.util.Algorithms;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXTrackAnalysis;
import net.osmand.gpx.GPXUtilities;
import net.osmand.server.WebSecurityConfiguration.OsmAndProUser;
import net.osmand.server.api.repo.PremiumUserDevicesRepository.PremiumUserDevice;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFile;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFileNoData;
import net.osmand.server.controllers.pub.GpxController;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;
import org.xmlpull.v1.XmlPullParserException;

@Controller
@RequestMapping("/mapapi")
public class MapApiController {

	protected static final Log LOGGER = LogFactory.getLog(MapApiController.class);
	private static final String ANALYSIS = "analysis";
	private static final String METADATA = "metadata";
	private static final String SRTM_ANALYSIS = "srtm-analysis";
	private static final String DONE_SUFFIX = "-done";

	private static final long ANALYSIS_RERUN = 1692026215870l; // 14-08-2023

	private static final String INFO_KEY = "info";
											   

	@Autowired
	UserdataController userdataController;
	
	@Autowired
	GpxController gpxController;
	
	@Autowired
	PremiumUserFilesRepository userFilesRepository;
	
	@Autowired
	AuthenticationManager authManager;
	
	@Autowired
	PremiumUsersRepository usersRepository;
	
	@Autowired
	UserdataService userdataService;
	
	@Autowired
	protected StorageService storageService;
	
	@Autowired
	protected PremiumUserFilesRepository filesRepository;
	
	@Autowired
	protected GpxService gpxService;
	
	@Autowired
	WebGpxParser webGpxParser;
	
	@Autowired
	UserSessionResources session;
	
	@Autowired
	OsmAndMapsService osmAndMapsService;
	
	@Autowired
	private EmailSenderService emailSender;
	
	@Autowired
	protected DeviceSubscriptionsRepository subscriptionsRepo;
	
	OsmandRegions osmandRegions;
	
	Gson gson = new Gson();
	
	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();
	
	JsonParser jsonParser = new JsonParser();

	public static class UserPasswordPost {
		public String username;
		public String password;
		public String token;
	}
	
	public static class EmailSenderInfo {
		public String email;
		public String action;
	}

	@GetMapping(path = { "/auth/loginForm" }, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public AbstractResource loginForm() {
		return new ClassPathResource("/test-map-pro-login.html");
	}

	@GetMapping(path = { "/auth/info" }, produces = "application/json")
	@ResponseBody
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

	@PostMapping(path = { "/auth/login" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> loginUser(@RequestBody UserPasswordPost us, HttpServletRequest request, java.security.Principal user) throws ServletException {
		if (user != null) {
			request.logout();
		}
		UsernamePasswordAuthenticationToken pwt = new UsernamePasswordAuthenticationToken(us.username, us.password);
		try {
			//Authentication res = 
			authManager.authenticate(pwt);
			// System.out.println(res);
		} catch (AuthenticationException e) {
			return ResponseEntity.badRequest().body(String.format("Authentication '%s' has failed", us.username));
		}
		request.login(us.username, us.password); // SecurityContextHolder.getContext().getAuthentication();
		return okStatus();
	}
	
	@PostMapping(path = {"/auth/delete-account"})
	@ResponseBody
	public ResponseEntity<String> deleteAccount(@RequestBody UserPasswordPost us, HttpServletRequest request) throws ServletException {
		if (emailSender.isEmail(us.username)) {
			PremiumUserDevice dev = checkUser();
			if (dev == null) {
				return tokenNotValid();
			}
			return userdataService.deleteAccount(us, dev, request);
		}
		return ResponseEntity.badRequest().body("Please enter valid email");
	}

	@PostMapping(path = { "/auth/activate" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> activateMapUser(@RequestBody UserPasswordPost us, HttpServletRequest request)
			throws ServletException, IOException {
		ResponseEntity<String> res = userdataService.webUserActivate(us.username, us.token, us.password);
		if (res.getStatusCodeValue() < 300) {
			request.logout();
			request.login(us.username, us.password);
			return okStatus();
		}
		return res;
	}

	@PostMapping(path = { "/auth/logout" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> logoutMapUser(HttpServletRequest request) throws ServletException {
		request.logout();
		return okStatus();
	}

	@PostMapping(path = { "/auth/register" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> registerMapUser(@RequestBody UserPasswordPost us, HttpServletRequest request)
			throws ServletException, IOException {
		return userdataService.webUserRegister(us.username);
	}
	
	public PremiumUserDevicesRepository.PremiumUserDevice checkUser() {
		Object user = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
		if (user instanceof WebSecurityConfiguration.OsmAndProUser) {
			return ((WebSecurityConfiguration.OsmAndProUser) user).getUserDevice();
		}
		return null;
	}
	
	private ResponseEntity<String> tokenNotValid() {
	    return new ResponseEntity<>("Unauthorized", HttpStatus.UNAUTHORIZED);

	}
	
	@PostMapping(value = "/upload-file", consumes = MULTIPART_FORM_DATA_VALUE)
	@ResponseBody
	public ResponseEntity<String> uploadFile(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
	                                     @RequestParam String name, @RequestParam String type) throws IOException {
		// This could be slow series of checks (token, user, subscription, amount of space):
		// probably it's better to support multiple file upload without these checks
		PremiumUserDevice dev = checkUser();
		
		if (dev == null) {
			return tokenNotValid();
		}
		userdataService.uploadMultipartFile(file, dev, name, type, System.currentTimeMillis());
		
		return okStatus();
	}
	
	@PostMapping(value = "/delete-file")
	@ResponseBody
	public ResponseEntity<String> deleteFile(@RequestParam String name, @RequestParam String type) {
		PremiumUserDevice dev = checkUser();
		if (dev == null) {
			return userdataService.tokenNotValid();
		}
		userdataService.deleteFile(name, type, null, null, dev);
		return userdataService.ok();
	}
	
	@PostMapping(value = "/delete-file-version")
	@ResponseBody
	public ResponseEntity<String> deleteFile(@RequestParam String name,
	                                         @RequestParam String type,
	                                         @RequestParam Long updatetime) {
		PremiumUserDevice dev = checkUser();
		if (dev == null) {
			return userdataService.tokenNotValid();
		} else {
			return userdataService.deleteFileVersion(updatetime, dev.userid, name, type, null);
		}
	}
	
	@GetMapping(value = "/rename-file")
	@ResponseBody
	public ResponseEntity<String> renameFile(@RequestParam String oldName,
	                                         @RequestParam String newName,
	                                         @RequestParam String type,
	                                         @RequestParam boolean saveCopy) throws IOException {
		PremiumUserDevice dev = checkUser();
		if (dev == null) {
			return userdataService.tokenNotValid();
		}
		if (!oldName.equals(newName)) {
			return userdataService.renameFile(oldName, newName, type, dev, saveCopy);
		}
		return ResponseEntity.badRequest().body("Old track name and new track name are the same!");
	}
	
	@GetMapping(value = "/rename-folder")
	@ResponseBody
	public ResponseEntity<String> renameFolder(@RequestParam String folderName,
	                                           @RequestParam String type,
	                                           @RequestParam String newFolderName) throws IOException {
		PremiumUserDevice dev = checkUser();
		if (dev == null) {
			return userdataService.tokenNotValid();
		}
		return userdataService.renameFolder(folderName, newFolderName, type, dev);
	}
	
	@GetMapping(value = "/delete-folder")
	@ResponseBody
	public ResponseEntity<String> deleteFolder(@RequestParam String folderName,
	                                           @RequestParam String type) {
		PremiumUserDevice dev = checkUser();
		if (dev == null) {
			return userdataService.tokenNotValid();
		}
		return userdataService.deleteFolder(folderName, type, dev);
	}
	
	@GetMapping(value = "/list-files")
	@ResponseBody
	public ResponseEntity<String> listFiles(
			@RequestParam(name = "name", required = false) String name,
			@RequestParam(name = "type", required = false) String type,
			@RequestParam(name = "allVersions", required = false, defaultValue = "false") boolean allVersions) throws IOException, SQLException {
		PremiumUserDevice dev = checkUser();
		if (dev == null) {
			return tokenNotValid();
		}
		UserFilesResults res = userdataService.generateFiles(dev.userid, name, allVersions, true, type);
		for (UserFileNoData nd : res.uniqueFiles) {
			String ext = nd.name.substring(nd.name.lastIndexOf('.') + 1);
			boolean isGPZTrack = nd.type.equalsIgnoreCase("gpx") && ext.equalsIgnoreCase("gpx") && !analysisPresent(ANALYSIS, nd.details);
			boolean isFavorite = nd.type.equals(FILE_TYPE_FAVOURITES) && ext.equalsIgnoreCase("gpx") && !analysisPresentFavorites(ANALYSIS, nd.details);
			if (isGPZTrack || isFavorite) {
				Optional<UserFile> of = userFilesRepository.findById(nd.id);
				if (of.isPresent()) {
					GPXTrackAnalysis analysis = null;
					UserFile uf = of.get();
					InputStream in = uf.data != null ? new ByteArrayInputStream(uf.data)
							: userdataService.getInputStream(uf);
					if (in != null) {
						GPXFile gpxFile = GPXUtilities.loadGPXFile(new GZIPInputStream(in));
						if (isGPZTrack) {
							analysis = getAnalysis(uf, gpxFile);
							if (gpxFile.metadata != null) {
								uf.details.add(METADATA, gson.toJsonTree(gpxFile.metadata));
							}
						} else {
							Map<String, WebGpxParser.PointsGroup> groups = webGpxParser.getPointsGroups(gpxFile);
							Map<String, Map<String,String>> pointGroupsAnalysis = new HashMap<>();
							groups.keySet().forEach(k -> {
								Map<String, String> groupInfo = new HashMap<>();
								WebGpxParser.PointsGroup group = groups.get(k);
								groupInfo.put("color", group.color);
								groupInfo.put("groupSize", String.valueOf(group.points.size()));
								groupInfo.put("hidden", String.valueOf(isHidden(group)));
								pointGroupsAnalysis.put(k, groupInfo);
							});
							uf.details.add("pointGroups", gson.toJsonTree(gsonWithNans.toJson(pointGroupsAnalysis)));
						}
					}
					saveAnalysis(ANALYSIS, uf, analysis);
					nd.details = uf.details.deepCopy();
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
		}
		return ResponseEntity.ok(gson.toJson(res));
	}
	
	private boolean isHidden(WebGpxParser.PointsGroup group) {
		for (WebGpxParser.Wpt wpt:  group.points) {
			if (wpt.ext.extensions.get("hidden") != null && wpt.ext.extensions.get("hidden").equals("true")) {
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
		return details != null && details.has(tag + DONE_SUFFIX)
				&& details.get(tag + DONE_SUFFIX).getAsLong() >= ANALYSIS_RERUN;
	}
	
	@GetMapping(value = "/download-file")
	@ResponseBody
	public void getFile(HttpServletResponse response, HttpServletRequest request,
			@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "updatetime", required = false) Long updatetime) throws IOException, SQLException {
		PremiumUserDevice dev = checkUser();
		if (dev == null) {
			ResponseEntity<String> error = tokenNotValid();
			if (error != null) {
				response.setStatus(error.getStatusCodeValue());
				response.getWriter().write(error.getBody());
				return;
			}
		}
		userdataService.getFile(response, request, name, type, updatetime, dev);
	}
	
	@GetMapping(value = "/get-gpx-info")
	@ResponseBody
	public ResponseEntity<String> getGpxInfo(HttpServletResponse response, HttpServletRequest request,
			@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "updatetime", required = false) Long updatetime) throws IOException, SQLException {
		PremiumUserDevice dev = checkUser();
		InputStream bin = null;
		try {
			UserFile userFile = userdataService.getUserFile(name, type, updatetime, dev);
			if (analysisPresent(ANALYSIS, userFile)) {
				return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, userFile.details.get(ANALYSIS))));
			}
			bin = userdataService.getInputStream(dev, userFile);
			
			GPXFile gpxFile = GPXUtilities.loadGPXFile(new GZIPInputStream(bin));
			if (gpxFile == null) {
				return ResponseEntity.badRequest().body(String.format("File %s not found", userFile.name));
			}
			GPXTrackAnalysis analysis = getAnalysis(userFile, gpxFile);
			if (!analysisPresent(ANALYSIS, userFile)) {
				saveAnalysis(ANALYSIS, userFile, analysis);
			}
			return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, analysis)));
		} finally {
			if (bin != null) {
				bin.close();
			}
		}
	}

	private GPXTrackAnalysis getAnalysis(UserFile file, GPXFile gpxFile) {
		gpxFile.path = file.name;
		// file.clienttime == null ? 0 : file.clienttime.getTime()
		GPXTrackAnalysis analysis = gpxFile.getAnalysis(0); // keep 0
		gpxService.cleanupFromNan(analysis);
		return analysis;
	}

	private void saveAnalysis(String tag, UserFile file, GPXTrackAnalysis analysis) {
		if (file.details == null) {
			file.details = new JsonObject();
		}
		if (analysis != null) {
			analysis.pointAttributes.clear();
			analysis.availableAttributes.clear();
		}
		file.details.add(tag, gsonWithNans.toJsonTree(analysis));
		file.details.addProperty(tag + DONE_SUFFIX, System.currentTimeMillis());
		userFilesRepository.save(file);
	}
	

	@GetMapping(path = {"/get-srtm-gpx-info"}, produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> getSrtmGpx(HttpServletResponse response, HttpServletRequest request,
			@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "updatetime", required = false) Long updatetime) throws IOException {
		PremiumUserDevice dev = checkUser();
		InputStream bin = null;
		try {
			UserFile userFile = userdataService.getUserFile(name, type, updatetime, dev);
			if (analysisPresent(SRTM_ANALYSIS, userFile)) {
				return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, userFile.details.get(SRTM_ANALYSIS))));
			}
			bin = userdataService.getInputStream(dev, userFile);
			GPXFile gpxFile = GPXUtilities.loadGPXFile(new GZIPInputStream(bin));
			if (gpxFile == null) {
				return ResponseEntity.badRequest().body(String.format("File %s not found", userFile.name));
			}
			GPXFile srtmGpx = gpxService.calculateSrtmAltitude(gpxFile, null);
			GPXTrackAnalysis analysis = srtmGpx == null ? null : getAnalysis(userFile, srtmGpx);
			if (!analysisPresent(SRTM_ANALYSIS, userFile)) {
				saveAnalysis(SRTM_ANALYSIS, userFile, analysis);
			}
			return ResponseEntity.ok(gson.toJson(Collections.singletonMap(INFO_KEY, analysis)));
		} finally {
			if (bin != null) {
				bin.close();
			}
		}
	}
	
	@PostMapping(value = "/download-backup")
	@ResponseBody
	public void createBackup(HttpServletResponse response,
	                         @RequestParam(name = "updatetime", required = false) boolean includeDeleted,
	                         @RequestParam String format,
	                         @RequestBody List<String> data) throws IOException {
		PremiumUserDevice dev = checkUser();
		if (dev == null) {
			ResponseEntity<String> error = tokenNotValid();
			response.setStatus(error.getStatusCodeValue());
			if (error.getBody() != null) {
				response.getWriter().write(error.getBody());
			}
			return;
		}
		userdataService.getBackup(response, dev, Set.copyOf(data), includeDeleted, format);
	}
	
	@PostMapping(value = "/download-backup-folder")
	@ResponseBody
	public void createBackupFolder(@RequestParam String format,
	                               @RequestParam String folderName,
	                               @RequestParam String type,
	                               HttpServletResponse response) throws IOException {
		PremiumUserDevice dev = checkUser();
		if (dev == null) {
			ResponseEntity<String> error = tokenNotValid();
			response.setStatus(error.getStatusCodeValue());
			if (error.getBody() != null) {
				response.getWriter().write(error.getBody());
			}
			return;
		}
		userdataService.getBackupFolder(response, dev, folderName, format, type);
	}
	
	@GetMapping(path = { "/check_download" }, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public ResponseEntity<String> checkDownload(@RequestParam(value = "file_name", required = false) String fn,
			@RequestParam(value = "file_size", required = false) String sz) throws IOException {
		return okStatus();
	}
	
	@RequestMapping(path = {"/download-obf"})
	@ResponseBody
	public ResponseEntity<Resource> downloadObf(HttpServletResponse response, @RequestBody List<String> names)
			throws IOException, SQLException, XmlPullParserException, InterruptedException {
		PremiumUserDevice dev = checkUser();
		InputStream is = null;
		FileInputStream fis = null;
		File targetObf = null;
		try (OutputStream os = response.getOutputStream()) {
			Map<String, GPXFile> files = new HashMap<>();
			for (String name : names) {
				UserFile userFile = userdataService.getUserFile(name, "GPX", null, dev);
				if (userFile != null) {
					is = userdataService.getInputStream(dev, userFile);
					GPXFile file = GPXUtilities.loadGPXFile(new GZIPInputStream(is), null, false);
					files.put(name, file);
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
	@ResponseBody
	public ResponseEntity<String> getAccountInfo() {
		final String ACCOUNT_KEY = "account";
		final String FREE_ACCOUNT = "Free";
		final String PRO_ACCOUNT = "Osmand Pro";
		final String TYPE_SUB = "type";
		final String START_TIME_KEY = "startTime";
		final String EXPIRE_TIME_KEY = "expireTime";
		final String MAX_ACCOUNT_SIZE = "maxAccSize";
		
		PremiumUserDevice dev = checkUser();
		PremiumUsersRepository.PremiumUser pu = usersRepository.findById(dev.userid);
		Map<String, String> info = new HashMap<>();
		
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
	@ResponseBody
	public ResponseEntity<String> sendCode(@RequestBody EmailSenderInfo data) {
		if (emailSender.isEmail(data.email)) {
			PremiumUserDevice dev = checkUser();
			if (dev == null) {
				return tokenNotValid();
			}
			return userdataService.sendCode(data.email, data.action, dev);
		}
		return ResponseEntity.badRequest().body("Please enter valid email");
	}
	
	@PostMapping(path = {"/auth/confirm-code"})
	@ResponseBody
	public ResponseEntity<String> confirmCode(@RequestBody String code) {
		PremiumUserDevice dev = checkUser();
		if (dev == null) {
			return tokenNotValid();
		}
		return userdataService.confirmCode(code, dev);
	}
	
	@PostMapping(path = {"/auth/change-email"})
	@ResponseBody
	public ResponseEntity<String> changeEmail(@RequestBody UserPasswordPost us, HttpServletRequest request) throws ServletException {
		if (us.username != null) {
			us.username = us.username.toLowerCase().trim();
		}
		if (emailSender.isEmail(us.username)) {
			PremiumUserDevice dev = checkUser();
			if (dev == null) {
				return tokenNotValid();
			}
			return userdataService.changeEmail(us, dev, request);
		}
		return ResponseEntity.badRequest().body("Please enter valid email");
	}
	
	@GetMapping(path = {"/regions-by-latlon"})
	@ResponseBody
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