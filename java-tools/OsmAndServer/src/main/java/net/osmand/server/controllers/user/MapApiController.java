package net.osmand.server.controllers.user;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.ClassPathResource;
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
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.osmand.GPXUtilities;
import net.osmand.GPXUtilities.GPXFile;
import net.osmand.GPXUtilities.GPXTrackAnalysis;
import net.osmand.server.WebSecurityConfiguration.OsmAndProUser;
import net.osmand.server.api.repo.PremiumUserDevicesRepository.PremiumUserDevice;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFile;
import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFileNoData;
import net.osmand.server.controllers.pub.GpxController;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;

@Controller
@RequestMapping("/mapapi")
public class MapApiController {

	protected static final Log LOGGER = LogFactory.getLog(MapApiController.class);
	private static final String ANALYSIS = "analysis";
	private static final String SRTM_ANALYSIS = "srtm-analysis";
	private static final String DONE_SUFFIX = "-done";
	private static final long ANALYSIS_RERUN = 1645060571000l; // 17-02-2022
											   

	@Autowired
	UserdataController userdataController;
	
	@Autowired
	GpxController gpxController;
	
	@Autowired
	PremiumUserFilesRepository userFilesRepository;
	
	@Autowired
	AuthenticationManager authManager;
	
	Gson gson = new Gson();

	public static class UserPasswordPost {
		public String username;
		public String password;
		public String token;
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

	@PostMapping(path = { "/auth/activate" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> activateMapUser(@RequestBody UserPasswordPost us, HttpServletRequest request)
			throws ServletException, IOException {
		ResponseEntity<String> res = userdataController.webUserActivate(us.username, us.token, us.password);
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
		return userdataController.webUserRegister(us.username);
	}
	
	private ResponseEntity<String> tokenNotValid() {
	    return new ResponseEntity<String>("Unauthorized", HttpStatus.UNAUTHORIZED);

	}

	private PremiumUserDevice checkUser() {
		Object user = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
		if (user instanceof OsmAndProUser) {
			return ((OsmAndProUser) user).getUserDevice();
		}
		return null;
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
		UserFilesResults res = userdataController.generateFiles(dev.userid, name, type, allVersions, true);
		for (UserFileNoData nd : res.uniqueFiles) {
			String ext = nd.name.substring(nd.name.lastIndexOf('.') + 1);
			if (nd.type.equalsIgnoreCase("gpx") && ext.equalsIgnoreCase("gpx") && !analysisPresent(ANALYSIS, nd.details)) {
				GPXTrackAnalysis analysis = null;
				Optional<UserFile> of = userFilesRepository.findById(nd.id);
				UserFile uf = of.get();
				if (uf != null) {
					try {
						InputStream in = uf.data != null ? new ByteArrayInputStream(uf.data)
								: userdataController.getInputStream(uf);
						if (in != null) {
							GPXFile gpxFile = GPXUtilities.loadGPXFile(new GZIPInputStream(in));
							if (gpxFile != null) {
								analysis = getAnalysis(uf, gpxFile);
							}
						}
					} catch (RuntimeException e) {
					}
					saveAnalysis(ANALYSIS, uf, analysis);
					nd.details = uf.details.deepCopy();
				}
			}
			if (analysisPresent(ANALYSIS, nd.details)) {
				nd.details.get(ANALYSIS).getAsJsonObject().remove("speedData");
				nd.details.get(ANALYSIS).getAsJsonObject().remove("elevationData");
			}
			if (analysisPresent(SRTM_ANALYSIS, nd.details)) {
				nd.details.get(SRTM_ANALYSIS).getAsJsonObject().remove("speedData");
				nd.details.get(SRTM_ANALYSIS).getAsJsonObject().remove("elevationData");
			}
		}
		return ResponseEntity.ok(gson.toJson(res));
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
		userdataController.getFile(response, request, name, type, updatetime, dev);
	}
	
	@SuppressWarnings("unchecked")
	@GetMapping(value = "/get-gpx-info")
	@ResponseBody
	public ResponseEntity<String> getGpxInfo(HttpServletResponse response, HttpServletRequest request,
			@RequestParam(name = "name", required = true) String name,
			@RequestParam(name = "type", required = true) String type,
			@RequestParam(name = "updatetime", required = false) Long updatetime) throws IOException, SQLException {
		PremiumUserDevice dev = checkUser();
		InputStream bin = null;
		try {
			ResponseEntity<String>[] error = new ResponseEntity[] { null };
			UserFile userFile = userdataController.getUserFile(name, type, updatetime, dev);
			if (analysisPresent(ANALYSIS, userFile)) {
				return ResponseEntity.ok(gson.toJson(Collections.singletonMap("info", userFile.details.get(ANALYSIS))));
			}
			bin = userdataController.getInputStream(dev, error, userFile);
			ResponseEntity<String> err = error[0];
			if (err != null) {
				response.setStatus(err.getStatusCodeValue());
				response.getWriter().write(err.getBody());
				return err;
			}
			GPXFile gpxFile = GPXUtilities.loadGPXFile(new GZIPInputStream(bin));
			if (gpxFile == null) {
				return ResponseEntity.badRequest().body(String.format("File %s not found", userFile.name));
			}
			GPXTrackAnalysis analysis = getAnalysis(userFile, gpxFile);
			if (!analysisPresent(ANALYSIS, userFile)) {
				saveAnalysis(ANALYSIS, userFile, analysis);
			}
			return ResponseEntity.ok(gson.toJson(Collections.singletonMap("info", analysis)));
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
		gpxController.cleanupFromNan(analysis);
		return analysis;
	}

	private void saveAnalysis(String tag, UserFile file, GPXTrackAnalysis analysis) {
		if (file.details == null) {
			file.details = new JsonObject();
		}
		// store data in db to speed up retrieval
		// clear speed data 
//		if (analysis != null) {
//			analysis.speedData.clear();
//			analysis.elevationData.clear();
//		}
		file.details.add(tag, gson.toJsonTree(analysis));
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
			@SuppressWarnings("unchecked")
			ResponseEntity<String>[] error = new ResponseEntity[] { null };
			UserFile userFile = userdataController.getUserFile(name, type, updatetime, dev);
			if (analysisPresent(SRTM_ANALYSIS, userFile)) {
				return ResponseEntity.ok(gson.toJson(Collections.singletonMap("info", userFile.details.get(SRTM_ANALYSIS))));
			}
			bin = userdataController.getInputStream(dev, error, userFile);
			ResponseEntity<String> err = error[0];
			if (err != null) {
				response.setStatus(err.getStatusCodeValue());
				response.getWriter().write(err.getBody());
				return err;
			}
			GPXFile gpxFile = GPXUtilities.loadGPXFile(new GZIPInputStream(bin));
			if (gpxFile == null) {
				return ResponseEntity.badRequest().body(String.format("File %s not found", userFile.name));
			}
			GPXFile srtmGpx = gpxController.calculateSrtmAltitude(gpxFile, null);
			GPXTrackAnalysis analysis = srtmGpx == null ? null : getAnalysis(userFile, srtmGpx);
			if (!analysisPresent(SRTM_ANALYSIS, userFile)) {
				saveAnalysis(SRTM_ANALYSIS, userFile, analysis);
			}
			return ResponseEntity.ok(gson.toJson(Collections.singletonMap("info", analysis)));
		} finally {
			if (bin != null) {
				bin.close();
			}
		}
	}
	
	
	@GetMapping(path = { "/check_download" }, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public ResponseEntity<String> checkDownload(@RequestParam(value = "file_name", required = false) String fn,
			@RequestParam(value = "file_size", required = false) String sz) throws IOException {
		return okStatus();
	}
}
