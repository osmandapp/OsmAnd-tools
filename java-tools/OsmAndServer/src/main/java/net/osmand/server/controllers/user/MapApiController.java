package net.osmand.server.controllers.user;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;

import net.osmand.server.WebSecurityConfiguration.OsmAndProUser;
import net.osmand.server.api.repo.PremiumUserDevicesRepository.PremiumUserDevice;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;

@Controller
@RequestMapping("/map/api")
public class MapApiController {
	protected static final Log LOGGER = LogFactory.getLog(MapApiController.class);

	@Autowired
	UserdataController userdataController;

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
		return gson.toJson(user);
	}

	private String okStatus() {
		return gson.toJson(Collections.singletonMap("status", "ok"));
	}

	@PostMapping(path = { "/auth/login" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public String loginUser(@RequestBody UserPasswordPost us, HttpServletRequest request) throws ServletException {
		// request.logout();
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
		}
		return res;
	}

	@PostMapping(path = { "/auth/logout" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public String logoutMapUser(HttpServletRequest request) throws ServletException {
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
		OsmAndProUser user = (OsmAndProUser) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
		if (user != null) {
			return user.getUserDevice();
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
		UserFilesResults res = userdataController.generateFiles(dev.userid, name, type, allVersions);
		return ResponseEntity.ok(gson.toJson(res));
	}


	@GetMapping(path = { "/check_download" }, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String checkDownload(@RequestParam(value = "file_name", required = false) String fn,
			@RequestParam(value = "file_size", required = false) String sz) throws IOException {
		return okStatus();
	}
}
