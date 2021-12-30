package net.osmand.server.controllers.user;

import java.io.IOException;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;

import net.osmand.server.OsmAndProUserDetailsService;

@Controller
@RequestMapping("/map/api")
public class MapApiController {
    protected static final Log LOGGER = LogFactory.getLog(MapApiController.class);
    
    @Autowired
    OsmAndProUserDetailsService userDetailsService;
	
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
	
    
    @GetMapping(path = { "/auth/info" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public String userInfo(java.security.Principal user) {
		String pg = "Authorized page. Information about " +  user.getName() + ": ";
		if (user instanceof OAuth2Authentication) {
			OAuth2Authentication oAuth2Authentication = (OAuth2Authentication) user;
			Authentication authentication = oAuth2Authentication.getUserAuthentication();
			pg += authentication.getDetails();
			pg += authentication.getAuthorities();
		} else if(user instanceof AbstractAuthenticationToken) {
			pg += user.getClass().getName() + ". Roles: " + ((AbstractAuthenticationToken)user).getAuthorities();
		}
		System.out.println(pg);
		return gson.toJson(user);
	}


	private String okStatus() {
		return gson.toJson(Collections.singletonMap("status", "OK"));
	}
    
	@PostMapping(path = { "/auth/activate" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public String activateMapUser(@RequestBody UserPasswordPost us, HttpServletRequest request) throws ServletException {
		userDetailsService.activateUser(us.username, us.password);
		request.logout();
		request.login(us.username, us.password);
		return okStatus();
	}
	
	@PostMapping(path = { "/auth/logout" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public String logoutMapUser(HttpServletRequest request) throws ServletException {
		request.logout();
		return okStatus();
	}
	
	@PostMapping(path = { "/auth/register" }, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public String registerMapUser(@RequestBody UserPasswordPost us, HttpServletRequest request) throws ServletException {
		userDetailsService.registerUser(us.username);
		return okStatus();
	}

    @GetMapping(path = { "/check_download" }, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String checkDownload(@RequestParam(value = "file_name", required = false) String fn,
			@RequestParam(value = "file_size", required = false) String sz) throws IOException {
    	return okStatus();
	}
}
