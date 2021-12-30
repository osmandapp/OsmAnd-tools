package net.osmand.server.api.repo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/info")
public class InfoController {

	protected static final Log logger = LogFactory.getLog(InfoController.class);
	
    @RequestMapping("/user")
    public String index(java.security.Principal user) throws Exception {
		String pg = "Authorized page. Information about " +  user.getName() + ": ";
		if (user instanceof OAuth2Authentication) {
			OAuth2Authentication oAuth2Authentication = (OAuth2Authentication) user;
			Authentication authentication = oAuth2Authentication.getUserAuthentication();
			pg += authentication.getDetails();
			pg += authentication.getAuthorities();
		} else if(user instanceof AbstractAuthenticationToken) {
			pg += user.getClass().getName() + ". Roles: " + ((AbstractAuthenticationToken)user).getAuthorities();
		}
		
    	return pg;
    }

}