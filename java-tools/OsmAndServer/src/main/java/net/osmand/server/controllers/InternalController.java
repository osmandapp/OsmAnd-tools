package net.osmand.server.controllers;

import net.osmand.MapCreatorVersion;

import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InternalController {

    @RequestMapping("/internal")
    public String index(java.security.Principal user) throws Exception {
//    	IndexCreator.main(null);
		String pg = "Internal page. Greetings from: " + MapCreatorVersion.APP_MAP_CREATOR_FULL_NAME + " "
				+ user.getName();
		if (user instanceof OAuth2Authentication) {
			OAuth2Authentication oAuth2Authentication = (OAuth2Authentication) user;
			Authentication authentication = oAuth2Authentication.getUserAuthentication();
			pg += authentication.getDetails();
		}
    	return pg;
    }

}