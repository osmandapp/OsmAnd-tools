package net.osmand.server.api.repo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/info")
public class InfoController {

	protected static final Log logger = LogFactory.getLog(InfoController.class);
	
    @RequestMapping("/user")
    public String index(java.security.Principal user) throws Exception {
		if (user == null) {
			return "Non authorized";
		}
    	String pg = "Authorized page. Information about " +  user.getName() + ": ";
		if (user instanceof AbstractAuthenticationToken) {
			pg += user.getClass().getName() + ".\n Roles: " + ((AbstractAuthenticationToken) user).getAuthorities();
			pg += ".\n Details : " + ((AbstractAuthenticationToken) user).getDetails();
			// pg += ".\n Principal : " + ((AbstractAuthenticationToken)user).getPrincipal();
		}
		
    	return pg;
    }

}