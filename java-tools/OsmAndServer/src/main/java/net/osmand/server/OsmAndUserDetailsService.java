package net.osmand.server;

import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.stereotype.Service;

@Service
public class OsmAndUserDetailsService implements UserDetailsService {


    @Override
    public UserDetails loadUserByUsername(String username) {
    	// TODO load from database
    	return new User(username, "{noop}test", AuthorityUtils.createAuthorityList(WebSecurityConfiguration.ROLE_PRO_USER));
    }
    

}