package net.osmand.server;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
public class OsmAndProUserDetailsService implements UserDetailsService {

	// TODO move to db
	public Map<String, String> users = new LinkedHashMap<>();
	
	@Autowired
	PasswordEncoder encoder;

    @Override
	public UserDetails loadUserByUsername(String username) {
		String pwd = users.get(username);
		if (pwd == null) {
			return null;
		}
		// TODO load from database
		return new User(username, pwd, AuthorityUtils.createAuthorityList(WebSecurityConfiguration.ROLE_PRO_USER));
	}

	public void activateUser(String username, String password) {
		// TODO Auto-generated method stub
		String encodedPwd = encoder.encode(password);
		users.put(username, encodedPwd);
	}

	public void registerUser(String username) {
		// TODO Auto-generated method stub
		// TODO send email
		
	}
    

}