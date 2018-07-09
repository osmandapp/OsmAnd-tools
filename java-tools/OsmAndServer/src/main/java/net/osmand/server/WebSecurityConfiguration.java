package net.osmand.server;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.security.oauth2.resource.FixedAuthoritiesExtractor;
import org.springframework.boot.autoconfigure.security.oauth2.resource.UserInfoTokenServices;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.oauth2.client.OAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.client.filter.OAuth2ClientAuthenticationProcessingFilter;
import org.springframework.security.oauth2.client.filter.OAuth2ClientContextFilter;
import org.springframework.security.oauth2.client.token.grant.code.AuthorizationCodeResourceDetails;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableOAuth2Client;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;

@Configuration
@EnableOAuth2Client
public class WebSecurityConfiguration extends WebSecurityConfigurerAdapter {
	
    @Autowired
	OAuth2ClientContext oauth2ClientContext;
    
    @Value("${google.resource.userInfoUri}")
    String googleUserInfoUri;
    
    @Value("${admin.emails}")
    private String adminEmails;
    
    private Set<String> adminEmailsSet = new TreeSet<>();
    
    private static final Log LOG = LogFactory.getLog(WebSecurityConfiguration.class);
    
    public static final String ROLE_ADMIN = "ROLE_ADMIN";
    public static final String ROLE_USER = "ROLE_USER";

	
    @Override
	protected void configure(HttpSecurity http) throws Exception {
    	for(String admin: adminEmails.split(",")) {
    		adminEmailsSet.add(admin.trim());
    	}
    	LOG.info("Admin logins are:" + adminEmailsSet);
    	// http.csrf().disable().antMatcher("/**");
    	http.csrf().csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse());
    	
    	// all top level are accessible without login
    	http.authorizeRequests().antMatchers("/actuator/**", "/admin/**").hasAuthority(ROLE_ADMIN)
    							.antMatchers("/", "/*", "/login/**", "/webjars/**", "/error/**", "/tracker/s/**").permitAll()
    							.anyRequest().authenticated();
		http.exceptionHandling().authenticationEntryPoint(new LoginUrlAuthenticationEntryPoint("/login"));
		http.logout().logoutSuccessUrl("/").permitAll();
		http.addFilterBefore(ssoFilter(), BasicAuthenticationFilter.class);
    	
	}

	@Bean
	public FilterRegistrationBean<OAuth2ClientContextFilter> oauth2ClientFilterRegistration(OAuth2ClientContextFilter filter) {
		FilterRegistrationBean<OAuth2ClientContextFilter> registration = new FilterRegistrationBean<OAuth2ClientContextFilter>();
		registration.setFilter(filter);
		registration.setOrder(-100);
		return registration;
	}
	
    private javax.servlet.Filter ssoFilter() {
		OAuth2ClientAuthenticationProcessingFilter filter = new OAuth2ClientAuthenticationProcessingFilter("/login");
		OAuth2RestTemplate template = new OAuth2RestTemplate(google(), oauth2ClientContext);
		filter.setRestTemplate(template);
		UserInfoTokenServices tokenServices = new UserInfoTokenServices(googleUserInfoUri,
				google().getClientId());
		// sub (id), name, email, picture - picture url, email_verified, gender
		tokenServices.setAuthoritiesExtractor(new FixedAuthoritiesExtractor() {
			@Override
			public List<GrantedAuthority> extractAuthorities(Map<String, Object> map) {
				Object email = map.get("email");
				if(adminEmailsSet.contains(email) && 
						"true".equals(map.get("email_verified") + "")) {
					LOG.warn("Admin '" + email + "' logged in");
					return AuthorityUtils.createAuthorityList(ROLE_USER, ROLE_ADMIN);
				}
				LOG.info("User '" + email + "' logged in");
				return AuthorityUtils.createAuthorityList(ROLE_USER);
			}
		});
		// tokenServices.setPrincipalExtractor();
		tokenServices.setRestTemplate(template);
		filter.setTokenServices(tokenServices);
		return filter;
	}
    

	@Bean
	@ConfigurationProperties("google.client")
	public AuthorizationCodeResourceDetails google() {
		return new AuthorizationCodeResourceDetails();
	}
 
	
}