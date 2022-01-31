package net.osmand.server;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpServletRequest;

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
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.client.OAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.client.filter.OAuth2ClientAuthenticationProcessingFilter;
import org.springframework.security.oauth2.client.filter.OAuth2ClientContextFilter;
import org.springframework.security.oauth2.client.token.grant.code.AuthorizationCodeResourceDetails;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableOAuth2Client;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;

import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserDevicesRepository.PremiumUserDevice;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.server.api.repo.PremiumUsersRepository.PremiumUser;
import net.osmand.server.controllers.pub.UserdataController;

@Configuration
@EnableOAuth2Client
public class WebSecurityConfiguration extends WebSecurityConfigurerAdapter {
	
    @Autowired
	OAuth2ClientContext oauth2ClientContext;
    
    @Value("${google.resource.userInfoUri}")
    String googleUserInfoUri;
    
    @Value("${admin.emails}")
    private String adminEmails;
    
    @Autowired
	protected PremiumUsersRepository usersRepository;
    
    @Autowired
	protected PremiumUserDevicesRepository devicesRepository;
    
    
    private Set<String> adminEmailsSet = new TreeSet<>();
    
    private static final Log LOG = LogFactory.getLog(WebSecurityConfiguration.class);
    
    public static final String ROLE_ADMIN = "ROLE_ADMIN";
    public static final String ROLE_PRO_USER = "ROLE_PRO_USER";
    public static final String ROLE_USER = "ROLE_USER";
    
    
	public static class OsmAndProUser extends User {

		private static final long serialVersionUID = -881322456618342435L;
		PremiumUserDevice userDevice;

		public OsmAndProUser(String username, String password, PremiumUserDevice pud,
				List<GrantedAuthority> authorities) {
			super(username, password, authorities);
			this.userDevice = pud;
		}

		public PremiumUserDevice getUserDevice() {
			return userDevice;
		}
	}
    

    @Override
    protected void configure(final AuthenticationManagerBuilder auth) throws Exception {
    	auth.userDetailsService(new UserDetailsService() {
    	    @Override
			public UserDetails loadUserByUsername(String username) {
				PremiumUser pu = usersRepository.findByEmail(username);
				if (pu == null) {
					throw new UsernameNotFoundException(username);
				}
				PremiumUserDevice pud = devicesRepository.findTopByUseridAndDeviceidOrderByUpdatetimeDesc(pu.id,
						UserdataController.TOKEN_DEVICE_WEB);
				if (pud == null) {
					throw new UsernameNotFoundException(username);
				}
				
				return new OsmAndProUser(username, pud.accesstoken, pud,
						AuthorityUtils.createAuthorityList(WebSecurityConfiguration.ROLE_PRO_USER));
			}
    	});
    }
    
    @Override
	protected void configure(HttpSecurity http) throws Exception {
    	// http.csrf().disable().antMatcher("/**");
    	// 1. CSRF
    	Set<String> enabledMethods = new TreeSet<>(
    			Arrays.asList("GET", "HEAD", "TRACE", "OPTIONS", "POST", "DELETE"));
    	http.csrf().requireCsrfProtectionMatcher(new RequestMatcher() {
			
			@Override
			public boolean matches(HttpServletRequest request) {
				String method = request.getMethod();
				if(method != null && !enabledMethods.contains(method)) {
					String url = request.getServletPath();
					if (request.getPathInfo() != null) {
						url += request.getPathInfo();
					}
					if(url.startsWith("/api/") || url.startsWith("/subscription/")) {
						return false;
					}
					return true;
				}
				return false;
			}
		}).csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse());
    	
    	// 2. Configure admins - all top level are accessible without login
    	for (String admin : adminEmails.split(",")) {
			adminEmailsSet.add(admin.trim());
		}
    	LOG.info("Admin logins are:" + adminEmailsSet);
    	http.authorizeRequests().antMatchers("/actuator/**", "/admin/**").hasAuthority(ROLE_ADMIN)
    							.antMatchers("/map/api/auth/**").permitAll()
    							.antMatchers("/map/api/**").hasAuthority(ROLE_PRO_USER)
    							.antMatchers("/u/**").hasAnyAuthority(ROLE_USER, ROLE_PRO_USER, ROLE_ADMIN) // user
//    							.antMatchers("/", "/*", "/login/**", "/webjars/**", "/error/**").permitAll()
    							.anyRequest().permitAll();
    	LoginUrlAuthenticationEntryPoint oauthAdminLogin = new LoginUrlAuthenticationEntryPoint("/login");
		if (getApplicationContext().getEnvironment().acceptsProfiles("production")) {
			oauthAdminLogin.setForceHttps(true);
		}
		http.formLogin().loginPage("/map/api/auth/loginForm").
				loginProcessingUrl("/map/api/auth/loginProcess").defaultSuccessUrl("/map/loginSuccess");
		LoginUrlAuthenticationEntryPoint mapLogin = new LoginUrlAuthenticationEntryPoint("/map/loginForm");
		if (getApplicationContext().getEnvironment().acceptsProfiles("production")) {
			mapLogin.setForceHttps(true);
		}
		http.exceptionHandling()
				.defaultAuthenticationEntryPointFor(mapLogin, new AntPathRequestMatcher("/map/api/**"))
				.defaultAuthenticationEntryPointFor(oauthAdminLogin, new AntPathRequestMatcher("**"));
		http.addFilterBefore(ssoFilter("/login"), BasicAuthenticationFilter.class);
        
		http.rememberMe().tokenValiditySeconds(3600*24*14);
		http.logout().deleteCookies("JSESSIONID").
			logoutSuccessUrl("/").logoutRequestMatcher(new AntPathRequestMatcher("/logout")).permitAll();
    	
	}

	@Bean
	public FilterRegistrationBean<OAuth2ClientContextFilter> oauth2ClientFilterRegistration(OAuth2ClientContextFilter filter) {
		FilterRegistrationBean<OAuth2ClientContextFilter> registration = new FilterRegistrationBean<OAuth2ClientContextFilter>();
		registration.setFilter(filter);
		registration.setOrder(-100);
		return registration;
	}
	
    private javax.servlet.Filter ssoFilter(String url) {
		OAuth2ClientAuthenticationProcessingFilter filter = new OAuth2ClientAuthenticationProcessingFilter(url);
		OAuth2RestTemplate template = new OAuth2RestTemplate(google(), oauth2ClientContext);
		filter.setRestTemplate(template);
		UserInfoTokenServices tokenServices = new UserInfoTokenServices(googleUserInfoUri,
				google().getClientId());
		// sub (id), name, email, picture - picture url, email_verified, gender
		tokenServices.setAuthoritiesExtractor(new FixedAuthoritiesExtractor() {
			@Override
			public List<GrantedAuthority> extractAuthorities(Map<String, Object> map) {
				Object email = map.get("email");
				if (adminEmailsSet.contains(email) && "true".equals(map.get("email_verified") + "")) {
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
    
	@Bean("authenticationManager")
	@Override
	public AuthenticationManager authenticationManagerBean() throws Exception {
		return super.authenticationManagerBean();
	}
    

	@Bean
	@ConfigurationProperties("google.client")
	public AuthorizationCodeResourceDetails google() {
		return new AuthorizationCodeResourceDetails();
	}
	
	@Bean
	public PasswordEncoder passwordEncoder() {
	    DelegatingPasswordEncoder delegatingPasswordEncoder = 
	    		(DelegatingPasswordEncoder) PasswordEncoderFactories.createDelegatingPasswordEncoder();
	    delegatingPasswordEncoder.setDefaultPasswordEncoderForMatches(new BCryptPasswordEncoder());
		return delegatingPasswordEncoder;
	}
	
}