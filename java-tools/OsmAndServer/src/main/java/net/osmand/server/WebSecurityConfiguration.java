package net.osmand.server;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.env.Profiles;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.client.userinfo.DefaultOAuth2UserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableOAuth2Client;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.user.DefaultOAuth2User;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.util.UriComponentsBuilder;

import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserDevicesRepository.PremiumUserDevice;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.server.api.repo.PremiumUsersRepository.PremiumUser;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.util.Algorithms;

@Configuration
@EnableOAuth2Client
public class WebSecurityConfiguration extends WebSecurityConfigurerAdapter {
	
	protected static final Log LOG = LogFactory.getLog(WebSecurityConfiguration.class);
	public static final String ROLE_PRO_USER = "ROLE_PRO_USER";
	public static final String ROLE_ADMIN = "ROLE_ADMIN";
	public static final String ROLE_USER = "ROLE_USER";
    
    @Value("${admin.api-oauth2-url}")
    private String adminOauth2Url;
    
    @Autowired
	protected PremiumUsersRepository usersRepository;
    
    @Autowired
	protected PremiumUserDevicesRepository devicesRepository;
    
    
    
    
    
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
				PremiumUserDevice pud = devicesRepository.findTopByUseridAndDeviceidOrderByUdpatetimeDesc(pu.id,
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
    	http.cors().configurationSource(corsConfigurationSource());
    	
    	http.authorizeRequests().antMatchers("/actuator/**", "/admin/**").hasAuthority(ROLE_ADMIN)
    							.antMatchers("/mapapi/auth/**").permitAll()
    							.antMatchers("/mapapi/**").hasAuthority(ROLE_PRO_USER)
    							.anyRequest().permitAll();
    	http.oauth2Login().userInfoEndpoint().userService(oauthGithubUserService());

    	
    	// SEE MapApiController.loginForm to test form
//		http.formLogin().loginPage("/mapapi/auth/loginForm").
//				loginProcessingUrl("/mapapi/auth/loginProcess").defaultSuccessUrl("/map/loginSuccess");
		LoginUrlAuthenticationEntryPoint mapLogin = new LoginUrlAuthenticationEntryPoint("/map/loginForm");
		if (getApplicationContext().getEnvironment().acceptsProfiles(Profiles.of("production"))) {
			mapLogin.setForceHttps(true);
		}
		http.exceptionHandling().defaultAuthenticationEntryPointFor(mapLogin, new AntPathRequestMatcher("/mapapi/**"));
		
		http.rememberMe().tokenValiditySeconds(3600*24*14);
		http.logout().deleteCookies("JSESSIONID").
			logoutSuccessUrl("/").logoutRequestMatcher(new AntPathRequestMatcher("/logout")).permitAll();
    	
	}

	private DefaultOAuth2UserService oauthGithubUserService() {
		// authorize with admin for specific group
		RestTemplate restTemplate = new RestTemplate();
		restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
			@Override
			public void handleError(ClientHttpResponse response) throws IOException {
			}
		});
		DefaultOAuth2UserService service = new DefaultOAuth2UserService() {
			@Override
    		public OAuth2User loadUser(OAuth2UserRequest userRequest) throws OAuth2AuthenticationException {
				LOG.warn("User to test");
				OAuth2User user = super.loadUser(userRequest);
				LOG.warn("User to test: " + user);
				if (user == null) {
					return null;
				}
				Set<GrantedAuthority> authorities = new LinkedHashSet<>();
				LOG.warn("Test with adminOauth2Url url: " + adminOauth2Url + " " + user.getAttribute("url"));
				if (!Algorithms.isEmpty(adminOauth2Url) && 
						user.getAttribute("url") != null
						&& user.getAttribute("url").toString().contains("github.com")) {
					Map<String, Object> orgs = checkPermissionAccess(adminOauth2Url, userRequest, user);
					LOG.warn("Get organisations: " + adminOauth2Url + " " + user.getAttribute("url"));
					// orgs.get("privacy").equals("closed");
					if (orgs != null) {
						authorities.add(new SimpleGrantedAuthority(ROLE_ADMIN));
					}
				}
				String userNameAttributeName = userRequest.getClientRegistration().getProviderDetails().
						getUserInfoEndpoint().getUserNameAttributeName();
				LOG.warn("User userNameAttributeName: " + userNameAttributeName);
    			return new DefaultOAuth2User(authorities, user.getAttributes(), userNameAttributeName);
    		}

			private Map<String, Object> checkPermissionAccess(Object orgUrl, OAuth2UserRequest userRequest, OAuth2User user) {
				String organizationUrl = String.valueOf(orgUrl);
				HttpHeaders headers = new HttpHeaders();
				headers.setBearerAuth(userRequest.getAccessToken().getTokenValue());
				headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
				URI uri = UriComponentsBuilder.fromUriString(organizationUrl).build().toUri();
				RequestEntity<?> request = new RequestEntity<>(headers, HttpMethod.GET, uri);
				ResponseEntity<Map<String, Object>> res = restTemplate.exchange(request, 
						new ParameterizedTypeReference<Map<String, Object>>() {});
				if (!res.getStatusCode().is2xxSuccessful()) {
					LOG.warn("Result status code from github: " + res.getStatusCode().name() + " " + res.getBody());
					return null;
				}
				return res.getBody();
			}
    		
    	};
    	return service;
	}

	@Bean("authenticationManager")
	@Override
	public AuthenticationManager authenticationManagerBean() throws Exception {
		return super.authenticationManagerBean();
	}
    

	@Bean
	public PasswordEncoder passwordEncoder() {
		DelegatingPasswordEncoder delegatingPasswordEncoder = (DelegatingPasswordEncoder) PasswordEncoderFactories
				.createDelegatingPasswordEncoder();
		delegatingPasswordEncoder.setDefaultPasswordEncoderForMatches(new BCryptPasswordEncoder());
		return delegatingPasswordEncoder;
	}

	@Bean
	CorsConfigurationSource corsConfigurationSource() {
		CorsConfiguration configuration = new CorsConfiguration();
		configuration.setAllowedOrigins(Arrays.asList("https://maptile.osmand.net",
				"https://docs.osmand.net", "https://osmand.net", "https://www.osmand.net", 
				"https://test.osmand.net", "https://osmbtc.org", "http://localhost:3000"));
		configuration.setAllowCredentials(true);
		configuration.setAllowedMethods(Arrays.asList(CorsConfiguration.ALL));
		configuration.setAllowedHeaders(Arrays.asList("Content-Type"));
		UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
		source.registerCorsConfiguration("/**", configuration);
		return source;
	}

}