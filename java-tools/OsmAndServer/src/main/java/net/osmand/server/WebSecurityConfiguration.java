package net.osmand.server;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.*;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
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
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.session.MapSessionRepository;
import org.springframework.session.data.redis.config.annotation.web.http.EnableRedisHttpSession;
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
import net.osmand.server.api.services.UserdataService;
import net.osmand.util.Algorithms;

@Configuration
@EnableOAuth2Client
public class WebSecurityConfiguration {
	
	protected static final Log LOG = LogFactory.getLog(WebSecurityConfiguration.class);
	public static final String ROLE_PRO_USER = "ROLE_PRO_USER";
	public static final String ROLE_ADMIN = "ROLE_ADMIN";
	public static final String ROLE_SUPPORT = "ROLE_SUPPORT";
	public static final String ROLE_USER = "ROLE_USER";
	private static final int SESSION_TTL_SECONDS = 3600 * 24 * 30;
    
    @Value("${admin.api-oauth2-url}")
    private String adminOauth2Url;

	@Value("${spring.session.redisHost}")
	private String redisHost;

	@Value("${spring.session.redisPort}")
	private String redisPort;


	@Autowired
	protected PremiumUsersRepository usersRepository;
    
    @Autowired
	protected PremiumUserDevicesRepository devicesRepository;
	
	@Autowired
	UserdataService userdataService;

	@Autowired
	private WebAccessConfig webAccessConfig;
    

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

	private boolean isRedisAvailable() {
		return redisHost != null && !redisHost.isEmpty() && redisPort != null && !redisPort.isEmpty();
	}

	@Bean
	@ConditionalOnExpression("T(org.springframework.util.StringUtils).isEmpty('${REDIS_HOST:}') && T(org.springframework.util.StringUtils).isEmpty('${REDIS_PORT:}')")
	public MapSessionRepository mapSessionRepository() {
		if (!isRedisAvailable()) {
			LOG.warn("Redis is not configured, falling back to MapSessionRepository.");
			MapSessionRepository repository = new MapSessionRepository(new ConcurrentHashMap<>());
			repository.setDefaultMaxInactiveInterval(Duration.ofSeconds(SESSION_TTL_SECONDS));
			return repository;
		}
		LOG.info("Redis configuration is detected, skipping MapSessionRepository.");
		return null;
	}

	@Configuration
	@ConditionalOnExpression("!T(org.springframework.util.StringUtils).isEmpty('${REDIS_HOST:}') && !T(org.springframework.util.StringUtils).isEmpty('${REDIS_PORT:}')")
	@EnableRedisHttpSession(maxInactiveIntervalInSeconds = SESSION_TTL_SECONDS)
	public class ConditionalRedisConfig {

		@Bean
		public RedisConnectionFactory redisConnectionFactory() {
			LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
					.commandTimeout(Duration.ofSeconds(5))  // Sets the maximum time to wait for a Redis command to complete
					.shutdownTimeout(Duration.ofMillis(100)) // Defines how long to wait when shutting down the Redis connection
					.build();
			return new LettuceConnectionFactory(
					new RedisStandaloneConfiguration(redisHost, Integer.parseInt(redisPort)), clientConfig);
		}
	}

	@Bean
	public UserDetailsService userDetailsService() {
		return username -> {
			PremiumUser pu = usersRepository.findByEmailIgnoreCase(username);
			if (pu == null) throw new UsernameNotFoundException(username);

			PremiumUserDevice pud = devicesRepository.findTopByUseridAndDeviceidOrderByUdpatetimeDesc(
					pu.id, userdataService.TOKEN_DEVICE_WEB);
			if (pud == null) throw new UsernameNotFoundException(username);

			return new OsmAndProUser(username, pud.accesstoken, pud,
					AuthorityUtils.createAuthorityList(ROLE_PRO_USER));
		};
	}

	@Bean
	public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
		http
				.sessionManagement(session -> session
						.sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED)
						.maximumSessions(1)
				)
				.addFilterBefore((request, response, chain) -> {
					if (request instanceof HttpServletRequest httpRequest) {
						String uri = httpRequest.getRequestURI();
						if (!uri.startsWith("/mapapi") && !uri.startsWith("/share")) {
							httpRequest.getSession(false);
						}
					}
					chain.doFilter(request, response);
				}, org.springframework.security.web.context.SecurityContextPersistenceFilter.class)
				.csrf(csrf -> csrf
						.csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
						.requireCsrfProtectionMatcher(request -> {
							String method = request.getMethod();
							Set<String> enabledMethods = Set.of("GET", "HEAD", "TRACE", "OPTIONS", "POST", "DELETE");
							if (method != null && !enabledMethods.contains(method)) {
								String url = request.getServletPath();
								if (request.getPathInfo() != null) {
									url += request.getPathInfo();
								}
								return !(url.startsWith("/api/") || url.startsWith("/subscription/"));
							}
							return false;
						})
				)
				.cors(cors -> cors.configurationSource(corsConfigurationSource()))
				.authorizeHttpRequests(auth -> auth
						.requestMatchers("/admin/order-management").hasAnyAuthority(ROLE_ADMIN, ROLE_SUPPORT)
						.requestMatchers("/admin/**").hasAuthority(ROLE_USER)
						.requestMatchers("/actuator/**").hasAuthority(ROLE_ADMIN)
						.requestMatchers("/mapapi/auth/**").permitAll()
						.requestMatchers("/mapapi/**").hasAuthority(ROLE_PRO_USER)
						.anyRequest().permitAll()
				)
				.oauth2Login(oauth -> oauth
						.userInfoEndpoint(userInfo -> userInfo.userService(oauthGithubUserService())))
						.exceptionHandling(ex -> ex
								.defaultAuthenticationEntryPointFor(
										new LoginUrlAuthenticationEntryPoint("/map/loginForm"),
										new AntPathRequestMatcher("/mapapi/**"))
						)
						.rememberMe(remember -> remember.tokenValiditySeconds(3600 * 24 * 14))
						.logout(logout -> logout
								.deleteCookies("JSESSIONID")
								.logoutSuccessUrl("/")
								.logoutRequestMatcher(new AntPathRequestMatcher("/logout"))
								.permitAll()
						);

		return http.build();
	}

	private DefaultOAuth2UserService oauthGithubUserService() {
		// authorize with admin for specific group
		RestTemplate restTemplate = new RestTemplate();
		restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
			@Override
			public void handleError(ClientHttpResponse response) throws IOException {
			}
		});
		return new DefaultOAuth2UserService() {
			@Override
    		public OAuth2User loadUser(OAuth2UserRequest userRequest) throws OAuth2AuthenticationException {
				OAuth2User user = super.loadUser(userRequest);
				if (user == null) {
					return null;
				}
				Set<GrantedAuthority> authorities = new LinkedHashSet<>();
				if (!Algorithms.isEmpty(adminOauth2Url) && 
						user.getAttribute("url") != null
						&& user.getAttribute("url").toString().contains("github.com")) {
					Map<String, Object> orgs = checkPermissionAccess(adminOauth2Url, userRequest, user);
					// orgs.get("privacy").equals("closed");
					if (orgs != null) {
						authorities.add(new SimpleGrantedAuthority(ROLE_USER));
					}
				}
				String email = user.getAttribute("email");
				String token = userRequest.getAccessToken().getTokenValue();

				if (email == null) {
					RestTemplate rest = new RestTemplate();
					HttpHeaders headers = new HttpHeaders();
					headers.setBearerAuth(token);
					headers.setAccept(List.of(MediaType.APPLICATION_JSON));
					HttpEntity<Void> entity = new HttpEntity<>(headers);

					ResponseEntity<List<Map<String, Object>>> resp = rest.exchange(
							"https://api.github.com/user/emails",
							HttpMethod.GET, entity,
							new ParameterizedTypeReference<>() {}
					);

					email = resp.getBody().stream()
							.filter(e -> Boolean.TRUE.equals(e.get("primary")) && Boolean.TRUE.equals(e.get("verified")))
							.map(e -> (String) e.get("email"))
							.findFirst()
							.orElse(null);
				}

				if (email != null) {
					if (webAccessConfig.getAdmins().contains(email)) {
						authorities.add(new SimpleGrantedAuthority(ROLE_ADMIN));
					}
					if (webAccessConfig.getSupport().contains(email)) {
						authorities.add(new SimpleGrantedAuthority(ROLE_SUPPORT));
					}
				}

				String userNameAttributeName = userRequest.getClientRegistration().getProviderDetails().
						getUserInfoEndpoint().getUserNameAttributeName();
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
					LOG.warn("Result status code from github: " + res.getStatusCode() + " " + res.getBody());
					return null;
				}
				return res.getBody();
			}
    		
    	};
	}

	@Bean
	public AuthenticationManager authenticationManager(PasswordEncoder passwordEncoder) {
		DaoAuthenticationProvider provider = new DaoAuthenticationProvider();
		provider.setUserDetailsService(userDetailsService());
		provider.setPasswordEncoder(passwordEncoder);
		return new ProviderManager(provider);
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
		source.registerCorsConfiguration("/**",  configuration);
		return source;
	}

}