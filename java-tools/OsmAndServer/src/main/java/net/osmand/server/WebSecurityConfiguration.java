package net.osmand.server;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;

import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.*;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableOAuth2Client;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.session.MapSessionRepository;
import org.springframework.session.data.redis.config.annotation.web.http.EnableRedisHttpSession;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.services.UserdataService;

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
	protected CloudUsersRepository usersRepository;
    
    @Autowired
	protected CloudUserDevicesRepository devicesRepository;
	
	@Autowired
	UserdataService userdataService;

	@Autowired
	private WebAccessConfig webAccessConfig;
    

	public static class OsmAndProUser extends User {

		private static final long serialVersionUID = -881322456618342435L;
		CloudUserDevice userDevice;

		public OsmAndProUser(String username, String password, CloudUserDevice pud,
				List<GrantedAuthority> authorities) {
			super(username, password, authorities);
			this.userDevice = pud;
		}

		public CloudUserDevice getUserDevice() {
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
			CloudUser pu = usersRepository.findByEmailIgnoreCase(username);
			if (pu == null) throw new UsernameNotFoundException(username);

			CloudUserDevice pud = devicesRepository
					.findTopByUseridAndDeviceidOrderByUdpatetimeDesc(pu.id, UserdataService.TOKEN_DEVICE_WEB);
			if (pud == null) throw new UsernameNotFoundException(username);

			Set<GrantedAuthority> auths = new LinkedHashSet<>();
			auths.add(new SimpleGrantedAuthority(ROLE_PRO_USER));

			String email = pu.email;
			if (webAccessConfig.getAdmins().contains(email)) {
				auths.add(new SimpleGrantedAuthority(ROLE_ADMIN));
			}
			if (webAccessConfig.getSupport().contains(email)) {
				auths.add(new SimpleGrantedAuthority(ROLE_SUPPORT));
			}

			return new OsmAndProUser(username, pud.accesstoken, pud, new ArrayList<>(auths));
		};
	}


	@Bean
	public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
		LoginUrlAuthenticationEntryPoint adminEntryPoint =
				new LoginUrlAuthenticationEntryPoint("/map/account/") {
					@Override
					protected String determineUrlToUseForThisRequest(
							HttpServletRequest request,
							HttpServletResponse response,
							AuthenticationException exception) {

						String target = request.getRequestURI()
								+ (request.getQueryString() != null ? "?" + request.getQueryString() : "");
						String loginUrl = super.determineUrlToUseForThisRequest(request, response, exception);
						return loginUrl
								+ "?redirect="
								+ URLEncoder.encode(target, StandardCharsets.UTF_8);
					}
				};
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
						.requestMatchers("/admin/security-error").permitAll()
						.requestMatchers("/admin/releases/**").hasAnyAuthority(ROLE_ADMIN, ROLE_SUPPORT)
//						.requestMatchers("/admin/issues/**").hasAnyAuthority(ROLE_ADMIN, ROLE_SUPPORT)
						.requestMatchers("/admin/issues/**").permitAll()
						.requestMatchers("/admin/test/**").permitAll()
						.requestMatchers("/admin/order-mgmt/**").hasAnyAuthority(ROLE_ADMIN, ROLE_SUPPORT)
						.requestMatchers("/admin/**").hasAuthority(ROLE_ADMIN)
						.requestMatchers("/actuator/**").hasAuthority(ROLE_ADMIN)
						.requestMatchers("/mapapi/auth/**").permitAll()
						.requestMatchers("/mapapi/**").hasAuthority(ROLE_PRO_USER)
						.anyRequest().permitAll()
				)
				.exceptionHandling(ex -> ex
						.accessDeniedHandler(new AccessDeniedHandler() {
							@Override
							public void handle(HttpServletRequest request,
											   HttpServletResponse response,
											   AccessDeniedException accessDeniedException) throws IOException, ServletException {
								response.setStatus(HttpServletResponse.SC_FORBIDDEN);
								request.getRequestDispatcher(request.getContextPath() + "/admin/security-error").forward(request, response);
							}
						})
						.defaultAuthenticationEntryPointFor(
								adminEntryPoint,
								new AntPathRequestMatcher("/mapapi/**"))
						.defaultAuthenticationEntryPointFor(
								adminEntryPoint,
								new AntPathRequestMatcher("/admin/**")
						)
				)
				.rememberMe(rm -> rm
						.tokenValiditySeconds(3600 * 24 * 14)
				)
				.logout(logout -> logout
						.deleteCookies("JSESSIONID")
						.logoutSuccessUrl("/")
						.logoutRequestMatcher(new AntPathRequestMatcher("/logout"))
						.permitAll()
				);

		return http.build();
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