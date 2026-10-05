package net.osmand.server.controllers.pub;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.osmand.server.WebSecurityConfiguration.OsmAndProUser;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.repo.OAuthClientsRepository.OAuthClient;
import net.osmand.server.api.services.OAuthService;
import net.osmand.server.api.services.OAuthService.OAuthException;
import net.osmand.server.api.services.OAuthService.RegisteredClient;
import net.osmand.server.api.services.OAuthService.Tokens;

/**
 * OAuth 2.1 endpoints for MCP clients (see OAuthService) and the account settings to turn them off.
 * Sign-in reuses the OsmAnd Cloud web login (/map/account/).
 */
@RestController
@ConditionalOnProperty(name = "osmand.oauth.enabled", havingValue = "true")
public class OAuthController {

	protected static final Log LOG = LogFactory.getLog(OAuthController.class);

	public static final String AUTHORIZATION_SERVER_PATH = "/.well-known/oauth-authorization-server";
	public static final String PROTECTED_RESOURCE_PATH = "/.well-known/oauth-protected-resource";
	private static final String LOGIN_PAGE = "/map/account/";
	private static final String PENDING_ATTR = "oauthPending:";
	private static final String TEMPLATE = "pub/oauth";

	@Autowired
	private OAuthService oauthService;

	@Autowired
	private CloudUsersRepository usersRepository;

	@Autowired
	private TemplateEngine templateEngine;

	private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	// authorization request kept in the session between the consent page and the decision
	private record PendingAuthorization(int userId, String clientId, String redirectUri, String state,
	                                    String codeChallenge, String resource) implements java.io.Serializable {
	}

	public static String baseUrl() {
		return ServletUriComponentsBuilder.fromCurrentContextPath().build().toUriString();
	}

	// ---------- discovery ----------

	@GetMapping(path = AUTHORIZATION_SERVER_PATH, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> authorizationServerMetadata() {
		String base = baseUrl();
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("issuer", base);
		m.put("authorization_endpoint", base + "/oauth/authorize");
		m.put("token_endpoint", base + "/oauth/token");
		m.put("registration_endpoint", base + "/oauth/register");
		m.put("revocation_endpoint", base + "/oauth/revoke");
		m.put("response_types_supported", List.of("code"));
		m.put("grant_types_supported", List.of("authorization_code", "refresh_token"));
		m.put("code_challenge_methods_supported", List.of("S256"));
		m.put("token_endpoint_auth_methods_supported", List.of(OAuthService.AUTH_METHOD_NONE,
				OAuthService.AUTH_METHOD_POST, OAuthService.AUTH_METHOD_BASIC));
		m.put("scopes_supported", List.copyOf(OAuthService.SCOPES));
		m.put("authorization_response_iss_parameter_supported", true);
		return json(HttpStatus.OK, m);
	}

	@GetMapping(path = { PROTECTED_RESOURCE_PATH, PROTECTED_RESOURCE_PATH + McpController.MCP_PATH },
			produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> protectedResourceMetadata() {
		String base = baseUrl();
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("resource", base + McpController.MCP_PATH);
		m.put("authorization_servers", List.of(base));
		m.put("scopes_supported", List.copyOf(OAuthService.SCOPES));
		m.put("bearer_methods_supported", List.of("header"));
		m.put("resource_name", "OsmAnd");
		return json(HttpStatus.OK, m);
	}

	// ---------- client registration (RFC 7591) ----------

	public static class RegistrationRequest {
		public List<String> redirect_uris;
		public String client_name;
		public String token_endpoint_auth_method;
	}

	@PostMapping(path = "/oauth/register", consumes = MediaType.APPLICATION_JSON_VALUE,
			produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> register(@RequestBody String body, HttpServletRequest request) {
		RegistrationRequest r;
		try {
			r = gson.fromJson(body, RegistrationRequest.class);
		} catch (RuntimeException e) {
			r = null;
		}
		if (r == null) {
			return oauthError(new OAuthException("invalid_client_metadata", "JSON body expected"));
		}
		try {
			RegisteredClient rc = oauthService.registerClient(r.client_name,
					r.redirect_uris, r.token_endpoint_auth_method);
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("client_id", rc.client.clientid);
			if (rc.secret != null) {
				m.put("client_secret", rc.secret);
				m.put("client_secret_expires_at", 0);
			}
			m.put("client_id_issued_at", rc.client.createtime.getTime() / 1000);
			m.put("client_name", rc.client.clientname);
			m.put("redirect_uris", List.of(rc.client.redirecturis.split(" ")));
			m.put("grant_types", List.of("authorization_code", "refresh_token"));
			m.put("response_types", List.of("code"));
			m.put("token_endpoint_auth_method", rc.authMethod);
			return json(HttpStatus.CREATED, m);
		} catch (OAuthException e) {
			return oauthError(e);
		}
	}

	// ---------- authorization ----------

	@GetMapping(path = "/oauth/authorize", produces = MediaType.TEXT_HTML_VALUE)
	public ResponseEntity<String> authorize(@RequestParam(name = "response_type", required = false) String responseType,
	                                        @RequestParam(name = "client_id", required = false) String clientId,
	                                        @RequestParam(name = "redirect_uri", required = false) String redirectUri,
	                                        @RequestParam(name = "state", required = false) String state,
	                                        @RequestParam(name = "scope", required = false) String scope,
	                                        @RequestParam(name = "code_challenge", required = false) String codeChallenge,
	                                        @RequestParam(name = "code_challenge_method", required = false) String challengeMethod,
	                                        @RequestParam(name = "resource", required = false) String resource,
	                                        HttpServletRequest request) {
		// never redirect to an unverified redirect_uri
		OAuthClient client = oauthService.getClient(clientId);
		if (client == null) {
			return message(HttpStatus.BAD_REQUEST, "Unknown application",
					"This application is not registered. Connect it again from the assistant.", List.of());
		}
		if (!oauthService.isRegisteredRedirect(client, redirectUri)) {
			return message(HttpStatus.BAD_REQUEST, "Invalid request",
					"The return address of the application does not match its registration.", List.of());
		}
		if (!"code".equals(responseType)) {
			return redirect(redirectUri, Map.of("error", "unsupported_response_type"), state);
		}
		if (codeChallenge == null || codeChallenge.length() < 43 || !"S256".equals(challengeMethod)) {
			return redirect(redirectUri, Map.of("error", "invalid_request",
					"error_description", "PKCE with S256 is required"), state);
		}
		if (resource != null && !isOurResource(resource)) {
			return redirect(redirectUri, Map.of("error", "invalid_target"), state);
		}
		CloudUserDevice dev = currentUser();
		if (dev == null) {
			String back = request.getRequestURI() + "?" + request.getQueryString();
			return ResponseEntity.status(HttpStatus.FOUND)
					.header(HttpHeaders.LOCATION, LOGIN_PAGE + "?redirect=" + urlEncode(back)).build();
		}
		CloudUser pu = usersRepository.findById(dev.userid);
		String host = OAuthService.redirectHost(redirectUri);
		Link back = new Link("Return to " + host, redirectUrl(redirectUri, Map.of("error", "access_denied"), state));
		if (!oauthService.isPro(pu)) {
			return message(HttpStatus.OK, "OsmAnd Pro required",
					"Connecting AI assistants such as " + client.clientname + " is part of OsmAnd Pro.",
					List.of(new Link("See OsmAnd Pro", "/pricing"), back));
		}
		if (!oauthService.isEnabled(pu)) {
			return message(HttpStatus.OK, "Turn on AI assistants", "To connect " + client.clientname
					+ ", first turn on AI assistants in your OsmAnd account settings, then connect again.",
					List.of(new Link("Open account settings", LOGIN_PAGE), back));
		}
		String nonce = UUID.randomUUID().toString();
		request.getSession(true).setAttribute(PENDING_ATTR + nonce, new PendingAuthorization(dev.userid,
				client.clientid, redirectUri, state, codeChallenge, resource));
		Context ctx = new Context();
		ctx.setVariable("consent", true);
		ctx.setVariable("client", client.clientname);
		ctx.setVariable("initial", initial(client.clientname));
		ctx.setVariable("email", pu.email);
		ctx.setVariable("emailInitial", initial(pu.email));
		ctx.setVariable("host", host);
		ctx.setVariable("nonce", nonce);
		ctx.setVariable("groups", OAuthService.SCOPE_GROUPS);
		ctx.setVariable("checked", oauthService.defaultScopes(scope));
		return page(HttpStatus.OK, client.clientname + " wants to connect to OsmAnd", ctx);
	}

	@PostMapping(path = "/oauth/authorize")
	public ResponseEntity<String> authorizeDecision(@RequestParam String nonce, @RequestParam String decision,
	                                                @RequestParam(name = "scope", required = false) List<String> scope,
	                                                HttpServletRequest request) {
		HttpSession session = request.getSession(false);
		Object o = session == null ? null : session.getAttribute(PENDING_ATTR + nonce);
		CloudUserDevice dev = currentUser();
		if (!(o instanceof PendingAuthorization p) || dev == null || dev.userid != p.userId) {
			return message(HttpStatus.BAD_REQUEST, "Request expired",
					"This request is no longer valid. Start the connection again from the assistant.", List.of());
		}
		session.removeAttribute(PENDING_ATTR + nonce);
		OAuthClient client = oauthService.getClient(p.clientId);
		String granted = oauthService.grantedScope(scope);
		if (client == null || !"allow".equals(decision) || granted.isEmpty()) {
			return redirect(p.redirectUri, Map.of("error", "access_denied"), p.state);
		}
		CloudUser pu = usersRepository.findById(p.userId);
		if (!oauthService.isEnabled(pu) || !oauthService.isPro(pu)) {
			return redirect(p.redirectUri, Map.of("error", "access_denied"), p.state);
		}
		String code = oauthService.createCode(p.userId, client, p.redirectUri, p.codeChallenge, granted, p.resource);
		LOG.info("OAuth access granted by user " + p.userId + " to client " + client.clientid + " scope " + granted);
		return redirect(p.redirectUri, Map.of("code", code), p.state);
	}

	// ---------- tokens ----------

	@PostMapping(path = "/oauth/token", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
			produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> token(@RequestParam(name = "grant_type", required = false) String grantType,
	                                    @RequestParam(name = "code", required = false) String code,
	                                    @RequestParam(name = "redirect_uri", required = false) String redirectUri,
	                                    @RequestParam(name = "code_verifier", required = false) String codeVerifier,
	                                    @RequestParam(name = "refresh_token", required = false) String refreshToken,
	                                    @RequestParam(name = "client_id", required = false) String clientId,
	                                    @RequestParam(name = "client_secret", required = false) String clientSecret,
	                                    @RequestParam(name = "resource", required = false) String resource,
	                                    HttpServletRequest request) {
		try {
			String[] basic = basicCredentials(request);
			if (basic != null) {
				clientId = basic[0];
				clientSecret = basic[1];
			}
			OAuthClient client = oauthService.authenticateClient(clientId, clientSecret);
			if (resource != null && !isOurResource(resource)) {
				throw new OAuthException("invalid_target", "Unknown resource");
			}
			Tokens t;
			if ("authorization_code".equals(grantType)) {
				t = oauthService.exchangeCode(client, code, redirectUri, codeVerifier);
			} else if ("refresh_token".equals(grantType)) {
				t = oauthService.refresh(client, refreshToken);
			} else {
				throw new OAuthException("unsupported_grant_type", "Unsupported grant_type");
			}
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("access_token", t.accessToken);
			m.put("token_type", "Bearer");
			m.put("expires_in", OAuthService.ACCESS_TTL_MS / 1000);
			m.put("refresh_token", t.refreshToken);
			m.put("scope", t.scope);
			return json(HttpStatus.OK, m);
		} catch (OAuthException e) {
			return oauthError(e);
		}
	}

	@PostMapping(path = "/oauth/revoke", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
	public ResponseEntity<String> revoke(@RequestParam(required = false) String token) {
		// RFC 7009: 200 also for unknown tokens
		oauthService.revokeToken(token);
		return ResponseEntity.ok().build();
	}

	// ---------- account settings (web session, /mapapi needs login) ----------

	@GetMapping(path = "/mapapi/oauth/connections", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> connections() {
		CloudUserDevice dev = currentUser();
		if (dev == null) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
		}
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("enabled", oauthService.isEnabled(usersRepository.findById(dev.userid)));
		m.put("connections", oauthService.getConnections(dev.userid));
		m.put("groups", OAuthService.SCOPE_GROUPS);
		return json(HttpStatus.OK, m);
	}

	@PostMapping(path = "/mapapi/oauth/enabled", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> setEnabled(@RequestParam boolean enabled, HttpServletRequest request) {
		CloudUserDevice dev = currentUser();
		if (!isSameOrigin(request)) {
			return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
		}
		if (dev == null) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
		}
		oauthService.setEnabled(dev.userid, enabled);
		return connections();
	}

	@PostMapping(path = "/mapapi/oauth/revoke", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> revokeConnection(@RequestParam int id, HttpServletRequest request) {
		CloudUserDevice dev = currentUser();
		if (!isSameOrigin(request)) {
			return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
		}
		if (dev == null) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
		}
		if (!oauthService.revokeConnection(dev.userid, id)) {
			return ResponseEntity.badRequest().body(gson.toJson(Map.of("error", "unknown connection")));
		}
		return connections();
	}

	@PostMapping(path = "/mapapi/oauth/scope", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> setConnectionScope(@RequestParam int id,
	                                                 @RequestParam(name = "scope", required = false) List<String> scope,
	                                                 HttpServletRequest request) {
		CloudUserDevice dev = currentUser();
		if (!isSameOrigin(request)) {
			return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
		}
		if (dev == null) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
		}
		if (!oauthService.setConnectionScope(dev.userid, id, scope)) {
			return ResponseEntity.badRequest().body(gson.toJson(Map.of("error", "unknown connection or empty scope")));
		}
		return connections();
	}

	// ---------- helpers ----------

	private CloudUserDevice currentUser() {
		Authentication a = SecurityContextHolder.getContext().getAuthentication();
		if (a != null && a.getPrincipal() instanceof OsmAndProUser u) {
			return u.getUserDevice();
		}
		return null;
	}

	// CSRF guard for the account settings POSTs: the session cookie is SameSite=None and the app's CSRF matcher
	// skips POST, so a foreign page could submit a form. Browsers always send Origin (or Referer) on such a form.
	private static boolean isSameOrigin(HttpServletRequest request) {
		String base = baseUrl();
		String origin = request.getHeader(HttpHeaders.ORIGIN);
		if (origin != null) {
			return origin.equals(base);
		}
		String referer = request.getHeader(HttpHeaders.REFERER);
		return referer == null || referer.equals(base) || referer.startsWith(base + "/");
	}

	private boolean isOurResource(String resource) {
		String base = baseUrl();
		String r = resource.endsWith("/") ? resource.substring(0, resource.length() - 1) : resource;
		return r.equals(base) || r.equals(base + McpController.MCP_PATH);
	}

	private static String[] basicCredentials(HttpServletRequest request) {
		String h = request.getHeader(HttpHeaders.AUTHORIZATION);
		if (h == null || !h.startsWith("Basic ")) {
			return null;
		}
		try {
			String s = new String(Base64.getDecoder().decode(h.substring(6).trim()), StandardCharsets.UTF_8);
			int i = s.indexOf(':');
			if (i < 0) {
				return null;
			}
			// RFC 6749 2.3.1: both parts are form-urlencoded
			return new String[] { java.net.URLDecoder.decode(s.substring(0, i), StandardCharsets.UTF_8),
					java.net.URLDecoder.decode(s.substring(i + 1), StandardCharsets.UTF_8) };
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	private static String redirectUrl(String redirectUri, Map<String, String> params, String state) {
		StringBuilder sb = new StringBuilder(redirectUri);
		char sep = redirectUri.contains("?") ? '&' : '?';
		Map<String, String> all = new LinkedHashMap<>(params);
		if (state != null) {
			all.put("state", state);
		}
		all.put("iss", baseUrl());
		for (Map.Entry<String, String> e : all.entrySet()) {
			sb.append(sep).append(e.getKey()).append('=').append(urlEncode(e.getValue()));
			sep = '&';
		}
		return sb.toString();
	}

	private static ResponseEntity<String> redirect(String redirectUri, Map<String, String> params, String state) {
		return ResponseEntity.status(HttpStatus.FOUND)
				.header(HttpHeaders.LOCATION, redirectUrl(redirectUri, params, state)).build();
	}

	private static String urlEncode(String s) {
		return URLEncoder.encode(s, StandardCharsets.UTF_8);
	}

	private ResponseEntity<String> json(HttpStatus status, Object body) {
		return ResponseEntity.status(status).cacheControl(CacheControl.noStore())
				.contentType(MediaType.APPLICATION_JSON).body(gson.toJson(body));
	}

	private ResponseEntity<String> oauthError(OAuthException e) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("error", e.error);
		m.put("error_description", e.getMessage());
		return json(e.status, m);
	}

	public record Link(String text, String href) {
	}

	private static String initial(String s) {
		return s == null || s.isEmpty() ? "?" : s.substring(0, 1).toUpperCase();
	}

	private ResponseEntity<String> message(HttpStatus status, String title, String text, List<Link> links) {
		Context ctx = new Context();
		ctx.setVariable("consent", false);
		ctx.setVariable("message", text);
		ctx.setVariable("links", links);
		return page(status, title, ctx);
	}

	// templates/pub/oauth.html in web-server-config (thymeleaf prefix)
	private ResponseEntity<String> page(HttpStatus status, String title, Context ctx) {
		ctx.setVariable("title", title);
		String html = templateEngine.process(TEMPLATE, ctx);
		return ResponseEntity.status(status).contentType(MediaType.TEXT_HTML).cacheControl(CacheControl.noStore())
				.header("X-Frame-Options", "DENY").body(html);
	}
}
