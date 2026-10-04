package net.osmand.server.controllers.pub;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.springframework.web.util.HtmlUtils;

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

	@Autowired
	private OAuthService oauthService;

	@Autowired
	private CloudUsersRepository usersRepository;

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
			RegisteredClient rc = oauthService.registerClient(request.getRemoteAddr(), r.client_name,
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
			return page(HttpStatus.BAD_REQUEST, "Unknown application",
					"<p>This application is not registered. Connect it again from the assistant.</p>");
		}
		if (!oauthService.isRegisteredRedirect(client, redirectUri)) {
			return page(HttpStatus.BAD_REQUEST, "Invalid request",
					"<p>The return address of the application does not match its registration.</p>");
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
		String clientName = HtmlUtils.htmlEscape(client.clientname);
		String host = HtmlUtils.htmlEscape(OAuthService.redirectHost(redirectUri));
		if (!oauthService.isEnabled(pu)) {
			return page(HttpStatus.OK, "Access is turned off",
					"<p>Access for AI assistants and other connected applications is turned off in your OsmAnd account "
							+ "settings, so <b>" + clientName + "</b> cannot be connected.</p>"
							+ "<p><a href=\"" + HtmlUtils.htmlEscape(LOGIN_PAGE) + "\">Open account settings</a></p>"
							+ "<p><a href=\"" + HtmlUtils.htmlEscape(redirectUrl(redirectUri,
									Map.of("error", "access_denied"), state)) + "\">Return to " + host + "</a></p>");
		}
		if (!oauthService.isPro(pu)) {
			return page(HttpStatus.OK, "OsmAnd Pro required",
					"<p>Connecting AI assistants such as <b>" + clientName + "</b> is part of OsmAnd Pro.</p>"
							+ "<p><a href=\"/pricing\">See OsmAnd Pro</a></p>"
							+ "<p><a href=\"" + HtmlUtils.htmlEscape(redirectUrl(redirectUri,
									Map.of("error", "access_denied"), state)) + "\">Return to " + host + "</a></p>");
		}
		String nonce = UUID.randomUUID().toString();
		request.getSession(true).setAttribute(PENDING_ATTR + nonce, new PendingAuthorization(dev.userid,
				client.clientid, redirectUri, state, codeChallenge, resource));
		Set<String> checked = oauthService.defaultScopes(scope);
		String n = HtmlUtils.htmlEscape(nonce);
		String initial = HtmlUtils.htmlEscape(client.clientname.isEmpty() ? "?"
				: client.clientname.substring(0, 1).toUpperCase());
		return page(HttpStatus.OK, client.clientname + " wants to connect to OsmAnd",
				"<div class=\"app\">" + initial + "</div><div class=\"arrows\">&#8644;</div>"
						+ "<div class=\"logo\">" + LOGO_SVG + "</div>",
				"<p class=\"lead\"><b>" + clientName + "</b> will be able to use your OsmAnd Cloud data "
						+ "as you allow below.</p>"
						+ "<div class=\"acct\"><span class=\"avatar\">" + HtmlUtils.htmlEscape(
								pu.email.substring(0, 1).toUpperCase()) + "</span><div><small>Signed in as</small><b>"
						+ HtmlUtils.htmlEscape(pu.email) + "</b></div></div>"
						+ "<form method=\"post\" action=\"/oauth/authorize\">"
						+ permissionTable(checked)
						+ "<p class=\"hint\">Edits are saved as new versions, older versions stay in OsmAnd Cloud. "
						+ "You can change access or disconnect at any time in your OsmAnd account, Connected apps.</p>"
						+ "<input type=\"hidden\" name=\"nonce\" value=\"" + n + "\">"
						+ "<div class=\"actions\"><button name=\"decision\" value=\"allow\" class=\"primary\">Allow</button>"
						+ "<button name=\"decision\" value=\"deny\">Cancel</button></div>"
						+ "<p class=\"fine\">After you allow it, you return to <b>" + host + "</b>. "
						+ "Continue only if you started this connection there.</p></form>"
						+ PERMISSION_SCRIPT);
	}

	private static final String LOGO_SVG = "<svg viewBox=\"0 0 192 192\" width=\"40\" height=\"40\" aria-label=\"OsmAnd\">"
			+ "<path fill-rule=\"evenodd\" fill=\"#FF8800\" d=\"M168 96C168 125.833 149.856 151.429 124 162.353C110.634 168 "
			+ "106 171 102 179C101.611 179.779 101.278 180.52 100.969 181.208C99.6915 184.055 98.8184 186 96 186C93.1816 186 "
			+ "92.3085 184.055 91.0311 181.208C90.7221 180.52 90.3895 179.779 90 179C86 171 81 168 68 162.353C42.1445 151.429 "
			+ "24 125.833 24 96C24 56.2355 56.2355 24 96 24C135.765 24 168 56.2355 168 96ZM136 96.0001C136 118.091 118.091 "
			+ "136 96.0001 136C73.9087 136 56 118.091 56 96.0001C56 73.9087 73.9087 56 96.0001 56C118.091 56 136 73.9087 136 "
			+ "96.0001Z\"/></svg>";

	// edit includes view: ticking Edit ticks View, unticking View unticks Edit; Allow needs at least one
	private static final String PERMISSION_SCRIPT = "<script>(function(){"
			+ "var f=document.querySelector('form'),a=f.querySelector('button.primary');"
			+ "function q(k){return f.querySelector('input[value=\"'+k+'\"]');}"
			+ "function upd(){a.disabled=!f.querySelector('input[name=scope]:checked');}"
			+ "f.querySelectorAll('input[name=scope]').forEach(function(e){e.addEventListener('change',function(){"
			+ "var g=e.value.split(':')[0];"
			+ "if(e.checked&&e.value.endsWith(':write')){q(g+':read').checked=true;}"
			+ "if(!e.checked&&e.value.endsWith(':read')&&q(g+':write')){q(g+':write').checked=false;}upd();});});"
			+ "upd();})();</script>";

	private static String permissionTable(Set<String> checked) {
		StringBuilder sb = new StringBuilder("<table class=\"perm\"><thead><tr><th></th><th>View</th><th>Edit</th>"
				+ "</tr></thead><tbody>");
		for (OAuthService.ScopeGroup g : OAuthService.SCOPE_GROUPS) {
			sb.append("<tr><td><b>").append(HtmlUtils.htmlEscape(g.title())).append("</b><small>")
					.append(HtmlUtils.htmlEscape(g.description())).append("</small></td>");
			sb.append("<td>").append(toggle(g, g.key() + OAuthService.READ, "view", checked)).append("</td><td>");
			if (g.write()) {
				sb.append(toggle(g, g.key() + OAuthService.WRITE, "edit", checked));
			}
			sb.append("</td></tr>");
		}
		return sb.append("</tbody></table>").toString();
	}

	private static String toggle(OAuthService.ScopeGroup g, String scope, String action, Set<String> checked) {
		return "<label class=\"sw\"><input type=\"checkbox\" name=\"scope\" value=\"" + HtmlUtils.htmlEscape(scope)
				+ "\" aria-label=\"" + HtmlUtils.htmlEscape(g.title() + ": " + action) + "\""
				+ (checked.contains(scope) ? " checked" : "") + "><i></i></label>";
	}

	@PostMapping(path = "/oauth/authorize")
	public ResponseEntity<String> authorizeDecision(@RequestParam String nonce, @RequestParam String decision,
	                                                @RequestParam(name = "scope", required = false) List<String> scope,
	                                                HttpServletRequest request) {
		HttpSession session = request.getSession(false);
		Object o = session == null ? null : session.getAttribute(PENDING_ATTR + nonce);
		CloudUserDevice dev = currentUser();
		if (!(o instanceof PendingAuthorization p) || dev == null || dev.userid != p.userId) {
			return page(HttpStatus.BAD_REQUEST, "Request expired",
					"<p>This request is no longer valid. Start the connection again from the assistant.</p>");
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
	public ResponseEntity<String> setEnabled(@RequestParam boolean enabled) {
		CloudUserDevice dev = currentUser();
		if (dev == null) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
		}
		oauthService.setEnabled(dev.userid, enabled);
		return connections();
	}

	@PostMapping(path = "/mapapi/oauth/revoke", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> revokeConnection(@RequestParam int id) {
		CloudUserDevice dev = currentUser();
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
	                                                 @RequestParam(name = "scope", required = false) List<String> scope) {
		CloudUserDevice dev = currentUser();
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

	private static final String PAGE_CSS = ":root{--bg:#f2f3f5;--card:#fff;--text:#1f2023;--muted:#6b6e76;"
			+ "--line:#e6e7ea;--accent:#237bff;--off:#c9ccd1;--soft:#eef4ff}"
			+ "@media (prefers-color-scheme: dark){:root{--bg:#141517;--card:#1f2023;--text:#eceef1;--muted:#9a9ea6;"
			+ "--line:#33353a;--off:#4a4d54;--soft:#1d2a40}}"
			+ "*{box-sizing:border-box}body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;"
			+ "background:var(--bg);color:var(--text);margin:0;padding:16px;font-size:15px;line-height:1.45}"
			+ ".card{max-width:520px;margin:32px auto;background:var(--card);border-radius:16px;padding:28px;"
			+ "box-shadow:0 2px 12px rgba(0,0,0,.08)}h1{font-size:22px;line-height:1.3;margin:0 0 8px;text-align:center}"
			+ "a{color:var(--accent)}.muted,.hint,.fine{color:var(--muted)}.hint{font-size:13px;margin:16px 0}"
			+ ".fine{font-size:12px;text-align:center;margin:16px 0 0}.lead{text-align:center;margin:0 0 20px}"
			+ ".pair{display:flex;align-items:center;justify-content:center;gap:14px;margin:0 0 18px}"
			+ ".app,.logo{width:56px;height:56px;border-radius:16px;display:flex;"
			+ "align-items:center;justify-content:center;background:var(--soft)}.app{font-size:26px;font-weight:600;"
			+ "color:var(--accent)}.arrows{color:var(--muted);font-size:22px}"
			+ ".acct{display:flex;align-items:center;gap:12px;border:1px solid var(--line);border-radius:12px;"
			+ "padding:10px 14px;margin:0 0 18px}.acct small{display:block;color:var(--muted);font-size:12px}"
			+ ".avatar{width:32px;height:32px;border-radius:50%;background:var(--accent);color:#fff;display:flex;"
			+ "align-items:center;justify-content:center;font-weight:600;flex:none}"
			+ ".perm{width:100%;border-collapse:collapse}.perm th{font-size:12px;font-weight:600;color:var(--muted);"
			+ "text-transform:uppercase;letter-spacing:.04em;padding:0 0 6px;width:64px;text-align:center}"
			+ ".perm td{padding:10px 0;border-top:1px solid var(--line);text-align:center;vertical-align:middle}"
			+ ".perm td:first-child{text-align:left}.perm small{display:block;color:var(--muted);font-size:13px}"
			+ ".sw{position:relative;display:inline-block;width:40px;height:24px;cursor:pointer}"
			+ ".sw input{position:absolute;opacity:0;width:100%;height:100%;margin:0;cursor:pointer;z-index:1}"
			+ ".sw i{position:absolute;inset:0;background:var(--off);border-radius:12px;transition:background .15s}"
			+ ".sw i:before{content:'';position:absolute;width:18px;height:18px;left:3px;top:3px;background:#fff;"
			+ "border-radius:50%;transition:transform .15s;box-shadow:0 1px 2px rgba(0,0,0,.3)}"
			+ ".sw input:checked+i{background:var(--accent)}.sw input:checked+i:before{transform:translateX(16px)}"
			+ ".sw input:focus-visible+i{outline:2px solid var(--accent);outline-offset:2px}"
			+ ".actions{display:flex;flex-direction:row-reverse;gap:12px}.actions button{flex:1}"
			+ "button{font:inherit;font-weight:600;padding:12px 20px;border-radius:10px;border:1px solid var(--line);"
			+ "background:var(--card);color:var(--text);cursor:pointer}button.primary{background:var(--accent);"
			+ "border-color:var(--accent);color:#fff}button:disabled{opacity:.5;cursor:default}";

	private static ResponseEntity<String> page(HttpStatus status, String title, String bodyHtml) {
		return page(status, title, "<div class=\"logo\">" + LOGO_SVG + "</div>", bodyHtml);
	}

	private static ResponseEntity<String> page(HttpStatus status, String title, String iconsHtml, String bodyHtml) {
		String html = "<!doctype html><html><head><meta charset=\"utf-8\">"
				+ "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
				+ "<title>" + HtmlUtils.htmlEscape(title) + " - OsmAnd</title><style>" + PAGE_CSS
				+ "</style></head><body><div class=\"card\"><div class=\"pair\">" + iconsHtml
				+ "</div><h1>" + HtmlUtils.htmlEscape(title) + "</h1>"
				+ bodyHtml + "</div></body></html>";
		return ResponseEntity.status(status).contentType(MediaType.TEXT_HTML).cacheControl(CacheControl.noStore())
				.header("X-Frame-Options", "DENY").body(html);
	}
}
