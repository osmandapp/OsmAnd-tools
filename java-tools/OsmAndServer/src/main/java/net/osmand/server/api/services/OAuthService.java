package net.osmand.server.api.services;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.repo.OAuthClientsRepository;
import net.osmand.server.api.repo.OAuthClientsRepository.OAuthClient;
import net.osmand.server.api.repo.OAuthGrantsRepository;
import net.osmand.server.api.repo.OAuthGrantsRepository.OAuthGrant;
import net.osmand.util.Algorithms;

/**
 * Minimal OAuth 2.1 authorization server for MCP clients (Claude, ChatGPT, Claude Code...):
 * dynamic client registration, authorization code + PKCE (S256), rotating refresh tokens.
 * The user signs in with the existing OsmAnd Cloud web session; tokens never grant /mapapi or /userdata access.
 */
@Service
public class OAuthService {

	protected static final Log LOG = LogFactory.getLog(OAuthService.class);

	public static final String ACCESS_TOKEN_PREFIX = "oat_";
	private static final String REFRESH_TOKEN_PREFIX = "ort_";
	private static final String CODE_PREFIX = "oac_";
	private static final String CLIENT_ID_PREFIX = "occ_";

	private static final long CODE_TTL_MS = 5 * 60 * 1000L;
	public static final long ACCESS_TTL_MS = 60 * 60 * 1000L;
	private static final long REFRESH_TTL_MS = 90L * 24 * 60 * 60 * 1000L;
	private static final long LAST_USE_UPDATE_MS = 10 * 60 * 1000L;

	private static final int MAX_REDIRECT_URIS = 5;
	private static final int MAX_URI_LENGTH = 512;
	private static final int MAX_CLIENT_NAME = 100;
	// all IPs together: hosted assistants (Claude.ai, ChatGPT) register every user's client from a few shared IPs
	private static final int MAX_REGISTRATIONS_PER_HOUR = 1000;
	private static final long UNUSED_CLIENT_TTL_MS = 24 * 60 * 60 * 1000L;
	private static final Set<String> FORBIDDEN_SCHEMES = Set.of("javascript", "data", "file", "vbscript", "about", "blob");

	public static final String AUTH_METHOD_NONE = "none";
	public static final String AUTH_METHOD_POST = "client_secret_post";
	public static final String AUTH_METHOD_BASIC = "client_secret_basic";

	public static final String READ = ":read";
	public static final String WRITE = ":write";
	public static final String SCOPE_ACCOUNT_READ = "account" + READ;

	// OsmAnd Cloud file types grouped as in the app backup screen; scope = group + ":read" / ":write"
	public enum CloudGroup {
		FAVORITES("favorites", "Favorites", "Saved places and their groups", "FAVOURITES"),
		TRACKS("tracks", "Tracks", "GPX tracks and track folders", "GPX", "GPX_DIR"),
		MARKERS("markers", "Map markers", "Markers and itineraries", "ACTIVE_MARKERS", "HISTORY_MARKERS",
				"ITINERARY_GROUPS"),
		OSM("osm", "OSM edits", "Your OpenStreetMap edits and notes", "OSM_EDITS", "OSM_NOTES"),
		HISTORY("history", "History", "Search and navigation history", "SEARCH_HISTORY", "NAVIGATION_HISTORY"),
		SETTINGS("settings", "Settings", "Profiles, plugins, quick actions, map sources", "GLOBAL", "PROFILE", "PLUGIN",
				"QUICK_ACTIONS", "POI_UI_FILTERS", "AVOID_ROADS", "ONLINE_ROUTING_ENGINES", "MAP_SOURCES", "DATA",
				"RESOURCES", "DOWNLOADS", "SUGGESTED_DOWNLOADS"),
		// FILE and any type unknown here: rendering styles, routing files, audio/video notes, maps
		FILES("files", "Other files", "Map styles, routing files, media notes, maps", "FILE");

		public final String key;
		public final String title;
		public final String description;
		public final Set<String> types;

		CloudGroup(String key, String title, String description, String... types) {
			this.key = key;
			this.title = title;
			this.description = description;
			this.types = Set.of(types);
		}

		public static CloudGroup ofType(String type) {
			for (CloudGroup g : values()) {
				if (g.types.contains(type)) {
					return g;
				}
			}
			return FILES;
		}
	}

	// rows of the permission table (consent page, account settings): scope prefix, title, description, has write
	public record ScopeGroup(String key, String title, String description, boolean write) {
	}

	public static final List<ScopeGroup> SCOPE_GROUPS = new ArrayList<>();
	public static final Set<String> SCOPES = new LinkedHashSet<>();
	static {
		SCOPE_GROUPS.add(new ScopeGroup("account", "Account", "Email and OsmAnd Pro status", false));
		for (CloudGroup g : CloudGroup.values()) {
			SCOPE_GROUPS.add(new ScopeGroup(g.key, g.title, g.description, true));
		}
		for (ScopeGroup g : SCOPE_GROUPS) {
			SCOPES.add(g.key + READ);
			if (g.write) {
				SCOPES.add(g.key + WRITE);
			}
		}
	}

	@Autowired
	private OAuthClientsRepository clientsRepository;

	@Autowired
	private OAuthGrantsRepository grantsRepository;

	@Autowired
	private CloudUsersRepository usersRepository;

	@Autowired
	private UserSubscriptionService userSubService;

	@Value("${osmand.oauth.default-user-enabled:false}")
	private boolean defaultUserEnabled;

	private final SecureRandom random = new SecureRandom();
	private final Deque<Long> registrations = new ArrayDeque<>();

	public static class OAuthException extends Exception {
		private static final long serialVersionUID = 1L;
		public final String error;
		public final HttpStatus status;

		public OAuthException(String error, String description) {
			this(error, description, HttpStatus.BAD_REQUEST);
		}

		public OAuthException(String error, String description, HttpStatus status) {
			super(description);
			this.error = error;
			this.status = status;
		}
	}

	public static class RegisteredClient {
		public OAuthClient client;
		public String secret; // returned once, only for confidential clients
		public String authMethod;
	}

	public static class Tokens {
		public String accessToken;
		public String refreshToken;
		public String scope;
	}

	// ---------- clients ----------

	public RegisteredClient registerClient(String name, List<String> redirectUris, String authMethod)
			throws OAuthException {
		if (!allowRegistration()) {
			throw new OAuthException("invalid_client_metadata", "Too many registrations, try later",
					HttpStatus.TOO_MANY_REQUESTS);
		}
		if (redirectUris == null || redirectUris.isEmpty() || redirectUris.size() > MAX_REDIRECT_URIS) {
			throw new OAuthException("invalid_redirect_uri", "1 to " + MAX_REDIRECT_URIS + " redirect_uris are required");
		}
		for (String uri : redirectUris) {
			if (!isValidRedirectUri(uri)) {
				throw new OAuthException("invalid_redirect_uri", "Redirect URI is not allowed: " + uri);
			}
		}
		if (authMethod == null || authMethod.isEmpty()) {
			authMethod = AUTH_METHOD_NONE;
		}
		if (!Set.of(AUTH_METHOD_NONE, AUTH_METHOD_POST, AUTH_METHOD_BASIC).contains(authMethod)) {
			throw new OAuthException("invalid_client_metadata", "Unsupported token_endpoint_auth_method " + authMethod);
		}
		RegisteredClient rc = new RegisteredClient();
		OAuthClient c = new OAuthClient();
		c.clientid = CLIENT_ID_PREFIX + randomToken(16);
		c.clientname = cleanName(name);
		c.redirecturis = String.join(" ", redirectUris);
		c.createtime = new Date();
		if (!AUTH_METHOD_NONE.equals(authMethod)) {
			rc.secret = randomToken(32);
			c.secrethash = hash(rc.secret);
		}
		clientsRepository.saveAndFlush(c);
		rc.client = c;
		rc.authMethod = authMethod;
		LOG.info("OAuth client registered " + c.clientid + " '" + c.clientname + "' " + c.redirecturis);
		return rc;
	}

	public OAuthClient getClient(String clientId) {
		if (clientId == null || !clientId.startsWith(CLIENT_ID_PREFIX)) {
			return null;
		}
		return clientsRepository.findByClientid(clientId);
	}

	public OAuthClient authenticateClient(String clientId, String clientSecret) throws OAuthException {
		OAuthClient c = getClient(clientId);
		if (c == null) {
			throw new OAuthException("invalid_client", "Unknown client", HttpStatus.UNAUTHORIZED);
		}
		if (c.secrethash != null && (clientSecret == null || !MessageDigest.isEqual(
				c.secrethash.getBytes(StandardCharsets.UTF_8), hash(clientSecret).getBytes(StandardCharsets.UTF_8)))) {
			throw new OAuthException("invalid_client", "Client authentication failed", HttpStatus.UNAUTHORIZED);
		}
		return c;
	}

	public boolean isRegisteredRedirect(OAuthClient c, String redirectUri) {
		if (redirectUri == null) {
			return false;
		}
		for (String registered : c.redirecturis.split(" ")) {
			if (registered.equals(redirectUri) || sameLoopbackIgnoringPort(registered, redirectUri)) {
				return true;
			}
		}
		return false;
	}

	public static String redirectHost(String redirectUri) {
		try {
			URI u = new URI(redirectUri);
			return u.getHost() != null ? u.getHost() : u.getScheme() + ":";
		} catch (URISyntaxException e) {
			return redirectUri;
		}
	}

	// ---------- authorization code ----------

	// scopes pre-checked on the consent page: view of the requested groups, or of all groups if none requested.
	// Edit is never pre-checked: clients request every scope_supported, the user ticks Edit.
	public Set<String> defaultScopes(String requested) {
		Set<String> res = new LinkedHashSet<>();
		if (requested != null) {
			for (String s : requested.trim().split("\\s+")) {
				String read = s.endsWith(WRITE) ? s.substring(0, s.length() - WRITE.length()) + READ : s;
				if (SCOPES.contains(read)) {
					res.add(read);
				}
			}
		}
		if (res.isEmpty()) {
			for (String s : SCOPES) {
				if (s.endsWith(READ)) {
					res.add(s);
				}
			}
		}
		return res;
	}

	// scopes the user checked, in canonical order; edit includes view
	public String grantedScope(java.util.Collection<String> checked) {
		Set<String> res = new LinkedHashSet<>();
		for (String s : SCOPES) {
			if (checked != null && (checked.contains(s)
					|| (s.endsWith(READ) && checked.contains(s.substring(0, s.length() - READ.length()) + WRITE)))) {
				res.add(s);
			}
		}
		return String.join(" ", res);
	}

	public String createCode(int userId, OAuthClient c, String redirectUri, String codeChallenge, String scope,
	                         String resource) {
		cleanup(userId);
		String code = CODE_PREFIX + randomToken(32);
		OAuthGrant g = new OAuthGrant();
		g.userid = userId;
		g.clientid = c.clientid;
		g.scope = scope;
		g.resource = resource;
		g.redirecturi = redirectUri;
		g.codehash = hash(code);
		g.codechallenge = codeChallenge;
		g.codeexpire = new Date(System.currentTimeMillis() + CODE_TTL_MS);
		g.createtime = new Date();
		grantsRepository.saveAndFlush(g);
		return code;
	}

	public Tokens exchangeCode(OAuthClient c, String code, String redirectUri, String codeVerifier)
			throws OAuthException {
		OAuthGrant g = code == null ? null : grantsRepository.findByCodehash(hash(code));
		if (g == null || !g.clientid.equals(c.clientid)) {
			throw new OAuthException("invalid_grant", "Unknown authorization code");
		}
		if (g.codeexpire == null) {
			// code replay: revoke what was issued for it (OAuth 2.1, 4.1.3)
			LOG.warn("OAuth code reused, revoking grant " + g.id + " of client " + c.clientid);
			grantsRepository.delete(g);
			throw new OAuthException("invalid_grant", "Authorization code was already used");
		}
		if (g.codeexpire.getTime() < System.currentTimeMillis()) {
			throw new OAuthException("invalid_grant", "Authorization code expired");
		}
		if (!g.redirecturi.equals(redirectUri)) {
			throw new OAuthException("invalid_grant", "redirect_uri does not match");
		}
		if (codeVerifier == null || !MessageDigest.isEqual(g.codechallenge.getBytes(StandardCharsets.US_ASCII),
				s256(codeVerifier).getBytes(StandardCharsets.US_ASCII))) {
			throw new OAuthException("invalid_grant", "PKCE verification failed");
		}
		checkUserAllowsOAuth(g.userid);
		g.codeexpire = null;
		return issueTokens(g);
	}

	public Tokens refresh(OAuthClient c, String refreshToken) throws OAuthException {
		OAuthGrant g = refreshToken == null ? null : grantsRepository.findByRefreshhash(hash(refreshToken));
		if (g == null || !g.clientid.equals(c.clientid)) {
			throw new OAuthException("invalid_grant", "Unknown refresh token");
		}
		if (g.refreshexpire == null || g.refreshexpire.getTime() < System.currentTimeMillis()) {
			grantsRepository.delete(g);
			throw new OAuthException("invalid_grant", "Refresh token expired");
		}
		checkUserAllowsOAuth(g.userid);
		return issueTokens(g);
	}

	public void revokeToken(String token) {
		if (token == null) {
			return;
		}
		String h = hash(token);
		OAuthGrant g = grantsRepository.findByAccesshash(h);
		if (g == null) {
			g = grantsRepository.findByRefreshhash(h);
		}
		if (g != null) {
			grantsRepository.delete(g);
		}
	}

	// returns the grant of a valid access token or null
	public OAuthGrant validateAccessToken(String token) {
		if (token == null || !token.startsWith(ACCESS_TOKEN_PREFIX)) {
			return null;
		}
		OAuthGrant g = grantsRepository.findByAccesshash(hash(token));
		long now = System.currentTimeMillis();
		if (g == null || g.accessexpire == null || g.accessexpire.getTime() < now) {
			return null;
		}
		CloudUser pu = usersRepository.findById(g.userid);
		if (!isEnabled(pu)) {
			return null;
		}
		if (g.lastusetime == null || now - g.lastusetime.getTime() > LAST_USE_UPDATE_MS) {
			g.lastusetime = new Date(now);
			grantsRepository.updateLastUseTime(g.id, g.lastusetime);
		}
		return g;
	}

	// ---------- user settings ----------

	// user's choice in account settings; null = never chosen, then the server default
	public boolean isEnabled(CloudUser pu) {
		return pu != null && (pu.oauthEnabled != null ? pu.oauthEnabled : defaultUserEnabled);
	}

	public void setEnabled(int userId, boolean enabled) {
		CloudUser pu = usersRepository.findById(userId);
		if (pu == null) {
			return;
		}
		pu.oauthEnabled = enabled;
		usersRepository.saveAndFlush(pu);
		if (!enabled) {
			grantsRepository.deleteAll(grantsRepository.findByUserid(userId));
			LOG.info("OAuth disabled by user " + userId + ", all grants revoked");
		}
	}

	public List<Map<String, Object>> getConnections(int userId) {
		cleanup(userId);
		List<Map<String, Object>> res = new ArrayList<>();
		for (OAuthGrant g : grantsRepository.findByUserid(userId)) {
			if (g.refreshhash == null) {
				continue;
			}
			OAuthClient c = clientsRepository.findByClientid(g.clientid);
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("id", g.id);
			m.put("client", c != null ? c.clientname : g.clientid);
			m.put("redirectHost", redirectHost(g.redirecturi));
			m.put("scope", g.scope);
			m.put("created", g.createtime != null ? g.createtime.getTime() : null);
			m.put("lastUsed", g.lastusetime != null ? g.lastusetime.getTime() : null);
			res.add(m);
		}
		return res;
	}

	public boolean revokeConnection(int userId, int grantId) {
		OAuthGrant g = grantsRepository.findById(grantId).orElse(null);
		if (g == null || g.userid != userId) {
			return false;
		}
		grantsRepository.delete(g);
		return true;
	}

	// user changed the permissions of a connection in account settings; applies to the next request
	public boolean setConnectionScope(int userId, int grantId, java.util.Collection<String> checked) {
		OAuthGrant g = grantsRepository.findById(grantId).orElse(null);
		String scope = grantedScope(checked);
		if (g == null || g.userid != userId || scope.isEmpty()) {
			return false;
		}
		g.scope = scope;
		grantsRepository.save(g);
		LOG.info("OAuth scope changed by user " + userId + " for client " + g.clientid + ": " + scope);
		return true;
	}

	// ---------- helpers ----------

	// AI assistants are an OsmAnd Pro feature, same check as Cloud uploads ("not Free" on the web)
	public boolean isPro(CloudUser pu) {
		return pu != null && userSubService.verifyAndRefreshProOrderId(pu) == null && !Algorithms.isEmpty(pu.orderid);
	}

	// checked on every token issue, so access ends at most ACCESS_TTL_MS after Pro expires or access is turned off
	private void checkUserAllowsOAuth(int userId) throws OAuthException {
		CloudUser pu = usersRepository.findById(userId);
		if (!isEnabled(pu)) {
			throw new OAuthException("invalid_grant", "Access for OAuth clients is not turned on in the OsmAnd account");
		}
		if (!isPro(pu)) {
			throw new OAuthException("invalid_grant", "OsmAnd Pro is required");
		}
	}

	private Tokens issueTokens(OAuthGrant g) {
		long now = System.currentTimeMillis();
		Tokens t = new Tokens();
		t.accessToken = ACCESS_TOKEN_PREFIX + randomToken(32);
		t.refreshToken = REFRESH_TOKEN_PREFIX + randomToken(32);
		t.scope = g.scope;
		g.accesshash = hash(t.accessToken);
		g.accessexpire = new Date(now + ACCESS_TTL_MS);
		g.refreshhash = hash(t.refreshToken);
		g.refreshexpire = new Date(now + REFRESH_TTL_MS);
		g.lastusetime = new Date(now);
		grantsRepository.saveAndFlush(g);
		return t;
	}

	// drops unused codes and expired connections of the user
	private void cleanup(int userId) {
		long now = System.currentTimeMillis();
		List<OAuthGrant> stale = new ArrayList<>();
		for (OAuthGrant g : grantsRepository.findByUserid(userId)) {
			boolean unusedCode = g.refreshhash == null && (g.codeexpire == null || g.codeexpire.getTime() < now);
			boolean expired = g.refreshexpire != null && g.refreshexpire.getTime() < now;
			if (unusedCode || expired) {
				stale.add(g);
			}
		}
		grantsRepository.deleteAll(stale);
	}

	private boolean allowRegistration() {
		long now = System.currentTimeMillis();
		synchronized (registrations) {
			while (!registrations.isEmpty() && now - registrations.peekFirst() > 3600 * 1000L) {
				registrations.pollFirst();
			}
			if (registrations.size() >= MAX_REGISTRATIONS_PER_HOUR) {
				return false;
			}
			registrations.addLast(now);
		}
		return true;
	}

	// registration is anonymous, so drop clients that never got a connection
	@Scheduled(fixedRate = 60 * 60 * 1000L)
	public void deleteUnusedClients() {
		int n = clientsRepository.deleteUnusedBefore(new Date(System.currentTimeMillis() - UNUSED_CLIENT_TTL_MS));
		if (n > 0) {
			LOG.info("OAuth unused clients deleted: " + n);
		}
	}

	// https for web clients, http only for loopback (RFC 8252 7.3), private-use schemes for native apps (7.1)
	private static boolean isValidRedirectUri(String uri) {
		if (uri == null || uri.length() > MAX_URI_LENGTH) {
			return false;
		}
		try {
			URI u = new URI(uri);
			String scheme = u.getScheme();
			if (scheme == null || u.getFragment() != null) {
				return false;
			}
			scheme = scheme.toLowerCase();
			if ("https".equals(scheme)) {
				return u.getHost() != null;
			}
			if ("http".equals(scheme)) {
				return isLoopback(u.getHost());
			}
			return !FORBIDDEN_SCHEMES.contains(scheme);
		} catch (URISyntaxException e) {
			return false;
		}
	}

	private static boolean sameLoopbackIgnoringPort(String registered, String requested) {
		try {
			URI a = new URI(registered);
			URI b = new URI(requested);
			return "http".equals(a.getScheme()) && "http".equals(b.getScheme()) && isLoopback(a.getHost())
					&& a.getHost().equals(b.getHost()) && String.valueOf(a.getRawPath()).equals(String.valueOf(b.getRawPath()))
					&& String.valueOf(a.getRawQuery()).equals(String.valueOf(b.getRawQuery()));
		} catch (URISyntaxException e) {
			return false;
		}
	}

	private static boolean isLoopback(String host) {
		return "localhost".equals(host) || "127.0.0.1".equals(host) || "[::1]".equals(host);
	}

	private static String cleanName(String name) {
		if (name == null || name.isBlank()) {
			return "Unnamed client";
		}
		name = name.replaceAll("\\p{Cntrl}", " ").trim();
		return name.length() > MAX_CLIENT_NAME ? name.substring(0, MAX_CLIENT_NAME) : name;
	}

	private String randomToken(int bytes) {
		byte[] b = new byte[bytes];
		random.nextBytes(b);
		return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
	}

	public static String hash(String token) {
		return java.util.HexFormat.of().formatHex(sha256(token));
	}

	private static String s256(String verifier) {
		return Base64.getUrlEncoder().withoutPadding().encodeToString(sha256(verifier));
	}

	private static byte[] sha256(String s) {
		try {
			return MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.US_ASCII));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}
}
