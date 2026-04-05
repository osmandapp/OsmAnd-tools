package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.GarminUserConnectionRepository;
import net.osmand.server.api.repo.GarminUserConnectionRepository.GarminUserConnection;
import net.osmand.server.api.services.OsmAndMapsService;

@RestController
public class GarminConnectController {

	private static final Gson GSON = new Gson();

	private static final Log LOG = LogFactory.getLog(GarminConnectController.class);

	private static final String AUTH_URL = "https://connect.garmin.com/oauth2Confirm";
	private static final String TOKEN_URL = "https://diauth.garmin.com/di-oauth2-service/oauth/token";
	private static final String USER_ID_URL = "https://apis.garmin.com/wellness-api/rest/user/id";
	private static final String USER_PERMISSIONS_URL = "https://apis.garmin.com/wellness-api/rest/user/permissions";
	private static final String PARTNER_REGISTRATION_URL = "https://apis.garmin.com/wellness-api/rest/user/registration";

	private static final String JSON_PERMISSIONS = "permissions";

	private static final String PERM_HISTORICAL_DATA_EXPORT = "HISTORICAL_DATA_EXPORT";
	private static final String PERM_ACTIVITY_EXPORT = "ACTIVITY_EXPORT";

	private static final String JSON_ACCESS_TOKEN = "access_token";
	private static final String JSON_REFRESH_TOKEN = "refresh_token";
	private static final String JSON_EXPIRES_IN = "expires_in";
	private static final String JSON_USER_ID = "userId";

	private static final String REDIS_KEY_PREFIX = "garmin:pkce:";
	private static final Duration PKCE_TTL = Duration.ofMinutes(10);

	private static final SecureRandom SECURE_RANDOM = new SecureRandom();

	private final HttpClient httpClient = HttpClient.newHttpClient();

	@Autowired
	private GarminUserConnectionRepository garminUserConnectionRepository;

	@Autowired
	private OsmAndMapsService osmAndMapsService;

	@Autowired(required = false)
	private RedisConnectionFactory redisConnectionFactory;

	private StringRedisTemplate redisTemplate;

	private final ConcurrentHashMap<String, PendingEntry> localPendingMap = new ConcurrentHashMap<>();

	@Value("${GARMIN_CLIENT_ID:}")
	private String clientId;

	@Value("${GARMIN_CLIENT_SECRET:}")
	private String clientSecret;

	@Value("${GARMIN_REDIRECT_URI:}")
	private String redirectUri;

	@Value("${GARMIN_SUCCESS_REDIRECT:}")
	private String successRedirect;

	private record PendingEntry(int userid, String codeVerifier, long expiresAt) {
	}

	private enum DisconnectResult {
		OK,
		NOT_LINKED
	}

	@PostConstruct
	private void init() {
		if (redisConnectionFactory != null) {
			redisTemplate = new StringRedisTemplate(redisConnectionFactory);
			redisTemplate.afterPropertiesSet();
			LOG.info("GarminConnectController: PKCE store backed by Redis");
		} else {
			LOG.info("GarminConnectController: PKCE store backed by in-memory map (single instance only)");
		}
	}

	private void storePending(String state, int userid, String codeVerifier) {
		String json = GSON.toJson(Map.of("userid", userid, "codeVerifier", codeVerifier));
		if (redisTemplate != null) {
			redisTemplate.opsForValue().set(REDIS_KEY_PREFIX + state, json, PKCE_TTL);
		} else {
			long expiresAt = System.currentTimeMillis() + PKCE_TTL.toMillis();
			localPendingMap.put(state, new PendingEntry(userid, codeVerifier, expiresAt));
			evictExpiredEntries();
		}
	}

	private PendingEntry removePending(String state) {
		if (redisTemplate != null) {
			String json = redisTemplate.opsForValue().getAndDelete(REDIS_KEY_PREFIX + state);
			if (json == null) {
				return null;
			}
			JsonObject obj = new JsonParser().parse(json).getAsJsonObject();
			return new PendingEntry(obj.get("userid").getAsInt(), obj.get("codeVerifier").getAsString(), 0);
		} else {
			PendingEntry entry = localPendingMap.remove(state);
			if (entry == null || System.currentTimeMillis() > entry.expiresAt) {
				return null;
			}
			return entry;
		}
	}

	private void evictExpiredEntries() {
		long now = System.currentTimeMillis();
		localPendingMap.values().removeIf(pendingEntry -> now > pendingEntry.expiresAt);
	}

	public GarminUserConnection getConnectionOrNull(int userid) {
		return garminUserConnectionRepository.findByUserid(userid);
	}

	@GetMapping("/mapapi/garmin/connect/start")
	public void start(HttpServletResponse response) throws IOException {
		if (isNotConfigured()) {
			response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Garmin OAuth is not configured");
			return;
		}
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Login required");
			return;
		}
		String verifier = generateCodeVerifier();
		String challenge = codeChallengeS256(verifier);
		String state = UUID.randomUUID().toString();
		storePending(state, dev.userid, verifier);

		String url = UriComponentsBuilder.fromUriString(AUTH_URL)
				.queryParam("response_type", "code")
				.queryParam("client_id", clientId)
				.queryParam("code_challenge", challenge)
				.queryParam("code_challenge_method", "S256")
				.queryParam("redirect_uri", redirectUri)
				.queryParam("state", state)
				.encode(StandardCharsets.UTF_8)
				.build()
				.toUriString();

		response.sendRedirect(url);
	}

	@GetMapping("/garmin/oauth/callback")
	public void callback(
			@RequestParam(value = "code", required = false) String code,
			@RequestParam(value = "state", required = false) String state,
			@RequestParam(value = "error", required = false) String error,
			HttpServletResponse response) throws IOException {
		if (error != null) {
			LOG.warn("Garmin OAuth error: " + error);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Garmin error: " + error);
			return;
		}
		if (code == null || code.isBlank() || state == null || state.isBlank()) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing code or state");
			return;
		}
		PendingEntry pending = removePending(state);
		if (pending == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid or expired state");
			return;
		}
		if (isNotConfigured()) {
			response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Garmin OAuth is not configured");
			return;
		}

		try {
			JsonObject tokenJson = exchangeAuthorizationCode(code, pending.codeVerifier, redirectUri);
			if (tokenJson == null) {
				response.sendError(HttpServletResponse.SC_BAD_GATEWAY, "Token exchange failed");
				return;
			}

			GarminUserConnection row = getConnectionOrNull(pending.userid);
			if (row == null) {
				row = new GarminUserConnection();
				row.userid = pending.userid;
			}
			applyTokenResponse(row, tokenJson);

			String garminUserId = fetchGarminUserId(row.accessToken);
			if (garminUserId == null) {
				LOG.warn("Garmin OAuth: could not fetch Garmin user id for userid=" + pending.userid);
				response.sendError(HttpServletResponse.SC_BAD_GATEWAY, "Could not load Garmin user id");
				return;
			}

			GarminUserConnection existing = garminUserConnectionRepository.findByGarminUserId(garminUserId);
			if (existing != null && existing.userid != pending.userid) {
				response.sendError(HttpServletResponse.SC_CONFLICT, "This Garmin account is already linked to another OsmAnd user");
				return;
			}

			row.garminUserId = garminUserId;
			applyUserPermissionsFromGarmin(row);
			try {
				garminUserConnectionRepository.save(row);
			} catch (DataIntegrityViolationException ex) {
				LOG.warn("Garmin link failed unique constraint (garmin_user_id)", ex);
				response.sendError(HttpServletResponse.SC_CONFLICT, "This Garmin account is already linked to another OsmAnd user");
				return;
			}

			LOG.info("Garmin linked for OsmAnd userid=" + pending.userid + " garminUserId=" + garminUserId);

			response.sendRedirect(successRedirect + (successRedirect.contains("?") ? "&" : "?") + "garmin=connected");
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Interrupted");
		} catch (Exception e) {
			LOG.error("Garmin callback failed", e);
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}

	@PostMapping("/mapapi/garmin/disconnect")
	public void disconnect(HttpServletResponse response) throws IOException {
		if (isNotConfigured()) {
			response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Garmin OAuth is not configured");
			return;
		}
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Login required");
			return;
		}
		try {
			DisconnectResult r = partnerDisconnect(dev.userid);
			switch (r) {
				case NOT_LINKED -> response.sendRedirect(successRedirect
						+ (successRedirect.contains("?") ? "&" : "?") + "garmin=not_linked");
				case OK -> response.sendRedirect(successRedirect
						+ (successRedirect.contains("?") ? "&" : "?") + "garmin=disconnected");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Interrupted");
		} catch (Exception e) {
			LOG.error("Garmin disconnect failed", e);
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}

	@GetMapping(value = "/mapapi/garmin/status", produces = "application/json")
	public ResponseEntity<String> status() {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(GSON.toJson(Map.of("linked", false)));
		}
		GarminUserConnection row = getConnectionOrNull(dev.userid);
		if (row == null) {
			return ResponseEntity.ok(GSON.toJson(Map.of("linked", false)));
		}
		return ResponseEntity.ok(GSON.toJson(Map.of("linked", true, "garminUserId", row.garminUserId)));
	}

	private JsonObject exchangeAuthorizationCode(String code, String codeVerifier, String redirectUriParam)
			throws IOException, InterruptedException {
		String form = "grant_type=authorization_code"
				+ "&client_id=" + urlEncode(clientId)
				+ "&client_secret=" + urlEncode(clientSecret)
				+ "&code=" + urlEncode(code)
				+ "&code_verifier=" + urlEncode(codeVerifier)
				+ "&redirect_uri=" + urlEncode(redirectUriParam);
		HttpRequest tokenReq = HttpRequest.newBuilder()
				.uri(URI.create(TOKEN_URL))
				.timeout(Duration.ofSeconds(30))
				.header("Content-Type", "application/x-www-form-urlencoded")
				.POST(HttpRequest.BodyPublishers.ofString(form))
				.build();
		HttpResponse<String> tokenRes = httpClient.send(tokenReq, HttpResponse.BodyHandlers.ofString());
		if (tokenRes.statusCode() / 100 != 2) {
			LOG.warn("Garmin token exchange failed: HTTP " + tokenRes.statusCode() + " " + tokenRes.body());
			return null;
		}
		return new JsonParser().parse(tokenRes.body()).getAsJsonObject();
	}

	private void applyUserPermissionsFromGarmin(GarminUserConnection row) {
		row.historicalDataExport = false;
		row.activityExport = false;
		try {
			JsonArray permArray = fetchGarminPermissionsArray(row.accessToken);
			if (permArray == null) {
				return;
			}
			Set<String> granted = permissionsToSet(permArray);
			row.historicalDataExport = granted.contains(PERM_HISTORICAL_DATA_EXPORT);
			row.activityExport = granted.contains(PERM_ACTIVITY_EXPORT);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.warn("Garmin user permissions interrupted");
		} catch (Exception e) {
			LOG.warn("Garmin user permissions request failed", e);
		}
	}

	private JsonArray fetchGarminPermissionsArray(String accessToken) throws IOException, InterruptedException {
		HttpRequest req = HttpRequest.newBuilder()
				.uri(URI.create(USER_PERMISSIONS_URL))
				.timeout(Duration.ofSeconds(15))
				.header("Authorization", "Bearer " + accessToken)
				.GET()
				.build();
		HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
		if (res.statusCode() / 100 != 2) {
			LOG.warn("Garmin user permissions failed: HTTP " + res.statusCode() + " " + res.body());
			return null;
		}
		return extractPermissionsArray(res.body());
	}

	private static JsonArray extractPermissionsArray(String jsonBody) {
		JsonElement root = new JsonParser().parse(jsonBody);
		if (root.isJsonObject()) {
			JsonObject obj = root.getAsJsonObject();
			if (obj.has(JSON_PERMISSIONS) && obj.get(JSON_PERMISSIONS).isJsonArray()) {
				return obj.getAsJsonArray(JSON_PERMISSIONS);
			}
		} else if (root.isJsonArray()) {
			return root.getAsJsonArray();
		}
		LOG.warn("Garmin user permissions: expected object with \"permissions\" array or a JSON array");
		return null;
	}

	private static Set<String> permissionsToSet(JsonArray permArray) {
		Set<String> granted = new HashSet<>();
		for (JsonElement e : permArray) {
			if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isString()) {
				granted.add(e.getAsString());
			}
		}
		return granted;
	}

	private String fetchGarminUserId(String accessToken) throws IOException, InterruptedException {
		HttpRequest userReq = HttpRequest.newBuilder()
				.uri(URI.create(USER_ID_URL))
				.timeout(Duration.ofSeconds(15))
				.header("Authorization", "Bearer " + accessToken)
				.GET()
				.build();
		HttpResponse<String> userRes = httpClient.send(userReq, HttpResponse.BodyHandlers.ofString());
		if (userRes.statusCode() / 100 != 2) {
			LOG.warn("Garmin user id failed: HTTP " + userRes.statusCode() + " " + userRes.body());
			return null;
		}
		JsonObject userJson = new JsonParser().parse(userRes.body()).getAsJsonObject();
		return userJson.get(JSON_USER_ID).getAsString();
	}

	private void applyTokenResponse(GarminUserConnection row, JsonObject tokenJson) {
		row.accessToken = tokenJson.get(JSON_ACCESS_TOKEN).getAsString();
		if (tokenJson.has(JSON_REFRESH_TOKEN) && !tokenJson.get(JSON_REFRESH_TOKEN).isJsonNull()
				&& !tokenJson.get(JSON_REFRESH_TOKEN).getAsString().isEmpty()) {
			row.refreshToken = tokenJson.get(JSON_REFRESH_TOKEN).getAsString();
		}
		int expiresIn = tokenJson.has(JSON_EXPIRES_IN) ? tokenJson.get(JSON_EXPIRES_IN).getAsInt() : 86400;
		row.accessExpiresTime = System.currentTimeMillis() + Math.max(60, expiresIn - 600) * 1000L;
	}

	private boolean ensureAccessTokenFresh(GarminUserConnection row) throws IOException, InterruptedException {
		if (row == null) {
			return false;
		}
		if (System.currentTimeMillis() < row.accessExpiresTime) {
			return true;
		}
		return refreshAccessToken(row);
	}

	private boolean refreshAccessToken(GarminUserConnection row) throws IOException, InterruptedException {
		if (row == null || row.refreshToken == null || row.refreshToken.isBlank()) {
			if (row != null) {
				LOG.warn("Garmin refresh skipped: no refresh_token for userid=" + row.userid);
			}
			return false;
		}
		String form = "grant_type=refresh_token"
				+ "&client_id=" + urlEncode(clientId)
				+ "&client_secret=" + urlEncode(clientSecret)
				+ "&refresh_token=" + urlEncode(row.refreshToken);
		HttpRequest req = HttpRequest.newBuilder()
				.uri(URI.create(TOKEN_URL))
				.timeout(Duration.ofSeconds(30))
				.header("Content-Type", "application/x-www-form-urlencoded")
				.POST(HttpRequest.BodyPublishers.ofString(form))
				.build();
		HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
		if (res.statusCode() / 100 != 2) {
			LOG.warn("Garmin refresh_token failed: HTTP " + res.statusCode() + " " + res.body());
			return false;
		}
		JsonObject tokenJson = new JsonParser().parse(res.body()).getAsJsonObject();
		applyTokenResponse(row, tokenJson);
		garminUserConnectionRepository.save(row);
		return true;
	}

	private DisconnectResult partnerDisconnect(int userid) throws IOException, InterruptedException {
		GarminUserConnection row = garminUserConnectionRepository.findByUserid(userid);
		if (row == null) {
			return DisconnectResult.NOT_LINKED;
		}
		if (!ensureAccessTokenFresh(row)) {
			LOG.warn("Garmin disconnect: token refresh failed for userid=" + userid);
		}
		HttpRequest del = HttpRequest.newBuilder()
				.uri(URI.create(PARTNER_REGISTRATION_URL))
				.timeout(Duration.ofSeconds(30))
				.header("Authorization", "Bearer " + row.accessToken)
				.DELETE()
				.build();
		HttpResponse<String> res = httpClient.send(del, HttpResponse.BodyHandlers.ofString());
		int code = res.statusCode();
		if (code / 100 != 2 && code != 404) {
			LOG.warn("Garmin DELETE user/registration failed: HTTP " + code + " " + res.body());
		}
		garminUserConnectionRepository.delete(row);
		return DisconnectResult.OK;
	}

	private boolean isNotConfigured() {
		return clientId == null || clientId.isBlank()
				|| clientSecret == null || clientSecret.isBlank()
				|| redirectUri == null || redirectUri.isBlank();
	}

	private static String urlEncode(String v) {
		return URLEncoder.encode(Objects.toString(v, ""), StandardCharsets.UTF_8);
	}

	private static String generateCodeVerifier() {
		byte[] bytes = new byte[32];
		SECURE_RANDOM.nextBytes(bytes);
		return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
	}

	private static String codeChallengeS256(String verifier) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			byte[] digest = md.digest(verifier.getBytes(StandardCharsets.US_ASCII));
			return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}
}
