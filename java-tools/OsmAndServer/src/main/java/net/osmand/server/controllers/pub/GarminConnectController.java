package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

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
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.GarminUserConnectionRepository;
import net.osmand.server.api.repo.GarminUserConnectionRepository.GarminUserConnection;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.StorageService.InternalZipFile;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.utils.GarminFitToGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import net.osmand.shared.KException;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.io.KFile;

@RestController
public class GarminConnectController {

	private static final Gson GSON = new Gson();

	private static final Log LOG = LogFactory.getLog(GarminConnectController.class);

	private static final String AUTH_URL = "https://connect.garmin.com/oauth2Confirm";
	private static final String TOKEN_URL = "https://diauth.garmin.com/di-oauth2-service/oauth/token";
	private static final String USER_ID_URL = "https://apis.garmin.com/wellness-api/rest/user/id";
	private static final String USER_PERMISSIONS_URL = "https://apis.garmin.com/wellness-api/rest/user/permissions";
	private static final String PARTNER_REGISTRATION_URL = "https://apis.garmin.com/wellness-api/rest/user/registration";
	private static final String BACKFILL_ACTIVITIES_URL = "https://apis.garmin.com/wellness-api/rest/backfill/activities";

	private static final String JSON_PERMISSIONS = "permissions";
	private static final String JSON_ACTIVITY_FILES = "activityFiles";
	private static final String JSON_USER_PERMISSIONS_CHANGE = "userPermissionsChange";
	private static final String JSON_DEREGISTRATIONS = "deregistrations";

	private static final String PERM_HISTORICAL_DATA_EXPORT = "HISTORICAL_DATA_EXPORT";
	private static final String PERM_ACTIVITY_EXPORT = "ACTIVITY_EXPORT";

	private static final String JSON_ACCESS_TOKEN = "access_token";
	private static final String JSON_REFRESH_TOKEN = "refresh_token";
	private static final String JSON_EXPIRES_IN = "expires_in";
	private static final String JSON_USER_ID = "userId";

	private static final String REDIS_KEY_PREFIX = "garmin:pkce:";
	private static final Duration PKCE_TTL = Duration.ofMinutes(10);

	private static final long MAX_BACKFILL_RANGE_SEC = 30L * 24 * 3600;
	private static final int ACTIVITY_BACKFILL_DEFAULT_DAYS_BACK = 180;

	// for tests
	private static final long SEC_PER_DAY = 24L * 3600;
	private static final long backfillTestSingleDayStartUtcSec = 1775433600L;

	private static final Pattern SAFE_GARMIN_SUMMARY_ID = Pattern.compile("^[a-zA-Z0-9._-]+$");
	private static final int MAX_ACTIVITY_NAME_PREFIX_LEN = 100;
	private static final String GPX_FOLDER_GARMIN = "Garmin";

	private static final SecureRandom SECURE_RANDOM = new SecureRandom();

	private final HttpClient httpClient = HttpClient.newHttpClient();

	@Autowired
	private GarminUserConnectionRepository garminUserConnectionRepository;

	@Autowired
	private OsmAndMapsService osmAndMapsService;

	@Autowired
	private CloudUserDevicesRepository devicesRepository;

	@Autowired
	private UserdataService userdataService;

	@Autowired
	private CloudUserFilesRepository cloudUserFilesRepository;

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
			if (!obj.has("userid") || obj.get("userid").isJsonNull()) {
				return null;
			}
			String codeVerifier = jsonObjectMemberAsString(obj, "codeVerifier");
			if (codeVerifier == null) {
				return null;
			}
			return new PendingEntry(obj.get("userid").getAsInt(), codeVerifier, 0);
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

			if (row.historicalDataExport) {
				handleActivityBackfill(pending.userid);
			}

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

	@PostMapping(value = "/garmin/webhook/activity-ping", consumes = "application/json")
	public ResponseEntity<Void> activityPing(@RequestBody String body) {
		handleActivityPingPayloadAsync(body);
		return ResponseEntity.ok().build();
	}

	@PostMapping(value = "/garmin/webhook/user-permissions", consumes = "application/json")
	public ResponseEntity<Void> userPermissionsWebhook(@RequestBody String body) {
		try {
			applyUserPermissionsWebhookPayload(body);
		} catch (Exception e) {
			LOG.error("Garmin user permissions webhook: processing failed", e);
		}
		return ResponseEntity.ok().build();
	}

	@PostMapping(value = "/garmin/webhook/deregistrations", consumes = "application/json")
	public ResponseEntity<Void> deregistrationsWebhook(@RequestBody String body) {
		try {
			applyDeregistrationsWebhookPayload(body);
		} catch (Exception e) {
			LOG.error("Garmin deregistrations webhook: processing failed", e);
		}
		return ResponseEntity.ok().build();
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
		applyGarminPermissionFlags(row, Set.of());
		try {
			JsonArray permArray = fetchGarminPermissionsArray(row.accessToken);
			if (permArray == null) {
				return;
			}
			applyGarminPermissionFlags(row, permissionsToSet(permArray));
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

	private static void applyGarminPermissionFlags(GarminUserConnection row, Set<String> granted) {
		row.historicalDataExport = granted.contains(PERM_HISTORICAL_DATA_EXPORT);
		row.activityExport = granted.contains(PERM_ACTIVITY_EXPORT);
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
		return jsonObjectMemberAsString(userJson, JSON_USER_ID);
	}

	private void applyTokenResponse(GarminUserConnection row, JsonObject tokenJson) {
		String access = jsonObjectMemberAsString(tokenJson, JSON_ACCESS_TOKEN);
		row.accessToken = access != null ? access : "";
		String refresh = jsonObjectMemberAsString(tokenJson, JSON_REFRESH_TOKEN);
		if (refresh != null) {
			row.refreshToken = refresh;
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

	private void handleActivityPingPayloadAsync(String body) {
		CompletableFuture.runAsync(() -> {
			try {
				processActivityPingBody(body);
			} catch (Exception e) {
				LOG.error("Garmin activity ping: processing failed", e);
			}
		});
	}

	private void applyUserPermissionsWebhookPayload(String body) {
		if (body == null || body.isBlank()) {
			return;
		}
		JsonElement root = new JsonParser().parse(body);
		if (!root.isJsonObject()) {
			return;
		}
		JsonObject obj = root.getAsJsonObject();
		JsonElement changes = obj.get(JSON_USER_PERMISSIONS_CHANGE);
		if (changes == null || !changes.isJsonArray()) {
			return;
		}
		for (JsonElement el : changes.getAsJsonArray()) {
			if (!el.isJsonObject()) {
				continue;
			}
			JsonObject entry = el.getAsJsonObject();
			String garminUserId = jsonObjectMemberAsString(entry, JSON_USER_ID);
			JsonElement perms = entry.get(JSON_PERMISSIONS);
			if (garminUserId == null || perms == null || !perms.isJsonArray()) {
				continue;
			}
			GarminUserConnection conn = garminUserConnectionRepository.findByGarminUserId(garminUserId);
			if (conn == null) {
				continue;
			}
			applyGarminPermissionFlags(conn, permissionsToSet(perms.getAsJsonArray()));
			garminUserConnectionRepository.save(conn);
		}
	}

	private void applyDeregistrationsWebhookPayload(String body) {
		if (body == null || body.isBlank()) {
			return;
		}
		JsonElement root = new JsonParser().parse(body);
		if (!root.isJsonObject()) {
			return;
		}
		JsonObject obj = root.getAsJsonObject();
		JsonElement entries = obj.get(JSON_DEREGISTRATIONS);
		if (entries == null || !entries.isJsonArray()) {
			return;
		}
		for (JsonElement el : entries.getAsJsonArray()) {
			if (!el.isJsonObject()) {
				continue;
			}
			String garminUserId = jsonObjectMemberAsString(el.getAsJsonObject(), JSON_USER_ID);
			if (garminUserId == null) {
				continue;
			}
			GarminUserConnection conn = garminUserConnectionRepository.findByGarminUserId(garminUserId);
			if (conn != null) {
				garminUserConnectionRepository.delete(conn);
				LOG.info("Garmin deregistration: removed link userid=" + conn.userid + " garminUserId=" + garminUserId);
			}
		}
	}

	private void handleActivityBackfill(int userid) {
		CompletableFuture.runAsync(() -> {
			try {
				runActivityBackfillForUser(userid);
			} catch (Exception e) {
				LOG.error("Garmin backfill: failed for userid=" + userid, e);
			}
		});
	}

	/** Processes the JSON body of the Garmin activity ping, which contains an array of recently completed activity files.
	 * For each file, checks permissions and downloads/uploads if valid. */
	private void processActivityPingBody(String body) {
		if (body == null || body.isBlank()) {
			return;
		}
		JsonElement root = new JsonParser().parse(body);
		if (!root.isJsonObject()) {
			return;
		}
		JsonObject obj = root.getAsJsonObject();
		if (!obj.has(JSON_ACTIVITY_FILES) || !obj.get(JSON_ACTIVITY_FILES).isJsonArray()) {
			return;
		}
		Map<String, GarminUserConnection> connByGarminUserId = new HashMap<>();
		Map<String, CloudUserDevice> webDevByGarminUserId = new HashMap<>();
		for (JsonElement el : obj.getAsJsonArray(JSON_ACTIVITY_FILES)) {
			if (el.isJsonObject()) {
				processOneActivityFile(el.getAsJsonObject(), connByGarminUserId, webDevByGarminUserId);
			}
		}
	}

	/** Runs the backfill for one user, splitting into multiple requests if needed. */
	private void runActivityBackfillForUser(int userid) throws IOException, InterruptedException {
		GarminUserConnection row = garminUserConnectionRepository.findByUserid(userid);
		if (row == null || !row.historicalDataExport) {
			return;
		}
		if (isNotConfigured()) {
			LOG.warn("Garmin backfill: OAuth client not configured");
			return;
		}
		if (!ensureAccessTokenFresh(row)) {
			LOG.warn("Garmin backfill: cannot refresh access token userid=" + userid);
			return;
		}
		long endSec;
		long startSec;
		if (backfillTestSingleDayStartUtcSec >= 0L) {
			startSec = backfillTestSingleDayStartUtcSec;
			endSec = startSec + SEC_PER_DAY - 1;
		} else {
			endSec = System.currentTimeMillis() / 1000L;
			startSec = endSec - ACTIVITY_BACKFILL_DEFAULT_DAYS_BACK * 24L * 3600L;
		}
		for (long wStart = startSec; wStart <= endSec; ) {
			long wEnd = Math.min(wStart + MAX_BACKFILL_RANGE_SEC - 1, endSec);
			String uri = BACKFILL_ACTIVITIES_URL
					+ "?summaryStartTimeInSeconds=" + wStart
					+ "&summaryEndTimeInSeconds=" + wEnd;
			HttpRequest req = HttpRequest.newBuilder()
					.uri(URI.create(uri))
					.timeout(Duration.ofSeconds(60))
					.header("Authorization", "Bearer " + row.accessToken)
					.GET()
					.build();
			HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
			int code = res.statusCode();
			if (code == 202 || code == 409) {
				LOG.info("Garmin backfill/activities: HTTP " + code + " range " + wStart + ".." + wEnd + " userid=" + userid);
			} else {
				LOG.warn("Garmin backfill/activities: HTTP " + code + " " + res.body());
			}
			wStart = wEnd + 1;
		}
	}

	private void processOneActivityFile(JsonObject o, Map<String, GarminUserConnection> connByGarminUserId,
			Map<String, CloudUserDevice> webDevByGarminUserId) {
		// Check user
		String garminUserId = jsonObjectMemberAsString(o, JSON_USER_ID);
		String callbackUrl = jsonObjectMemberAsString(o, "callbackURL");
		if (callbackUrl == null || garminUserId == null) {
			LOG.warn("Garmin activityFiles: missing userId or callbackURL");
			return;
		}
		GarminUserConnection conn = connByGarminUserId.computeIfAbsent(garminUserId,
				id -> garminUserConnectionRepository.findByGarminUserId(id));
		if (conn == null) {
			LOG.warn("Garmin activityFiles: no linked OsmAnd user for garminUserId=" + garminUserId);
			return;
		}
		// Check permissions
		if (!conn.activityExport) {
			LOG.warn("Garmin activityFiles: ACTIVITY_EXPORT not granted, userid=" + conn.userid + " garminUserId=" + garminUserId);
			return;
		}
		CloudUserDevice dev = webDevByGarminUserId.computeIfAbsent(garminUserId,
				id -> devicesRepository.findTopByUseridAndDeviceidOrderByUdpatetimeDesc(conn.userid,
						UserdataService.TOKEN_DEVICE_WEB));
		if (dev == null) {
			LOG.warn("Garmin activityFiles: no web device for userid=" + conn.userid);
			return;
		}
		String fileType = jsonObjectMemberAsString(o, "fileType");
		if (fileType == null || !fileType.equalsIgnoreCase("FIT")) {
			return;
		}
		// Check file name and skip if already exists in cloud
		String baseFileName = buildGarminActivityBaseFileName(o);
		if (baseFileName == null) {
			LOG.warn("Garmin activityFiles: missing or invalid summaryId / file base name");
			return;
		}
		String cloudName = GPX_FOLDER_GARMIN + "/" + baseFileName + ".gpx";
		if (cloudUserFilesRepository.existsByUseridAndNameAndType(conn.userid, cloudName, UserdataService.FILE_TYPE_GPX)) {
			LOG.info("Garmin activityFiles: skip duplicate " + cloudName + " userid=" + conn.userid);
			return;
		}
		// Check access token and refresh if needed
		try {
			if (!ensureAccessTokenFresh(conn)) {
				LOG.warn("Garmin activityFiles: token refresh failed userid=" + conn.userid);
				return;
			}
		} catch (IOException | InterruptedException e) {
			reinterruptIfInterrupted(e);
			LOG.warn("Garmin activityFiles: token refresh error userid=" + conn.userid, e);
			return;
		}
		byte[] raw;
		try {
			raw = httpGetCallbackBytes(callbackUrl, conn.accessToken);
		} catch (IOException | InterruptedException e) {
			reinterruptIfInterrupted(e);
			LOG.warn("Garmin activityFiles: download failed baseFileName=" + baseFileName, e);
			return;
		}
		File tmp = null;
		try {
			tmp = File.createTempFile("garmin-act-", ".gpx");
			long creatTime;
			GpxFile gpx = GarminFitToGpxParser.fromFitBytes(raw, baseFileName);
			if (gpx == null) {
				try {
					Files.deleteIfExists(tmp.toPath());
				} catch (IOException e) {
					LOG.warn("Garmin activityFiles: FIT parse failed userid=" + conn.userid);
				}
				return;
			}
			creatTime = gpx.getMetadata().getTime();
			KException werr = GpxUtilities.INSTANCE.writeGpxFile(new KFile(tmp.getAbsolutePath()), gpx);
			if (werr != null) {
				try {
					Files.deleteIfExists(tmp.toPath());
				} catch (IOException e) {
					LOG.warn("Garmin activityFiles: GPX write failed userid=" + conn.userid + " " + werr.getMessage());
				}
				return;
			}
			InternalZipFile zip = InternalZipFile.buildFromFileAndDelete(tmp);
			tmp = null;
			userdataService.validateUserForUpload(dev, UserdataService.FILE_TYPE_GPX, zip.getSize());
			userdataService.uploadFile(zip, dev, cloudName, UserdataService.FILE_TYPE_GPX, creatTime);
			LOG.info("Garmin activityFiles: stored FIT as GPX " + cloudName + " userid=" + conn.userid);
		} catch (OsmAndPublicApiException e) {
			LOG.warn("Garmin activityFiles: cloud upload rejected userid=" + conn.userid + " " + e.getMessage());
		} catch (Exception e) {
			LOG.warn("Garmin activityFiles: upload failed userid=" + conn.userid, e);
		} finally {
			if (tmp != null) {
				try {
					Files.deleteIfExists(tmp.toPath());
				} catch (IOException ignored) {
					LOG.info("Garmin activityFiles: failed to delete temp file " + tmp.getAbsolutePath());
				}
			}
		}
	}

	/** Returns null if member is missing, null, not a string or blank */
	private static String jsonObjectMemberAsString(JsonObject o, String member) {
		JsonElement el = o.get(member);
		if (el == null || el.isJsonNull() || !el.isJsonPrimitive()) {
			return null;
		}
		JsonPrimitive p = el.getAsJsonPrimitive();
		if (!p.isString()) {
			return null;
		}
		String s = p.getAsString();
		return s.isBlank() ? null : s;
	}

	private static void reinterruptIfInterrupted(Exception e) {
		if (e instanceof InterruptedException) {
			Thread.currentThread().interrupt();
		}
	}

	/** Cloud file basename: {@code summaryId}, or {@code <sanitizedActivityName>-<summaryId>} when the title passes the same safe-character rules. */
	private static String buildGarminActivityBaseFileName(JsonObject activityFilePingEntry) {
		String summaryId = jsonObjectMemberAsString(activityFilePingEntry, "summaryId");
		if (summaryId == null) {
			return null;
		}
		String sid = summaryId.trim();
		if (sid.endsWith("-file")) {
			sid = sid.substring(0, sid.length() - 5);
		}
		if (sid.length() > 200 || !SAFE_GARMIN_SUMMARY_ID.matcher(sid).matches()) {
			return null;
		}
		String activityName = jsonObjectMemberAsString(activityFilePingEntry, "activityName");
		String baseName = sid;
		if (activityName != null && !activityName.isBlank()) {
			Pattern nonFileSafeActivityName = Pattern.compile("[^a-zA-Z0-9._-]+");
			String t = nonFileSafeActivityName.matcher(activityName.trim()).replaceAll("_").replaceAll("_+", "_");
			t = t.replaceAll("^_+|_+$", "");
			if (!t.isEmpty() && SAFE_GARMIN_SUMMARY_ID.matcher(t).matches()) {
				if (t.length() > MAX_ACTIVITY_NAME_PREFIX_LEN) {
					t = t.substring(0, MAX_ACTIVITY_NAME_PREFIX_LEN).replaceAll("_+$", "");
				}
				if (!t.isEmpty()) {
					baseName = t + "-" + sid;
				}
			}
		}
		return baseName;
	}

	/** Returns the downloaded bytes. Throws IOException on HTTP errors or if the URL is expired (HTTP 410). */
	private byte[] httpGetCallbackBytes(String url, String bearerAccessToken) throws IOException, InterruptedException {
		HttpRequest.Builder rb = HttpRequest.newBuilder()
				.uri(URI.create(url))
				.timeout(Duration.ofSeconds(60));
		if (bearerAccessToken != null && !bearerAccessToken.isBlank()) {
			rb.header("Authorization", "Bearer " + bearerAccessToken);
		}
		HttpRequest req = rb.GET().build();
		HttpResponse<byte[]> res = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
		int code = res.statusCode();
		if (code == 410) {
			throw new IOException("Garmin callback URL expired (HTTP 410)");
		}
		if (code / 100 != 2) {
			throw new IOException("Garmin callback HTTP " + code);
		}
		return res.body();
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
