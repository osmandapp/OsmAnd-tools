package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import net.osmand.server.api.services.GarminConnectService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.GarminUserConnectionRepository.GarminUserConnection;
import net.osmand.server.api.services.OsmAndMapsService;

@RestController
public class GarminConnectController {

	private static final Gson GSON = new Gson();
	private static final Log LOG = LogFactory.getLog(GarminConnectController.class);

	private static final String REDIS_KEY_PREFIX = "garmin:pkce:";
	private static final java.time.Duration PKCE_TTL = java.time.Duration.ofMinutes(10);

	private static final String GARMIN_STATUS_LINKED_KEY = "linked";
	private static final String GARMIN_STATUS_SYNC_TIME_MS_KEY = "syncTimeMs";

	private static final SecureRandom SECURE_RANDOM = new SecureRandom();

	@Autowired
	private GarminConnectService garminConnectService;

	@Autowired
	private OsmAndMapsService osmAndMapsService;

	@Autowired(required = false)
	private RedisConnectionFactory redisConnectionFactory;

	private StringRedisTemplate redisTemplate;

	private final ConcurrentHashMap<String, PendingEntry> localPendingMap = new ConcurrentHashMap<>();

	private record PendingEntry(int userid, String codeVerifier, long expiresAt) {
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

	@GetMapping("/mapapi/garmin/connect/start")
	public void start(HttpServletResponse response) throws IOException {
		if (!garminConnectService.isOAuthConfigured()) {
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

		String url = UriComponentsBuilder.fromUriString(GarminConnectService.AUTH_URL)
				.queryParam("response_type", "code")
				.queryParam("client_id", garminConnectService.getClientId())
				.queryParam("code_challenge", challenge)
				.queryParam("code_challenge_method", "S256")
				.queryParam("redirect_uri", garminConnectService.getRedirectUri())
				.queryParam("state", state)
				.encode(StandardCharsets.UTF_8)
				.build()
				.toUriString();

		response.sendRedirect(url);
	}

	@GetMapping("/garmin/oauth/callback")
	public void callback(@RequestParam(value = "code", required = false) String code,
	                     @RequestParam(value = "state", required = false) String state,
	                     @RequestParam(value = "error", required = false) String error,
	                     HttpServletResponse response) throws IOException {
		if (error != null) {
			LOG.warn("Garmin OAuth error: " + error);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Garmin authorization failed");
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
		if (!garminConnectService.isOAuthConfigured()) {
			response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Garmin OAuth is not configured");
			return;
		}

		try {
			GarminConnectService.GarminLinkResult linkResult = garminConnectService.linkAfterAuthorization(
					pending.userid, code, pending.codeVerifier);
			switch (linkResult.status()) {
				case OK -> {
					if (linkResult.historicalDataExport()) {
						handleActivityBackfill(pending.userid);
					}
					String successRedirect = garminConnectService.getSuccessRedirect();
					response.sendRedirect(successRedirect
							+ (successRedirect.contains("?") ? "&" : "?") + "garmin=connected");
				}
				case TOKEN_EXCHANGE_FAILED ->
						response.sendError(HttpServletResponse.SC_BAD_GATEWAY, "Token exchange failed");
				case GARMIN_USER_ID_FAILED ->
						response.sendError(HttpServletResponse.SC_BAD_GATEWAY, "Could not load Garmin user id");
				case CONFLICT -> response.sendError(HttpServletResponse.SC_CONFLICT,
						"This Garmin account is already linked to another OsmAnd user");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Interrupted");
		} catch (Exception e) {
			LOG.error("Garmin callback failed", e);
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Garmin link failed");
		}
	}

	@PostMapping("/mapapi/garmin/disconnect")
	public void disconnect(HttpServletResponse response) throws IOException {
		if (!garminConnectService.isOAuthConfigured()) {
			response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Garmin OAuth is not configured");
			return;
		}
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Login required");
			return;
		}
		try {
			GarminConnectService.PartnerDisconnectResult r = garminConnectService.partnerDisconnect(dev.userid);
			String successRedirect = garminConnectService.getSuccessRedirect();
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
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Garmin disconnect failed");
		}
	}

	@PostMapping(value = "/garmin/webhook/activity-ping", consumes = "application/json")
	public ResponseEntity<Void> activityPing(@RequestBody String body, HttpServletRequest request) {
		return runGarminWebhook(request, () -> handleActivityPingPayloadAsync(body));
	}

	@PostMapping(value = "/garmin/webhook/user-permissions", consumes = "application/json")
	public ResponseEntity<Void> userPermissionsWebhook(@RequestBody String body, HttpServletRequest request) {
		return runGarminWebhook(request, () -> {
			try {
				garminConnectService.applyUserPermissionsWebhookPayload(body);
			} catch (Exception e) {
				LOG.error("Garmin user permissions webhook: processing failed", e);
			}
		});
	}

	@PostMapping(value = "/garmin/webhook/deregistrations", consumes = "application/json")
	public ResponseEntity<Void> deregistrationsWebhook(@RequestBody String body, HttpServletRequest request) {
		return runGarminWebhook(request, () -> {
			try {
				garminConnectService.applyDeregistrationsWebhookPayload(body);
			} catch (Exception e) {
				LOG.error("Garmin deregistrations webhook: processing failed", e);
			}
		});
	}

	@GetMapping(value = "/mapapi/garmin/status", produces = "application/json")
	public ResponseEntity<String> status() {
		CloudUserDevice dev = osmAndMapsService.checkUser();
		if (dev == null) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(GSON.toJson(Map.of(GARMIN_STATUS_LINKED_KEY, false)));
		}
		GarminUserConnection row = garminConnectService.getConnectionOrNull(dev.userid);
		if (row == null) {
			return ResponseEntity.ok(GSON.toJson(Map.of(GARMIN_STATUS_LINKED_KEY, false)));
		}
		return ResponseEntity.ok(GSON.toJson(Map.of(GARMIN_STATUS_LINKED_KEY, true,
				GARMIN_STATUS_SYNC_TIME_MS_KEY, row.lastGarminImportAt)));
	}

	private void handleActivityBackfill(int userid) {
		CompletableFuture.runAsync(() -> {
			try {
				garminConnectService.runActivityBackfillForUser(userid);
			} catch (Exception e) {
				LOG.error("Garmin backfill: failed for userid=" + userid, e);
			}
		});
	}

	private void handleActivityPingPayloadAsync(String body) {
		CompletableFuture.runAsync(() -> {
			try {
				garminConnectService.processActivityPingBody(body);
			} catch (Exception e) {
				LOG.error("Garmin activity ping: processing failed", e);
			}
		});
	}

	// Redis-based or in-memory store for pending PKCE verifiers, keyed by state.
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

	// Garmin webhooks validation
	private ResponseEntity<Void> runGarminWebhook(HttpServletRequest request, Runnable action) {
		String webhookClientId = garminConnectService.getClientId();
		if (webhookClientId == null || webhookClientId.isBlank()) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
		}
		if (!webhookClientId.equals(request.getHeader("garmin-client-id"))) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
		}
		action.run();
		return ResponseEntity.ok().build();
	}

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
