package net.osmand.server.ws;

import java.security.Principal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import jakarta.servlet.http.HttpServletRequest;

import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;

@RestController
@RequestMapping("/api/translation")
public class UserTranslationsController {

	private static final Log LOG = LogFactory.getLog(UserTranslationsController.class);

	@Autowired
	private UserTranslationsService userTranslationsService;

	@Autowired
	private RateLimitService rateLimitService;

	private final Gson gson = new Gson();

	public record AuthenticateRequest(String translationId, String password, String alias) {}

	public record CreateTranslationRequest(String password) {}


	/**
	 * Creates a new translation session.
	 * Request body: {"password": "secret"} (optional password)
	 * Response: {"sessionId": "translation123"}
	 *
	 * @param request JSON request with optional password
	 * @param headers WebSocket headers
	 * @param principal user principal
	 * @return UserTranslationDTO with session ID
	 */
	@MessageMapping("/translation/create")
	public UserTranslationDTO createTranslation(@Payload(required = false) CreateTranslationRequest request,
	                                             SimpMessageHeaderAccessor headers,
	                                             Principal principal) {
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null) {
			return null;
		}
		UserTranslation translation = userTranslationsService.createTranslation(user, null, headers);

		if (request != null && hasPassword(request.password())) {
			userTranslationsService.setTranslationPassword(translation, request.password());
		}

		return new UserTranslationDTO(translation.getSessionId());
	}

	@MessageMapping("/translation/{translationId}/load")
	public void loadTranslationHistory(@DestinationVariable String translationId,
	                                   SimpMessageHeaderAccessor headers,
	                                   Principal principal) {
		if (headers == null || !isValidTranslationId(translationId)) {
			return;
		}
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		if (ust == null) {
			return;
		}

		CloudUser user = userTranslationsService.getUser(principal, headers, true);
		if (user == null || !userTranslationsService.hasOperationPermission(ust, user, headers)) {
			userTranslationsService.sendError("Access denied to translation", headers);
			return;
		}

		userTranslationsService.load(ust, headers);
	}

	@MessageMapping("/whoami")
	public void whoami(SimpMessageHeaderAccessor headers, Principal principal) {
		CloudUser user = userTranslationsService.getUser(principal, headers, true);
		if (user != null) {
			userTranslationsService.whoami(user, headers);
		}
	}

	@MessageMapping("/translation/{translationId}/startSharing")
	public String startSharing(@DestinationVariable String translationId,
	                           SimpMessageHeaderAccessor headers,
	                           Principal principal) {
		if (!isValidTranslationId(translationId)) {
			return null;
		}
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null || ust == null) {
			return null;
		}
		try {
			userTranslationsService.startSharing(ust, user, headers);
			return "OK";
		} catch (SecurityException e) {
			return null;
		}
	}

	@MessageMapping("/translation/{translationId}/stopSharing")
	public String stopSharing(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers,
	                          Principal principal) {
		if (!isValidTranslationId(translationId)) {
			return null;
		}
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null || ust == null) {
			return null;
		}
		try {
			userTranslationsService.stopSharing(ust, user, headers);
			return "OK";
		} catch (SecurityException e) {
			return null;
		}
	}

	@MessageMapping("/translation/{translationId}/sendMessage")
	public void sendMessage(@DestinationVariable String translationId, @Payload String message,
	                        Principal principal, SimpMessageHeaderAccessor headers) {
		if (!isValidTranslationId(translationId)) {
			return;
		}
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		if (ust != null) {
			CloudUser user = userTranslationsService.getUser(principal, headers, true);
			if (user != null) {
				try {
					userTranslationsService.sendMessage(ust, user, message, headers);
				} catch (SecurityException e) {
					// Access denied - send error message
					userTranslationsService.sendError("Access denied to translation", headers);
				}
			}
		}
	}

	@MessageMapping("/translation/{translationId}/setPassword")
	public String setPassword(@DestinationVariable String translationId, @Payload String password,
	                          SimpMessageHeaderAccessor headers, Principal principal) {
		if (!isValidTranslationId(translationId)) {
			return null;
		}
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (ust == null || user == null) {
			return null;
		}
		if (ust.getOwnerId() != user.id) {
			userTranslationsService.sendError("Only owner can set password", headers);
			return null;
		}
		userTranslationsService.setTranslationPassword(ust, password);
		ust.getVerifiedUsers().clear();
		return "OK";
	}

	private boolean isValidTranslationId(String translationId) {
		if (translationId == null || translationId.isEmpty()) {
			return false;
		}
		if (translationId.length() > 128) {
			return false;
		}
		// Allow only alphanumeric characters and safe characters
		return translationId.matches(UserTranslationsService.VALID_TRANSLATION_ID_PATTERN);
	}

	private boolean hasPassword(String password) {
		return password != null && !password.isEmpty() && !password.equals("{}");
	}

	/**
	 * Authenticates user for room access and returns Bearer token.
	 * POST /api/translation/authenticate
	 * Body: {"translationId": "room123", "password": "secret", "alias": "user1"}
	 * Response: {"token": "bearer_token_string"}
	 */
	@PostMapping("/authenticate")
	public ResponseEntity<String> authenticate(@RequestBody AuthenticateRequest request, HttpServletRequest httpRequest) {
		try {
			// Get client IP for rate limiting (forward-headers-strategy: native handles X-Forwarded-For)
			String clientIp = httpRequest != null ? httpRequest.getRemoteAddr() : RateLimitService.UNKNOWN_IP;

			if (rateLimitService.isRateLimited(clientIp)) {
				LOG.warn("Rate limited authentication attempt from IP: " + clientIp);
				return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
					.body("Too many failed authentication attempts. Please try again later.");
			}

			if (request.translationId() == null || request.translationId().isEmpty()) {
				return ResponseEntity.badRequest().body("translationId is required");
			}
			if (!request.translationId().matches(UserTranslationsService.VALID_TRANSLATION_ID_PATTERN)) {
				return ResponseEntity.badRequest().body("Invalid translationId format");
			}

			int userId = 0;
			// Note: In REST context, we don't have WebSocket session, so userId is 0 for anonymous users
			// This is fine - tokens work for anonymous users too

			String token = userTranslationsService.authenticateRoom(
				request.translationId(),
				request.password(),
				userId,
				request.alias()
			);

			if (token == null) {
				rateLimitService.trackFailedAttempt(clientIp);
				LOG.warn("Authentication failed for translationId: " + request.translationId() + " from IP: " + clientIp);
				return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Authentication failed");
			}

			rateLimitService.resetRateLimit(clientIp);
			
			JsonObject response = new JsonObject();
			response.addProperty("token", token);
			return ResponseEntity.ok(gson.toJson(response));
			
		} catch (Exception e) {
			LOG.error("Error in room authentication", e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Internal server error");
		}
	}
}