package net.osmand.server.ws;

import java.security.Principal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Controller;

import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;

@Controller
public class UserTranslationsController {

	// Client sends first 16 hex chars of SHA-256(key) as the translation ID.
	private static final int TRANSLATION_ID_HEX_LENGTH = 16;

	@Autowired
	private UserTranslationsService userTranslationsService;

	// Body may include fromTime / toTime (epoch ms) to load only messages in that
	// interval. Missing or <= 0 bounds mean unbounded, so {} loads the full history.
	public record LoadRequest(long fromTime, long toTime) {}
	// Target user for approve/deny share commands.
	public record ShareUserRequest(int userId) {}

	@MessageMapping("/translation/{translationId}/load")
	public void loadTranslation(@DestinationVariable String translationId,
	                            @Payload LoadRequest req,
	                            SimpMessageHeaderAccessor headers,
	                            Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		if (ust == null) {
			return;
		}
		CloudUser user = userTranslationsService.getUser(principal, headers, true);
		userTranslationsService.load(ust, req.fromTime(), req.toTime(), user, headers);
	}
	
	@MessageMapping("/translation/{translationId}/startSharing")
	public void startSharing(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers,
			Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null || ust == null) {
			return;
		}
		userTranslationsService.startSharing(ust, user, headers);
	}

	@MessageMapping("/translation/{translationId}/stopSharing")
	public void stopSharing(@DestinationVariable String translationId,
	                        SimpMessageHeaderAccessor headers,
	                        Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null || ust == null) {
			return;
		}
		userTranslationsService.stopSharing(ust, user, headers);
	}

	@MessageMapping("/translation/{translationId}/requestShare")
	public void requestShare(@DestinationVariable String translationId,
	                         SimpMessageHeaderAccessor headers,
	                         Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null || ust == null) {
			return;
		}
		userTranslationsService.requestShare(ust, user);
	}

	@MessageMapping("/translation/{translationId}/approveShare")
	public void approveShare(@DestinationVariable String translationId, @Payload ShareUserRequest req,
	                         SimpMessageHeaderAccessor headers,
	                         Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null || ust == null) {
			return;
		}
		if (req == null || req.userId() <= 0) {
			userTranslationsService.sendError("userId is required", headers);
			return;
		}
		userTranslationsService.approveShare(ust, user, req.userId(), headers);
	}

	@MessageMapping("/translation/{translationId}/denyShare")
	public void denyShare(@DestinationVariable String translationId, @Payload ShareUserRequest req,
	                      SimpMessageHeaderAccessor headers,
	                      Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null || ust == null) {
			return;
		}
		if (req == null || req.userId() <= 0) {
			userTranslationsService.sendError("userId is required", headers);
			return;
		}
		userTranslationsService.denyShare(ust, user, req.userId(), headers);
	}

	public record CreateRequest(String translationId, int durationHours, long creationDate) {}

	// One time call — response is sent via /user/queue/updates (sendPrivateMessage).
	@MessageMapping("/translation/create")
	public void createTranslation(@Payload CreateRequest req, SimpMessageHeaderAccessor headers, Principal principal) {
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null) {
			return;
		}
		String translationId = req != null ? req.translationId() : null;
		if (translationId == null || !translationId.matches("[0-9a-f]{" + TRANSLATION_ID_HEX_LENGTH + "}")) {
			userTranslationsService.sendError(
					"translationId must be " + TRANSLATION_ID_HEX_LENGTH + " lowercase hex characters", headers);
			return;
		}
		if (UserTranslationsService.isDevTestMode()) {
			userTranslationsService.createTranslation(user, translationId, req.durationHours(), req.creationDate(), headers);
		} else {
			userTranslationsService.createTranslation(user, translationId, req.durationHours(), headers);
		}
	}

	@MessageMapping("/translation/{translationId}/delete")
	public void deleteTranslation(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers,
			Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null || ust == null) {
			return;
		}
		userTranslationsService.deleteTranslation(ust, user, headers);
	}

}