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

	@Autowired
	private UserTranslationsService userTranslationsService;


	@MessageMapping("/translation/create")
	public Object createTranslation(@Payload(required = false) String password,
	                                SimpMessageHeaderAccessor headers,
	                                Principal principal) {
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null) {
			return null;
		}
		UserTranslation translation = userTranslationsService.createTranslation(user, null, headers);

		if (isValidPassword(password)) {
			userTranslationsService.setTranslationPassword(translation, password);
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

	private boolean isValidPassword(String password) {
		if (password == null) {
			return false;
		}
		String trimmed = password.trim();
		return !trimmed.isEmpty() && !trimmed.equals("{}");
	}
}