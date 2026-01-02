package net.osmand.server.ws;

import java.security.Principal;
import java.util.Map;

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

	@MessageMapping("/translation/{translationId}/history")
	public void loadHistory(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		if (ust != null) {
			userTranslationsService.loadHistory(ust, headers);
		}
	}

	@MessageMapping("/translation/{translationId}/sendMessage")
	public void sendMessage(@DestinationVariable String translationId, @Payload TranslationMessage message,
			Principal principal, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		Map<String, Object> attributes = headers.getSessionAttributes();
		String storedAlias = (attributes != null) ? (String) attributes.get(UserTranslationsService.ALIAS) : null;
		if (ust != null) {
			userTranslationsService.sendMessage(ust, message, principal, storedAlias);
		}
	}

	// One time call (subscription) returns map with TRANSLATION_ID
	@MessageMapping("/translation/create")
	public Object createTranslation(SimpMessageHeaderAccessor headers, Principal principal) {
		CloudUser user = userTranslationsService.getUserFromPrincipal(principal);
		if (user == null) {
			return userTranslationsService.sendError("No authenticated user", headers);
		}
		return userTranslationsService.createTranslation(user, null, headers);
	}

}