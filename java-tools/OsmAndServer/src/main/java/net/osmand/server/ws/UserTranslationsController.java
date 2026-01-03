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

//	@Value("${spring.devtools.restart.enabled:false}")
	// FIXME
//	private boolean isDevToolsActive = true;
	
	@MessageMapping("/translation/{translationId}/load")
	public void loadTranslation(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		if (ust != null) {
			userTranslationsService.loadHistory(ust, headers);
		}
	}
	
	@MessageMapping("/translation/{translationId}/startSharing")
	public String startSharing(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers,
			Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = validateUser(principal, headers);
		if (user == null) {
			return null;
		}
		if (ust != null) {
			userTranslationsService.startSharing(ust, user, headers);
		}
		return "OK";
	}
	
	@MessageMapping("/translation/{translationId}/stopSharing")
	public String stopSharing(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers,
			Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = validateUser(principal, headers);
		if (user == null) {
			return null;
		}
		if (ust != null) {
			userTranslationsService.stopSharing(ust, user, headers);
		}
		return "OK";
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
		CloudUser user = validateUser(principal, headers);
		if (user == null) {
			return null;
		}
		return userTranslationsService.createTranslation(user, null, headers);
	}

	private CloudUser validateUser(Principal principal, SimpMessageHeaderAccessor headers) {
		principal = headers.getUser();
		CloudUser us = userTranslationsService.getUserFromPrincipal(principal);
		//if (isDevToolsActive) {
		boolean isDevToolsActive = false;
		if (us == null && isDevToolsActive) {
			us = new CloudUser();
			us.id = TranslationMessage.SENDER_SYSTEM_ID;
			us.nickname = TranslationMessage.SENDER_SYSTEM;
			us.email = TranslationMessage.SENDER_SYSTEM;
		}
		if (us == null) {
			userTranslationsService.sendError("No authenticated user", headers);
		}
		return us;
	}

}