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

	// One time call (subscription) returns map with TRANSLATION_ID
	@MessageMapping("/translation/create")
	public Object createTranslation(SimpMessageHeaderAccessor headers, Principal principal) {
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null) {
			return null;
		}
		return userTranslationsService.createTranslation(user, null, headers);
	}

	@MessageMapping("/translation/{translationId}/load")
	public void loadTranslationHistory(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers) {
		if (headers == null) {
			return;
		}
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		if (ust != null) {
			userTranslationsService.load(ust, headers);
		}
	}
	
	@MessageMapping("/whoami")
	public void whoami(SimpMessageHeaderAccessor headers, Principal principal) {
		CloudUser user = userTranslationsService.getUser(principal, headers, true);
		if (user != null) {
			userTranslationsService.whoami(user, headers);
		}
	}
	
	@MessageMapping("/translation/{translationId}/startSharing")
	public String startSharing(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers,
			Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
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
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null) {
			return null;
		}
		if (ust != null) {
			userTranslationsService.stopSharing(ust, user, headers);
		}
		return "OK";
	}


	@MessageMapping("/translation/{translationId}/sendMessage")
	public void sendMessage(@DestinationVariable String translationId, @Payload String message,
			Principal principal, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		if (ust != null) {
			CloudUser user = userTranslationsService.getUser(principal, headers, true);
			if (user != null) {
				userTranslationsService.sendMessage(ust, user, message);
			}
		}
	}

	

}