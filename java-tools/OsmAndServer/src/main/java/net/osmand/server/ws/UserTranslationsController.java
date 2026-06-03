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

	@MessageMapping("/translation/{translationId}/load")
	public void loadTranslation(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers) {
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
	public void stopSharing(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers,
			Principal principal) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null || ust == null) {
			return;
		}
		userTranslationsService.stopSharing(ust, user, headers);
	}


	@MessageMapping("/translation/{translationId}/sendMessage")
	public void sendMessage(@DestinationVariable String translationId, @Payload String message,
			Principal principal, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		if (ust != null) {
			CloudUser user = userTranslationsService.getUser(principal, headers, true);
			userTranslationsService.sendMessage(ust, user, message);
		}
	}

	// One time call — response is sent via /user/queue/updates (sendPrivateMessage)
	@MessageMapping("/translation/create")
	public void createTranslation(SimpMessageHeaderAccessor headers, Principal principal) {
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null) {
			return;
		}
		userTranslationsService.createTranslation(user, null, headers);
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