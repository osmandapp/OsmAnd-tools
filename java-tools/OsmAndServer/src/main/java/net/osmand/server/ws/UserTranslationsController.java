package net.osmand.server.ws;

import java.security.Principal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;

@Controller
public class UserTranslationsController {

	private static final String TRANSLATION_DURATION_HOURS = "durationHours";

	Gson gson = new Gson();

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
	public void createTranslation(@Payload String body, SimpMessageHeaderAccessor headers, Principal principal) {
		CloudUser user = userTranslationsService.getUser(principal, headers);
		if (user == null) {
			return;
		}
		int durationHours = 0;
		try {
			JsonObject json = gson.fromJson(body, JsonObject.class);
			if (json.has(TRANSLATION_DURATION_HOURS)) {
				durationHours = json.get(TRANSLATION_DURATION_HOURS).getAsInt();
			}
		} catch (Exception ignored) {
		}
		userTranslationsService.createTranslation(user, null, durationHours, headers);
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