package net.osmand.server.ws;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.annotation.SubscribeMapping;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;

import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.CloudUserDevicesRepository;

@Controller
public class UserTranslationsController {

	@Autowired
	private UserTranslationsService userTranslationsService;

	@MessageMapping("/translation/{translationId}/history")
	public void loadHistory(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		if (ust != null) {
			userTranslationsService.loadHistory(translationId, headers.getSessionId());
		}
	}

	@MessageMapping("/translation/{translationId}/sendMessage")
	public void sendMessage(@DestinationVariable String translationId, @Payload TranslationMessage message,
			Principal principal, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = userTranslationsService.getTranslation(translationId, headers);
		if (ust != null) {
			userTranslationsService.sendMessage(translationId, message, principal, headers);
		}
	}

	// One time call (subscription) returns map with TRANSLATION_ID
	@MessageMapping("/translation/create")
	public Object createTranslation(SimpMessageHeaderAccessor headers, Principal principal) {
		if (!(principal instanceof Authentication)) {
//			return userTranslationsService.createTranslation(null, "test", headers.getSessionId());
			return userTranslationsService.sendError("No authenticated user", headers.getSessionId());
		}
		Object user = ((Authentication) principal).getPrincipal();
		CloudUserDevicesRepository.CloudUserDevice dev = null;
		WebSecurityConfiguration.OsmAndProUser userObj = null;
		if (user instanceof WebSecurityConfiguration.OsmAndProUser) {
			userObj = ((WebSecurityConfiguration.OsmAndProUser) user);
			dev = ((WebSecurityConfiguration.OsmAndProUser) user).getUserDevice();
		}
		if (dev == null) {
			return userTranslationsService.sendError("No authenticated user", headers.getSessionId());
		}
		return userTranslationsService.createTranslation(dev, userObj.getUsername(), headers.getSessionId());
	}

}