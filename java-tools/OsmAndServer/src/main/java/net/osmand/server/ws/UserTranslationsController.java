package net.osmand.server.ws;

import java.security.Principal;

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
	private UserTranslationsService userTranlsationService;

	@MessageMapping("/translation/{translationId}/sendMessage")
	public void sendMessage(@DestinationVariable String translationId, @Payload TranslationMessage message,
			Principal principal, SimpMessageHeaderAccessor headers) {
		userTranlsationService.sendMessage(translationId, message, principal, headers);
	}

	// One time call (subscription) returns map with TRANSLATION_ID
	@SubscribeMapping("/translation/create")
	public Object createTranslation(SimpMessageHeaderAccessor headers, Principal principal) {
		if (!(principal instanceof Authentication)) {
			return userTranlsationService.sendError("No authenticated user", headers);
		}
		Object user = ((Authentication) principal).getPrincipal();
		CloudUserDevicesRepository.CloudUserDevice dev = null;
		WebSecurityConfiguration.OsmAndProUser userObj = null;
		if (user instanceof WebSecurityConfiguration.OsmAndProUser) {
			userObj = ((WebSecurityConfiguration.OsmAndProUser) user);
			dev = ((WebSecurityConfiguration.OsmAndProUser) user).getUserDevice();
		}
		if (dev == null) {
			return userTranlsationService.sendError("No authenticated user", headers);
		}
		return userTranlsationService.createTranslation(dev, userObj.getUsername());
	}

}