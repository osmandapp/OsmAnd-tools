package net.osmand.server.ws;

import java.security.Principal;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import com.google.gson.Gson;

import jakarta.servlet.http.HttpServletRequest;
import net.osmand.data.LatLon;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;

@Service
public class UserTranslationsService {

    @Autowired
    private SimpMessagingTemplate template;
    
    @Autowired
	protected CloudUserDevicesRepository devicesRepository;
   
    static final String TOPIC_TRANSLATION = "/topic/translation/";
    static final String TOPIC_ERRORS = "/queue/errors";
    static final String TRANSLATION_ID = "translationId";
    static final String ALIAS = "alias";
    
    private Map<String, UserTranslation> translations = new ConcurrentHashMap<String, UserTranslation>();
    
    Gson gson = new Gson();

	private String getStoredName(Principal principal, SimpMessageHeaderAccessor headers) {
		if (principal != null) {
			return principal.getName();
		}
		Map<String, Object> attributes = headers.getSessionAttributes();
		String storedAlias = (attributes != null) ? (String) attributes.get("stored_alias") : null;
		return (storedAlias != null) ? "Unidentified " + storedAlias : "Unidentified Anonymous";
	}
	
	public Object createTranslation(CloudUserDevice dev, String pu) {
		Random rnd = new Random();
		String translationId = "room-"+rnd.nextInt(1000);
		UserTranslation ust = new UserTranslation(pu, translationId);
		ust.setCreationDate(System.currentTimeMillis());
		translations.put(ust.getId(), ust);
		return gson.toJson(Map.of(TRANSLATION_ID, ust.getId()));
	}

	public ResponseEntity<String> sendMessage(String translationId, CloudUserDevice dev, CloudUser pu, HttpServletRequest request) {
		return ResponseEntity.ok(gson.toJson(pu));
	}
	

	private UserTranslation getTranslation(String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = translations.get(translationId);
		if (ust == null) {
			sendError("Translation doesn't exist", headers);
		}
		return ust;
	}

	public Object sendError(String error, SimpMessageHeaderAccessor headers) {
		SimpMessageHeaderAccessor header = SimpMessageHeaderAccessor.create();
		header.setSessionId(headers.getSessionId());
		header.setLeaveMutable(true);
		template.convertAndSendToUser(headers.getSessionId(), TOPIC_ERRORS, error,
				headers.getMessageHeaders());
		return Map.of("error", error);
	}
	
    public void sendMessage(@DestinationVariable String translationId, 
    		@Payload TranslationMessage message, Principal principal, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = getTranslation(translationId, headers);
		if (ust != null) {
			TranslationMessage msg = new TranslationMessage(message.content, getStoredName(principal, headers),
					TranslationMessage.TYPE_MSG_TEXT);
			sendMessage(ust, msg);
		}
	}

    // One time call (subscription)
//    @SubscribeMapping("/translation/{translationId}/history")
//    public Map<String, Object> getHistory(@DestinationVariable String translationId, SimpMessageHeaderAccessor headers) {
//    	UserTranslation ust = getTranslation(translationId, headers);
//		if (ust == null) {
//			throw new IllegalArgumentException("Translation room not found: " + translationId);
//		}
//		return Map.of("history", ust.getMessages());
//    }
    
    private void sendMessage(UserTranslation ust, TranslationMessage msg) {
    	ust.getMessages().add(msg);
    	template.convertAndSend(TOPIC_TRANSLATION + ust.getId(), msg);
	}

	@EventListener
	public void handleSessionSubscribeEvent(SessionSubscribeEvent event) {
		StompHeaderAccessor headers = StompHeaderAccessor.wrap(event.getMessage());
		String destination = headers.getDestination();
		if (destination != null && destination.startsWith(TOPIC_TRANSLATION)) {
			String translationId = destination.replace(TOPIC_TRANSLATION, "");
			Principal principal = headers.getUser();
			String alias = headers.getFirstNativeHeader(ALIAS);
			if (alias == null && principal != null) {
				alias = principal.getName();
			}
			if (alias == null || alias.isEmpty()) {
				alias = new Random().nextInt(100000) + "";
			}
			// This map persists for the life of the connection
			Map<String, Object> attributes = headers.getSessionAttributes();
			if (attributes != null) {
				attributes.put(ALIAS, alias);
			}
			template.convertAndSend(destination,
					new TranslationMessage(alias, TranslationMessage.SENDER_SYSTEM, TranslationMessage.TYPE_MSG_JOIN));
			
			UserTranslation ust = getTranslation(translationId, headers);
			if (ust != null) {
				for (TranslationMessage msg : ust.getMessages()) {
					String sessionId = headers.getSessionId();
					SimpMessageHeaderAccessor header = SimpMessageHeaderAccessor.create();
					header.setSessionId(sessionId);
					header.setLeaveMutable(true);
					// Use the template to target this specific session on the topic
					template.convertAndSendToUser(sessionId, TOPIC_TRANSLATION + translationId, msg,
							header.getMessageHeaders());
				}
			}
		}
	}
    
    public void sendLocation(String chatId, LatLon l, Principal principal) {
		template.convertAndSend(TOPIC_TRANSLATION + chatId, 
				new TranslationMessage(gson.toJson(l), principal, TranslationMessage.TYPE_MSG_LOCATION));
	}

    
}