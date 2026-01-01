package net.osmand.server.ws;

import java.security.Principal;
import java.util.HashMap;
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
    static final String QUEUE_USER_UPDATES = "/queue/updates";
    static final String TRANSLATION_ID = "translationId";
    static final String ALIAS = "alias";
    
    private Map<String, UserTranslation> translations = new ConcurrentHashMap<String, UserTranslation>();
    
    Gson gson = new Gson();
    
    public void sendPrivateMessage(String sessionId, String type, Object data) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("type", type);
        payload.put("data", data);
        
        // Sends to: /user/{sessionId}/queue/updates
        SimpMessageHeaderAccessor header = SimpMessageHeaderAccessor.create();
        header.setSessionId(sessionId);
        header.setLeaveMutable(true);
        template.convertAndSendToUser(sessionId, QUEUE_USER_UPDATES, payload, header.getMessageHeaders());
    }

	private String getStoredName(Principal principal, SimpMessageHeaderAccessor headers) {
		if (principal != null) {
			return principal.getName();
		}
		Map<String, Object> attributes = headers.getSessionAttributes();
		String storedAlias = (attributes != null) ? (String) attributes.get(ALIAS) : null;
		return (storedAlias != null) ? "Unidentified " + storedAlias : "Unidentified Anonymous";
	}
	
	public Object createTranslation(CloudUserDevice dev, String username, String sessionId) {
		Random rnd = new Random();
		String translationId = "room-" + rnd.nextInt(1000);
		UserTranslation ust = new UserTranslation(username, translationId);
		ust.setCreationDate(System.currentTimeMillis());
		translations.put(ust.getId(), ust);
		Map<String, String> obj = Map.of(TRANSLATION_ID, ust.getId());
		if (sessionId != null) {
			sendPrivateMessage(sessionId, "CREATED", obj);
		}
		return obj;
    }
	
	public ResponseEntity<String> sendMessage(String translationId, CloudUserDevice dev, CloudUser pu, HttpServletRequest request) {
		return ResponseEntity.ok(gson.toJson(pu));
	}
	

	public UserTranslation getTranslation(String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = translations.get(translationId);
		if (ust == null) {
			sendError("Translation doesn't exist", headers.getSessionId());
		}
		return ust;
	}

	public void loadHistory(String translationId, String sessionId) {
		UserTranslation ust = translations.get(translationId);
		if (ust != null) {
			// Send history list to user
			sendPrivateMessage(sessionId, "HISTORY", ust.getMessages());
		} else {
			sendPrivateMessage(sessionId, "ERROR", "Room not found");
		}
	}

	// Standard Error Helper
	public String sendError(String error, String sessionId) {
		sendPrivateMessage(sessionId, "ERROR", error);
		return error;
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

    
    private void sendMessage(UserTranslation ust, TranslationMessage msg) {
    	ust.getMessages().add(msg);
    	template.convertAndSend(TOPIC_TRANSLATION + ust.getId(), msg);
	}

	@EventListener
	public void handleSessionSubscribeEvent(SessionSubscribeEvent event) {
		StompHeaderAccessor headers = StompHeaderAccessor.wrap(event.getMessage());
		String destination = headers.getDestination();
		if (destination != null && destination.startsWith(TOPIC_TRANSLATION)) {
//			String translationId = destination.replace(TOPIC_TRANSLATION, "");
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
			
//			UserTranslation ust = getTranslation(translationId, headers);
//			if (ust != null) {
//				for (TranslationMessage msg : ust.getMessages()) {
//					String sessionId = headers.getSessionId();
//					SimpMessageHeaderAccessor header = SimpMessageHeaderAccessor.create();
//					header.setSessionId(sessionId);
//					header.setLeaveMutable(true);
//					// Use the template to target this specific session on the topic
//					template.convertAndSend( destination, msg);
//				}
//			}
		}
	}
    
    public void sendLocation(String chatId, LatLon l, Principal principal) {
		template.convertAndSend(TOPIC_TRANSLATION + chatId, 
				new TranslationMessage(gson.toJson(l), principal, TranslationMessage.TYPE_MSG_LOCATION));
	}

    
}