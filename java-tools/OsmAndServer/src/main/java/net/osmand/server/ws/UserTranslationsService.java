package net.osmand.server.ws;

import java.security.Principal;
import java.security.SecureRandom;
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
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import com.google.gson.Gson;

import jakarta.servlet.http.HttpServletRequest;
import net.osmand.data.LatLon;
import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.util.Algorithms;

@Service
public class UserTranslationsService {

    @Autowired
    private SimpMessagingTemplate template;
    
    @Autowired
	protected CloudUserDevicesRepository devicesRepository;
   
    static final String TOPIC_TRANSLATION = "/topic/translation/";
    static final String QUEUE_USER_UPDATES = "/queue/updates";
    static final String TRANSLATION_ID = "translationId";
    static final String ALIAS = "alias";
    
    static final String USER_UPD_TYPE_HISTORY = "HISTORY";
    static final String USER_UPD_TYPE_ERROR = "ERROR";
    static final String USER_UPD_TYPE_TRANSLATION = "TRANSLATION";
    
    static final String TRANSLATION_MISSING = "Translation doesn't exist";
    
    private Map<String, UserTranslation> translations = new ConcurrentHashMap<String, UserTranslation>();
    
    Gson gson = new Gson();
    
    Random random = new SecureRandom();
    
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
	
	public CloudUser getUserFromPrincipal(Principal principal) {
		if (principal instanceof Authentication) {
			Object user = ((Authentication) principal).getPrincipal();
			if (user instanceof WebSecurityConfiguration.OsmAndProUser) {
				CloudUser userObj = ((WebSecurityConfiguration.OsmAndProUser) user).getUser();
				if (userObj != null) {
					return userObj;
				}
			}
		}
		return null;
	}
	
	public CloudUserDevice getUserDeviceFromPrincipal(Principal principal) {
		if (principal instanceof Authentication) {
			Object user = ((Authentication) principal).getPrincipal();
			if (user instanceof WebSecurityConfiguration.OsmAndProUser) {
				CloudUserDevice userObj = ((WebSecurityConfiguration.OsmAndProUser) user).getUserDevice();
				if (userObj != null) {
					return userObj;
				}
			}
		}
		return null;
	}
	
	public Object createTranslation(CloudUser user, SimpMessageHeaderAccessor headers) {
//		CloudUsersRepository.CloudUser user = userdataService.getUserById(dev.userid);
		long time = System.currentTimeMillis();
		String translationId = Long.toHexString(time * 100L + random.nextInt(100));
		UserTranslation ust = new UserTranslation(translationId, user == null ? -1: user.id);
		ust.setCreationDate(time);
		translations.put(ust.getId(), ust);
		Map<String, String> obj = Map.of(TRANSLATION_ID, ust.getId());
		if (headers != null) {
			sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
		}
		return obj;
    }
	

	public UserTranslation getTranslation(String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = translations.get(translationId);
		if (ust == null) {
			sendError(TRANSLATION_MISSING, headers);
		}
		return ust;
	}

	public void loadHistory(UserTranslation ust, SimpMessageHeaderAccessor headers) {
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_HISTORY, ust.getMessages());
	}

	public String sendError(String error, SimpMessageHeaderAccessor headers) {
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_ERROR, error);
		return error;
	}
	
	
    public boolean sendMessage(UserTranslation ust, TranslationMessage message, 
			Principal principal, String storedAlias) {
		CloudUser user = getUserFromPrincipal(principal);
		CloudUserDevice userDevice = getUserDeviceFromPrincipal(principal);
		TranslationMessage msg = prepareMessageAuthor(userDevice, user, storedAlias);
		msg.content = message.content;
		msg.type = TranslationMessage.TYPE_MSG_TEXT;
		rawSendMessage(ust, msg);
		return true;
	}
    

    public boolean sendLocation(UserTranslation ust, LatLon l, Principal principal) {
    	CloudUser user = getUserFromPrincipal(principal);
    	CloudUserDevice userDevice = getUserDeviceFromPrincipal(principal);
    	TranslationMessage msg = prepareMessageAuthor(userDevice, user, null);
    	if (Algorithms.isEmpty(msg.sender)) {
			return false;
		}
		msg.content = gson.toJson(l);
		msg.type = TranslationMessage.TYPE_MSG_LOCATION;
		rawSendMessage(ust, msg);
		return true;
	}
    
    public ResponseEntity<String> sendMessage(String translationId, CloudUserDevice dev, CloudUser pu, HttpServletRequest request) {
    	UserTranslation ust = getTranslation(translationId, null);
		if (ust != null) {
			// TODO parse from HttpServletRequest request vars location / device
			String message = "Hello"; 
			TranslationMessage msg = prepareMessageAuthor(dev, pu, null);
			msg.content = message;
			msg.type = TranslationMessage.TYPE_MSG_TEXT;
			rawSendMessage(ust, msg);
			return ResponseEntity.ok(gson.toJson(pu));
		}
		return ResponseEntity.notFound().build();
	}
    

	private TranslationMessage prepareMessageSystem() {
		TranslationMessage tm = new TranslationMessage();
		tm.sendUserId = TranslationMessage.SENDER_SYSTEM_ID;
		tm.sender = TranslationMessage.SENDER_SYSTEM;
		return tm;
	}
    
	private TranslationMessage prepareMessageAuthor(CloudUserDevice dev, CloudUser pu, String alias) {
		TranslationMessage tm = new TranslationMessage();
		tm.sendUserId = TranslationMessage.SENDER_ANONYMOUS_ID;
		if (dev != null) {
			tm.sendDeviceId = dev.id;
			tm.sendUserId = dev.userid;
		}
		if (pu != null) {
			tm.sendUserId = pu.id;
			tm.sender = pu.nickname;
		}
		if (Algorithms.isEmpty(tm.sender)) {
			tm.sender = alias;
		}
		if (Algorithms.isEmpty(tm.sender) && pu != null) {
			tm.sender = pu.email.substring(0, pu.email.length() / 2) + "...";
		}
		return tm;
	}
    
    private void rawSendMessage(UserTranslation ust, TranslationMessage msg) {
    	template.convertAndSend(TOPIC_TRANSLATION + ust.getId(), msg);
    	ust.getMessages().add(msg);
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
			TranslationMessage msg = prepareMessageSystem();
			msg.content = alias;
			msg.type = TranslationMessage.TYPE_MSG_JOIN;
			template.convertAndSend(destination, msg);
		}
	}
    

    
}