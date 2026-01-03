package net.osmand.server.ws;

import java.security.Principal;
import java.security.SecureRandom;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import com.google.gson.Gson;

import jakarta.servlet.http.HttpServletRequest;
import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.Algorithms;

@Service
public class UserTranslationsService {

    @Autowired
    private SimpMessagingTemplate template;
    
    @Autowired
	protected CloudUserDevicesRepository devicesRepository;
   
    public static final String TRANSLATION_ID = "translationId";
    
    static final String TOPIC_TRANSLATION = "/topic/translation/";
    static final String QUEUE_USER_UPDATES = "/queue/updates";
    static final String ALIAS = "alias";
    
    static final String USER_UPD_TYPE_ERROR = "ERROR";
    static final String USER_UPD_TYPE_TRANSLATION = "TRANSLATION";
    
    static final String TRANSLATION_MISSING = "Translation doesn't exist";
    
    private Map<String, UserTranslation> translations = new ConcurrentHashMap<>();
    
    private Map<Integer, Deque<UserTranslation>> translationsByUser = new ConcurrentHashMap<>();
    
    private Map<Integer, Deque<WptPt>> locationByUser = new ConcurrentHashMap<>();
    
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
	
	// TODO delete translation
	public UserTranslation createTranslation(CloudUser user, String translationId, SimpMessageHeaderAccessor headers) {
		long time = System.currentTimeMillis();
		if (translationId == null) {
			translationId = Long.toHexString(time * 100L + random.nextInt(100));
		}
		UserTranslation ust = new UserTranslation(translationId, user == null ? -1: user.id);
		ust.setCreationDate(time);
		translations.put(ust.getId(), ust);
		int uid = user != null ? user.id : TranslationMessage.SENDER_ANONYMOUS_ID;
		if (!translationsByUser.containsKey(uid)) {
			translationsByUser.putIfAbsent(uid, new ConcurrentLinkedDeque<UserTranslation>());
		}
		translationsByUser.get(uid).add(ust);
		UserTranslationObject obj = new UserTranslationObject(ust.getId());
		if (headers != null) {
			sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
		}
		return ust;
    }
	

	public UserTranslation getTranslation(String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = translations.get(translationId);
		if (ust == null) {
			sendError(TRANSLATION_MISSING, headers);
		}
		return ust;
	}

	public void loadHistory(UserTranslation ust, SimpMessageHeaderAccessor headers) {
		UserTranslationObject obj = new UserTranslationObject(ust.getId());
		obj.setHistory(ust.getMessages());
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
	}
	
	public void startSharing(UserTranslation ust, CloudUser user, SimpMessageHeaderAccessor headers) {
		TranslationSharingOptions opts = new TranslationSharingOptions();
		opts.startTime = System.currentTimeMillis();
		opts.expireTime = System.currentTimeMillis() + 60 * 60 * 1000;
		opts.userId = user.id;
		opts.nickname = Algorithms.isEmpty(user.nickname) ? obfuscateEmail(user) : user.nickname;
		
		Deque<WptPt> locations = locationByUser.get(user.id);
		if(locations != null && !locations.isEmpty()) {
			ust.sendLocation(user.id, locations.getLast());
		}
		
		UserTranslationObject obj = new UserTranslationObject(ust.getId());
		ust.getSharingOptions().add(opts);
		obj.setShareLocations(ust);
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
	}
	
	public void stopSharing(UserTranslation ust, CloudUser user, SimpMessageHeaderAccessor headers) {
		Deque<TranslationSharingOptions> opts = ust.getSharingOptions();
		Iterator<TranslationSharingOptions> it = opts.iterator();
		while (it.hasNext()) {
			TranslationSharingOptions opt = it.next();
			if (opt.userId == user.id) {
				it.remove();
			}
		}
		UserTranslationObject obj = new UserTranslationObject(ust.getId());
		obj.setShareLocations(ust);
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
	}

	public String sendError(String error, SimpMessageHeaderAccessor headers) {
		if (headers != null) {
			sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_ERROR, error);
		}
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

	public boolean sendDeviceMessage(CloudUserDevice dev, CloudUser pu, HttpServletRequest request) {
		WptPt wptPt = new WptPt();
		try {
			wptPt.setLat(Double.parseDouble(request.getParameter("lat")));
			wptPt.setLon(Double.parseDouble(request.getParameter("lon")));
			wptPt.setTime(Long.parseLong(request.getParameter("timestamp")));
		} catch (RuntimeException e) {
			return false;
		}
		try {
			wptPt.setHdop(Double.parseDouble(request.getParameter("hdop")));
			wptPt.setEle(Double.parseDouble(request.getParameter("altitude")));
			wptPt.setSpeed(Long.parseLong(request.getParameter("speed")));
		} catch (RuntimeException e) {
			// ignore exception as they could flood
		}
		Deque<WptPt> deque = locationByUser.get(dev.userid);
		if (deque == null) {
			deque = new ConcurrentLinkedDeque<WptPt>();
			locationByUser.put(dev.userid, deque);
		}
		deque.push(wptPt);
		TranslationMessage msg = prepareMessageAuthor(dev, pu, null);
//		msg.content = wptPt.toString();
		msg.content = gson.toJson(Map.of("point", wptPt));
		msg.type = TranslationMessage.TYPE_MSG_TEXT; // TODO location
		Deque<UserTranslation> translations = translationsByUser.get(dev.userid);
		long timeMillis = System.currentTimeMillis();
		for (UserTranslation ust : translations) {
			Deque<TranslationSharingOptions> sharingOptions = ust.getSharingOptions();
			for (TranslationSharingOptions o : sharingOptions) {
				if (o.userId == dev.userid && timeMillis < o.expireTime) {
					ust.sendLocation(dev.userid, wptPt);
					rawSendMessage(ust, msg);
				}
			}
		}
		return true;
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
			tm.sender = obfuscateEmail(pu);
		}
		return tm;
	}


	private String obfuscateEmail(CloudUser pu) {
		return pu.email.substring(0, pu.email.length() / 2) + "...";
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