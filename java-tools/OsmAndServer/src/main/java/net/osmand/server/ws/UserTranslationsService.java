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
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import jakarta.servlet.http.HttpServletRequest;
import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.ws.TranslationMessage.TranslationMessageType;
import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.Algorithms;

@Service
public class UserTranslationsService {

   
    public static final String TRANSLATION_ID = "translationId";
	public static final String ALIAS = "alias";

    
    static final String TOPIC_TRANSLATION = "/topic/translation/";
    static final String QUEUE_USER_UPDATES = "/queue/updates";
    
    static final String USER_UPD_TYPE_ERROR = "ERROR";
    static final String USER_UPD_TYPE_TRANSLATION = "TRANSLATION";
    static final String USER_UPD_TYPE_USER_INFO = "USER_INFO";
    
    static final String TRANSLATION_MISSING = "Translation doesn't exist";

    private Map<String, UserTranslation> translations = new ConcurrentHashMap<>();
    private Map<Integer, Deque<UserTranslation>> translationsByUser = new ConcurrentHashMap<>();
    private Map<Integer, Deque<WptPt>> locationByUser = new ConcurrentHashMap<>();
    private Map<String, String> anonymousUsers = new ConcurrentHashMap<>(); 
    
    Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    Random random = new SecureRandom();
    
    @Autowired
    private SimpMessagingTemplate template;
    
    @Autowired
	protected CloudUserDevicesRepository devicesRepository;
    
    private final Environment environment;
    
	
	public UserTranslationsService(Environment environment) {
        this.environment = environment;
    }
    
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

    
    public CloudUser getUser(Principal principal, SimpMessageHeaderAccessor headers) {
		boolean production = environment.acceptsProfiles(Profiles.of("production"));		
		return getUser(principal, headers, !production);
	}
	
	public CloudUser getUser(Principal principal, SimpMessageHeaderAccessor headers, boolean allowAnonymous) {
		CloudUser us = getUserFromPrincipal(principal);
		if (us == null && allowAnonymous) {
			us = new CloudUser();
			Map<String, Object> attributes = headers.getSessionAttributes();
			String oalias = (attributes != null) ? (String) attributes.get(ALIAS) : null;
			String sessionId = headers.getSessionId();
			if (Algorithms.isEmpty(oalias)) {
				oalias = TranslationMessage.SENDER_ANONYMOUS;
			}
			String alias = oalias;
			anonymousUsers.putIfAbsent(alias, sessionId);
			while (!sessionId.equals(anonymousUsers.get(alias))) {
				alias = oalias + " " + random.nextInt(1000);
				anonymousUsers.putIfAbsent(alias, sessionId);
			}
			attributes.put(ALIAS, alias);
			us.id = TranslationMessage.SENDER_ANONYMOUS_ID;
			us.nickname = alias;
			us.email = us.nickname + "@example.com";
		}
		if (us == null) {
			sendError("No authenticated user", headers);
		}
		return us;
	}
	
	private  CloudUser getUserFromPrincipal(Principal principal) {
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

	// deleteTranslation
	public UserTranslation createTranslation(CloudUser user, String translationId, SimpMessageHeaderAccessor headers) {
		long time = System.currentTimeMillis();
		if (translationId == null) {
			translationId = Long.toHexString(time * 100L + random.nextInt(100));
		}
		UserTranslation ust = new UserTranslation(translationId, user == null ? -1: user.id);
		ust.setCreationDate(time);
		translations.put(ust.getId(), ust);
		userJoinTranslation(ust, user.id);
		UserTranslationObject obj = new UserTranslationObject(ust.getId());
		if (headers != null) {
			sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
		}
		return ust;
    }

	private void userJoinTranslation(UserTranslation ust, int uid) {
		if (!translationsByUser.containsKey(uid)) {
			translationsByUser.putIfAbsent(uid, new ConcurrentLinkedDeque<UserTranslation>());
		}
		translationsByUser.get(uid).add(ust);
	}
	

	public UserTranslation getTranslation(String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = translations.get(translationId);
		if (ust == null) {
			sendError(TRANSLATION_MISSING, headers);
		}
		return ust;
	}

	public void load(UserTranslation ust, SimpMessageHeaderAccessor headers) {
		UserTranslationObject obj = new UserTranslationObject(ust.getId());
		obj.setHistory(ust.getMessages());
		obj.setShareLocations(ust);
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
	}
	
	public void startSharing(UserTranslation ust, CloudUser user, SimpMessageHeaderAccessor headers) {
		TranslationSharingOptions opts = new TranslationSharingOptions();
		opts.startTime = System.currentTimeMillis();
		opts.expireTime = System.currentTimeMillis() + 60 * 60 * 1000;
		opts.userId = user.id;
		opts.nickname = getNickname(user);

		Deque<WptPt> locations = locationByUser.get(user.id);
		if (locations != null && !locations.isEmpty()) {
			ust.sendLocation(user.id, locations.getLast());
		}
		
		if(!environment.acceptsProfiles(Profiles.of("production"))) {
			startSimulation(user, ust);
		}
		UserTranslationObject obj = new UserTranslationObject(ust.getId());
		ust.getSharingOptions().add(opts);
		obj.setShareLocations(ust);
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
	}
	
	private void startSimulation(CloudUser user, UserTranslation ust) {
		Thread simThread = new Thread(() -> {
			double simLat = 50.4501;
			double simLon = 30.5234;
			boolean gone = false;
			while (!gone) {
				gone = true;
				for (TranslationSharingOptions u : ust.getSharingOptions()) {
					if (u.userId == user.id) {
						gone = false;
						break;
					}
				}
				try {
					Thread.sleep(5000); // Wait 5 seconds
					simLat += (Math.random() - 0.5) * 0.001;
					simLon += (Math.random() - 0.5) * 0.001;
					WptPt pt = new WptPt(simLat, simLon);
					pt.setTime(System.currentTimeMillis());
					pt.setSpeed(Math.random() * 5); // Random speed 0-5 m/s
					sendLocation(null, user, pt);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				} catch (Exception e) {
					System.err.println("Simulation error: " + e.getMessage());
				}
			}
		});
		simThread.setDaemon(true); // Ensure thread doesn't block app shutdown
		simThread.setName("LocSim-" + user.nickname);
		simThread.start();
	}

	public void stopSharing(UserTranslation ust, CloudUser user, SimpMessageHeaderAccessor headers) {
		Deque<TranslationSharingOptions> opts = ust.getSharingOptions();
		Iterator<TranslationSharingOptions> it = opts.iterator();
		int userId = user.id;
		while (it.hasNext()) {
			TranslationSharingOptions opt = it.next();
			if (opt.userId == userId) {
				it.remove();
			}
		}
		UserTranslationObject obj = new UserTranslationObject(ust.getId());
		obj.setShareLocations(ust);
		rawSendMessage(ust, prepareMessageSystem().setType(TranslationMessageType.METADATA).setContent(obj));
//		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
	}
	
	public boolean whoami(CloudUser user, SimpMessageHeaderAccessor headers) {
		Map<String, Object> u = new HashMap<>();
		u.put("nickname", user.nickname);
		u.put("email", user.email);
		u.put("id", user.id);
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_USER_INFO, u);
		return true;
	}

	public String sendError(String error, SimpMessageHeaderAccessor headers) {
		if (headers != null) {
			sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_ERROR, error);
		}
		return error;
	}
	
    public boolean sendMessage(UserTranslation ust, CloudUser user, Object message) {
		TranslationMessage msg = prepareMessageAuthor(null, user);
		msg.content = message;
		msg.type = TranslationMessageType.TEXT;
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
			wptPt.setSpeed(Double.parseDouble(request.getParameter("speed")));
		} catch (RuntimeException e) {
			// ignore exception as they could flood
		}
		sendLocation(dev, pu, wptPt);
		return true;
	}

	public void sendLocation(CloudUserDevice dev, CloudUser pu, WptPt wptPt) {
		int userId = dev == null ? pu.id : dev.userid; 
		Deque<WptPt> deque = locationByUser.get(userId);
		if (deque == null) {
			deque = new ConcurrentLinkedDeque<WptPt>();
			locationByUser.put(userId, deque);
		}
		deque.push(wptPt);
		TranslationMessage msg = prepareMessageAuthor(dev, pu);
		msg.content = Map.of("point", wptPt);
		msg.type = TranslationMessageType.LOCATION;
		Deque<UserTranslation> translations = translationsByUser.get(userId);
		long timeMillis = System.currentTimeMillis();
		for (UserTranslation ust : translations) {
			Deque<TranslationSharingOptions> sharingOptions = ust.getSharingOptions();
			for (TranslationSharingOptions o : sharingOptions) {
				if (o.userId == userId && timeMillis < o.expireTime) {
					ust.sendLocation(userId, wptPt);
					rawSendMessage(ust, msg);
					break;
				}
			}
		}
	}
    

	private TranslationMessage prepareMessageSystem() {
		TranslationMessage tm = new TranslationMessage();
		tm.sendUserId = TranslationMessage.SENDER_SYSTEM_ID;
		tm.sender = TranslationMessage.SENDER_SYSTEM;
		return tm;
	}
    
	private TranslationMessage prepareMessageAuthor(CloudUserDevice dev, CloudUser pu) {
		TranslationMessage tm = new TranslationMessage();
		if (dev != null) {
			tm.sendDeviceId = dev.id;
		}
		tm.sendUserId = pu.id;
		tm.sender = getNickname(pu);
		return tm;
	}

    private void rawSendMessage(UserTranslation ust, TranslationMessage msg) {
    	template.convertAndSend(TOPIC_TRANSLATION + ust.getId(), msg);
    	ust.getMessages().add(msg);
	}
    

	private String getNickname(CloudUser user) {
		if (!Algorithms.isEmpty(user.nickname)) {
			return user.nickname;
		}
		return user.email.substring(0, user.email.length() / 2) + "...";
	}

	@EventListener
	public void handleSessionSubscribeEvent(SessionSubscribeEvent event) {
		StompHeaderAccessor headers = StompHeaderAccessor.wrap(event.getMessage());
		String destination = headers.getDestination();
		if (destination != null && destination.startsWith(TOPIC_TRANSLATION)) {
			String translationId = destination.replace(TOPIC_TRANSLATION, "");
			CloudUser user = getUser(headers.getUser(), headers, true);
			UserTranslation ust = getTranslation(translationId, headers);
			TranslationMessage msg = prepareMessageSystem().setType(TranslationMessageType.JOIN)
					.setContent(getNickname(user));
			template.convertAndSend(destination, msg);
			if (user.id > 0) {
				userJoinTranslation(ust, user.id);
			}
		}
	}
	
	@EventListener
	public void onDisconnectEvent(SessionDisconnectEvent event) {
		StompHeaderAccessor headers = StompHeaderAccessor.wrap(event.getMessage());
		Map<String, Object> attributes = headers.getSessionAttributes();
		String oalias = (attributes != null) ? (String) attributes.get(ALIAS) : null;
		if (oalias != null && event.getSessionId().equals(anonymousUsers.get(oalias))) {
			anonymousUsers.remove(oalias);
		}
	}

    
}