package net.osmand.server.ws;

import java.security.Principal;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import com.google.gson.Gson;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.ws.TranslationMessage.TranslationMessageType;
import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;

import net.osmand.util.Algorithms;


@Service
public class UserTranslationsService {

    private static final Log LOG = LogFactory.getLog(UserTranslationsService.class);

   
    public static final String TRANSLATION_ID = "translationId";
	public static final String ALIAS = "alias";
	public static final String ENCRYPTED_DATA = "encryptedData";
	public static final String DEVICE_ID = "deviceId";
	public static final String ACCESS_TOKEN = "accessToken";

    
    static final String TOPIC_TRANSLATION = "/topic/translation/";
    static final String QUEUE_USER_UPDATES = "/queue/updates";
    
    static final String USER_UPD_TYPE_ERROR = "ERROR";
    static final String USER_UPD_TYPE_TRANSLATION = "TRANSLATION";
    static final String USER_UPD_TYPE_USER_INFO = "USER_INFO";
    
    static final String TRANSLATION_MISSING = "Translation doesn't exist";

    private static final String REDIS_MSG_KEY_PREFIX = "livetrack:msg:";
    private static final Duration MESSAGES_TTL = Duration.ofDays(7);

    @Autowired(required = false)
    private RedisConnectionFactory redisConnectionFactory;

    private StringRedisTemplate redisTemplate;

    @PostConstruct
    private void init() {
        if (redisConnectionFactory != null) {
            redisTemplate = new StringRedisTemplate(redisConnectionFactory);
            redisTemplate.afterPropertiesSet();
            LOG.info("UserTranslationsService: message store backed by Redis (7-day TTL)");
        } else {
            LOG.info("UserTranslationsService: message store backed by in-memory deque (single instance only)");
        }
    }

    private static final String REDIS_TRANSLATION_KEY_PREFIX = "livetrack:translation:";

    // Active translation sessions. Metadata is persisted in Redis; this map holds the live
    // object with session state (sharingOptions) for the duration of the server process.
    private final Map<String, UserTranslation> activeSessions = new ConcurrentHashMap<>();
    private final Map<Integer, Deque<UserTranslation>> shareLocTranslationsByUser = new ConcurrentHashMap<>();
    // alias → STOMP sessionId for anonymous connections.
    private final Map<String, String> anonymousUsers = new ConcurrentHashMap<>();

    private final Gson gson = new Gson();
    private final Random random = new SecureRandom();
    
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

	private static class TranslationMeta {
		String id;
		long owner;
		long creationDate;
		long durationMs;

		TranslationMeta() {}

		TranslationMeta(String id, long owner, long creationDate, long durationMs) {
			this.id = id;
			this.owner = owner;
			this.creationDate = creationDate;
			this.durationMs = durationMs;
		}
	}

	private void saveTranslationToRedis(UserTranslation ust) {
		if (redisTemplate == null) return;
		TranslationMeta meta = new TranslationMeta(ust.getId(), ust.getOwner(), ust.getCreationDate(), ust.getDurationMs());
		redisTemplate.opsForValue().set(REDIS_TRANSLATION_KEY_PREFIX + ust.getId(), gson.toJson(meta), MESSAGES_TTL);
	}

	private UserTranslation loadTranslationFromRedis(String translationId) {
		if (redisTemplate == null) return null;
		String json = redisTemplate.opsForValue().get(REDIS_TRANSLATION_KEY_PREFIX + translationId);
		if (json == null) return null;
		TranslationMeta meta = gson.fromJson(json, TranslationMeta.class);
		UserTranslation ust = new UserTranslation(meta.id, (int) meta.owner);
		ust.setCreationDate(meta.creationDate);
		ust.setDurationMs(meta.durationMs);
		return ust;
	}

	public UserTranslation createTranslation(CloudUser user, String translationId, int durationHours, SimpMessageHeaderAccessor headers) {
		boolean exists = redisTemplate != null
				? redisTemplate.hasKey(REDIS_TRANSLATION_KEY_PREFIX + translationId)
				: activeSessions.containsKey(translationId);  // fallback when Redis unavailable
		if (exists) {
			sendError("translationId already exists", headers);
			return null;
		}
		long time = System.currentTimeMillis();
		UserTranslation ust = new UserTranslation(translationId, user == null ? -1: user.id);
		ust.setCreationDate(time);
		ust.setDurationMs(durationHours == 0 ? UserTranslation.PERMANENT_DURATION_MS : durationHours * 60 * 60 * 1000L);
		activeSessions.put(ust.getId(), ust);
		saveTranslationToRedis(ust);
		shareLocationByUser(ust, user.id);
		UserTranslationPlainObject obj = new UserTranslationPlainObject(ust.getId());
		obj.ownerUserId = ust.getOwner();
		if (headers != null) {
			sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
		}
		return ust;
    }

	private void shareLocationByUser(UserTranslation ust, int uid) {
		Deque<UserTranslation> deque = shareLocTranslationsByUser.computeIfAbsent(uid, k -> new ConcurrentLinkedDeque<>());
		if (!deque.contains(ust)) {
			deque.add(ust);
		}
	}
	

	public UserTranslation getTranslation(String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = activeSessions.computeIfAbsent(translationId, this::loadTranslationFromRedis);
		if (ust == null) {
			sendError(TRANSLATION_MISSING, headers);
		}
		return ust;
	}

	public void load(UserTranslation ust, long fromTime, long toTime, SimpMessageHeaderAccessor headers) {
		UserTranslationPlainObject obj = new UserTranslationPlainObject(ust.getId());
		obj.ownerUserId = ust.getOwner();
		obj.creationDate = ust.getCreationDate();
		double minScore = fromTime <= 0 ? Double.NEGATIVE_INFINITY : fromTime;
		double maxScore = toTime <= 0 ? Double.POSITIVE_INFINITY : toTime;
		if (redisTemplate != null) {
			String key = REDIS_MSG_KEY_PREFIX + ust.getId();
			Set<String> jsons = redisTemplate.opsForZSet().rangeByScore(key, minScore, maxScore);
			List<TranslationMessage> messages = jsons == null ? Collections.emptyList() :
				jsons.stream().map(j -> gson.fromJson(j, TranslationMessage.class)).toList();
			obj.setHistory(messages);
		} else {
			List<TranslationMessage> messages = ust.getMessages().stream()
					.filter(m -> (fromTime <= 0 || m.serverReceiveTime >= fromTime)
							&& (toTime <= 0 || m.serverReceiveTime <= toTime))
					.toList();
			obj.setHistory(messages);
		}
		obj.setShareLocations(ust);
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
	}

	public void startSharing(UserTranslation ust, CloudUser user, SimpMessageHeaderAccessor headers) {
		TranslationSharingOptions opts = new TranslationSharingOptions();
		opts.startTime = System.currentTimeMillis();
		opts.expireTime = opts.startTime + ust.getDurationMs();
		opts.userId = user.id;
		opts.nickname = getNickname(user);

		UserTranslationPlainObject obj = new UserTranslationPlainObject(ust.getId());
		ust.getSharingOptions().add(opts);
		shareLocationByUser(ust, user.id);

		obj.ownerUserId = ust.getOwner();
		obj.setShareLocations(ust);
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
		template.convertAndSend(TOPIC_TRANSLATION + ust.getId(),
				prepareMessageSystem().setType(TranslationMessageType.METADATA).setContent(obj));
	}

	public boolean deleteTranslation(UserTranslation ust, CloudUser user, SimpMessageHeaderAccessor headers) {
		if (ust.getOwner() != user.id) {
			sendError("Only the owner can delete the translation", headers);
			return false;
		}
		activeSessions.remove(ust.getId());
		if (redisTemplate != null) {
			redisTemplate.delete(REDIS_MSG_KEY_PREFIX + ust.getId());
			redisTemplate.delete(REDIS_TRANSLATION_KEY_PREFIX + ust.getId());
		}
		// Notify all viewers so they can clean up.
		rawSendMessage(ust, prepareMessageSystem().setType(TranslationMessageType.DELETE).setContent(ust.getId()));
		return true;
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
		UserTranslationPlainObject obj = new UserTranslationPlainObject(ust.getId());
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

	public boolean sendEncryptedDeviceMessage(CloudUserDevice dev, CloudUser pu, String encData, String clientDeviceId,
	                                          String clientAccessToken) {
		if (clientDeviceId != null && clientAccessToken != null
				&& (dev == null || !dev.deviceid.equals(clientDeviceId) || !dev.accesstoken.equals(clientAccessToken))) {
			return false;
		}

		int userId = dev != null ? dev.userid : pu.id;
		Deque<UserTranslation> userTranslations = shareLocTranslationsByUser.get(userId);
		if (userTranslations == null || userTranslations.isEmpty()) {
			return false;
		}
		TranslationMessage msg = prepareMessageAuthor(dev, pu);
		msg.content = Map.of(ENCRYPTED_DATA, encData);
		msg.type = TranslationMessageType.LOCATION;
		long timeMillis = System.currentTimeMillis();
		boolean sent = false;
		for (UserTranslation ust : userTranslations) {
			for (TranslationSharingOptions o : ust.getSharingOptions()) {
				if (o.userId == userId && timeMillis < o.expireTime) {
					rawSendMessage(ust, msg);
					sent = true;
					break;
				}
			}
		}
		return sent;
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
        if (msg.serverReceiveTime == 0) {
            msg.serverReceiveTime = System.currentTimeMillis();
        }
    	template.convertAndSend(TOPIC_TRANSLATION + ust.getId(), msg);
        if (redisTemplate != null) {
            String key = REDIS_MSG_KEY_PREFIX + ust.getId();
            redisTemplate.opsForZSet().add(key, gson.toJson(msg), msg.serverReceiveTime);
            redisTemplate.expire(key, MESSAGES_TTL);
        } else {
            ust.getMessages().add(msg);
        }
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
			CloudUser user = getUser(headers.getUser(), headers, true);
			TranslationMessage msg = prepareMessageSystem().setType(TranslationMessageType.JOIN)
					.setContent(getNickname(user));
			template.convertAndSend(destination, msg);
			
//			String translationId = destination.replace(TOPIC_TRANSLATION, "");
//			UserTranslation ust = getTranslation(translationId, headers);
//			if (user.id > 0) {
//				userJoinTranslation(ust, user.id);
//			}
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