package net.osmand.server.ws;

import java.security.Principal;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
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
import org.springframework.web.socket.messaging.SessionUnsubscribeEvent;

import com.google.gson.Gson;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUsersRepository;
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
    static final String USER_UPD_TYPE_SHARE_REQUEST = "SHARE_REQUEST";
    static final String USER_UPD_TYPE_SHARE_APPROVED = "SHARE_APPROVED";
    static final String USER_UPD_TYPE_SHARE_DENIED = "SHARE_DENIED";
    
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
    private static final String REDIS_USER_SHARES_PREFIX = "livetrack:usershares:";

    // Active translation sessions. Metadata is persisted in Redis; this map holds the live
    // object with session state (sharingOptions) for the duration of the server process.
    private final Map<String, UserTranslation> activeSessions = new ConcurrentHashMap<>();
    private final Map<Integer, Deque<UserTranslation>> shareLocTranslationsByUser = new ConcurrentHashMap<>();
    // alias → STOMP sessionId for anonymous connections.
    private final Map<String, String> anonymousUsers = new ConcurrentHashMap<>();
    // sessionId → (subscriptionId → subscribed translation topic) — to emit LEAVE on unsubscribe/disconnect.
    private final Map<String, Map<String, String>> sessionSubscriptions = new ConcurrentHashMap<>();
    // translation topic → (sessionId → viewer nickname) — current viewers, for the roster snapshot.
    private final Map<String, Map<String, String>> viewersByTopic = new ConcurrentHashMap<>();

    private final Gson gson = new Gson();
    private final Random random = new SecureRandom();
    
    @Autowired
    private SimpMessagingTemplate template;
    
    @Autowired
	protected CloudUserDevicesRepository devicesRepository;

    @Autowired
	protected CloudUsersRepository usersRepository;
    
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
			if (attributes != null) {
				attributes.put(ALIAS, alias);
			}
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
		Set<Integer> allowedSharers;
		List<TranslationSharingOptions> sharingOptions;
		Map<Integer, String> pendingRequests;
	}

	private void saveTranslationToRedis(UserTranslation ust) {
		if (redisTemplate == null) return;
		TranslationMeta meta = new TranslationMeta();
		meta.id = ust.getId();
		meta.owner = ust.getOwner();
		meta.creationDate = ust.getCreationDate();
		meta.durationMs = ust.getDurationMs();
		meta.allowedSharers = new HashSet<>(ust.getAllowedSharers());
		meta.sharingOptions = new ArrayList<>(ust.getSharingOptions());
		meta.pendingRequests = new HashMap<>(ust.getPendingShareRequests());
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
		if (meta.allowedSharers != null) {
			ust.getAllowedSharers().addAll(meta.allowedSharers);
		}
		if (meta.pendingRequests != null) {
			ust.getPendingShareRequests().putAll(meta.pendingRequests);
		}
		if (meta.sharingOptions != null) {
			for (TranslationSharingOptions o : meta.sharingOptions) {
				ust.getSharingOptions().add(o);
				shareLocationByUser(ust, o.userId);
			}
		}
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
		UserTranslation ust = new UserTranslation(translationId, user.id);
		ust.setCreationDate(time);
		ust.setDurationMs(durationHours <= 0 ? UserTranslation.PERMANENT_DURATION_MS : durationHours * 60 * 60 * 1000L);
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
		if (redisTemplate != null) {
			String key = REDIS_USER_SHARES_PREFIX + uid;
			redisTemplate.opsForSet().add(key, ust.getId());
			redisTemplate.expire(key, MESSAGES_TTL);
		}
	}

	// Rebuilds in-memory routing for a user from Redis (used after a restart when a device posts
	// before any viewer has reloaded the translation).
	private void restoreUserShares(int uid) {
		if (redisTemplate == null) {
			return;
		}
		Set<String> tids = redisTemplate.opsForSet().members(REDIS_USER_SHARES_PREFIX + uid);
		if (tids == null) {
			return;
		}
		for (String tid : tids) {
			getTranslation(tid, null);
		}
	}
	

	public UserTranslation getTranslation(String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = activeSessions.computeIfAbsent(translationId, this::loadTranslationFromRedis);
		if (ust == null) {
			sendError(TRANSLATION_MISSING, headers);
		}
		return ust;
	}

	public void load(UserTranslation ust, long fromTime, long toTime, CloudUser user, SimpMessageHeaderAccessor headers) {
		UserTranslationPlainObject obj = new UserTranslationPlainObject(ust.getId());
		obj.ownerUserId = ust.getOwner();
		obj.creationDate = ust.getCreationDate();
		if (user != null && ust.getOwner() == user.id && !ust.getPendingShareRequests().isEmpty()) {
			obj.setPendingRequests(ust.getPendingShareRequests());
		}
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
		obj.setShareLocations(ust, user != null ? user.id : 0);
		obj.viewers = currentViewers(ust.getId());
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
	}

	public void startSharing(UserTranslation ust, CloudUser user, SimpMessageHeaderAccessor headers) {
		if (!isAllowedToShare(ust, user.id)) {
			sendError("Not allowed to share in this translation", headers);
			return;
		}
		UserTranslationPlainObject obj = addSharer(ust, user.id, getNickname(user));
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
		template.convertAndSend(TOPIC_TRANSLATION + ust.getId(),
				prepareMessageSystem().setType(TranslationMessageType.METADATA).setContent(obj));
	}

	private boolean isAllowedToShare(UserTranslation ust, int userId) {
		return ust.getOwner() == userId || ust.getAllowedSharers().contains(userId);
	}

	// Registers (or refreshes) userId as a sharer of the translation with a room-unique nickname.
	private UserTranslationPlainObject addSharer(UserTranslation ust, int userId, String baseNickname) {
		ust.getSharingOptions().removeIf(o -> o.userId == userId);
		TranslationSharingOptions opts = new TranslationSharingOptions();
		opts.startTime = System.currentTimeMillis();
		opts.expireTime = opts.startTime + ust.getDurationMs();
		opts.userId = userId;
		opts.nickname = uniqueNickname(ust, userId, baseNickname);
		ust.getSharingOptions().add(opts);
		shareLocationByUser(ust, userId);
		saveTranslationToRedis(ust);

		UserTranslationPlainObject obj = new UserTranslationPlainObject(ust.getId());
		obj.ownerUserId = ust.getOwner();
		obj.setShareLocations(ust);
		return obj;
	}

	// Ensures the nickname is unique among other sharers in the same room (appends " 2", " 3", ...).
	private String uniqueNickname(UserTranslation ust, int userId, String base) {
		Set<String> taken = new HashSet<>();
		for (TranslationSharingOptions o : ust.getSharingOptions()) {
			if (o.userId != userId) {
				taken.add(o.nickname);
			}
		}
		if (!taken.contains(base)) {
			return base;
		}
		int n = 2;
		while (taken.contains(base + " " + n)) {
			n++;
		}
		return base + " " + n;
	}

	// A viewer (with the link) asks the owner for permission to broadcast into the translation.
	public void requestShare(UserTranslation ust, CloudUser user) {
		if (isAllowedToShare(ust, user.id)) {
			return;
		}
		String nickname = getNickname(user);
		ust.getPendingShareRequests().put(user.id, nickname);
		saveTranslationToRedis(ust);
		Map<String, Object> data = new HashMap<>();
		data.put("translationId", ust.getId());
		data.put("userId", user.id);
		data.put("nickname", nickname);
		notifyUser((int) ust.getOwner(), USER_UPD_TYPE_SHARE_REQUEST, data);
	}

	public boolean requestShareFromDevice(String translationId, CloudUser user) {
		UserTranslation ust = getTranslation(translationId, null);
		if (ust == null) {
			return false;
		}
		requestShare(ust, user);
		return true;
	}

	public void approveShare(UserTranslation ust, CloudUser owner, int targetUserId, SimpMessageHeaderAccessor headers) {
		if (ust.getOwner() != owner.id) {
			sendError("Only the owner can approve sharing", headers);
			return;
		}
		ust.getPendingShareRequests().remove(targetUserId);
		ust.getAllowedSharers().add(targetUserId);
		CloudUser target = usersRepository.findById(targetUserId);
		String base = target != null ? getNickname(target) : "User " + targetUserId;
		UserTranslationPlainObject obj = addSharer(ust, targetUserId, base);
		template.convertAndSend(TOPIC_TRANSLATION + ust.getId(),
				prepareMessageSystem().setType(TranslationMessageType.METADATA).setContent(obj));
		notifyUser(target, USER_UPD_TYPE_SHARE_APPROVED, ust.getId());
	}

	public void denyShare(UserTranslation ust, CloudUser owner, int targetUserId, SimpMessageHeaderAccessor headers) {
		if (ust.getOwner() != owner.id) {
			sendError("Only the owner can deny sharing", headers);
			return;
		}
		ust.getPendingShareRequests().remove(targetUserId);
		ust.getAllowedSharers().remove(targetUserId);
		ust.getSharingOptions().removeIf(o -> o.userId == targetUserId);
		saveTranslationToRedis(ust);
		notifyUser(targetUserId, USER_UPD_TYPE_SHARE_DENIED, ust.getId());
	}

	private void notifyUser(int userId, String type, Object data) {
		notifyUser(usersRepository.findById(userId), type, data);
	}

	private void notifyUser(CloudUser u, String type, Object data) {
		if (u == null || Algorithms.isEmpty(u.email)) {
			return;
		}
		Map<String, Object> payload = new HashMap<>();
		payload.put("type", type);
		payload.put("data", data);
		template.convertAndSendToUser(u.email, QUEUE_USER_UPDATES, payload);
	}

	public boolean deleteTranslation(UserTranslation ust, CloudUser user, SimpMessageHeaderAccessor headers) {
		if (ust.getOwner() != user.id) {
			sendError("Only the owner can delete the translation", headers);
			return false;
		}
		activeSessions.remove(ust.getId());
		// Drop this translation from every sharer's routing index (in-memory and Redis).
		for (TranslationSharingOptions o : ust.getSharingOptions()) {
			Deque<UserTranslation> deque = shareLocTranslationsByUser.get(o.userId);
			if (deque != null) {
				deque.remove(ust);
			}
			if (redisTemplate != null) {
				redisTemplate.opsForSet().remove(REDIS_USER_SHARES_PREFIX + o.userId, ust.getId());
			}
		}
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
		// User no longer shares here — drop the routing index entry (in-memory and Redis).
		Deque<UserTranslation> deque = shareLocTranslationsByUser.get(userId);
		if (deque != null) {
			deque.remove(ust);
		}
		if (redisTemplate != null) {
			redisTemplate.opsForSet().remove(REDIS_USER_SHARES_PREFIX + userId, ust.getId());
		}
		saveTranslationToRedis(ust);
		UserTranslationPlainObject obj = new UserTranslationPlainObject(ust.getId());
		obj.setShareLocations(ust);
		rawSendMessage(ust, prepareMessageSystem().setType(TranslationMessageType.METADATA).setContent(obj));
	}
	
	public String sendError(String error, SimpMessageHeaderAccessor headers) {
		if (headers != null) {
			sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_ERROR, error);
		}
		return error;
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
			restoreUserShares(userId);
			userTranslations = shareLocTranslationsByUser.get(userId);
		}
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
					msg.sender = o.nickname;
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
		if (destination == null || !destination.startsWith(TOPIC_TRANSLATION)) {
			return;
		}
		String sessionId = headers.getSessionId();
		String nickname = getNickname(getUser(headers.getUser(), headers, true));
		sessionSubscriptions.computeIfAbsent(sessionId, k -> new ConcurrentHashMap<>())
				.put(headers.getSubscriptionId(), destination);
		viewersByTopic.computeIfAbsent(destination, k -> new ConcurrentHashMap<>()).put(sessionId, nickname);
		template.convertAndSend(destination,
				prepareMessageSystem().setType(TranslationMessageType.JOIN).setContent(nickname));
	}

	@EventListener
	public void handleSessionUnsubscribeEvent(SessionUnsubscribeEvent event) {
		StompHeaderAccessor headers = StompHeaderAccessor.wrap(event.getMessage());
		removeViewer(headers.getSessionId(), headers.getSubscriptionId());
	}

	@EventListener
	public void onDisconnectEvent(SessionDisconnectEvent event) {
		String sessionId = event.getSessionId();
		Map<String, String> subs = sessionSubscriptions.remove(sessionId);
		if (subs != null) {
			for (String destination : subs.values()) {
				emitLeave(sessionId, destination);
			}
		}
		StompHeaderAccessor headers = StompHeaderAccessor.wrap(event.getMessage());
		Map<String, Object> attributes = headers.getSessionAttributes();
		String oalias = (attributes != null) ? (String) attributes.get(ALIAS) : null;
		if (oalias != null && sessionId.equals(anonymousUsers.get(oalias))) {
			anonymousUsers.remove(oalias);
		}
	}

	private void removeViewer(String sessionId, String subId) {
		Map<String, String> subs = sessionSubscriptions.get(sessionId);
		if (subs == null) {
			return;
		}
		String destination = subs.remove(subId);
		if (subs.isEmpty()) {
			sessionSubscriptions.remove(sessionId);
		}
		emitLeave(sessionId, destination);
	}

	private void emitLeave(String sessionId, String destination) {
		Map<String, String> viewers = destination != null ? viewersByTopic.get(destination) : null;
		if (viewers == null) {
			return;
		}
		String nickname = viewers.remove(sessionId);
		if (viewers.isEmpty()) {
			viewersByTopic.remove(destination);
		}
		if (nickname != null) {
			template.convertAndSend(destination,
					prepareMessageSystem().setType(TranslationMessageType.LEAVE).setContent(nickname));
		}
	}

	private List<String> currentViewers(String translationId) {
		Map<String, String> viewers = viewersByTopic.get(TOPIC_TRANSLATION + translationId);
		return viewers == null ? Collections.emptyList() : new ArrayList<>(new HashSet<>(viewers.values()));
	}

    
}