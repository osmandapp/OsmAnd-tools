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

import net.osmand.server.security.JwtTokenProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.security.core.Authentication;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import jakarta.servlet.http.HttpServletRequest;
import net.osmand.server.security.WebSecurityConfiguration;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.ws.TranslationMessage.TranslationMessageType;
import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.Algorithms;

@Service
public class UserTranslationsService {

	private static final Log LOG = LogFactory.getLog(UserTranslationsService.class);
	
    public static final String TRANSLATION_ID = "translationId";

	public static final String X_ALIAS = "X-Alias"; // User alias header
	public static final String X_DEVICE_ID = "X-Device-Id"; // Device ID header

    
    static final String TOPIC_TRANSLATION = "/topic/translation/";
    static final String QUEUE_USER_UPDATES = "/queue/updates";
    
    static final String USER_UPD_TYPE_ERROR = "ERROR";
    static final String USER_UPD_TYPE_TRANSLATION = "TRANSLATION";
    static final String USER_UPD_TYPE_USER_INFO = "USER_INFO";
    
    static final String TRANSLATION_MISSING = "Translation doesn't exist";
    
    private static final long DEFAULT_SHARING_DURATION_MS = 60 * 60 * 1000; // 1 hour
	
	// Maximum number of active translations per user
	private static final int MAX_TRANSLATIONS_PER_USER = 50;
	
	// Maximum number of devices per user that can share in a single translation
	private static final int MAX_DEVICES_PER_USER_PER_TRANSLATION = 5;

	// Regex pattern for valid translation IDs: alphanumeric characters, underscores, and hyphens
	public static final String VALID_TRANSLATION_ID_PATTERN = "^[a-zA-Z0-9_-]+$";

    // implement 1-3 day storage for location
    private final Map<Integer, Deque<WptPt>> userLocationHistory = new ConcurrentHashMap<>();
    // store last 1-3 days locations are present (?)
    private final Map<String, UserTranslation> activeTranslations = new ConcurrentHashMap<>();
    
    // no need to be persistent
    private final Map<Integer, Deque<UserTranslation>> shareLocTranslationsByUser = new ConcurrentHashMap<>();
    private final Map<String, String> anonymousUsers = new ConcurrentHashMap<>();
    
    private static final long ROOM_TOKEN_VALIDITY_MS = 30 * 60 * 1000; // 30 minutes

    private final Random random = new SecureRandom();
    
    @Autowired
    private SimpMessagingTemplate template;
    
    @Autowired
	protected CloudUserDevicesRepository devicesRepository;
    
    @Autowired
    private JwtTokenProvider jwtTokenProvider;
    
    private final Environment environment;
    
    private final BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    public UserTranslationsService(Environment environment) {
        this.environment = environment;
    }

    public void setTranslationPassword(UserTranslation translation, String plainPassword) {
        if (translation == null) {
            throw new IllegalArgumentException("Translation cannot be null");
        }
        if (plainPassword == null || plainPassword.isEmpty()) {
            translation.setPassword(null);
            return;
        }
        String passwordHash = passwordEncoder.encode(plainPassword);
        translation.setPassword(passwordHash);
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

    // User management
    public CloudUser getUser(Principal principal, SimpMessageHeaderAccessor headers) {
		boolean production = environment.acceptsProfiles(Profiles.of("production"));		
		return getUser(principal, headers, !production);
	}
	
	public CloudUser getUser(Principal principal, SimpMessageHeaderAccessor headers, boolean allowAnonymous) {
		CloudUser us = getUserFromPrincipal(principal);
		if (us == null && allowAnonymous) {
			us = new CloudUser();
			Map<String, Object> attributes = headers.getSessionAttributes();
			String oalias = (attributes != null) ? (String) attributes.get(X_ALIAS) : null;
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
			attributes.put(X_ALIAS, alias);
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

	// Session management

	/**
	 * Creates a new translation (sharing session).
	 * Enforces limits on the number of translations per user.
	 * 
	 * @param user user creating the translation (null for anonymous)
	 * @param translationId optional translation ID (generated if null)
	 * @param headers WebSocket headers
	 * @return created UserTranslation
	 * @throws IllegalStateException if user has reached the maximum number of translations
	 */
	public UserTranslation createTranslation(CloudUser user, String translationId, SimpMessageHeaderAccessor headers) {
		if (user != null && user.id > 0) {
			int userTranslationCount = countUserTranslations(user.id);
			if (userTranslationCount >= MAX_TRANSLATIONS_PER_USER) {
				String error = String.format("Maximum number of translations (%d) reached", MAX_TRANSLATIONS_PER_USER);
				sendError(error, headers);
				throw new IllegalStateException(error);
			}
		}
		
		long time = System.currentTimeMillis();
		if (translationId == null) {
			translationId = Long.toHexString(time * 100L + random.nextInt(100));
		}
		UserTranslation ust = new UserTranslation(translationId, user == null ? -1: user.id);
		ust.setCreationDate(time);
		activeTranslations.put(ust.getSessionId(), ust);
		if (user != null) {
			shareLocationByUser(ust, user.id);
		}
		UserTranslationDTO obj = new UserTranslationDTO(ust.getSessionId());
		if (headers != null) {
			sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
		}
		return ust;
    }

	private void shareLocationByUser(UserTranslation ust, int uid) {
		shareLocTranslationsByUser.computeIfAbsent(uid, k -> new ConcurrentLinkedDeque<>()).add(ust);
	}

	private long extractDeviceIdFromHeaders(SimpMessageHeaderAccessor headers) {
		if (headers == null) {
			return 0;
		}
		String deviceIdHeader = headers.getFirstNativeHeader(X_DEVICE_ID);
		if (deviceIdHeader == null || deviceIdHeader.isEmpty()) {
			return 0;
		}
		long deviceId = Algorithms.parseLongSilently(deviceIdHeader, 0);
		if (deviceId == 0) {
			LOG.warn("Invalid deviceId format in header: " + deviceIdHeader);
		}
		return deviceId;
	}

	private long calculateExpireTime() {
		return System.currentTimeMillis() + DEFAULT_SHARING_DURATION_MS;
	}

	private int countUserTranslations(int userId) {
		int count = 0;
		for (UserTranslation t : activeTranslations.values()) {
			if (t.getOwnerId() == userId) {
				count++;
			}
		}
		return count;
	}

	private long countUserDevicesInTranslation(UserTranslation translation, int userId) {
		Map<Long, Boolean> uniqueDevices = new HashMap<>();
		for (TranslationSharingOptions sharer : translation.getActiveSharers()) {
			if (sharer.userId == userId && sharer.deviceId > 0) {
				uniqueDevices.put(sharer.deviceId, true);
			}
		}
		return uniqueDevices.size();
	}
	
	/**
	 * Finds an active sharer for a user and device in a translation.
	 * 
	 * @param translation translation to search in
	 * @param userId user ID
	 * @param deviceId device ID (0 means any device)
	 * @return TranslationSharingOptions if found, null otherwise
	 */
	private TranslationSharingOptions findSharer(UserTranslation translation, int userId, long deviceId) {
		long currentTime = System.currentTimeMillis();
		for (TranslationSharingOptions sharer : translation.getActiveSharers()) {
			if (sharer.userId == userId && currentTime < sharer.expireTime) {
				// If deviceId is 0, match any device; otherwise match specific device or deviceId == 0 in sharer
				if (deviceId == 0 || sharer.deviceId == 0 || sharer.deviceId == deviceId) {
					return sharer;
				}
			}
		}
		return null;
	}

	private void updateSharerExpireTime(TranslationSharingOptions sharer) {
		sharer.expireTime = calculateExpireTime();
	}

	public UserTranslation getTranslation(String translationId, SimpMessageHeaderAccessor headers) {
		UserTranslation ust = activeTranslations.get(translationId);
		if (ust == null) {
			sendError(TRANSLATION_MISSING, headers);
		}
		return ust;
	}

	/**
	 * Returns a translation without sending an error to the client (for use in interceptors).
	 *
	 * @param translationId translation (session) ID
	 * @return UserTranslation instance or null if not found
	 */
	public UserTranslation getTranslationSilent(String translationId) {
		return activeTranslations.get(translationId);
	}

	public void load(UserTranslation ust, SimpMessageHeaderAccessor headers) {
		UserTranslationDTO obj = new UserTranslationDTO(ust.getSessionId());
		obj.setHistory(ust.getMessages());
		obj.setSharingUsers(ust);
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
	}

	// Sharing management

	public void recordVerifiedAccess(String translationId, int userId) {
		if (translationId != null) {
			UserTranslation translation = activeTranslations.get(translationId);
			if (translation != null) {
				translation.getVerifiedUsers().add(userId);
			}
		}
	}
	
	/**
	 * Generates a JWT Bearer token for room access after password verification.
	 * Token is valid for 30 minutes.
	 * 
	 * @param translationId room ID
	 * @param userId user ID
	 * @param alias user alias
	 * @return Bearer token string
	 */
	public String generateRoomToken(String translationId, int userId, String alias) {
		if (translationId == null || translationId.isEmpty()) {
			return null;
		}
		
		String token = jwtTokenProvider.createRoomToken(translationId, alias, ROOM_TOKEN_VALIDITY_MS);
		LOG.debug("Generated JWT room token for translation " + translationId + ", user " + userId);
		return token;
	}
	
	/**
	 * Authenticates user for room access using password and returns Bearer token.
	 * 
	 * @param translationId room ID
	 * @param password room password (plain text)
	 * @param userId user ID (optional, 0 if anonymous)
	 * @param alias user alias
	 * @return Bearer token if authentication successful, null otherwise
	 */
	public String authenticateRoom(String translationId, String password, int userId, String alias) {
		UserTranslation translation = activeTranslations.get(translationId);
		if (translation == null) {
			LOG.warn("Room authentication failed: translation not found " + translationId);
			return null;
		}
		
		String passwordHash = translation.getPassword();
		// If room has no password, allow access
		if (passwordHash == null || passwordHash.isEmpty()) {
			return generateRoomToken(translationId, userId, alias);
		}
		
		// If user already verified, allow access
		if (userId > 0 && translation.getVerifiedUsers().contains(userId)) {
			return generateRoomToken(translationId, userId, alias);
		}
		
		// Check password
		if (password == null || password.isEmpty()) {
			LOG.warn("Room authentication failed: password required for " + translationId);
			return null;
		}
		
		try {
			boolean matches = passwordEncoder.matches(password, passwordHash);
			if (matches) {
				if (userId > 0) {
					translation.getVerifiedUsers().add(userId);
				}
				LOG.debug("Room authentication successful for " + translationId + ", user " + userId);
				return generateRoomToken(translationId, userId, alias);
			} else {
				LOG.warn("Room authentication failed: invalid password for " + translationId);
				return null;
			}
		} catch (Exception e) {
			LOG.warn("Room authentication error for " + translationId + ": " + e.getMessage(), e);
			return null;
		}
	}
	
	/**
	 * Checks if a user has permission to perform operations on a translation.
	 * Users can operate on translations if they are:
	 * 1. The owner of the translation
	 * 2. An active sharer (sharing location in the translation)
	 * 3. Have verified access via password (cached after successful subscription)
	 * 4. Public translation (no password)
	 * 
	 * @param translation translation to check
	 * @param user user to check permissions for
	 * @param deviceId optional device ID to check (0 means any device)
	 * @param headers WebSocket headers (may contain password header and sessionId)
	 * @return true if user has permission, false otherwise
	 */
	public boolean hasOperationPermission(UserTranslation translation, CloudUser user, long deviceId, SimpMessageHeaderAccessor headers) {
		if (translation == null || user == null) {
			return false;
		}

		if (translation.getOwnerId() == user.id) {
			return true;
		}

		if (findSharer(translation, user.id, deviceId) != null) {
			return true;
		}

		String password = translation.getPassword();
		if (password != null && !password.isEmpty()) {
			// Password-protected rooms require JWT token authentication
			if (translation.getVerifiedUsers().contains(user.id)) {
				return true;
			}
			LOG.debug("Password-protected translation " + translation.getSessionId() + " requires JWT Bearer token");
			return false;
		}
		
		// Public translation - allow everyone
		return true;
	}

	public boolean hasOperationPermission(UserTranslation translation, CloudUser user, SimpMessageHeaderAccessor headers) {
		return hasOperationPermission(translation, user, 0, headers);
	}
	
	/**
	 * Starts sharing location in a translation.
	 * Requires that the user has permission (owner or active sharer).
	 * Enforces limit on number of devices per user per translation.
	 * 
	 * @param ust translation to share in
	 * @param user user starting to share
	 * @param headers WebSocket headers (may contain deviceId header)
	 * @throws SecurityException if user doesn't have permission or device limit exceeded
	 */
	public void startSharing(UserTranslation ust, CloudUser user, SimpMessageHeaderAccessor headers) {
		if (!hasOperationPermission(ust, user, headers)) {
			String error = "Permission denied: user is not owner or active sharer";
			sendError(error, headers);
			throw new SecurityException(error);
		}

		long deviceId = extractDeviceIdFromHeaders(headers);
		TranslationSharingOptions existingSharer = findSharer(ust, user.id, deviceId);
		if (existingSharer != null) {
			// Already sharing, just update expire time
			updateSharerExpireTime(existingSharer);
			return;
		}

		if (deviceId > 0) {
			long totalDevicesForUser = countUserDevicesInTranslation(ust, user.id);
			if (totalDevicesForUser >= MAX_DEVICES_PER_USER_PER_TRANSLATION) {
				String error = String.format("Maximum number of devices (%d) per user per translation reached", MAX_DEVICES_PER_USER_PER_TRANSLATION);
				sendError(error, headers);
				throw new SecurityException(error);
			}
		}
		
		// Create new sharing options
		TranslationSharingOptions opts = new TranslationSharingOptions();
		opts.startTime = System.currentTimeMillis();
		opts.expireTime = calculateExpireTime();
		opts.userId = user.id;
		opts.deviceId = deviceId;
		opts.nickname = getNickname(user);

		// Send last known location if available
		Deque<WptPt> locationHistory = userLocationHistory.get(user.id);
		if (locationHistory != null && !locationHistory.isEmpty()) {
			ust.sendLocation(user.id, locationHistory.getLast());
		}
		
		// Start simulation in non-production environments
		if(!environment.acceptsProfiles(Profiles.of("production"))) {
			startSimulation(user, ust);
		}
		
		// Add sharer and notify clients
		ust.getActiveSharers().add(opts);
		shareLocationByUser(ust, user.id);
		
		UserTranslationDTO obj = new UserTranslationDTO(ust.getSessionId());
		obj.setSharingUsers(ust);
		sendPrivateMessage(headers.getSessionId(), USER_UPD_TYPE_TRANSLATION, obj);
	}
	
	/**
	 * Starts location simulation for testing purposes (non-production only).
	 * 
	 * NOTE: Creates unmanaged threads which could lead to resource leaks.
	 * This is acceptable for testing/simulation purposes only.
	 * For production code, consider using ExecutorService or @Async with proper thread pool configuration.
	 * 
	 * @param user user to simulate location for
	 * @param ust translation session
	 */
	private void startSimulation(CloudUser user, UserTranslation ust) {
		Thread simThread = new Thread(() -> {
			double simLat = 50.4501;
			double simLon = 30.5234;
			boolean gone = false;
			while (!gone) {
				gone = true;
				for (TranslationSharingOptions sharing : ust.getActiveSharers()) {
					if (sharing.userId == user.id) {
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
					LOG.error("Simulation error", e);
				}
			}
		});
		simThread.setDaemon(true); // Ensure thread doesn't block app shutdown
		simThread.setName("LocSim-" + user.nickname);
		simThread.start();
	}

	/**
	 * Stops sharing location in a translation.
	 * Users can stop their own sharing (optionally for specific device), owners can stop anyone's sharing.
	 * 
	 * @param ust translation to stop sharing in
	 * @param user user stopping sharing
	 * @param headers WebSocket headers (may contain deviceId header)
	 * @throws SecurityException if user doesn't have permission
	 */
	public void stopSharing(UserTranslation ust, CloudUser user, SimpMessageHeaderAccessor headers) {
		if (ust == null || user == null) {
			sendError("Invalid translation or user", headers);
			return;
		}

		long deviceId = extractDeviceIdFromHeaders(headers);
		boolean isOwner = ust.getOwnerId() == user.id;
		Deque<TranslationSharingOptions> activeSharers = ust.getActiveSharers();
		Iterator<TranslationSharingOptions> it = activeSharers.iterator();
		int userId = user.id;
		boolean removed = false;
		
		while (it.hasNext()) {
			TranslationSharingOptions sharing = it.next();
			// Owner can remove anyone, users can only remove themselves
			boolean matchesUser = sharing.userId == userId || isOwner;
			boolean matchesDevice = deviceId == 0 || sharing.deviceId == 0 || sharing.deviceId == deviceId;
			
			if (matchesUser && matchesDevice) {
				it.remove();
				removed = true;
				if (isOwner && sharing.userId != userId) {
					break;
				}
				if (deviceId > 0 && sharing.deviceId == deviceId) {
					break;
				}
			}
		}
		
		if (!removed && !isOwner) {
			String error = "Permission denied: cannot stop sharing for other users or devices";
			sendError(error, headers);
			throw new SecurityException(error);
		}
		
		UserTranslationDTO obj = new UserTranslationDTO(ust.getSessionId());
		obj.setSharingUsers(ust);
		rawSendMessage(ust, prepareMessageSystem().setType(TranslationMessageType.METADATA).setContent(obj));
	}

	// Message sending
	
    /**
     * Sends a message to a translation.
     * Requires that the user has permission (owner or active sharer).
     * 
     * @param ust translation to send message to
     * @param user user sending the message
     * @param message message content
     * @return true if message was sent, false otherwise
     * @throws SecurityException if user doesn't have permission
     */
    public boolean sendMessage(UserTranslation ust, CloudUser user, Object message, SimpMessageHeaderAccessor headers) {
		if (ust == null || user == null) {
			return false;
		}

		if (!hasOperationPermission(ust, user, headers)) {
			LOG.warn("User " + user.id + " attempted to send message to translation " + ust.getSessionId() + " without permission");
			throw new SecurityException("Access denied to translation: " + ust.getSessionId());
		}
		
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
		long deviceId = dev != null ? dev.id : 0;
		
		userLocationHistory.computeIfAbsent(userId, k -> new ConcurrentLinkedDeque<>()).push(wptPt);
		TranslationMessage msg = prepareMessageAuthor(dev, pu);
		msg.content = Map.of("point", wptPt);
		msg.type = TranslationMessageType.LOCATION;
		
		Deque<UserTranslation> translations = shareLocTranslationsByUser.get(userId);
		if (translations == null) {
			return;
		}
		
		// Send location to all translations where user is actively sharing
		for (UserTranslation ust : translations) {
			TranslationSharingOptions sharer = findSharer(ust, userId, deviceId);
			if (sharer != null) {
				ust.sendLocation(userId, wptPt);
				rawSendMessage(ust, msg);
			}
		}
	}

	private void rawSendMessage(UserTranslation ust, TranslationMessage msg) {
		template.convertAndSend(TOPIC_TRANSLATION + ust.getSessionId(), msg);
		ust.getMessages().add(msg);
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
		String oalias = (attributes != null) ? (String) attributes.get(X_ALIAS) : null;
		if (oalias != null && event.getSessionId().equals(anonymousUsers.get(oalias))) {
			anonymousUsers.remove(oalias);
		}
	}

    
}