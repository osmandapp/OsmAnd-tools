package net.osmand.server.ws;

import java.security.Principal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;

import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;

@Component
public class SubscriptionInterceptor implements ChannelInterceptor {

	private static final Log LOG = LogFactory.getLog(SubscriptionInterceptor.class);

	private static final int MAX_TRANSLATION_ID_LENGTH = 128;
	// Rate limiting: max failed attempts per IP/session before blocking
	private static final int MAX_FAILED_ATTEMPTS = 5;
	private static final long RATE_LIMIT_WINDOW_MS = 60 * 1000; // 1 minute

	private final Map<String, FailedAttemptTracker> failedAttempts = new ConcurrentHashMap<>();
	
	@Autowired
	@Lazy
	private UserTranslationsService translationsService;
	
	private final BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
	
	/**
	 * Tracks failed access attempts for rate limiting.
	 */
	private static class FailedAttemptTracker {
		private final AtomicInteger count = new AtomicInteger(0);
		private volatile long resetTime = System.currentTimeMillis() + RATE_LIMIT_WINDOW_MS;
		
		public int incrementAndGet() {
			long now = System.currentTimeMillis();
			if (now > resetTime) {
				count.set(0);
				resetTime = now + RATE_LIMIT_WINDOW_MS;
			}
			return count.incrementAndGet();
		}
		
		public boolean isBlocked() {
			return count.get() >= MAX_FAILED_ATTEMPTS && System.currentTimeMillis() < resetTime;
		}
	}

    @Override
    public Message<?> preSend(@NotNull Message<?> message, @NotNull MessageChannel channel) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(message);
        
        // Propagate SecurityContext from HTTP session to WebSocket message
        // This ensures Principal (Authentication) is available for access control
        SecurityContext securityContext = SecurityContextHolder.getContext();
        Authentication authentication = securityContext.getAuthentication();
        if (authentication != null && accessor.getUser() == null) {
            accessor.setUser(authentication);
        }
        
        Principal user = accessor.getUser();
		if (StompCommand.CONNECT.equals(accessor.getCommand())) {
			// Access authentication header(s) and invoke accessor.setUser(user)
			String alias = accessor.getFirstNativeHeader(UserTranslationsService.ALIAS);
			if (alias != null) {
				Map<String, Object> sessionAttributes = accessor.getSessionAttributes();
				if (sessionAttributes != null) {
					sessionAttributes.put(UserTranslationsService.ALIAS, alias);
				}
			}
		} else if (StompCommand.SUBSCRIBE.equals(accessor.getCommand())) {
            String destination = accessor.getDestination();
            if (destination != null && destination.startsWith(UserTranslationsService.TOPIC_TRANSLATION)) {
                String translationId = destination.substring(UserTranslationsService.TOPIC_TRANSLATION.length());

                if (!isValidTranslationId(translationId)) {
                    String errorMessage = "Invalid translation ID format";
                    logAccessAttempt(translationId, user, false, "Invalid ID format");
                    throw new MessageDeliveryException(message, errorMessage, 
                        new AccessDeniedException(errorMessage));
                }

                SimpMessageHeaderAccessor simpHeaders = SimpMessageHeaderAccessor.wrap(message);
                String sessionId = simpHeaders.getSessionId();
                if (isRateLimited(sessionId)) {
                    String errorMessage = "Too many failed access attempts. Please try again later.";
                    logAccessAttempt(translationId, user, false, "Rate limited");
                    throw new MessageDeliveryException(message, errorMessage, 
                        new AccessDeniedException(errorMessage));
                }
                
                boolean allowed = isUserAllowed(translationId, user, simpHeaders);
                
                if (!allowed) {
                    trackFailedAttempt(sessionId);
                    String errorMessage = "Access denied to translation: " + translationId;
                    logAccessAttempt(translationId, user, false, "Access denied");
                    throw new MessageDeliveryException(message, errorMessage, 
                        new AccessDeniedException(errorMessage));
                }
                resetFailedAttempts(sessionId);
                CloudUser cloudUser = translationsService.getUser(user, simpHeaders, true);
                if (cloudUser != null) {
                    translationsService.recordVerifiedAccess(translationId, cloudUser.id);
                }
                logAccessAttempt(translationId, user, true, "Access granted");
            }
        }
        return message;
    }

    private boolean isValidTranslationId(String translationId) {
        if (translationId == null || translationId.isEmpty()) {
            return false;
        }

        if (translationId.length() > MAX_TRANSLATION_ID_LENGTH) {
            return false;
        }
        
        // Allow only alphanumeric characters and common safe characters
        return translationId.matches("^[a-zA-Z0-9_-]+$");
    }
    
    /**
     * Checks if the session is rate limited due to too many failed attempts.
     * 
     * @param sessionId WebSocket session ID
     * @return true if rate limited, false otherwise
     */
    private boolean isRateLimited(String sessionId) {
        if (sessionId == null) {
            return false;
        }
        FailedAttemptTracker tracker = failedAttempts.get(sessionId);
        return tracker != null && tracker.isBlocked();
    }

    private void trackFailedAttempt(String sessionId) {
        if (sessionId == null) {
            return;
        }
        failedAttempts.computeIfAbsent(sessionId, k -> new FailedAttemptTracker())
            .incrementAndGet();
    }
    
    /**
     * Resets failed attempts counter for successful access.
     * 
     * @param sessionId WebSocket session ID
     */
    private void resetFailedAttempts(String sessionId) {
        if (sessionId != null) {
            failedAttempts.remove(sessionId);
        }
    }

    private void logAccessAttempt(String translationId, Principal principal, boolean allowed, String reason) {
        String userId = principal != null ? principal.getName() : "anonymous";
        if (allowed) {
            LOG.debug("Access granted to translation " + translationId + " for user " + userId);
        } else {
            LOG.warn("Access denied to translation " + translationId + " for user " + userId + ": " + reason);
        }
    }
    
    /**
     * Checks whether the user is allowed to subscribe to the translation (sharing session).
     * Access rules:
     * 1. Translation does not exist -> access denied
     * 2. Owner of the translation -> always allowed
     * 3. Active sharer -> allowed while sharing is not expired
     * 4. Translation with password -> allowed only with correct password header (BCrypt)
     * 5. Public translation (no password) -> allowed for everyone
     *
     * @param translationId translation (session) ID
     * @param principal     user principal (may be null for anonymous users)
     * @param headers       message headers (used to read password and session info)
     * @return true if access is allowed, false otherwise
     */
    private boolean isUserAllowed(String translationId, Principal principal, SimpMessageHeaderAccessor headers) {
        UserTranslation translation = translationsService.getTranslationSilent(translationId);
        if (translation == null) {
            return false;
        }

        CloudUser user = translationsService.getUser(principal, headers, true);
        if (user == null) {
            return false;
        }

        if (translation.getOwnerId() == user.id) {
            return true;
        }

        // Check if user is an active sharer
        // Get deviceId from header if specified
        long deviceId = 0;
        String deviceIdHeader = headers.getFirstNativeHeader("deviceId");
        if (deviceIdHeader != null && !deviceIdHeader.isEmpty()) {
            try {
                deviceId = Long.parseLong(deviceIdHeader);
            } catch (NumberFormatException e) {
                // Invalid deviceId format - ignore
            }
        }
        
        for (TranslationSharingOptions sharer : translation.getActiveSharers()) {
            if (sharer.userId == user.id) {
                long currentTime = System.currentTimeMillis();
                if (currentTime < sharer.expireTime) {
                    // If deviceId is specified in sharing, check if it matches
                    // If sharer.deviceId == 0, allow any device
                    if (sharer.deviceId == 0 || deviceId == 0 || sharer.deviceId == deviceId) {
                        return true;
                    }
                }
            }
        }

        // Check password if translation is password-protected
        String password = translation.getPassword();
        if (password != null && !password.isEmpty()) {
            CloudUser cloudUser = translationsService.getUser(principal, headers, true);
            if (cloudUser != null) {
                // Check if user already verified password for this translation
                if (translation.getVerifiedUsers().contains(cloudUser.id)) {
                    return true; // Already verified for this translation
                }
            }
            
            String providedPassword = headers.getFirstNativeHeader("password");
            if (providedPassword == null || providedPassword.isEmpty()) {
                return false;
            }

            // This prevents timing attacks
            try {
                boolean matches = passwordEncoder.matches(providedPassword, password);
                if (matches && cloudUser != null) {
                    // Remember verified user for this translation only
                    translation.getVerifiedUsers().add(cloudUser.id);
                }
                return matches;
            } catch (Exception e) {
                LOG.warn("Error verifying password for translation " + translationId + ": " + e.getMessage());
                return false;
            }
        }

        // Public translation - allow everyone
        return true;
    }
}