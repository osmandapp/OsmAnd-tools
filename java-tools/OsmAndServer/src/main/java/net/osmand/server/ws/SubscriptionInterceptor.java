package net.osmand.server.ws;

import java.security.Principal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import org.springframework.stereotype.Component;

import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.ws.UserTranslation.TranslationSharingOptions;
import net.osmand.util.Algorithms;

@Component
public class SubscriptionInterceptor implements ChannelInterceptor {

	private static final Log LOG = LogFactory.getLog(SubscriptionInterceptor.class);

	private static final int MAX_TRANSLATION_ID_LENGTH = 128;
	private static final int MAX_FAILED_ATTEMPTS = 5;

	private final Map<String, Integer> failedAttempts = new ConcurrentHashMap<>();

	@Autowired
	@Lazy
	private UserTranslationsService translationsService;

    @Override
    public Message<?> preSend(@NotNull Message<?> message, @NotNull MessageChannel channel) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(message);
        SecurityContext securityContext = SecurityContextHolder.getContext();
        Authentication authentication = securityContext.getAuthentication();
        if (authentication != null && accessor.getUser() == null) {
            accessor.setUser(authentication);
        }

        Principal user = accessor.getUser();
		if (StompCommand.CONNECT.equals(accessor.getCommand())) {
			String alias = accessor.getFirstNativeHeader(UserTranslationsService.X_ALIAS);
			if (alias != null && !alias.isEmpty()) {
				Map<String, Object> sessionAttributes = accessor.getSessionAttributes();
				if (sessionAttributes != null) {
					sessionAttributes.put(UserTranslationsService.X_ALIAS, alias);
				}
				LOG.debug("WebSocket CONNECT: alias=" + alias);
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
                // Check for X-Password header for password-protected translations
                String passwordHash = simpHeaders.getFirstNativeHeader("X-Password");
                if (passwordHash != null && !passwordHash.isEmpty()) {
                    CloudUser cloudUser = translationsService.getUser(user, simpHeaders, true);
                    int userId = cloudUser != null ? cloudUser.id : 0;
                    if (translationsService.verifyTranslationPassword(translationId, passwordHash, userId)) {
                        resetFailedAttempts(sessionId);
                        logAccessAttempt(translationId, user, true, "Access granted via X-Password header");
                        return message;
                    } else {
                        trackFailedAttempt(sessionId);
                        String errorMessage = "Invalid password";
                        logAccessAttempt(translationId, user, false, errorMessage);
                        throw new MessageDeliveryException(message, errorMessage, 
                            new AccessDeniedException(errorMessage));
                    }
                }

                // Check access permissions for the user
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
        return translationId.matches(UserTranslationsService.VALID_TRANSLATION_ID_PATTERN);
    }

    private boolean isRateLimited(String sessionId) {
        if (sessionId == null) {
            return false;
        }
        Integer count = failedAttempts.get(sessionId);
        return count != null && count >= MAX_FAILED_ATTEMPTS;
    }

    private void trackFailedAttempt(String sessionId) {
        if (sessionId == null) {
            return;
        }
        failedAttempts.merge(sessionId, 1, Integer::sum);
    }

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
     * 4. Translation with password -> requires X-Password header (checked above)
     * 5. Public translation (no password) -> allowed for everyone
     *
     * @param translationId translation (session) ID
     * @param principal     user principal (may be null for anonymous users)
     * @param headers       message headers (used to read session info)
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
        String deviceIdHeader = headers.getFirstNativeHeader(UserTranslationsService.X_DEVICE_ID);
        if (deviceIdHeader != null && !deviceIdHeader.isEmpty()) {
            deviceId = Algorithms.parseLongSilently(deviceIdHeader, 0);
            if (deviceId == 0) {
                LOG.warn("Invalid deviceId format in header: " + deviceIdHeader);
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

        // Password-protected translations require X-Password header (checked above)
        // If we reach here without X-Password, access is denied for password-protected translations
        String password = translation.getPassword();
        if (password != null && !password.isEmpty()) {
            LOG.debug("Password-protected translation " + translationId + " requires X-Password header");
            return false;
        }

        // Public translation - allow everyone
        return true;
    }
}