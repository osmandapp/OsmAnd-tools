package net.osmand.server.ws;

import java.security.Principal;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.stereotype.Component;

@Component
public class SubscriptionInterceptor implements ChannelInterceptor {

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(message);
        Principal user = accessor.getUser();
        if (StompCommand.SUBSCRIBE.equals(accessor.getCommand())) {
            String destination = accessor.getDestination();
            if (destination != null && destination.startsWith(UserTranslationsService.TOPIC_TRANSLATION)) {
                String translationId = destination.replace(UserTranslationsService.TOPIC_TRANSLATION, "");
                if (!isUserAllowed(translationId, user)) {
                    // CRITICAL: Throwing an exception here effectively "Rejects" the subscription.
                    // The broker never receives this command.
                    throw new IllegalArgumentException("Access Denied to room: " + translationId);
                }
            }
        }
        return message;
    }

    private boolean isUserAllowed(String translationId, Principal user) {
        return true; 
    }
}