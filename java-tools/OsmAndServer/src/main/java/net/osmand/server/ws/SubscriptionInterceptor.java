package net.osmand.server.ws;

import java.util.Map;

import org.springframework.lang.NonNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.stereotype.Component;

@Component
public class SubscriptionInterceptor implements ChannelInterceptor {

	// Runs for every inbound STOMP frame: on CONNECT, stores the client-provided anonymous
	// alias in the session attributes so it can be used later as the anonymous user's nickname.
	@Override
	public Message<?> preSend(@NonNull Message<?> message, @NonNull MessageChannel channel) {
		StompHeaderAccessor accessor = StompHeaderAccessor.wrap(message);
		if (StompCommand.CONNECT.equals(accessor.getCommand())) {
			String alias = accessor.getFirstNativeHeader(UserTranslationsService.ALIAS);
			Map<String, Object> attributes = accessor.getSessionAttributes();
			if (alias != null && attributes != null) {
				attributes.put(UserTranslationsService.ALIAS, alias);
			}
		}
		return message;
	}
}
