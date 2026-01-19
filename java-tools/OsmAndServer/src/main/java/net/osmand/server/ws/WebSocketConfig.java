package net.osmand.server.ws;

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

	private static final long HEARTBEAT_TIMEOUT = 10000;
	
	@Autowired
	private SubscriptionInterceptor subscriptionInterceptor;
	
	@Autowired
	private Environment environment;

	@Override
	public void configureClientInboundChannel(ChannelRegistration registration) {
		// Add the interceptor to the "Inbound" channel (messages coming FROM client)
		// SubscriptionInterceptor handles both SecurityContext propagation and access control
		registration.interceptors(subscriptionInterceptor);
	}

	@Override
	public void configureMessageBroker(MessageBrokerRegistry config) {
		config.enableSimpleBroker("/topic", "/queue").setTaskScheduler(heartbeatScheduler())
				.setHeartbeatValue(new long[] {HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT});
		config.setApplicationDestinationPrefixes("/app");
		config.setUserDestinationPrefix("/user");
	}
	
	@Bean
    public TaskScheduler heartbeatScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("wss-heartbeat-thread-");
        scheduler.initialize();
        return scheduler;
    }
	
	@Override
	public void registerStompEndpoints(@NotNull StompEndpointRegistry registry) {
		boolean isProduction = environment.acceptsProfiles(Profiles.of("production"));
		
		if (isProduction) {
			// In production, only allow connections from osmand.net domains over HTTPS/WSS
			registry.addEndpoint("/osmand-websocket")
				.setAllowedOriginPatterns("https://*.osmand.net")
				.withSockJS();
		} else {
			registry.addEndpoint("/osmand-websocket");
		}
	}

}