package net.osmand.server.ws;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

	private static final long HEARBIT_TIMEOUT = 5000;
	@Autowired
	private SubscriptionInterceptor subscriptionInterceptor;

	@Override
	public void configureClientInboundChannel(ChannelRegistration registration) {
		// Add the interceptor to the "Inbound" channel (messages coming FROM client)
		registration.interceptors(subscriptionInterceptor);
	}

	@Override
	public void configureMessageBroker(MessageBrokerRegistry config) {
		config.enableSimpleBroker("/topic", "/queue")
		.setTaskScheduler(heartbeatScheduler()) // <--- REQUIRED for heartbeats
        .setHeartbeatValue(new long[]{HEARBIT_TIMEOUT, HEARBIT_TIMEOUT})
		;
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
	public void registerStompEndpoints(StompEndpointRegistry registry) {
		registry.addEndpoint("/osmand-websocket");
	}

}