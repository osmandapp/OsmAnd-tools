package net.osmand.server.ws;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.util.HtmlUtils;

import java.security.Principal;

@Controller
public class GreetingController {

    @Autowired
    private SimpMessagingTemplate template;

    /**
     * Handles Sending Messages
     */
    @MessageMapping("/chat/{chatId}/sendMessage")
    public void sendMessage(@DestinationVariable String chatId, 
                            @Payload ChatMessage message, 
                            Principal principal) {
        
        // LOGIC: Determine the Display Name
        String displayName;
        
        if (principal != null) {
            // Case 1: Registered User (Ignore client-provided alias)
            displayName = principal.getName();
        } else {
            // Case 2: Unregistered User (Use alias with prefix)
            // Default to "Anonymous" if they sent empty string
            String clientAlias = (message.getSender() != null && !message.getSender().isEmpty()) 
                                 ? message.getSender() 
                                 : "Anonymous";
            displayName = "Unidentified " + clientAlias;
        }

        // Format and Broadcast
        String safeContent = HtmlUtils.htmlEscape(message.getContent());
        String safeName = HtmlUtils.htmlEscape(displayName);
        
        String finalHtml = "<b>" + safeName + ":</b> " + safeContent;

        template.convertAndSend("/topic/chat/" + chatId, new ChatMessage(finalHtml));
    }

    /**
     * Handles User Joining
     */
    @MessageMapping("/chat/{chatId}/addUser")
    public void addUser(@DestinationVariable String chatId, 
                        @Payload ChatMessage message, 
                        Principal principal) {
        
        String displayName;
        
        if (principal != null) {
            displayName = principal.getName();
        } else {
            String clientAlias = (message.getSender() != null && !message.getSender().isEmpty()) 
                                 ? message.getSender() 
                                 : "Anonymous";
            displayName = "Unidentified " + clientAlias;
        }
        
        String safeName = HtmlUtils.htmlEscape(displayName);
        String msg = "<span style='color: #888; font-style: italic;'>User <b>" + safeName + "</b> joined the room.</span>";
        
        template.convertAndSend("/topic/chat/" + chatId, new ChatMessage(msg));
    }

    // --- Inner Class for Message Structure ---
    public static class ChatMessage {
        private String sender; // Client sends alias here
        private String content;

        public ChatMessage() {}
        public ChatMessage(String content) { this.content = content; }

        public String getSender() { return sender; }
        public void setSender(String sender) { this.sender = sender; }
        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }
    }
}