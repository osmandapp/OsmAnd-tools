package net.osmand.server.ws;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.util.HtmlUtils;

@Controller
public class GreetingController {

    @Autowired
    private SimpMessagingTemplate template;

    // 1. Handle regular chat messages
    @MessageMapping("/chat/{chatId}/sendMessage")
    public void sendMessage(@DestinationVariable String chatId, @Payload ChatMessage chatMessage) {
        // Format: "<b>Name:</b> Message"
        String safeName = HtmlUtils.htmlEscape(chatMessage.getSender());
        String safeContent = HtmlUtils.htmlEscape(chatMessage.getContent());
        String finalHtml = "<b>" + safeName + ":</b> " + safeContent;

        // Broadcast to everyone in this room
        template.convertAndSend("/topic/chat/" + chatId, new ChatMessage(finalHtml));
    }

    // 2. Handle "Join" events
    @MessageMapping("/chat/{chatId}/addUser")
    public void addUser(@DestinationVariable String chatId, @Payload ChatMessage chatMessage) {
        String safeName = HtmlUtils.htmlEscape(chatMessage.getSender());
        String msg = "<span style='color: #888; font-style: italic;'>User <b>" + safeName + "</b> joined the room.</span>";
        
        // Broadcast system message
        template.convertAndSend("/topic/chat/" + chatId, new ChatMessage(msg));
    }


    // --- INNER CLASS (No separate file needed) ---
    public static class ChatMessage {
        private String sender;
        private String content;

        public ChatMessage() {}
        
        // Constructor for sending simple text back to client
        public ChatMessage(String content) {
            this.content = content;
        }

        public String getSender() { return sender; }
        public void setSender(String sender) { this.sender = sender; }
        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }
    }
}