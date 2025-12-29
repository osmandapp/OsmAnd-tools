package net.osmand.server.ws;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.util.HtmlUtils;

@Controller
public class GreetingController {

    @Autowired
    private SimpMessagingTemplate template;

    // We catch messages sent to /app/chat/{chatId}/sendMessage
    @MessageMapping("/chat/{chatId}/sendMessage")
    public void sendMessage(@DestinationVariable String chatId, ChatMessage message) {
        
        String safeName = HtmlUtils.htmlEscape(message.getSender());
        String safeContent = HtmlUtils.htmlEscape(message.getContent());
        
        // Construct the final message
        String response = "<b>" + safeName + ":</b> " + safeContent;

        // DYNAMIC BROADCAST: Send only to subscribers of this specific chat ID
        template.convertAndSend("/topic/chat/" + chatId, response);
    }
}