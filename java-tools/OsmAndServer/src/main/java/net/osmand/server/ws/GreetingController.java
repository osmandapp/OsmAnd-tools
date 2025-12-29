package net.osmand.server.ws;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.util.HtmlUtils;

@Controller
public class GreetingController {

    @Autowired
    private SimpMessagingTemplate template;

    @MessageMapping("/hello")
    public void greeting(HelloMessage message) {
        // We expect 'message' to contain both the SENDER NAME and the CONTENT now.
        // We will repurpose HelloMessage to hold both, or create a new ChatMessage class.
        // For simplicity, let's assume HelloMessage now has a 'content' field too,
        // or we just format the string here.
        
        String sender = HtmlUtils.htmlEscape(message.getName());
        String text = HtmlUtils.htmlEscape(message.getContent()); // You need to add this field to HelloMessage!

        String formattedMessage = "<b>" + sender + ":</b> " + text;

        // Broadcast immediately to ALL subscribers
        template.convertAndSend("/topic/greetings", new Greeting(formattedMessage));
    }
}