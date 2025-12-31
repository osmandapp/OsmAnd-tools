package net.osmand.server.ws;

import java.security.Principal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Controller;

@Controller
public class UserTranslationsController {

    @Autowired
    private UserTranslationsService service;
    
    
    @MessageMapping("/translation/{translationId}/sendMessage")
    public void sendMessage(@DestinationVariable String translationId, 
    		@Payload TranslationMessage message, Principal principal, SimpMessageHeaderAccessor headers) {
    	service.sendMessage(translationId, message, principal, headers);
	}

    
}