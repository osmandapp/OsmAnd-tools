package net.osmand.server.ws;

import java.security.Principal;

public class TranslationMessage {
	
	public static final String TYPE_MSG_TEXT = "text";
	public static final String TYPE_MSG_LOCATION = "location";
	public static final String TYPE_MSG_JOIN = "join";
	
	public static final String SENDER_SYSTEM = "System";

	public String sender;
	public String content;
	public String type;
	
	public TranslationMessage() {}
	
	public TranslationMessage(TranslationMessage message, Principal principal, String type) {
		sender = getName(message, principal);
		content = message.content;
		this.type = type;
	}
	
	public TranslationMessage(String content, Principal principal, String type) {
		sender = principal.getName();
		this.content = content;
		this.type = type;
	}
	
	public TranslationMessage(String content, String user, String type) {
		sender = user;
		this.content = content;
		this.type = type;
	}
	

	private String getName(TranslationMessage message, Principal principal) {
		String displayName;
        if (principal != null) {
            displayName = principal.getName();
        } else {
			String clientAlias = (message.sender != null && !message.sender.isEmpty()) ? message.sender : "Anonymous";
            displayName = "Unidentified " + clientAlias;
        }
		return displayName;
	}
	
	

}