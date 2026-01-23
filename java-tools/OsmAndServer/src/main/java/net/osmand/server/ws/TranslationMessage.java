package net.osmand.server.ws;

public class TranslationMessage {
	
	public enum TranslationMessageType {
		TEXT,
		LOCATION,
		JOIN,
		LEAVE,
		METADATA
	}
	
	public static final String SENDER_SYSTEM = "System";
	public static final String SENDER_ANONYMOUS = "Anonymous";
	public static final int SENDER_SYSTEM_ID = 0;
	public static final int SENDER_ANONYMOUS_ID = -1;

	public String sender;
	public long sendDeviceId;
	public long sendUserId;
	public Object content;
	public TranslationMessageType type;
	
	public TranslationMessage() {}

	public TranslationMessage setType(TranslationMessageType type) {
		this.type = type;
		return this;
	}
	
	public TranslationMessage setContent(Object content) {
		this.content = content;
		return this;
	}
	

}