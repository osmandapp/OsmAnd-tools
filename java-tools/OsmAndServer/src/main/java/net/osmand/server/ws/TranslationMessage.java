package net.osmand.server.ws;

public class TranslationMessage {
	
	public static final String TYPE_MSG_TEXT = "text";
	public static final String TYPE_MSG_LOCATION = "location";
	public static final String TYPE_MSG_JOIN = "join";
	
	public static final String SENDER_SYSTEM = "System";
	public static final String SENDER_ANONYMOUS = "Anonymous";
	public static final int SENDER_SYSTEM_ID = 0;
	public static final int SENDER_ANONYMOUS_ID = -1;

	public String sender;
	public long sendDeviceId;
	public long sendUserId;
	public Object content;
	public String type;
	
	public TranslationMessage() {}
	
	
	

}