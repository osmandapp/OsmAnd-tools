package net.osmand.server.ws;

public class TranslationMessage {

	public enum TranslationMessageType {
		TEXT,       // chat text (currently unused on the client)
		LOCATION,   // encrypted location point
		JOIN,       // a viewer joined: their nickname
		LEAVE,      // a viewer left: their nickname
		METADATA,   // sharing snapshot: who is currently sharing
		DELETE      // translation removed by owner: its id
	}

	public static final String SENDER_SYSTEM = "System";
	public static final String SENDER_ANONYMOUS = "Anonymous";
	public static final int SENDER_SYSTEM_ID = 0;
	public static final int SENDER_ANONYMOUS_ID = -1;

	public String sender;             // author nickname (or "System")
	public long sendDeviceId;         // author device id (0 for system)
	public long sendUserId;           // author user id (0 for system)
	public Object content;            // payload, shape depends on type
	public TranslationMessageType type;
	public long serverReceiveTime;    // epoch ms; used as Redis ZSet score and delta cursor

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