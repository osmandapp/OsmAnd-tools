package net.osmand.server.ws;

public class ChatMessage {
    private String sender;
    private String content;

    // Getters, Setters, Constructors
    public ChatMessage() {}
    public ChatMessage(String sender, String content) {
        this.sender = sender;
        this.content = content;
    }
    public String getSender() { return sender; }
    public void setSender(String sender) { this.sender = sender; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
}