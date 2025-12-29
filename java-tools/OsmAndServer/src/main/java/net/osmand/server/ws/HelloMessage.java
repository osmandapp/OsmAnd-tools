package net.osmand.server.ws;

public class HelloMessage {
    private String name;    // The sender
    private String content; // The text message

    public HelloMessage() {}
    
    public HelloMessage(String name, String content) {
        this.name = name;
        this.content = content;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
}