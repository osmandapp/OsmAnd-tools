package net.osmand.server.assist.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.telegram.telegrambots.api.objects.User;

public class NewDevice {

    @JsonProperty("device_name")
    private String deviceName;

    @JsonProperty("chat_id")
    private long chatId;

    @JsonProperty("user")
    private User user;

    public String getDeviceName() {
        return deviceName;
    }

    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }

    public long getChatId() {
        return chatId;
    }

    public void setChatId(long chatId) {
        this.chatId = chatId;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    @Override
    public String toString() {
        return "NewDevice{" +
                "deviceName='" + deviceName + '\'' +
                ", chatId=" + chatId +
                ", user=" + user +
                '}';
    }
}
