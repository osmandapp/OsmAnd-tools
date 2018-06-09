package net.osmand.mailsender.data;

import com.google.gson.annotations.SerializedName;

public class BlockedUser {
    @SerializedName("created")
    private String created;

    @SerializedName("email")
    private String email;

    @SerializedName("reason")
    private String reason;

    public String getCreated() {
        return created;
    }

    public String getEmail() {
        return email;
    }

    public String getReason() {
        return reason;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }
}

