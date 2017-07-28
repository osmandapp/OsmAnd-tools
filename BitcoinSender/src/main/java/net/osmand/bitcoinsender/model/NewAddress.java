package net.osmand.bitcoinsender.model;

import com.google.gson.annotations.SerializedName;

public class NewAddress {
    public String network;

    @SerializedName("user_id")
    public int userId;

    public String address;
    public String label;
}
