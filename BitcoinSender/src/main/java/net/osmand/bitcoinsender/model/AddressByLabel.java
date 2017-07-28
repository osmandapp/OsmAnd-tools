package net.osmand.bitcoinsender.model;

import com.google.gson.annotations.SerializedName;

public class AddressByLabel {
    public String network;

    @SerializedName("user_id")
    public int userID;

    public String address;
    public String label;

    @SerializedName("available_balance")
    public String availableBalance;

    @SerializedName("pending_received_balance")
    public String pendingReceivedBalance;
}
