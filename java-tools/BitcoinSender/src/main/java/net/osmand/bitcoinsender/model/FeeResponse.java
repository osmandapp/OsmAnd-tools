package net.osmand.bitcoinsender.model;

import com.google.gson.annotations.SerializedName;

public class FeeResponse {
    @SerializedName("network")
    public String ntwrk;

    @SerializedName("estimated_network_fee")
    public String fee;
}
