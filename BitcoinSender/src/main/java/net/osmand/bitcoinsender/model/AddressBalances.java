package net.osmand.bitcoinsender.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class AddressBalances {
    public String network;

    @SerializedName("available_balance")
    public String availableBalance;

    @SerializedName("pending_received_balance")
    public String pedingReceivedBalance;

    public List<Balance> balances;
}
