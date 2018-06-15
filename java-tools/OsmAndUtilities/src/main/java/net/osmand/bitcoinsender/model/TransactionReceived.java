package net.osmand.bitcoinsender.model;


import com.google.gson.annotations.SerializedName;

import java.util.List;

public class TransactionReceived {
    public String txid;

    @SerializedName("from_greem_address")
    public boolean fromGreenAddress;

    public long time;
    public int confirmations;

    public List<Amount> amountsReceived;
    public List<String> senders;

    public double confidence;

    @SerializedName("propagated_by_nodes")
    public int propagatedByNodes;
}
